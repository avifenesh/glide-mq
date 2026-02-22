import { Worker as WorkerThread } from 'worker_threads';
import { fork, ChildProcess } from 'child_process';
import type { MainToChild, ChildToMain } from './types';
import { toSerializedJob } from './types';
import type { Job } from '../job';
import { GlideMQError } from '../errors';

interface PoolWorker {
  thread: WorkerThread | ChildProcess;
  busy: boolean;
  currentReject?: (err: Error) => void;
  currentCleanup?: () => void;
  send: (msg: MainToChild) => void;
  onMsg: (handler: (msg: ChildToMain) => void) => void;
  offMsg: (handler: (msg: ChildToMain) => void) => void;
}

interface Waiter {
  resolve: (pw: PoolWorker) => void;
  reject: (err: Error) => void;
}

let nextInvocationId = 0;

/**
 * Pool of worker threads or child processes that execute sandboxed processors.
 * Manages lifecycle, IPC message routing, and concurrency.
 */
export class SandboxPool {
  private processorPath: string;
  private useWorkerThreads: boolean;
  private maxWorkers: number;
  private runnerPath: string;
  private workers: PoolWorker[] = [];
  private waiters: Waiter[] = [];
  private _closed = false;

  constructor(processorPath: string, useWorkerThreads: boolean, maxWorkers: number, runnerPath: string) {
    this.processorPath = processorPath;
    this.useWorkerThreads = useWorkerThreads;
    this.maxWorkers = maxWorkers;
    this.runnerPath = runnerPath;
  }

  private spawn(): PoolWorker {
    let thread: WorkerThread | ChildProcess;

    if (this.useWorkerThreads) {
      thread = new WorkerThread(this.runnerPath, {
        workerData: { processorPath: this.processorPath },
      });
    } else {
      thread = fork(this.runnerPath, [this.processorPath], {
        stdio: 'pipe',
      });
    }

    const pw: PoolWorker = this.useWorkerThreads
      ? {
          thread,
          busy: false,
          send: (msg) => (thread as WorkerThread).postMessage(msg),
          onMsg: (handler) => (thread as WorkerThread).on('message', handler),
          offMsg: (handler) => (thread as WorkerThread).off('message', handler),
        }
      : {
          thread,
          busy: false,
          send: (msg) => (thread as ChildProcess).send(msg),
          onMsg: (handler) => (thread as ChildProcess).on('message', handler as any),
          offMsg: (handler) => (thread as ChildProcess).off('message', handler as any),
        };

    const removeAndCleanup = (error: Error) => {
      const idx = this.workers.indexOf(pw);
      if (idx >= 0) this.workers.splice(idx, 1);

      if (pw.currentCleanup) {
        pw.currentCleanup();
      }

      if (pw.currentReject) {
        pw.currentReject(error);
        pw.currentReject = undefined;
      }

      // Wake next waiter with a fresh worker (if not closing)
      if (!this._closed && this.waiters.length > 0) {
        const replacement = this.spawn();
        replacement.busy = true;
        const waiter = this.waiters.shift()!;
        waiter.resolve(replacement);
      }
    };

    const onExit = (code: number | null) => {
      removeAndCleanup(new GlideMQError(`Sandbox worker exited with code ${code}`));
    };

    const onError = (err: Error) => {
      removeAndCleanup(err);
    };

    thread.on('exit', onExit);
    thread.on('error', onError);

    this.workers.push(pw);
    return pw;
  }

  private acquire(): Promise<PoolWorker> {
    const idle = this.workers.find((w) => !w.busy);
    if (idle) {
      idle.busy = true;
      return Promise.resolve(idle);
    }

    if (this.workers.length < this.maxWorkers) {
      const pw = this.spawn();
      pw.busy = true;
      return Promise.resolve(pw);
    }

    return new Promise<PoolWorker>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  private release(pw: PoolWorker): void {
    pw.busy = false;
    pw.currentReject = undefined;

    const waiter = this.waiters.shift();
    if (waiter) {
      pw.busy = true;
      waiter.resolve(pw);
    }
  }

  /**
   * Run a job in a sandbox worker. Returns the processor result.
   * The real Job object is used for proxying log/updateProgress/updateData calls.
   */
  async run<D, R>(job: Job<D, R>): Promise<R> {
    if (this._closed) {
      throw new GlideMQError('SandboxPool is closed');
    }

    const pw = await this.acquire();
    const serialized = toSerializedJob(job);
    const invocationId = String(++nextInvocationId);

    return new Promise<R>((resolve, reject) => {
      pw.currentReject = reject;
      let cleaned = false;

      const removeListener = () => {
        if (cleaned) return;
        cleaned = true;
        pw.currentCleanup = undefined;
        pw.offMsg(onMessage);
      };

      const onMessage = (msg: ChildToMain) => {
        try {
          switch (msg.type) {
            case 'completed':
              if (msg.id === invocationId) {
                removeListener();
                this.release(pw);
                resolve(msg.result as R);
              }
              break;

            case 'failed':
              if (msg.id === invocationId) {
                removeListener();
                this.release(pw);
                const err = new Error(msg.error);
                if (msg.stack) err.stack = msg.stack;
                reject(err);
              }
              break;

            case 'proxy-request':
              this.handleProxyRequest(pw, job, msg);
              break;
          }
        } catch (err) {
          removeListener();
          this.release(pw);
          reject(err);
        }
      };

      pw.currentCleanup = removeListener;
      pw.onMsg(onMessage);

      // Forward abort signal to sandbox worker
      const abortHandler = job.abortSignal
        ? () => {
            pw.send({ type: 'abort', id: invocationId });
          }
        : undefined;
      if (job.abortSignal && abortHandler) {
        if (job.abortSignal.aborted) {
          abortHandler();
        } else {
          job.abortSignal.addEventListener('abort', abortHandler, { once: true });
        }
      }

      const originalRemoveListener = removeListener;
      const cleanupAll = () => {
        originalRemoveListener();
        if (job.abortSignal && abortHandler) {
          job.abortSignal.removeEventListener('abort', abortHandler);
        }
      };
      pw.currentCleanup = cleanupAll;

      pw.send({
        type: 'process',
        id: invocationId,
        job: serialized,
      });
    });
  }

  private async handleProxyRequest(
    pw: PoolWorker,
    job: Job,
    msg: ChildToMain & { type: 'proxy-request' },
  ): Promise<void> {
    try {
      switch (msg.method) {
        case 'log':
          await job.log(msg.args[0] as string);
          break;
        case 'updateProgress':
          await job.updateProgress(msg.args[0] as number | object);
          break;
        case 'updateData':
          await job.updateData(msg.args[0]);
          break;
        default:
          throw new Error(`Unknown proxy method: ${msg.method}`);
      }
      pw.send({ type: 'proxy-response', id: msg.id });
    } catch (err: any) {
      pw.send({ type: 'proxy-response', id: msg.id, error: err?.message ?? String(err) });
    }
  }

  /** Terminate all workers and reject pending waiters. */
  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;

    const closedError = new GlideMQError('SandboxPool is closed');
    for (const waiter of this.waiters) {
      waiter.reject(closedError);
    }
    this.waiters.length = 0;

    const terminations: Promise<void>[] = [];
    for (const pw of this.workers) {
      terminations.push(
        new Promise<void>((resolve) => {
          pw.thread.once('exit', () => resolve());
          if (this.useWorkerThreads) {
            (pw.thread as WorkerThread).terminate();
          } else {
            (pw.thread as ChildProcess).kill();
          }
        }),
      );
    }

    await Promise.allSettled(terminations);
    this.workers.length = 0;
  }
}
