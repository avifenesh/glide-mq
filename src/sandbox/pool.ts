import { Worker as WorkerThread } from 'worker_threads';
import { fork, ChildProcess } from 'child_process';
import type { MainToChild, ChildToMain } from './types';
import { toSerializedJob } from './types';
import type { Job } from '../job';
import { GlideMQError, UnrecoverableError } from '../errors';

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
        stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
        execArgv: [],
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

    let cleaned = false;
    const onExit = (code: number | null) => {
      removeAndCleanup(new GlideMQError(`Sandbox worker exited with code ${code}`));
    };
    const onError = (err: Error) => {
      removeAndCleanup(err);
    };

    const removeAndCleanup = (error: Error) => {
      if (cleaned) return;
      cleaned = true;

      thread.off('exit', onExit);
      thread.off('error', onError);

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

      const abortHandler = job.abortSignal
        ? () => {
            pw.send({ type: 'abort', id: invocationId });
          }
        : undefined;

      const cleanup = () => {
        if (cleaned) return;
        cleaned = true;
        pw.currentCleanup = undefined;
        pw.offMsg(onMessage);
        if (job.abortSignal && abortHandler) {
          job.abortSignal.removeEventListener('abort', abortHandler);
        }
      };

      const onMessage = (msg: ChildToMain) => {
        try {
          switch (msg.type) {
            case 'completed':
              if (msg.id === invocationId) {
                cleanup();
                this.release(pw);
                resolve(msg.result as R);
              }
              break;

            case 'failed':
              if (msg.id === invocationId) {
                cleanup();
                this.release(pw);
                const err =
                  msg.errorName === 'UnrecoverableError' ? new UnrecoverableError(msg.error) : new Error(msg.error);
                if (msg.stack) err.stack = msg.stack;
                if (msg.discarded) job.discarded = true;
                reject(err);
              }
              break;

            case 'proxy-request':
              if (msg.id.startsWith(invocationId + ':')) {
                this.handleProxyRequest(pw, job, msg).catch(() => {
                  // Proxy send can fail if worker crashed mid-flight; safe to ignore
                });
              }
              break;
          }
        } catch (err) {
          cleanup();
          this.release(pw);
          reject(err);
        }
      };

      pw.currentCleanup = cleanup;
      pw.onMsg(onMessage);

      // Send process message first so runner creates the SandboxJob entry
      pw.send({
        type: 'process',
        id: invocationId,
        job: serialized,
      });

      // Forward abort signal to sandbox worker (after process message)
      if (job.abortSignal && abortHandler) {
        if (job.abortSignal.aborted) {
          abortHandler();
        } else {
          job.abortSignal.addEventListener('abort', abortHandler, { once: true });
        }
      }
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
        case 'discard':
          job.discarded = true;
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
  async close(force?: boolean): Promise<void> {
    if (this._closed) return;
    this._closed = true;

    const closedError = new GlideMQError('SandboxPool is closed');
    for (const waiter of this.waiters) {
      waiter.reject(closedError);
    }
    this.waiters.length = 0;

    const KILL_TIMEOUT = 5000;
    const FORCE_TIMEOUT = 2000;
    const terminations: Promise<void>[] = [];
    for (const pw of [...this.workers]) {
      terminations.push(
        new Promise<void>((resolve) => {
          // Check if child process already exited to avoid missing 'exit' event
          if (!this.useWorkerThreads) {
            const child = pw.thread as ChildProcess;
            if (child.exitCode !== null || child.signalCode !== null) {
              resolve();
              return;
            }
          }

          const timer = setTimeout(
            () => {
              if (!this.useWorkerThreads) {
                (pw.thread as ChildProcess).kill('SIGKILL');
              } else {
                (pw.thread as WorkerThread).terminate();
              }
              resolve();
            },
            force ? FORCE_TIMEOUT : KILL_TIMEOUT,
          );
          pw.thread.once('exit', () => {
            clearTimeout(timer);
            resolve();
          });

          if (this.useWorkerThreads) {
            (pw.thread as WorkerThread).terminate();
          } else {
            (pw.thread as ChildProcess).kill(force ? 'SIGKILL' : undefined);
          }
        }),
      );
    }

    await Promise.allSettled(terminations);
    this.workers.length = 0;
  }
}
