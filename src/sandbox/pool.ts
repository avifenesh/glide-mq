import { Worker as WorkerThread } from 'worker_threads';
import { fork, ChildProcess } from 'child_process';
import type { MainToChild, ChildToMain } from './types';
import { toSerializedJob } from './types';
import type { Job } from '../job';

interface PoolWorker {
  thread: WorkerThread | ChildProcess;
  busy: boolean;
  currentReject?: (err: Error) => void;
}

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
  private waiters: Array<(pw: PoolWorker) => void> = [];
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

    const pw: PoolWorker = { thread, busy: false };

    const onExit = (code: number | null) => {
      const idx = this.workers.indexOf(pw);
      if (idx >= 0) this.workers.splice(idx, 1);

      if (pw.currentReject) {
        pw.currentReject(new Error(`Sandbox worker exited with code ${code}`));
        pw.currentReject = undefined;
      }
    };

    const onError = (err: Error) => {
      if (pw.currentReject) {
        pw.currentReject(err);
        pw.currentReject = undefined;
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

    return new Promise<PoolWorker>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  private release(pw: PoolWorker): void {
    pw.busy = false;
    pw.currentReject = undefined;

    const waiter = this.waiters.shift();
    if (waiter) {
      pw.busy = true;
      waiter(pw);
    }
  }

  private sendToWorker(pw: PoolWorker, msg: MainToChild): void {
    if (this.useWorkerThreads) {
      (pw.thread as WorkerThread).postMessage(msg);
    } else {
      (pw.thread as ChildProcess).send(msg);
    }
  }

  /**
   * Run a job in a sandbox worker. Returns the processor result.
   * The real Job object is used for proxying log/updateProgress/updateData calls.
   */
  async run<D, R>(job: Job<D, R>): Promise<R> {
    if (this._closed) {
      throw new Error('SandboxPool is closed');
    }

    const pw = await this.acquire();
    const serialized = toSerializedJob(job);
    const invocationId = job.id;

    return new Promise<R>((resolve, reject) => {
      pw.currentReject = reject;

      const onMessage = (msg: ChildToMain) => {
        switch (msg.type) {
          case 'completed':
            if (msg.id === invocationId) {
              cleanup();
              resolve(msg.result as R);
            }
            break;

          case 'failed':
            if (msg.id === invocationId) {
              cleanup();
              const err = new Error(msg.error);
              if (msg.stack) err.stack = msg.stack;
              reject(err);
            }
            break;

          case 'proxy-request':
            this.handleProxyRequest(pw, job, msg);
            break;
        }
      };

      const cleanup = () => {
        if (this.useWorkerThreads) {
          (pw.thread as WorkerThread).off('message', onMessage);
        } else {
          (pw.thread as ChildProcess).off('message', onMessage);
        }
        pw.currentReject = undefined;
        this.release(pw);
      };

      if (this.useWorkerThreads) {
        (pw.thread as WorkerThread).on('message', onMessage);
      } else {
        (pw.thread as ChildProcess).on('message', onMessage);
      }

      this.sendToWorker(pw, {
        type: 'process',
        id: invocationId,
        job: serialized,
      });
    });
  }

  private async handleProxyRequest(pw: PoolWorker, job: Job, msg: ChildToMain & { type: 'proxy-request' }): Promise<void> {
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
      this.sendToWorker(pw, { type: 'proxy-response', id: msg.id });
    } catch (err: any) {
      this.sendToWorker(pw, { type: 'proxy-response', id: msg.id, error: err?.message ?? String(err) });
    }
  }

  /** Terminate all workers and reject pending waiters. */
  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;

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
