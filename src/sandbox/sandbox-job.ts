import type { JobOptions } from '../types';
import type { SerializedJob, ChildToMain, MainToChild } from './types';
import { GlideMQError } from '../errors';

/**
 * Job-like object used inside sandboxed processors (worker threads / child processes).
 * Proxies log/updateProgress/updateData over IPC to the main thread.
 * Other methods that require a Valkey client are not available.
 */
export class SandboxJob<D = any, R = any> {
  readonly id: string;
  readonly name: string;
  data: D;
  readonly opts: JobOptions;
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  progress: number | object;
  timestamp: number;
  processedOn: number | undefined;
  parentId?: string;
  parentQueue?: string;
  orderingKey?: string;
  orderingSeq?: number;
  groupKey?: string;
  cost?: number;

  abortSignal: AbortSignal;

  private abortController: AbortController;
  private pendingProxies = new Map<string, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();
  private proxySeq = 0;
  private invocationId: string;
  private sendMessage: (msg: ChildToMain) => void;

  constructor(serialized: SerializedJob, sendMessage: (msg: ChildToMain) => void, invocationId?: string) {
    this.id = serialized.id;
    this.name = serialized.name;
    this.data = serialized.data as D;
    this.opts = serialized.opts;
    this.attemptsMade = serialized.attemptsMade;
    this.progress = serialized.progress;
    this.timestamp = serialized.timestamp;
    this.processedOn = serialized.processedOn;
    this.parentId = serialized.parentId;
    this.parentQueue = serialized.parentQueue;
    this.orderingKey = serialized.orderingKey;
    this.orderingSeq = serialized.orderingSeq;
    this.groupKey = serialized.groupKey;
    this.cost = serialized.cost;
    this.returnvalue = undefined;
    this.failedReason = undefined;

    this.abortController = new AbortController();
    this.abortSignal = this.abortController.signal;
    this.invocationId = invocationId ?? '';
    this.sendMessage = sendMessage;
  }

  /** Append a log line. Proxied to the main thread. */
  async log(message: string): Promise<void> {
    await this.proxyCall('log', [message]);
  }

  /** Update job progress. Proxied to the main thread. */
  async updateProgress(progress: number | object): Promise<void> {
    await this.proxyCall('updateProgress', [progress]);
    this.progress = progress;
  }

  /** Replace job data. Proxied to the main thread. */
  async updateData(data: D): Promise<void> {
    await this.proxyCall('updateData', [data]);
    this.data = data;
  }

  /** @internal Trigger the abort signal. Called when main thread sends 'abort'. */
  _abort(): void {
    this.abortController.abort();
  }

  /** @internal Handle a proxy-response from the main thread. */
  handleProxyResponse(msg: MainToChild & { type: 'proxy-response' }): void {
    const pending = this.pendingProxies.get(msg.id);
    if (!pending) return;
    this.pendingProxies.delete(msg.id);
    if (msg.error) {
      pending.reject(new Error(msg.error));
    } else {
      pending.resolve(msg.result);
    }
  }

  /** @internal Reject all in-flight proxy calls. Called when the worker exits unexpectedly. */
  drainPendingProxies(reason: string): void {
    const err = new GlideMQError(reason);
    for (const pending of this.pendingProxies.values()) {
      pending.reject(err);
    }
    this.pendingProxies.clear();
  }

  async getState(): Promise<string> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async remove(): Promise<void> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async retry(): Promise<void> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isCompleted(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isFailed(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isDelayed(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isActive(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isWaiting(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async isRevoked(): Promise<boolean> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async moveToFailed(_err: Error): Promise<void> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async getChildrenValues(): Promise<Record<string, R>> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }
  async waitUntilFinished(): Promise<'completed' | 'failed'> {
    throw new GlideMQError('Method not available in sandboxed processor');
  }

  private proxyCall(method: 'log' | 'updateProgress' | 'updateData', args: unknown[]): Promise<unknown> {
    const id = `${this.invocationId}:${++this.proxySeq}`;
    return new Promise<unknown>((resolve, reject) => {
      this.pendingProxies.set(id, { resolve, reject });
      this.sendMessage({ type: 'proxy-request', id, method, args });
    });
  }
}
