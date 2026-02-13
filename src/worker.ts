import type { WorkerOptions, Processor, Client } from './types';
import { Job } from './job';
import { buildKeys } from './utils';
import { createClient, createBlockingClient, ensureFunctionLibrary } from './connection';

export class Worker<D = any, R = any> {
  readonly name: string;
  private opts: WorkerOptions;
  private processor: Processor<D, R>;
  private commandClient: Client | null = null;
  private blockingClient: Client | null = null;
  private running = false;
  private queueKeys: ReturnType<typeof buildKeys>;
  private consumerId: string;

  constructor(name: string, processor: Processor<D, R>, opts: WorkerOptions) {
    this.name = name;
    this.processor = processor;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
    this.consumerId = `worker-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  private async init(): Promise<void> {
    this.commandClient = await createClient(this.opts.connection);
    this.blockingClient = await createBlockingClient(this.opts.connection);
    await ensureFunctionLibrary(this.commandClient, ''); // TODO: pass library source
    // TODO: XGROUP CREATE if not exists
    this.running = true;
    this.poll();
  }

  private async poll(): Promise<void> {
    // TODO: XREADGROUP BLOCK loop
    // - Fetch jobs from stream
    // - Process with concurrency control
    // - FCALL glidemq_complete or glidemq_fail
    // - Run scheduler (promote delayed, reclaim stalled)
  }

  async pause(force?: boolean): Promise<void> {
    this.running = false;
    // TODO: If not force, wait for active jobs to finish
  }

  async resume(): Promise<void> {
    this.running = true;
    this.poll();
  }

  async close(force?: boolean): Promise<void> {
    this.running = false;
    // TODO: If not force, wait for active jobs
    if (this.commandClient) {
      this.commandClient.close();
      this.commandClient = null;
    }
    if (this.blockingClient) {
      this.blockingClient.close();
      this.blockingClient = null;
    }
  }

  on(event: string, handler: Function): void {
    // TODO: EventEmitter integration
  }

  async rateLimit(ms: number): Promise<void> {
    // TODO: FCALL glidemq_rateLimit
  }

  static RateLimitError = class extends Error {
    constructor() {
      super('Rate limit exceeded');
      this.name = 'RateLimitError';
    }
  };
}
