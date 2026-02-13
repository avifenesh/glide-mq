import type { JobOptions, Client } from './types';

export class Job<D = any, R = any> {
  readonly id: string;
  readonly name: string;
  readonly data: D;
  readonly opts: JobOptions;
  attemptsMade: number;
  returnvalue: R | undefined;
  failedReason: string | undefined;
  progress: number | object;
  timestamp: number;
  finishedOn: number | undefined;
  processedOn: number | undefined;
  parentId?: string;

  /** @internal */
  constructor(
    private client: Client,
    private queueKeys: ReturnType<typeof import('./utils').buildKeys>,
    id: string,
    name: string,
    data: D,
    opts: JobOptions,
  ) {
    this.id = id;
    this.name = name;
    this.data = data;
    this.opts = opts;
    this.attemptsMade = 0;
    this.progress = 0;
    this.timestamp = Date.now();
  }

  async updateProgress(progress: number | object): Promise<void> {
    // TODO: HSET job hash, XADD event
    this.progress = progress;
  }

  async updateData(data: D): Promise<void> {
    // TODO: HSET job hash
    (this as any).data = data;
  }

  async getChildrenValues(): Promise<Record<string, R>> {
    // TODO: Read deps set, fetch each child's returnvalue
    return {};
  }

  async moveToFailed(err: Error): Promise<void> {
    // TODO: FCALL glidemq_fail
    this.failedReason = err.message;
  }

  async remove(): Promise<void> {
    // TODO: FCALL glidemq_removeJob
  }

  async retry(): Promise<void> {
    // TODO: FCALL glidemq_retry
  }

  /** @internal */
  static fromHash<D, R>(
    client: Client,
    queueKeys: ReturnType<typeof import('./utils').buildKeys>,
    id: string,
    hash: Record<string, string>,
  ): Job<D, R> {
    const job = new Job<D, R>(
      client,
      queueKeys,
      id,
      hash.name || '',
      JSON.parse(hash.data || '{}'),
      JSON.parse(hash.opts || '{}'),
    );
    job.attemptsMade = parseInt(hash.attemptsMade || '0', 10);
    job.timestamp = parseInt(hash.timestamp || '0', 10);
    job.processedOn = hash.processedOn ? parseInt(hash.processedOn, 10) : undefined;
    job.finishedOn = hash.finishedOn ? parseInt(hash.finishedOn, 10) : undefined;
    job.returnvalue = hash.returnvalue ? JSON.parse(hash.returnvalue) : undefined;
    job.failedReason = hash.failedReason || undefined;
    job.parentId = hash.parentId || undefined;
    return job;
  }
}
