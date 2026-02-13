import type { QueueOptions, JobOptions, Client } from './types';
import { Job } from './job';
import { buildKeys } from './utils';
import { createClient, ensureFunctionLibrary } from './connection';

export class Queue<D = any, R = any> {
  readonly name: string;
  private opts: QueueOptions;
  private client: Client | null = null;
  private queueKeys: ReturnType<typeof buildKeys>;

  constructor(name: string, opts: QueueOptions) {
    this.name = name;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
  }

  private async getClient(): Promise<Client> {
    if (!this.client) {
      this.client = await createClient(this.opts.connection);
      await ensureFunctionLibrary(this.client, ''); // TODO: pass library source
    }
    return this.client;
  }

  async add(name: string, data: D, opts?: JobOptions): Promise<Job<D, R>> {
    // TODO: FCALL glidemq_addJob
    const client = await this.getClient();
    throw new Error('Not implemented');
  }

  async addBulk(jobs: { name: string; data: D; opts?: JobOptions }[]): Promise<Job<D, R>[]> {
    // TODO: FCALL glidemq_addBulk or pipeline of addJob calls
    const client = await this.getClient();
    throw new Error('Not implemented');
  }

  async getJob(id: string): Promise<Job<D, R> | null> {
    // TODO: HGETALL job hash, construct Job
    const client = await this.getClient();
    throw new Error('Not implemented');
  }

  async pause(): Promise<void> {
    // TODO: FCALL glidemq_pause
    const client = await this.getClient();
  }

  async resume(): Promise<void> {
    // TODO: FCALL glidemq_resume
    const client = await this.getClient();
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
