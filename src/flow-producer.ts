import type { FlowProducerOptions, FlowJob, Client } from './types';
import { createClient, ensureFunctionLibrary } from './connection';

export interface JobNode {
  job: import('./job').Job;
  children?: JobNode[];
}

export class FlowProducer {
  private opts: FlowProducerOptions;
  private client: Client | null = null;

  constructor(opts: FlowProducerOptions) {
    this.opts = opts;
  }

  async add(flow: FlowJob): Promise<JobNode> {
    // TODO: FCALL glidemq_addFlow
    throw new Error('Not implemented');
  }

  async addBulk(flows: FlowJob[]): Promise<JobNode[]> {
    // TODO: Pipeline of addFlow calls
    throw new Error('Not implemented');
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
