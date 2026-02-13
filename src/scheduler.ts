import type { Client } from './types';

// Internal scheduler - runs inside Worker
// Responsibilities:
// 1. Promote delayed/priority jobs from scheduled ZSet to stream
// 2. Reclaim stalled jobs via XAUTOCLAIM
// 3. Manage repeatable job schedulers

export class Scheduler {
  private client: Client;
  private queueKeys: ReturnType<typeof import('./utils').buildKeys>;
  private interval: ReturnType<typeof setInterval> | null = null;
  private promotionInterval: number;
  private stalledInterval: number;

  constructor(
    client: Client,
    queueKeys: ReturnType<typeof import('./utils').buildKeys>,
    opts: { promotionInterval?: number; stalledInterval?: number } = {},
  ) {
    this.client = client;
    this.queueKeys = queueKeys;
    this.promotionInterval = opts.promotionInterval ?? 5000;
    this.stalledInterval = opts.stalledInterval ?? 30000;
  }

  start(): void {
    // TODO: Start promotion + stalled recovery loops
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  private async promoteDelayed(): Promise<number> {
    // TODO: FCALL glidemq_promote
    return 0;
  }

  private async reclaimStalled(): Promise<number> {
    // TODO: FCALL glidemq_reclaimStalled via XAUTOCLAIM
    return 0;
  }
}
