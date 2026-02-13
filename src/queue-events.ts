import type { QueueEventsOptions, Client } from './types';
import { buildKeys } from './utils';
import { createBlockingClient } from './connection';

export class QueueEvents {
  readonly name: string;
  private opts: QueueEventsOptions;
  private client: Client | null = null;
  private queueKeys: ReturnType<typeof buildKeys>;
  private running = false;

  constructor(name: string, opts: QueueEventsOptions) {
    this.name = name;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
  }

  on(event: string, handler: Function): void {
    // TODO: XREAD BLOCK on events stream
  }

  async close(): Promise<void> {
    this.running = false;
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
