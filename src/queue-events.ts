import { EventEmitter } from 'events';
import type { QueueEventsOptions, Client } from './types';
import { buildKeys } from './utils';
import { createBlockingClient, ensureFunctionLibrary } from './connection';

export class QueueEvents extends EventEmitter {
  readonly name: string;
  private opts: QueueEventsOptions;
  private client: Client | null = null;
  private queueKeys: ReturnType<typeof buildKeys>;
  private running = false;
  private lastId: string;
  private initPromise: Promise<void>;
  private blockTimeout: number;

  constructor(name: string, opts: QueueEventsOptions) {
    super();
    this.name = name;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
    // '$' means only new messages from this point forward
    this.lastId = opts.lastEventId ?? '$';
    this.blockTimeout = opts.blockTimeout ?? 5000;
    this.initPromise = this.init();
  }

  /**
   * Wait until the QueueEvents instance is connected and listening.
   */
  async waitUntilReady(): Promise<void> {
    return this.initPromise;
  }

  private async init(): Promise<void> {
    this.client = await createBlockingClient(this.opts.connection);
    await ensureFunctionLibrary(
      this.client,
      undefined,
      this.opts.connection.clusterMode ?? false,
    );
    this.running = true;
    this.pollLoop();
  }

  private pollLoop(): void {
    if (!this.running) return;

    this.pollOnce()
      .then(() => {
        this.pollLoop();
      })
      .catch((err) => {
        if (this.running) {
          this.emit('error', err);
          setTimeout(() => this.pollLoop(), 1000);
        }
      });
  }

  private async pollOnce(): Promise<void> {
    if (!this.client || !this.running) return;

    // XREAD BLOCK {blockTimeout} COUNT 100 STREAMS {eventsKey} {lastId}
    const result = await this.client.xread(
      { [this.queueKeys.events]: this.lastId },
      { block: this.blockTimeout, count: 100 },
    );

    if (!result) {
      // Timeout with no new entries - loop again
      return;
    }

    // result: GlideRecord<StreamEntryDataType>
    // i.e. { key, value }[] where value = Record<entryId, [GlideString, GlideString][]>
    for (const streamEntry of result) {
      const entries = streamEntry.value;
      for (const [entryId, fieldPairs] of Object.entries(entries)) {
        if (!fieldPairs) continue;

        // Parse field pairs into a map
        const fields: Record<string, string> = {};
        for (const [f, v] of fieldPairs) {
          fields[String(f)] = String(v);
        }

        const eventType = fields.event;
        if (!eventType) continue;

        // Build the event payload: always include jobId, plus any extra fields
        const payload: Record<string, string> = {};
        for (const [key, value] of Object.entries(fields)) {
          if (key === 'event') continue; // don't include the event type in payload
          payload[key] = value;
        }

        this.emit(eventType, payload);

        // Update lastId to this entry so we don't re-read it
        this.lastId = String(entryId);
      }
    }
  }

  async close(): Promise<void> {
    this.running = false;
    // Wait for init to complete so client is available for cleanup
    try {
      await this.initPromise;
    } catch {
      // init may have failed - that's fine
    }
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
