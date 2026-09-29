import { EventEmitter } from 'events';
import type { QueueEventsOptions, Client } from './types';
import { buildKeys, nextReconnectDelay, reconnectWithBackoff } from './utils';
import { createBlockingClient, ensureFunctionLibrary } from './connection';
import { GlideMQError } from './errors';

export class QueueEvents extends EventEmitter {
  readonly name: string;
  private opts: QueueEventsOptions;
  private client: Client | null = null;
  private queueKeys: ReturnType<typeof buildKeys>;
  private running = false;
  private closing = false;
  private lastId: string;
  private initPromise: Promise<void>;
  private blockTimeout: number;
  private reconnectBackoff = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(name: string, opts: QueueEventsOptions) {
    super();
    if ((opts as any).client) {
      throw new GlideMQError(
        'QueueEvents does not accept an injected `client`. ' +
          'It uses blocking XREAD which requires a dedicated connection. ' +
          'Provide `connection` instead.',
      );
    }
    this.name = name;
    this.opts = opts;
    this.queueKeys = buildKeys(name, opts.prefix);
    // '$' means only new messages from this point forward
    this.lastId = opts.lastEventId ?? '$';
    this.blockTimeout = opts.blockTimeout ?? 5000;
    this.initPromise = this.init();
    // Callers that never await waitUntilReady() must not crash the process
    // with an unhandled rejection. Mirrors BaseWorker: surface it as 'error'.
    this.initPromise.catch((err) => {
      if (!this.closing) this.emit('error', err);
    });
  }

  /**
   * Wait until the QueueEvents instance is connected and listening.
   */
  async waitUntilReady(): Promise<void> {
    return this.initPromise;
  }

  private async init(): Promise<void> {
    this.client = await createBlockingClient(this.opts.connection);
    await ensureFunctionLibrary(this.client, undefined, this.opts.connection.clusterMode ?? false);
    this.running = true;
    this.pollLoop();
  }

  private pollLoop(): void {
    if (!this.running || this.closing) return;

    this.pollOnce()
      .then(() => {
        this.reconnectBackoff = 0;
        try {
          this.pollLoop();
        } catch (err) {
          if (!this.closing) this.emit('error', err);
        }
      })
      .catch((err) => {
        if (this.running && !this.closing) {
          this.emit('error', err);
          this.reconnectBackoff = nextReconnectDelay(this.reconnectBackoff);
          this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            void this.reconnectAndResume();
          }, this.reconnectBackoff);
        }
      });
  }

  private reconnectCtx = {
    isActive: () => this.running && !this.closing,
    getBackoff: () => this.reconnectBackoff,
    setBackoff: (ms: number) => {
      this.reconnectBackoff = ms;
    },
    onError: (err: unknown) => {
      this.emit('error', err);
    },
    setRetryTimer: (timer: ReturnType<typeof setTimeout> | null) => {
      this.reconnectTimer = timer;
    },
  };

  /**
   * Attempt to reconnect the client and resume polling after a connection error.
   */
  private async reconnectAndResume(): Promise<void> {
    await reconnectWithBackoff(
      this.reconnectCtx,
      async () => {
        if (this.client) {
          try {
            this.client.close();
          } catch {
            /* ignore */
          }
          this.client = null;
        }

        // close() can land during either await. Keep the new client local
        // until both finish so close() never misses a client created late.
        const client = await createBlockingClient(this.opts.connection);
        try {
          if (this.closing) throw new GlideMQError('QueueEvents closed during reconnect.');
          await ensureFunctionLibrary(client, undefined, this.opts.connection.clusterMode ?? false);
          if (this.closing) throw new GlideMQError('QueueEvents closed during reconnect.');
        } catch (err) {
          try {
            client.close();
          } catch {
            /* ignore */
          }
          throw err;
        }
        this.client = client;
      },
      () => this.pollLoop(),
    );
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
    for (let i = 0; i < result.length; i++) {
      const streamEntry = result[i];
      const entries = streamEntry.value;

      for (const [entryId, fieldPairs] of Object.entries(entries)) {
        if (!fieldPairs) continue;

        // Extract event type and payload from field pairs
        let eventType: string | undefined;
        const payload: Record<string, string> = Object.create(null);

        for (let j = 0; j < fieldPairs.length; j++) {
          const field = String(fieldPairs[j][0]);
          const value = String(fieldPairs[j][1]);

          if (field === 'event') {
            eventType = value;
          } else {
            payload[field] = value;
          }
        }

        if (!eventType) continue;

        this.emit(eventType, payload);

        // Update lastId to this entry so we don't re-read it
        this.lastId = String(entryId);
      }
    }
  }

  /**
   * Close the QueueEvents listener.
   * Idempotent: safe to call multiple times.
   */
  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    this.running = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
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
