/**
 * ScenarioContext builder. Tracks all created resources and cleans them up.
 */

import type { SeededRNG } from './rng';
import type { ScenarioContext, FuzzMode } from './types';
import type { ConnectionConfig } from '../helpers/fixture';
import { flushQueue, createCleanupClient } from '../helpers/fixture';

const { Queue } = require('../../dist/queue') as typeof import('../../src/queue');
const { Worker } = require('../../dist/worker') as typeof import('../../src/worker');
const { FlowProducer } = require('../../dist/flow-producer') as typeof import('../../src/flow-producer');
const { QueueEvents } = require('../../dist/queue-events') as typeof import('../../src/queue-events');
const { TestQueue, TestWorker } = require('../../dist/testing') as typeof import('../../src/testing');

const STANDALONE_CONNECTION: ConnectionConfig = {
  addresses: [{ host: 'localhost', port: 6379 }],
};

const CLUSTER_CONNECTION: ConnectionConfig = {
  addresses: [{ host: '127.0.0.1', port: 7000 }],
  clusterMode: true,
};

function connectionForMode(mode: FuzzMode): ConnectionConfig | null {
  if (mode === 'standalone') return STANDALONE_CONNECTION;
  if (mode === 'cluster') return CLUSTER_CONNECTION;
  return null;
}

let contextCounter = 0;

export function createScenarioContext(rng: SeededRNG, mode: FuzzMode): ScenarioContext {
  const connection = connectionForMode(mode);
  const resources: { type: string; instance: any; queueName?: string }[] = [];
  const prefix = `fz-${++contextCounter}`;

  function uid(): string {
    const tag = rng.nextInt(0, 0xffffff).toString(36);
    return `fuzz-${prefix}-${Date.now().toString(36)}-${tag}`;
  }

  function createQueue(name: string, opts?: any): any {
    if (mode === 'test') {
      const q = new TestQueue(name, opts);
      resources.push({ type: 'queue', instance: q, queueName: name });
      return q;
    }
    const q = new Queue(name, { connection: connection!, ...opts });
    resources.push({ type: 'queue', instance: q, queueName: name });
    return q;
  }

  function createWorker(name: string, processor: any, opts?: any): any {
    if (mode === 'test') {
      // In test mode, the first arg to TestWorker is the queue instance, not the name.
      // The caller must pass the queue as `name` or we look it up.
      // For simplicity, accept either a string name or a TestQueue instance.
      let queue: any = name;
      if (typeof name === 'string') {
        // Find the TestQueue with this name from tracked resources
        const found = resources.find(
          (r) => r.type === 'queue' && r.instance instanceof TestQueue && r.instance.name === name,
        );
        if (!found) {
          throw new Error(`No TestQueue named "${name}" found. Create the queue first.`);
        }
        queue = found.instance;
      }
      const w = new TestWorker(queue, processor, opts);
      resources.push({ type: 'worker', instance: w });
      return w;
    }
    const w = new Worker(name, processor, {
      connection: connection!,
      ...opts,
    });
    resources.push({ type: 'worker', instance: w });
    return w;
  }

  function createFlowProducer(): any {
    if (mode === 'test') {
      throw new Error('FlowProducer is not available in test mode');
    }
    const fp = new FlowProducer({ connection: connection! });
    resources.push({ type: 'flow-producer', instance: fp });
    return fp;
  }

  function createQueueEvents(name: string): any {
    if (mode === 'test') {
      throw new Error('QueueEvents is not available in test mode');
    }
    const qe = new QueueEvents(name, { connection: connection! });
    resources.push({ type: 'queue-events', instance: qe });
    return qe;
  }

  async function cleanup(): Promise<void> {
    // Close in reverse order: workers before queues
    const reversed = resources.slice().reverse();

    for (const r of reversed) {
      try {
        if (r.type === 'worker') {
          await r.instance.close(true);
        } else if (r.type === 'queue-events') {
          await r.instance.close();
        } else if (r.type === 'flow-producer') {
          await r.instance.close();
        } else if (r.type === 'queue') {
          await r.instance.close();
        }
      } catch {
        // Ignore close errors during cleanup
      }
    }

    // For real Valkey mode, obliterate queue data
    if (mode !== 'test' && connection) {
      let client: any;
      try {
        client = await createCleanupClient(connection);
        const queueNames = new Set<string>();
        for (const r of resources) {
          if (r.queueName) queueNames.add(r.queueName);
        }
        for (const name of queueNames) {
          await flushQueue(client, name);
        }
      } catch {
        // Ignore flush errors
      } finally {
        try {
          if (client) await client.close();
        } catch {
          // Ignore
        }
      }
    }

    resources.length = 0;
  }

  return {
    rng,
    mode,
    createQueue,
    createWorker,
    createFlowProducer,
    createQueueEvents,
    uid,
    cleanup,
  };
}
