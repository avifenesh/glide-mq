/**
 * Workflow helpers must leave returned Job instances usable.
 * chain/group/chord/dag currently close the FlowProducer client in finally,
 * so getState() and waitUntilFinished() fail after the helper returns.
 *
 * Requires: valkey-server on localhost:6379 and cluster on :7000-7005
 *
 * Run: npx vitest run tests/workflow-helper-client.test.ts
 */
import { it, expect, beforeAll, afterAll } from 'vitest';
import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

const { chain, group, chord, dag } = require('../dist/workflows') as typeof import('../src/workflows');

describeEachMode('Workflow helper returned jobs', (CONNECTION) => {
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    cleanupClient.close();
  });

  it('chain: returned job can getState after the helper returns', async () => {
    const Q = 'wf-helper-chain-' + Date.now();
    const node = await chain(Q, [{ name: 'only', data: { v: 1 } }], CONNECTION);

    await expect(node.job.getState()).resolves.toBe('waiting');
    await node.close();
    await expect(node.job.getState()).rejects.toThrow(/closed/);

    await flushQueue(cleanupClient, Q);
  });

  it('group: returned jobs can getState after the helper returns', async () => {
    const Q = 'wf-helper-group-' + Date.now();
    const node = await group(Q, [{ name: 'child', data: { v: 1 } }], CONNECTION);

    await expect(node.job.getState()).resolves.toBe('waiting-children');
    await expect(node.children![0].job.getState()).resolves.toBe('waiting');
    await node.close();

    await flushQueue(cleanupClient, Q);
  });

  it('chord: returned jobs can getState after the helper returns', async () => {
    const Q = 'wf-helper-chord-' + Date.now();
    const node = await chord(Q, [{ name: 'member', data: { v: 1 } }], { name: 'callback', data: {} }, CONNECTION);

    await expect(node.job.getState()).resolves.toBe('waiting-children');
    await expect(node.children![0].job.getState()).resolves.toBe('waiting');
    await node.close();

    await flushQueue(cleanupClient, Q);
  });

  it('dag: returned jobs can getState after the helper returns', async () => {
    const Q = 'wf-helper-dag-' + Date.now();
    const jobs = await dag(
      [
        { name: 'A', queueName: Q, data: { step: 'A' } },
        { name: 'B', queueName: Q, data: { step: 'B' }, deps: ['A'] },
      ],
      CONNECTION,
    );

    await expect(jobs.get('A')!.getState()).resolves.toBe('waiting');
    await expect(jobs.get('B')!.getState()).resolves.toBe('waiting-children');
    await jobs.close();

    await flushQueue(cleanupClient, Q);
  });

  it('dag: closes the owned client when addDAG fails', async () => {
    const Q = 'wf-helper-dag-cycle-' + Date.now();
    await expect(
      dag(
        [
          { name: 'A', queueName: Q, data: {}, deps: ['B'] },
          { name: 'B', queueName: Q, data: {}, deps: ['A'] },
        ],
        CONNECTION,
      ),
    ).rejects.toThrow(/cycle/);
  });
});
