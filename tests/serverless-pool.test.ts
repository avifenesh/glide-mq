/**
 * ServerlessPool integration tests.
 * Verifies connection caching, fingerprinting, and closeAll behavior.
 *
 * Requires: valkey-server on :6379 and cluster on :7000-7005
 */
import { it, expect, beforeAll, afterAll, beforeEach } from 'vitest';

const { ServerlessPool, serverlessPool } = require('../dist/serverless-pool') as typeof import('../src/serverless-pool');
const { buildKeys } = require('../dist/utils') as typeof import('../src/utils');

import { describeEachMode, createCleanupClient, flushQueue } from './helpers/fixture';

describeEachMode('ServerlessPool - caching', (CONNECTION) => {
  const Q1 = 'test-pool-cache1-' + Date.now();
  const Q2 = 'test-pool-cache2-' + Date.now();
  let pool: InstanceType<typeof ServerlessPool>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  beforeEach(() => {
    pool = new ServerlessPool();
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q1);
    await flushQueue(cleanupClient, Q2);
    cleanupClient.close();
  });

  it('returns same instance for same name + connection', () => {
    const p1 = pool.getProducer(Q1, { connection: CONNECTION });
    const p2 = pool.getProducer(Q1, { connection: CONNECTION });
    expect(p1).toBe(p2);
  });

  it('returns different instances for different queue names', () => {
    const p1 = pool.getProducer(Q1, { connection: CONNECTION });
    const p2 = pool.getProducer(Q2, { connection: CONNECTION });
    expect(p1).not.toBe(p2);
  });

  it('tracks pool size correctly', () => {
    expect(pool.size).toBe(0);
    pool.getProducer(Q1, { connection: CONNECTION });
    expect(pool.size).toBe(1);
    pool.getProducer(Q2, { connection: CONNECTION });
    expect(pool.size).toBe(2);
    // Same key should not increase size
    pool.getProducer(Q1, { connection: CONNECTION });
    expect(pool.size).toBe(2);
  });
});

describeEachMode('ServerlessPool - closeAll', (CONNECTION) => {
  const Q = 'test-pool-close-' + Date.now();
  let pool: InstanceType<typeof ServerlessPool>;
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('closes all cached producers and clears cache', async () => {
    pool = new ServerlessPool();
    const producer = pool.getProducer(Q, { connection: CONNECTION });
    // Force connection
    await producer.add('pool-test', { x: 1 });
    expect(pool.size).toBe(1);

    await pool.closeAll();
    expect(pool.size).toBe(0);

    // Producer should be closed
    await expect(producer.add('after-close', {})).rejects.toThrow('Producer is closed');
  });

  it('creates fresh instances after closeAll', async () => {
    pool = new ServerlessPool();
    const p1 = pool.getProducer(Q, { connection: CONNECTION });
    await p1.add('before-close', { a: 1 });
    await pool.closeAll();

    const p2 = pool.getProducer(Q, { connection: CONNECTION });
    expect(p2).not.toBe(p1);
    const id = await p2.add('after-close', { b: 2 });
    expect(id).not.toBeNull();
    await pool.closeAll();
  });
});

describeEachMode('ServerlessPool - end-to-end', (CONNECTION) => {
  const Q = 'test-pool-e2e-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('getProducer -> add job -> verify in Valkey -> closeAll', async () => {
    const pool = new ServerlessPool();
    const producer = pool.getProducer(Q, { connection: CONNECTION });
    const payload = { action: 'e2e-test', ts: Date.now() };
    const id = await producer.add('e2e-job', payload);
    expect(id).not.toBeNull();

    const keys = buildKeys(Q);
    const rawData = await cleanupClient.hget(keys.job(id!), 'data');
    expect(JSON.parse(String(rawData))).toEqual(payload);

    await pool.closeAll();
  });
});

describeEachMode('ServerlessPool - module singleton', (CONNECTION) => {
  const Q = 'test-pool-singleton-' + Date.now();
  let cleanupClient: any;

  beforeAll(async () => {
    cleanupClient = await createCleanupClient(CONNECTION);
  });

  afterAll(async () => {
    await serverlessPool.closeAll();
    await flushQueue(cleanupClient, Q);
    cleanupClient.close();
  });

  it('module-level singleton is importable and works', async () => {
    expect(serverlessPool).toBeDefined();
    expect(typeof serverlessPool.getProducer).toBe('function');
    expect(typeof serverlessPool.closeAll).toBe('function');

    const producer = serverlessPool.getProducer(Q, { connection: CONNECTION });
    const id = await producer.add('singleton-test', { s: 1 });
    expect(id).not.toBeNull();
  });
});
