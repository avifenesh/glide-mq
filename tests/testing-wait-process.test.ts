import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('TestQueue waitForJobs process lifetime', () => {
  it('keeps Node running until a delayed job completes', () => {
    const output = execFileSync(
      process.execPath,
      [
        '-e',
        `
      const { TestQueue, TestWorker } = require(${JSON.stringify(resolve('dist/testing.js'))});
      (async () => {
        const queue = new TestQueue('delayed-process');
        const worker = new TestWorker(queue, async () => 'done');
        const job = await queue.add('work', {}, { delay: 30 });
        await queue.waitForJobs([job], { timeout: 2000 });
        await worker.close();
        await queue.close();
        console.log('completed');
      })().catch(error => { console.error(error); process.exitCode = 1; });
    `,
      ],
      { encoding: 'utf8', timeout: 5000 },
    );
    expect(output.trim()).toBe('completed');
  });

  it('keeps Node running until an unprocessed job times out', () => {
    const output = execFileSync(
      process.execPath,
      [
        '-e',
        `
      const { TestQueue } = require(${JSON.stringify(resolve('dist/testing.js'))});
      (async () => {
        const queue = new TestQueue('timeout-process');
        const job = await queue.add('work', {});
        try {
          await queue.waitForJobs([job], { timeout: 30 });
          throw new Error('Expected a timeout');
        } catch (error) {
          if (!error.message.includes('pending ' + job.id)) throw error;
          console.log('timed out');
        } finally {
          await queue.close();
        }
      })().catch(error => { console.error(error); process.exitCode = 1; });
    `,
      ],
      { encoding: 'utf8', timeout: 5000 },
    );
    expect(output.trim()).toBe('timed out');
  });
});
