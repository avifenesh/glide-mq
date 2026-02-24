/**
 * Sandbox runner - executed inside worker threads or child processes.
 * Receives jobs over IPC, runs the user-defined processor, sends results back.
 */

import { isMainThread, parentPort, workerData } from 'worker_threads';
import type { MainToChild, ChildToMain, SerializedJob } from './types';
import { SandboxJob } from './sandbox-job';

const isThread = !isMainThread && parentPort != null;
const isChild = typeof process.send === 'function';

if (!isThread && !isChild) {
  // This file was loaded directly (not as a worker thread or child process)
  throw new Error('Sandbox runner must be loaded as a worker thread or child process');
}

function send(msg: ChildToMain): void {
  if (isThread) {
    parentPort!.postMessage(msg);
  } else {
    process.send!(msg);
  }
}

let cachedProcessor: ((job: any) => Promise<any>) | null = null;
const activeJobs = new Map<string, SandboxJob>();

async function loadProcessor(filePath: string): Promise<(job: any) => Promise<any>> {
  if (cachedProcessor) return cachedProcessor;

  let mod: any;
  if (filePath.endsWith('.mjs')) {
    // ESM modules must use import() with a file URL.
    // We use Function() to prevent TypeScript from compiling import() to require().
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { pathToFileURL } = require('url');
    mod = await (Function('p', 'return import(p)') as (p: string) => Promise<any>)(pathToFileURL(filePath).href);
  } else {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      mod = require(filePath);
    } catch (err: any) {
      if (err?.code === 'ERR_REQUIRE_ESM') {
        // Fallback: .js file in a "type": "module" project
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const { pathToFileURL } = require('url');
        mod = await (Function('p', 'return import(p)') as (p: string) => Promise<any>)(pathToFileURL(filePath).href);
      } else {
        throw err;
      }
    }
  }

  const fn = mod.default || mod;

  if (typeof fn !== 'function') {
    throw new Error(`Processor file ${filePath} does not export a function`);
  }

  cachedProcessor = fn;
  return fn;
}

async function handleProcess(id: string, serialized: SerializedJob): Promise<void> {
  const job = new SandboxJob(serialized, send, id);
  activeJobs.set(id, job);

  try {
    const processorPath = isThread ? workerData.processorPath : process.argv[2];
    const processor = await loadProcessor(processorPath);

    const result = await processor(job);

    activeJobs.delete(id);
    send({ type: 'completed', id, result });
  } catch (err: any) {
    activeJobs.delete(id);
    send({
      type: 'failed',
      id,
      error: sanitizePath(err?.message ?? String(err)) || 'Unknown error',
      stack: sanitizePath(err?.stack),
      errorName: err?.name,
      discarded: job.discarded,
    });
  }
}

const cwd = process.cwd();
// Escape potential regex special characters in cwd
const escapedCwd = cwd.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const cwdRegex = new RegExp(escapedCwd, 'g');

function sanitizePath(text: string | undefined): string | undefined {
  if (!text) return text;
  return text.replace(cwdRegex, '[PROJECT_ROOT]');
}

function handleMessage(msg: MainToChild): void {
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') return;

  switch (msg.type) {
    case 'process':
      handleProcess(msg.id, msg.job).catch((err) => {
        send({
          type: 'failed',
          id: msg.id,
          error: sanitizePath(err?.message ?? String(err)) || 'Unknown error',
        });
      });
      break;
    case 'abort': {
      const abortJob = activeJobs.get(msg.id);
      if (abortJob) {
        abortJob._abort();
      }
      break;
    }
    case 'proxy-response': {
      const proxyId = msg.id;
      const colonIdx = proxyId.indexOf(':');
      if (colonIdx >= 0) {
        const invId = proxyId.slice(0, colonIdx);
        const job = activeJobs.get(invId);
        if (job) job.handleProxyResponse(msg);
      }
      break;
    }
  }
}

if (isThread) {
  parentPort!.on('message', handleMessage);
} else {
  process.on('message', handleMessage);
}
