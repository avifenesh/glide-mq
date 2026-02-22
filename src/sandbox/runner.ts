/**
 * Sandbox runner - executed inside worker threads or child processes.
 * Receives jobs over IPC, runs the user-defined processor, sends results back.
 */

import { isMainThread, parentPort, workerData } from 'worker_threads';
import { pathToFileURL } from 'url';
import type { MainToChild, ChildToMain, SerializedJob } from './types';
import { SandboxJob } from './sandbox-job';

// Detect mode: worker thread or child process
const isThread = !isMainThread && parentPort != null;
const isChild = typeof process.send === 'function';

if (!isThread && !isChild) {
  // This file was loaded directly (not as a worker thread or child process)
  throw new Error('runner.ts must be loaded as a worker thread or child process');
}

function send(msg: ChildToMain): void {
  if (isThread) {
    parentPort!.postMessage(msg);
  } else {
    process.send!(msg);
  }
}

let cachedProcessor: ((job: any) => Promise<any>) | null = null;
let currentJob: SandboxJob | null = null;

async function loadProcessor(filePath: string): Promise<(job: any) => Promise<any>> {
  if (cachedProcessor) return cachedProcessor;

  // Use dynamic import for both CJS and ESM modules.
  // pathToFileURL ensures correct handling on all platforms.
  const mod = await import(pathToFileURL(filePath).href);
  const fn = mod.default || mod;

  if (typeof fn !== 'function') {
    throw new Error(`Processor file ${filePath} does not export a function`);
  }

  cachedProcessor = fn;
  return fn;
}

async function handleProcess(id: string, serialized: SerializedJob): Promise<void> {
  try {
    // The processor path is passed via workerData (threads) or argv (child process)
    const processorPath = isThread ? workerData.processorPath : process.argv[2];
    const processor = await loadProcessor(processorPath);

    const job = new SandboxJob(serialized, send);
    currentJob = job;

    const result = await processor(job);

    currentJob = null;
    send({ type: 'completed', id, result });
  } catch (err: any) {
    currentJob = null;
    send({
      type: 'failed',
      id,
      error: err?.message ?? String(err),
      stack: err?.stack,
    });
  }
}

function handleMessage(msg: MainToChild): void {
  switch (msg.type) {
    case 'process':
      handleProcess(msg.id, msg.job);
      break;
    case 'abort':
      if (currentJob) {
        currentJob._abort();
      }
      break;
    case 'proxy-response':
      if (currentJob) {
        currentJob.handleProxyResponse(msg);
      }
      break;
  }
}

if (isThread) {
  parentPort!.on('message', handleMessage);
} else {
  process.on('message', handleMessage);
}
