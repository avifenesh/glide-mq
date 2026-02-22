import path from 'path';
import fs from 'fs';
import type { Processor, SandboxOptions } from '../types';
import { SandboxPool } from './pool';
import { GlideMQError } from '../errors';

/**
 * Resolve the path to the compiled runner.js.
 * At runtime (from dist/), __dirname is dist/sandbox/ and runner.js is a sibling.
 * During vitest (from src/), __dirname is src/sandbox/ so we look in dist/.
 */
function resolveRunnerPath(): string {
  const candidate = path.join(__dirname, 'runner.js');
  if (fs.existsSync(candidate)) return candidate;

  // Fallback: traverse up from __dirname until we find dist/sandbox/runner.js
  let dir = __dirname;
  for (let i = 0; i < 5; i++) {
    const parent = path.dirname(dir);
    const distRunner = path.join(parent, 'dist', 'sandbox', 'runner.js');
    if (fs.existsSync(distRunner)) return distRunner;
    dir = parent;
  }

  throw new GlideMQError(
    'Cannot find sandbox runner.js. Run `npm run build` to compile the project.',
  );
}

/**
 * Create a sandboxed processor that runs a file-path processor in worker threads
 * (or child processes). Returns a standard Processor function and a close handle.
 */
export function createSandboxedProcessor<D = any, R = any>(
  processorPath: string,
  sandboxOpts: SandboxOptions | undefined,
  concurrency: number,
): { processor: Processor<D, R>; close: () => Promise<void> } {
  const absolutePath = path.resolve(processorPath);

  try {
    fs.accessSync(absolutePath, fs.constants.R_OK);
  } catch {
    throw new GlideMQError(`Processor file not found or not readable: ${absolutePath}`);
  }

  const useWorkerThreads = sandboxOpts?.useWorkerThreads !== false;
  const maxWorkers = sandboxOpts?.maxWorkers ?? concurrency;
  const runnerPath = resolveRunnerPath();

  const pool = new SandboxPool(absolutePath, useWorkerThreads, maxWorkers, runnerPath);

  const processor: Processor<D, R> = async (job) => {
    return pool.run<D, R>(job);
  };

  const close = () => pool.close();

  return { processor, close };
}
