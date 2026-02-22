import path from 'path';
import fs from 'fs';
import type { Processor, SandboxOptions } from '../types';
import { SandboxPool } from './pool';
import { GlideMQError } from '../errors';

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

  // Validate file exists
  try {
    fs.accessSync(absolutePath, fs.constants.R_OK);
  } catch {
    throw new GlideMQError(`Processor file not found or not readable: ${absolutePath}`);
  }

  const useWorkerThreads = sandboxOpts?.useWorkerThreads !== false;
  const maxWorkers = sandboxOpts?.maxWorkers ?? concurrency;

  const pool = new SandboxPool(absolutePath, useWorkerThreads, maxWorkers);

  const processor: Processor<D, R> = async (job) => {
    return pool.run<D, R>(job);
  };

  const close = () => pool.close();

  return { processor, close };
}
