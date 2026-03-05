/**
 * Run benchmark suites against a Docker-managed Valkey instance.
 *
 * Usage:
 *   npx tsx benchmarks/docker-run.ts user
 *   BENCH_PORT=6380 npx tsx benchmarks/docker-run.ts throughput
 */

import { spawnSync } from 'child_process';

function runOrThrow(command: string, args: string[], env?: NodeJS.ProcessEnv): void {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    env: env ?? process.env,
    shell: process.platform === 'win32',
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status ?? 'null'}`);
  }
}

function runStatus(command: string, args: string[], env?: NodeJS.ProcessEnv): { ok: boolean; out: string } {
  const result = spawnSync(command, args, {
    stdio: 'pipe',
    encoding: 'utf8',
    env: env ?? process.env,
    shell: process.platform === 'win32',
  });
  const out = `${result.stdout ?? ''}${result.stderr ?? ''}`;
  return { ok: result.status === 0, out };
}

function runCapture(command: string, args: string[]): { ok: boolean; out: string } {
  const result = spawnSync(command, args, {
    stdio: 'pipe',
    encoding: 'utf8',
  });
  const out = `${result.stdout ?? ''}${result.stderr ?? ''}`;
  return { ok: result.status === 0, out };
}

function cleanupContainer(container: string): void {
  spawnSync('docker', ['rm', '-f', container], { stdio: 'ignore' });
}

async function waitForValkey(container: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const valkeyCheck = runCapture('docker', ['exec', container, 'valkey-cli', 'ping']);
    if (valkeyCheck.ok && valkeyCheck.out.includes('PONG')) {
      return;
    }
    const redisCheck = runCapture('docker', ['exec', container, 'redis-cli', 'ping']);
    if (redisCheck.ok && redisCheck.out.includes('PONG')) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for Valkey in container '${container}'`);
}

async function main(): Promise<void> {
  const suite = process.argv[2] || 'user';
  const image = process.env.BENCH_DOCKER_IMAGE || 'valkey/valkey:8';
  const container = process.env.BENCH_DOCKER_CONTAINER || 'glidemq-bench-valkey';
  const requestedPort = process.env.BENCH_PORT;
  const portsToTry = requestedPort ? [String(requestedPort)] : ['6379', '6380', '46379', '56379'];

  cleanupContainer(container);

  try {
    runOrThrow('docker', ['pull', image]);

    let selectedPort: string | null = null;
    for (const port of portsToTry) {
      const runResult = runStatus('docker', ['run', '-d', '--name', container, '-p', `${port}:6379`, image]);
      if (runResult.ok) {
        selectedPort = port;
        break;
      }
      cleanupContainer(container);
      if (
        runResult.out.includes('port is already allocated') ||
        runResult.out.includes('Bind for 0.0.0.0') ||
        runResult.out.includes('Only one usage of each socket address')
      ) {
        console.log(`[docker-bench] host port ${port} is busy, trying another port...`);
        continue;
      }
      throw new Error(runResult.out.trim() || `docker run failed on port ${port}`);
    }

    if (!selectedPort) {
      throw new Error(`Could not bind Docker Valkey container to any host port: ${portsToTry.join(', ')}`);
    }

    console.log(`[docker-bench] suite=${suite} image=${image} host-port=${selectedPort}`);
    await waitForValkey(container, 30000);

    runOrThrow('npx', ['tsx', 'benchmarks/run.ts', suite], {
      ...process.env,
      BENCH_PORT: selectedPort,
    });
  } finally {
    cleanupContainer(container);
  }
}

main().catch((err) => {
  console.error('[docker-bench] failed:', err instanceof Error ? err.message : String(err));
  process.exit(1);
});
