import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient, GlideClusterClient } from 'speedkey';
import {
  createClient,
  createBlockingClient,
  ensureFunctionLibrary,
  createConsumerGroup,
} from '../src/connection';
import { ConnectionError } from '../src/errors';
import { LIBRARY_SOURCE, LIBRARY_VERSION } from '../src/functions/index';

// Mock speedkey module
vi.mock('speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };

  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
  };
});

function makeMockClient(overrides: Record<string, unknown> = {}) {
  return {
    fcall: vi.fn(),
    functionLoad: vi.fn(),
    xgroupCreate: vi.fn(),
    close: vi.fn(),
    ...overrides,
  };
}

const standaloneOpts = {
  addresses: [{ host: '127.0.0.1', port: 6379 }],
};

const clusterOpts = {
  addresses: [{ host: '127.0.0.1', port: 7000 }],
  clusterMode: true as const,
};

describe('createClient', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should create a standalone GlideClient when clusterMode is not set', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const client = await createClient(standaloneOpts);

    expect(GlideClient.createClient).toHaveBeenCalledWith({
      addresses: [{ host: '127.0.0.1', port: 6379 }],
      useTLS: undefined,
      credentials: undefined,
    });
    expect(GlideClusterClient.createClient).not.toHaveBeenCalled();
    expect(client).toBe(mockClient);
  });

  it('should create a GlideClusterClient when clusterMode is true', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClusterClient.createClient).mockResolvedValue(mockClient as any);

    const client = await createClient(clusterOpts);

    expect(GlideClusterClient.createClient).toHaveBeenCalledWith({
      addresses: [{ host: '127.0.0.1', port: 7000 }],
      useTLS: undefined,
      credentials: undefined,
    });
    expect(GlideClient.createClient).not.toHaveBeenCalled();
    expect(client).toBe(mockClient);
  });

  it('should pass TLS and credentials options through', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const opts = {
      addresses: [{ host: 'secure.example.com', port: 6380 }],
      useTLS: true,
      credentials: { username: 'admin', password: 'secret' },
    };

    await createClient(opts);

    expect(GlideClient.createClient).toHaveBeenCalledWith({
      addresses: [{ host: 'secure.example.com', port: 6380 }],
      useTLS: true,
      credentials: { username: 'admin', password: 'secret' },
    });
  });

  it('should throw ConnectionError when client creation fails', async () => {
    vi.mocked(GlideClient.createClient).mockRejectedValue(new Error('Connection refused'));

    await expect(createClient(standaloneOpts)).rejects.toThrow(ConnectionError);
    await expect(createClient(standaloneOpts)).rejects.toThrow(/standalone client.*Connection refused/);
  });

  it('should throw ConnectionError with cluster context when cluster creation fails', async () => {
    vi.mocked(GlideClusterClient.createClient).mockRejectedValue(new Error('No reachable nodes'));

    await expect(createClient(clusterOpts)).rejects.toThrow(ConnectionError);
    await expect(createClient(clusterOpts)).rejects.toThrow(/cluster client.*No reachable nodes/);
  });
});

describe('createBlockingClient', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should create a client using the same mechanism as createClient', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const client = await createBlockingClient(standaloneOpts);

    expect(GlideClient.createClient).toHaveBeenCalledTimes(1);
    expect(client).toBe(mockClient);
  });
});

describe('ensureFunctionLibrary', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should skip loading when version matches', async () => {
    const mockClient = makeMockClient({
      fcall: vi.fn().mockResolvedValue(LIBRARY_VERSION),
    });

    await ensureFunctionLibrary(mockClient as any);

    expect(mockClient.fcall).toHaveBeenCalledWith('glidemq_version', [], []);
    expect(mockClient.functionLoad).not.toHaveBeenCalled();
  });

  it('should load library when version mismatches', async () => {
    const mockClient = makeMockClient({
      fcall: vi.fn().mockResolvedValue('0'),
      functionLoad: vi.fn().mockResolvedValue('glidemq'),
    });

    await ensureFunctionLibrary(mockClient as any);

    expect(mockClient.functionLoad).toHaveBeenCalledWith(
      LIBRARY_SOURCE,
      { replace: true },
    );
  });

  it('should load library when fcall throws (function not found)', async () => {
    const mockClient = makeMockClient({
      fcall: vi.fn().mockRejectedValue(new Error('Function not loaded')),
      functionLoad: vi.fn().mockResolvedValue('glidemq'),
    });

    await ensureFunctionLibrary(mockClient as any);

    expect(mockClient.functionLoad).toHaveBeenCalledWith(
      LIBRARY_SOURCE,
      { replace: true },
    );
  });

  it('should use allPrimaries route when clusterMode is true', async () => {
    const mockClient = makeMockClient({
      fcall: vi.fn().mockRejectedValue(new Error('Function not loaded')),
      functionLoad: vi.fn().mockResolvedValue('glidemq'),
    });

    await ensureFunctionLibrary(mockClient as any, LIBRARY_SOURCE, true);

    expect(mockClient.functionLoad).toHaveBeenCalledWith(
      LIBRARY_SOURCE,
      { replace: true, route: 'allPrimaries' },
    );
  });

  it('should not use route when clusterMode is false', async () => {
    const mockClient = makeMockClient({
      fcall: vi.fn().mockRejectedValue(new Error('NOSCRIPT No matching script')),
      functionLoad: vi.fn().mockResolvedValue('glidemq'),
    });

    await ensureFunctionLibrary(mockClient as any, LIBRARY_SOURCE, false);

    expect(mockClient.functionLoad).toHaveBeenCalledWith(
      LIBRARY_SOURCE,
      { replace: true },
    );
  });

  it('should accept custom library source', async () => {
    const customSource = '#!lua name=custom\nreturn 1';
    const mockClient = makeMockClient({
      fcall: vi.fn().mockRejectedValue(new Error('No matching script')),
      functionLoad: vi.fn().mockResolvedValue('custom'),
    });

    await ensureFunctionLibrary(mockClient as any, customSource);

    expect(mockClient.functionLoad).toHaveBeenCalledWith(
      customSource,
      { replace: true },
    );
  });

  it('should coerce non-string fcall result for version comparison', async () => {
    const mockClient = makeMockClient({
      // Simulate fcall returning a number instead of string
      fcall: vi.fn().mockResolvedValue(Number(LIBRARY_VERSION)),
      functionLoad: vi.fn().mockResolvedValue('glidemq'),
    });

    // String(Number(LIBRARY_VERSION)) === LIBRARY_VERSION => should NOT load
    await ensureFunctionLibrary(mockClient as any);

    expect(mockClient.functionLoad).not.toHaveBeenCalled();
  });
});

describe('createConsumerGroup', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should create a consumer group with mkStream', async () => {
    const mockClient = makeMockClient({
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
    });

    await createConsumerGroup(mockClient as any, 'mystream', 'mygroup');

    expect(mockClient.xgroupCreate).toHaveBeenCalledWith(
      'mystream',
      'mygroup',
      '0',
      { mkStream: true },
    );
  });

  it('should use custom startId when provided', async () => {
    const mockClient = makeMockClient({
      xgroupCreate: vi.fn().mockResolvedValue('OK'),
    });

    await createConsumerGroup(mockClient as any, 'mystream', 'mygroup', '$');

    expect(mockClient.xgroupCreate).toHaveBeenCalledWith(
      'mystream',
      'mygroup',
      '$',
      { mkStream: true },
    );
  });

  it('should ignore BUSYGROUP error (group already exists)', async () => {
    const mockClient = makeMockClient({
      xgroupCreate: vi.fn().mockRejectedValue(
        new Error('BUSYGROUP Consumer Group name already exists'),
      ),
    });

    // Should not throw
    await expect(
      createConsumerGroup(mockClient as any, 'mystream', 'mygroup'),
    ).resolves.toBeUndefined();
  });

  it('should rethrow non-BUSYGROUP errors', async () => {
    const mockClient = makeMockClient({
      xgroupCreate: vi.fn().mockRejectedValue(
        new Error('WRONGTYPE Operation against a key holding the wrong kind of value'),
      ),
    });

    await expect(
      createConsumerGroup(mockClient as any, 'mystream', 'mygroup'),
    ).rejects.toThrow('WRONGTYPE');
  });
});
