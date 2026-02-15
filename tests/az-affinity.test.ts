import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GlideClient, GlideClusterClient } from '@glidemq/speedkey';
import { createClient, createBlockingClient } from '../src/connection';
import type { ConnectionOptions } from '../src/types';

vi.mock('@glidemq/speedkey', () => {
  const MockGlideClient = {
    createClient: vi.fn(),
  };
  const MockGlideClusterClient = {
    createClient: vi.fn(),
  };

  return {
    GlideClient: MockGlideClient,
    GlideClusterClient: MockGlideClusterClient,
    ServiceType: { Elasticache: 'Elasticache', MemoryDB: 'MemoryDB' },
  };
});

function makeMockClient() {
  return {
    fcall: vi.fn(),
    functionLoad: vi.fn(),
    xgroupCreate: vi.fn(),
    close: vi.fn(),
  };
}

describe('AZ-Affinity config passthrough', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should pass readFrom and clientAz to standalone client', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 6379 }],
      readFrom: 'AZAffinity',
      clientAz: 'us-east-1a',
    };

    await createClient(opts);

    expect(GlideClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        readFrom: 'AZAffinity',
        clientAz: 'us-east-1a',
      }),
    );
  });

  it('should pass readFrom and clientAz to cluster client', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClusterClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 7000 }],
      clusterMode: true,
      readFrom: 'AZAffinity',
      clientAz: 'us-east-1b',
    };

    await createClient(opts);

    expect(GlideClusterClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        readFrom: 'AZAffinity',
        clientAz: 'us-east-1b',
      }),
    );
  });

  it('should pass AZAffinityReplicasAndPrimary readFrom strategy', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClusterClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 7000 }],
      clusterMode: true,
      readFrom: 'AZAffinityReplicasAndPrimary',
      clientAz: 'eu-west-1a',
    };

    await createClient(opts);

    expect(GlideClusterClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        readFrom: 'AZAffinityReplicasAndPrimary',
        clientAz: 'eu-west-1a',
      }),
    );
  });

  it('should pass preferReplica readFrom without clientAz', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 6379 }],
      readFrom: 'preferReplica',
    };

    await createClient(opts);

    expect(GlideClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        readFrom: 'preferReplica',
        clientAz: undefined,
      }),
    );
  });

  it('should not include readFrom or clientAz when not specified', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 6379 }],
    };

    await createClient(opts);

    const calledConfig = vi.mocked(GlideClient.createClient).mock.calls[0][0];
    expect(calledConfig.readFrom).toBeUndefined();
    expect(calledConfig.clientAz).toBeUndefined();
  });

  it('should pass readFrom and clientAz through createBlockingClient', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: '127.0.0.1', port: 6379 }],
      readFrom: 'AZAffinity',
      clientAz: 'us-west-2c',
    };

    await createBlockingClient(opts);

    expect(GlideClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        readFrom: 'AZAffinity',
        clientAz: 'us-west-2c',
      }),
    );
  });

  it('should combine readFrom/clientAz with TLS and credentials', async () => {
    const mockClient = makeMockClient();
    vi.mocked(GlideClusterClient.createClient).mockResolvedValue(mockClient as any);

    const opts: ConnectionOptions = {
      addresses: [{ host: 'cluster.example.com', port: 6380 }],
      clusterMode: true,
      useTLS: true,
      credentials: { username: 'admin', password: 'secret' },
      readFrom: 'AZAffinity',
      clientAz: 'us-east-1a',
    };

    await createClient(opts);

    expect(GlideClusterClient.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        addresses: [{ host: 'cluster.example.com', port: 6380 }],
        useTLS: true,
        credentials: { username: 'admin', password: 'secret' },
        readFrom: 'AZAffinity',
        clientAz: 'us-east-1a',
      }),
    );
  });
});
