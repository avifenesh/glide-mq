import { decompress, MAX_JOB_DATA_SIZE } from '../src/utils';
import { gzipSync } from 'zlib';
import { describe, it, expect } from 'vitest';

describe('Security: Zip Bomb Protection', () => {
  it('should throw an error when decompressing data larger than MAX_JOB_DATA_SIZE', () => {
    // Create a string slightly larger than the max allowed size
    const largeString = 'a'.repeat(MAX_JOB_DATA_SIZE + 100);

    // Compress it
    const compressedBuffer = gzipSync(largeString);
    const payload = 'gz:' + compressedBuffer.toString('base64');

    // Attempt to decompress - expecting it to fail with the new security check
    expect(() => decompress(payload)).toThrow();
  });
});
