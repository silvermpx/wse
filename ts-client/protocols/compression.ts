// =============================================================================
// WebSocket Engine - Compression & MessagePack
// =============================================================================

import pako from 'pako';
import { encode, decode } from '@msgpack/msgpack';
import { logger } from '../utils/logger';
import { MEMORY_LIMITS } from '../constants';

type CompressionLevel = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | -1;

export class CompressionManager {
  private compressionThreshold: number;
  private compressionLevel: CompressionLevel;

  constructor(
    compressionThreshold = 1024,
    compressionLevel: CompressionLevel = 6
  ) {
    this.compressionThreshold = compressionThreshold;
    this.compressionLevel = compressionLevel;
  }

  // ---------------------------------------------------------------------------
  // Compression
  // ---------------------------------------------------------------------------

  shouldCompress(data: ArrayBuffer | string): boolean {
    const size = typeof data === 'string'
      ? new Blob([data]).size
      : data.byteLength;

    return size > this.compressionThreshold;
  }

  compress(data: ArrayBuffer): ArrayBuffer {
    try {
      const compressed = pako.deflate(new Uint8Array(data), { level: this.compressionLevel });
      logger.debug(`Compressed ${data.byteLength} -> ${compressed.byteLength} bytes`);
      return compressed.buffer.slice(compressed.byteOffset, compressed.byteOffset + compressed.byteLength);
    } catch (error) {
      logger.error('Compression failed:', error);
      throw error;
    }
  }

  decompress(data: ArrayBuffer): ArrayBuffer {
    try {
      const view = new Uint8Array(data);

      logger.debug('Decompressing data:', {
        length: data.byteLength,
        first10Bytes: Array.from(view.slice(0, 10)),
        hexFirst10: Array.from(view.slice(0, 10)).map(b => b.toString(16).padStart(2, '0')).join(' ')
      });

      // Try standard inflate (for zlib format)
      try {
        const decompressed = pako.inflate(view);
        if (decompressed.byteLength > MEMORY_LIMITS.MAX_MESSAGE_SIZE) {
          throw new Error(`Decompressed size ${decompressed.byteLength} exceeds limit ${MEMORY_LIMITS.MAX_MESSAGE_SIZE}`);
        }
        logger.debug(`Decompressed ${data.byteLength} -> ${decompressed.byteLength} bytes (standard inflate)`);
        return decompressed.buffer.slice(decompressed.byteOffset, decompressed.byteOffset + decompressed.byteLength);
      } catch (inflateError: any) {
        logger.debug('Standard inflate failed:', inflateError.message);

        if (inflateError.message && inflateError.message.includes('header')) {
          logger.debug('Trying raw inflate (no zlib header)...');
          try {
            const decompressed = pako.inflateRaw(view);
            if (decompressed.byteLength > MEMORY_LIMITS.MAX_MESSAGE_SIZE) {
              throw new Error(`Decompressed size ${decompressed.byteLength} exceeds limit ${MEMORY_LIMITS.MAX_MESSAGE_SIZE}`);
            }
            logger.debug(`Decompressed ${data.byteLength} -> ${decompressed.byteLength} bytes (raw inflate)`);
            return decompressed.buffer.slice(decompressed.byteOffset, decompressed.byteOffset + decompressed.byteLength);
          } catch (rawError) {
            logger.debug('Raw inflate also failed:', rawError);
          }
        }

        // Try gzip as last resort
        try {
          const decompressed = pako.ungzip(view);
          if (decompressed.byteLength > MEMORY_LIMITS.MAX_MESSAGE_SIZE) {
            throw new Error(`Decompressed size ${decompressed.byteLength} exceeds limit ${MEMORY_LIMITS.MAX_MESSAGE_SIZE}`);
          }
          logger.debug(`Decompressed ${data.byteLength} -> ${decompressed.byteLength} bytes (gzip)`);
          return decompressed.buffer.slice(decompressed.byteOffset, decompressed.byteOffset + decompressed.byteLength);
        } catch (gzipError) {
          logger.debug('Gzip decompression also failed');
        }

        throw inflateError;
      }
    } catch (error) {
      logger.error('Decompression failed:', error);
      throw error;
    }
  }

  decompressGzip(data: ArrayBuffer): ArrayBuffer {
    try {
      const result = pako.ungzip(new Uint8Array(data));
      if (result.byteLength > MEMORY_LIMITS.MAX_MESSAGE_SIZE) {
        throw new Error(`Decompressed gzip size ${result.byteLength} exceeds limit ${MEMORY_LIMITS.MAX_MESSAGE_SIZE}`);
      }
      return result.buffer.slice(result.byteOffset, result.byteOffset + result.byteLength);
    } catch (error) {
      logger.error('Gzip decompression failed:', error);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // MessagePack
  // ---------------------------------------------------------------------------

  packMsgPack(data: any): ArrayBuffer {
    try {
      const encoded = encode(data);
      return encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength) as ArrayBuffer;
    } catch (error) {
      logger.error('MessagePack encoding failed:', error);
      throw error;
    }
  }

  unpackMsgPack(data: ArrayBuffer): any {
    try {
      return decode(data);
    } catch (error) {
      logger.error('MessagePack decoding failed:', error);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Combined Operations
  // ---------------------------------------------------------------------------

  prepareForSending(message: any, useCompression: boolean = true): ArrayBuffer {
    const packed = this.packMsgPack(message);

    if (useCompression && this.shouldCompress(packed)) {
      const compressed = this.compress(packed);

      if (compressed.byteLength < packed.byteLength) {
        const header = new Uint8Array([67, 58]); // 'C:'
        const result = new Uint8Array(header.length + compressed.byteLength);
        result.set(header);
        result.set(new Uint8Array(compressed), header.length);
        return result.buffer;
      }
    }

    const header = new Uint8Array([77, 58]); // 'M:'
    const result = new Uint8Array(header.length + packed.byteLength);
    result.set(header);
    result.set(new Uint8Array(packed), header.length);
    return result.buffer;
  }

  processIncoming(data: ArrayBuffer): any {
    const view = new Uint8Array(data);

    if (view.length >= 2 && view[0] === 67 && view[1] === 58) {
      const compressed = data.slice(2);
      const decompressed = this.decompress(compressed);
      return this.unpackMsgPack(decompressed);
    }

    if (view.length >= 2 && view[0] === 77 && view[1] === 58) {
      const packed = data.slice(2);
      return this.unpackMsgPack(packed);
    }

    try {
      return this.unpackMsgPack(data);
    } catch {
      try {
        return JSON.parse(new TextDecoder().decode(data));
      } catch (error) {
        logger.error('Failed to process incoming data:', error);
        throw new Error('Unknown message format');
      }
    }
  }
}

// Singleton instance
export const compressionManager = new CompressionManager();
