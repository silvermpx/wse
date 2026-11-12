// =============================================================================
// WebSocket Engine - Compression & MessagePack
// =============================================================================

// Namespace import, not default: pako 3 dropped the default export from
// its ESM build — the default form type-checked (esModuleInterop) and
// passed node-side tests, then broke every consumer's vite/esbuild
// bundle with "Missing export" (wellwon caught it on 2.4.2).
import * as pako from 'pako';
import { encode, decode } from '@msgpack/msgpack';
import { logger } from '../utils/logger.js';
import { DEFAULT_MAX_INBOUND_MESSAGE_SIZE } from '../constants.js';

type CompressionLevel = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | -1;

/** Thrown when a compressed frame inflates past the configured size limit. */
export class DecompressedSizeLimitError extends Error {
  constructor(readonly limit: number) {
    super(`Decompressed message exceeds the ${limit}-byte limit`);
    this.name = 'DecompressedSizeLimitError';
  }
}

// Output is produced (and counted) in chunks of this size, so at most
// limit + INFLATE_CHUNK bytes are ever allocated for a hostile frame.
const INFLATE_CHUNK = 64 * 1024;

/**
 * Inflate `input`, aborting as soon as the output passes `limit` bytes.
 * pako.inflate() inflated the whole frame before the size check, so a 1 MiB
 * frame inflating ~1000x made the tab allocate about 1 GiB first.
 */
function inflateWithLimit(input: Uint8Array, limit: number, raw: boolean): Uint8Array {
  const inflator = new pako.Inflate({ raw, chunkSize: INFLATE_CHUNK });
  const chunks: Uint8Array[] = [];
  let total = 0;

  inflator.onData = (chunk: Uint8Array) => {
    total += chunk.length;
    if (total > limit) {
      throw new DecompressedSizeLimitError(limit);
    }
    // pako reuses nothing after onData, but copy defensively when the chunk
    // is a view into a larger buffer.
    chunks.push(chunk.length === chunk.buffer.byteLength ? chunk : chunk.slice());
  };
  inflator.onEnd = () => {
    // Result assembled below; keep pako from concatenating its own copy.
  };

  inflator.push(input, true);
  if (inflator.err) {
    throw new Error(inflator.msg || `inflate failed (${inflator.err})`);
  }
  if (!inflator.ended) {
    throw new Error('inflate failed: truncated stream');
  }

  if (chunks.length === 1) return chunks[0];
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength) as ArrayBuffer;
}

export class CompressionManager {
  private compressionThreshold: number;
  private compressionLevel: CompressionLevel;
  private maxDecompressedSize: number;

  constructor(
    compressionThreshold = 1024,
    compressionLevel: CompressionLevel = 6,
    maxDecompressedSize: number = DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
  ) {
    this.compressionThreshold = compressionThreshold;
    this.compressionLevel = compressionLevel;
    this.maxDecompressedSize = maxDecompressedSize;
  }

  /** Inbound size limit applied while inflating (bytes). */
  setMaxDecompressedSize(limit: number): void {
    this.maxDecompressedSize = limit > 0 ? limit : DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
  }

  getMaxDecompressedSize(): number {
    return this.maxDecompressedSize;
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
      return toArrayBuffer(compressed);
    } catch (error) {
      logger.error('Compression failed:', error);
      throw error;
    }
  }

  /**
   * Inflate zlib, gzip (auto-detected from the header) or raw deflate data.
   * Throws DecompressedSizeLimitError as soon as the output passes the limit.
   */
  decompress(data: ArrayBuffer, maxSize: number = this.maxDecompressedSize): ArrayBuffer {
    const view = new Uint8Array(data);
    try {
      // Default windowBits auto-detects the zlib and gzip wrappers.
      return toArrayBuffer(inflateWithLimit(view, maxSize, false));
    } catch (error) {
      if (error instanceof DecompressedSizeLimitError) {
        logger.warn(error.message);
        throw error;
      }
      // No zlib/gzip header: try raw deflate.
      try {
        return toArrayBuffer(inflateWithLimit(view, maxSize, true));
      } catch (rawError) {
        if (rawError instanceof DecompressedSizeLimitError) {
          logger.warn(rawError.message);
          throw rawError;
        }
        logger.error('Decompression failed:', error);
        throw error;
      }
    }
  }

  decompressGzip(data: ArrayBuffer, maxSize: number = this.maxDecompressedSize): ArrayBuffer {
    try {
      return toArrayBuffer(inflateWithLimit(new Uint8Array(data), maxSize, false));
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
      return toArrayBuffer(encoded);
    } catch (error) {
      logger.error('MessagePack encoding failed:', error);
      throw error;
    }
  }

  unpackMsgPack(data: ArrayBuffer): any {
    try {
      return decode(data);
    } catch (error) {
      logger.debug('MessagePack decoding failed:', error);
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
