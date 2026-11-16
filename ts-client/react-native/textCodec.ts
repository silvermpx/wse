// =============================================================================
// WebSocket Engine - React Native: TextDecoder and TextEncoder
// =============================================================================
//
// The MessagePack codec makes a TextDecoder and a TextEncoder as it loads;
// Hermes has no TextDecoder before React Native 0.85 (and no TextEncoder
// before 0.74), so loading the client would fail there. UTF-8 ones go in
// before anything else of the client loads, only where the runtime has none.

const g = globalThis as { TextDecoder?: unknown; TextEncoder?: unknown };

/** UTF-8 to a string; what is not UTF-8 reads as U+FFFD, as the WHATWG decoder does. */
class Utf8Decoder {
  readonly encoding = 'utf-8';
  readonly fatal = false;
  readonly ignoreBOM = false;

  decode(input?: ArrayBuffer | ArrayBufferView): string {
    if (!input) return '';
    const bytes =
      input instanceof ArrayBuffer
        ? new Uint8Array(input)
        : new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    const units: number[] = [];
    let out = '';
    let i = 0;
    const n = bytes.length;
    while (i < n) {
      const b = bytes[i];
      let cp = 0xfffd;
      let used = 1;
      if (b < 0x80) {
        cp = b;
      } else if (b >= 0xc2 && b <= 0xdf) {
        if (i + 1 < n && (bytes[i + 1] & 0xc0) === 0x80) {
          cp = ((b & 0x1f) << 6) | (bytes[i + 1] & 0x3f);
          used = 2;
        }
      } else if (b >= 0xe0 && b <= 0xef) {
        // The second byte's range rules out overlong forms and surrogates.
        const lo = b === 0xe0 ? 0xa0 : 0x80;
        const hi = b === 0xed ? 0x9f : 0xbf;
        if (i + 1 < n && bytes[i + 1] >= lo && bytes[i + 1] <= hi) {
          if (i + 2 < n && (bytes[i + 2] & 0xc0) === 0x80) {
            cp = ((b & 0x0f) << 12) | ((bytes[i + 1] & 0x3f) << 6) | (bytes[i + 2] & 0x3f);
            used = 3;
          } else {
            used = 2;
          }
        }
      } else if (b >= 0xf0 && b <= 0xf4) {
        const lo = b === 0xf0 ? 0x90 : 0x80;
        const hi = b === 0xf4 ? 0x8f : 0xbf;
        if (i + 1 < n && bytes[i + 1] >= lo && bytes[i + 1] <= hi) {
          if (i + 2 < n && (bytes[i + 2] & 0xc0) === 0x80) {
            if (i + 3 < n && (bytes[i + 3] & 0xc0) === 0x80) {
              cp =
                ((b & 0x07) << 18) | ((bytes[i + 1] & 0x3f) << 12) | ((bytes[i + 2] & 0x3f) << 6) | (bytes[i + 3] & 0x3f);
              used = 4;
            } else {
              used = 3;
            }
          } else {
            used = 2;
          }
        }
      }
      i += used;
      if (cp > 0xffff) {
        cp -= 0x10000;
        units.push(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
      } else {
        units.push(cp);
      }
      if (units.length >= 8192) {
        out += String.fromCharCode(...units);
        units.length = 0;
      }
    }
    return out + String.fromCharCode(...units);
  }
}

/** A string to UTF-8; a lone surrogate writes as U+FFFD. */
class Utf8Encoder {
  readonly encoding = 'utf-8';

  encode(input = ''): Uint8Array {
    const out = new Uint8Array(input.length * 3);
    return out.slice(0, this.encodeInto(input, out).written);
  }

  /** Writes what fits whole into `destination`: the UTF-16 units read and the bytes written. */
  encodeInto(input: string, destination: Uint8Array): { read: number; written: number } {
    let at = 0;
    let i = 0;
    while (i < input.length) {
      let cp = input.charCodeAt(i);
      let units = 1;
      if (cp >= 0xd800 && cp <= 0xdbff && i + 1 < input.length) {
        const low = input.charCodeAt(i + 1);
        if (low >= 0xdc00 && low <= 0xdfff) {
          cp = 0x10000 + ((cp - 0xd800) << 10) + (low - 0xdc00);
          units = 2;
        }
      }
      if (cp >= 0xd800 && cp <= 0xdfff) cp = 0xfffd;
      const size = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
      if (at + size > destination.length) break;
      if (size === 1) {
        destination[at++] = cp;
      } else if (size === 2) {
        destination[at++] = 0xc0 | (cp >> 6);
        destination[at++] = 0x80 | (cp & 0x3f);
      } else if (size === 3) {
        destination[at++] = 0xe0 | (cp >> 12);
        destination[at++] = 0x80 | ((cp >> 6) & 0x3f);
        destination[at++] = 0x80 | (cp & 0x3f);
      } else {
        destination[at++] = 0xf0 | (cp >> 18);
        destination[at++] = 0x80 | ((cp >> 12) & 0x3f);
        destination[at++] = 0x80 | ((cp >> 6) & 0x3f);
        destination[at++] = 0x80 | (cp & 0x3f);
      }
      i += units;
    }
    return { read: i, written: at };
  }
}

if (typeof g.TextDecoder === 'undefined') g.TextDecoder = Utf8Decoder;
if (typeof g.TextEncoder === 'undefined') g.TextEncoder = Utf8Encoder;

export {};
