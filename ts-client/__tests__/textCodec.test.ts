// A React Native runtime without TextDecoder or TextEncoder (Hermes before
// React Native 0.85 has no TextDecoder): the React Native entry installs
// UTF-8 ones before the MessagePack codec makes its own at load, and leaves
// a runtime that has them alone.

import { afterEach, describe, expect, it, vi } from 'vitest';

const NodeDecoder = globalThis.TextDecoder;
const NodeEncoder = globalThis.TextEncoder;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
});

const samples = ['', 'ascii', 'grüße', 'Привет', '漢字かな', 'emoji 🙂👍🏽', 'mixed ä€😀z'];

describe('a runtime without TextDecoder and TextEncoder', () => {
  it('gets UTF-8 ones that read and write as the platform does', async () => {
    vi.stubGlobal('TextDecoder', undefined);
    vi.stubGlobal('TextEncoder', undefined);
    await import('../react-native/textCodec.js');
    const decoder = new globalThis.TextDecoder();
    const encoder = new globalThis.TextEncoder();
    for (const text of samples) {
      const bytes = new NodeEncoder().encode(text);
      expect(encoder.encode(text)).toEqual(bytes);
      expect(decoder.decode(bytes)).toBe(text);
      expect(decoder.decode(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength))).toBe(text);
    }
    // What does not fit whole is left for the next call.
    expect(encoder.encodeInto('aé😀', new Uint8Array(4))).toEqual({ read: 2, written: 3 });
    expect(encoder.encodeInto('aé😀', new Uint8Array(7))).toEqual({ read: 4, written: 7 });
    // What is not UTF-8 reads as the replacement character, as the platform's does.
    for (const bad of [[0xff], [0xc3], [0xe2, 0x82], [0xf0, 0x9f, 0x98], [0xc0, 0x80], [0xed, 0xa0, 0x80]]) {
      const bytes = new Uint8Array(bad);
      expect(decoder.decode(bytes)).toBe(new NodeDecoder().decode(bytes));
    }
  });

  it('loads the client with them', async () => {
    vi.stubGlobal('TextDecoder', undefined);
    vi.stubGlobal('TextEncoder', undefined);
    const entry = await import('../react-native/index.js');
    expect(typeof entry.createReactNativeClient).toBe('function');
  });

  it('keeps the runtime’s own when it has them', async () => {
    await import('../react-native/textCodec.js');
    expect(globalThis.TextDecoder).toBe(NodeDecoder);
    expect(globalThis.TextEncoder).toBe(NodeEncoder);
  });
});
