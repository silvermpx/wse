// @vitest-environment happy-dom
//
// Unit-level checks for helpers changed in 3.0.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import * as pako from 'pako';
import { generateId } from '../utils/id.js';
import { Logger, LogLevel, ERROR_LOG_STORAGE_KEY, defaultLogLevel } from '../utils/logger.js';
import {
  DEFAULT_TOPICS,
  SUBSCRIPTION_STORAGE_KEY,
  WS_CLIENT_VERSION,
  clearWSEStorage,
  getSavedSubscriptions,
  loadSavedSubscriptions,
  saveSubscriptions,
} from '../constants.js';
import { CompressionManager, DecompressedSizeLimitError } from '../protocols/compression.js';
import { EventSequencer } from '../services/EventSequencer.js';
import { resolveOptions } from '../services/WSESession.js';
import * as api from '../index.js';

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  localStorage.clear();
  sessionStorage.clear();
});

describe('generateId', () => {
  it('uses crypto.randomUUID when present', () => {
    expect(generateId()).toMatch(UUID_V4);
  });

  it('falls back to getRandomValues outside secure contexts', () => {
    const real = globalThis.crypto;
    vi.stubGlobal('crypto', { getRandomValues: (a: Uint8Array) => real.getRandomValues(a) });
    const ids = new Set(Array.from({ length: 200 }, () => generateId()));
    expect(ids.size).toBe(200);
    for (const id of ids) expect(id).toMatch(UUID_V4);
  });

  it('still works without any Web Crypto', () => {
    vi.stubGlobal('crypto', undefined);
    expect(generateId()).toMatch(UUID_V4);
  });
});

describe('logger defaults', () => {
  it('defaults to WARN outside development and INFO in development', () => {
    // (Vite inlines import.meta.env in tests, so the detection itself is
    // checked against the built package in plain Node -- see the report.)
    expect(defaultLogLevel(false)).toBe(LogLevel.WARN);
    expect(defaultLogLevel(true)).toBe(LogLevel.INFO);
  });

  it('honours WSE_LOG_LEVEL', () => {
    vi.stubEnv('WSE_LOG_LEVEL', 'ERROR');
    vi.stubEnv('VITE_LOG_LEVEL', '');
    expect(new Logger().getLevel()).toBe(LogLevel.ERROR);
  });

  it('does not persist errors unless asked, and never persists their data', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const log = new Logger('T');
    log.error('send failed', { payload: 'card 4111 1111 1111 1111' });
    expect(localStorage.getItem(ERROR_LOG_STORAGE_KEY)).toBeNull();

    log.setPersistErrors(true);
    log.error('send failed', { payload: 'card 4111 1111 1111 1111' });
    const stored = localStorage.getItem(ERROR_LOG_STORAGE_KEY)!;
    expect(stored).toContain('send failed');
    expect(stored).not.toContain('4111');

    clearWSEStorage();
    expect(localStorage.getItem(ERROR_LOG_STORAGE_KEY)).toBeNull();
  });
});

describe('saved subscriptions', () => {
  it('ignores malformed or foreign data instead of crashing', () => {
    localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, '{not json');
    expect(loadSavedSubscriptions('alice')).toBeNull();
    expect(getSavedSubscriptions('alice')).toEqual(DEFAULT_TOPICS);

    localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, '42');
    expect(loadSavedSubscriptions(null)).toBeNull();

    localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, JSON.stringify({ userId: 'alice', topics: ['a', 7, null, 'b'] }));
    expect(loadSavedSubscriptions('alice')).toEqual(['a', 'b']);
    expect(loadSavedSubscriptions('bob')).toBeNull();
  });

  it('keys saved topics by user and treats the 2.x array format as anonymous', () => {
    saveSubscriptions(['x'], 'alice');
    expect(loadSavedSubscriptions('alice')).toEqual(['x']);
    expect(loadSavedSubscriptions(null)).toBeNull();

    localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, JSON.stringify(['legacy']));
    expect(loadSavedSubscriptions(null)).toEqual(['legacy']);
    expect(loadSavedSubscriptions('alice')).toBeNull();
  });

  it('clearWSEStorage removes every wse_ key from local and session storage', () => {
    localStorage.setItem('wse_subscriptions', '[]');
    localStorage.setItem('other_app', 'keep');
    sessionStorage.setItem('wse_debug_log', '[]');
    clearWSEStorage();
    expect(localStorage.getItem('wse_subscriptions')).toBeNull();
    expect(sessionStorage.getItem('wse_debug_log')).toBeNull();
    expect(localStorage.getItem('other_app')).toBe('keep');
  });
});

describe('CompressionManager limits', () => {
  it('inflates zlib, gzip and raw deflate data', () => {
    const text = new TextEncoder().encode('hello '.repeat(100));
    const cm = new CompressionManager();
    for (const packed of [pako.deflate(text), pako.gzip(text), pako.deflateRaw(text)]) {
      const out = cm.decompress(packed.buffer.slice(packed.byteOffset, packed.byteOffset + packed.byteLength) as ArrayBuffer);
      expect(new TextDecoder().decode(out)).toBe('hello '.repeat(100));
    }
  });

  it('stops inflating as soon as the output passes the limit', () => {
    const bomb = pako.deflate(new Uint8Array(32 * 1024 * 1024));
    const cm = new CompressionManager(1024, 6, 256 * 1024);
    const buffer = bomb.buffer.slice(bomb.byteOffset, bomb.byteOffset + bomb.byteLength) as ArrayBuffer;
    expect(() => cm.decompress(buffer)).toThrow(DecompressedSizeLimitError);
  });
});

describe('EventSequencer positions', () => {
  it('exposes and clears per-topic anchors', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('a', '01', 1);
    s.checkTopicStamp('b', '01', 5);
    expect(s.getTopicPositions(['a', 'c'])).toEqual({ a: { epoch: '01', offset: 1 } });
    s.clearTopics(['a']);
    expect(s.getTopicPositions()).toEqual({ b: { epoch: '01', offset: 5 } });
  });
});

describe('options and versions', () => {
  it('defaults to infinite reconnection and sane limits', () => {
    const o = resolveOptions({ endpoints: ['ws://x/wse'] });
    expect(o.reconnection.maxAttempts).toBe(-1);
    expect(o.maxMessageSize).toBe(16 * 1024 * 1024);
    expect(o.rateLimit).toEqual({});
    expect(resolveOptions({ endpoints: ['ws://x/wse'], rateLimit: false }).rateLimit).toBe(false);
    expect(o.diagnostics).toEqual({ enabled: false, metricsInterval: 60000 });
  });

  it('reports the package version everywhere', () => {
    const pkg = JSON.parse(readFileSync(join(process.cwd(), 'package.json'), 'utf8'));
    expect(WS_CLIENT_VERSION).toBe(pkg.version);
    expect(api.WSE_VERSION).toBe(pkg.version);
    expect(pkg.type).toBe('module');
  });

  it('no longer exports the removed AdaptiveQualityManager', () => {
    expect((api as Record<string, unknown>).AdaptiveQualityManager).toBeUndefined();
    expect(typeof api.useWSE).toBe('function');
  });
});
