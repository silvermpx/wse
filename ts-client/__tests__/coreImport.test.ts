// The core entry loads without React or zustand, and reads or changes
// nothing of the page on import.

import { describe, expect, it, vi } from 'vitest';

vi.mock('react', () => {
  throw new Error('wse-client/core imported react');
});
vi.mock('zustand', () => {
  throw new Error('wse-client/core imported zustand');
});
vi.mock('zustand/middleware', () => {
  throw new Error('wse-client/core imported zustand/middleware');
});

describe('wse-client/core', () => {
  it('imports without React, zustand or the page', async () => {
    const touched: string[] = [];
    const page = new Proxy(
      {},
      {
        get(_target, key) {
          touched.push(String(key));
          return undefined;
        },
      },
    );
    vi.stubGlobal('window', page);
    vi.stubGlobal('document', page);
    vi.stubGlobal('localStorage', page);
    vi.stubGlobal('indexedDB', page);
    try {
      const core = await import('../core/index.js');
      expect(typeof core.createWSEClient).toBe('function');
      const client = core.createWSEClient({ endpoints: ['ws://x.test/wse'] });
      expect(client.state.getSnapshot().connection).toBe('pending');
      client.destroy();
      expect(touched).toEqual([]);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
