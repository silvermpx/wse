// The core entry loads without React, Angular, Solid or zustand, and reads or changes
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
vi.mock('@angular/core', () => {
  throw new Error('wse-client/core imported @angular/core');
});
vi.mock('@angular/common', () => {
  throw new Error('wse-client/core imported @angular/common');
});
vi.mock('solid-js', () => {
  throw new Error('wse-client/core imported solid-js');
});
// Node's own modules are wse-client/node's alone: the core runs in browsers.
for (const name of ['node:fs', 'node:net', 'node:zlib', 'node:crypto', 'node:path']) {
  vi.doMock(name, () => {
    throw new Error(`wse-client/core imported ${name}`);
  });
}

describe('wse-client/core', () => {
  it('imports without React, Angular, Solid, zustand or the page', async () => {
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
