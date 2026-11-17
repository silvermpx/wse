// Electron's package, on an Electron of the test's: ipcMain, ipcRenderer,
// MessageChannelMain and a page's window over Node's MessageChannel, with the
// preload file Electron would load, run as Electron runs it.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage, stamped } from './helpers.js';
import { createWSEOwner, wsePreloadPath, type MessagePortMainLike, type WSEOwner } from '../electron/main.js';
import { createElectronClient } from '../electron/renderer.js';
import type { BridgeClient } from '../bridge/client.js';

const URL = 'ws://electron.test/wse';
const PRELOAD = readFileSync(new globalThis.URL('../electron/preload.cjs', import.meta.url), 'utf8');

/** Electron's MessagePortMain over one of Node's ports. */
class PortMain implements MessagePortMainLike {
  constructor(readonly port: MessagePort) {}
  postMessage(message: unknown): void {
    this.port.postMessage(message);
  }
  on(event: 'message' | 'close', listener: (event: any) => void): this {
    if (event === 'message') this.port.addEventListener('message', (e) => listener({ data: (e as MessageEvent).data }));
    else this.port.addEventListener('close', () => listener(undefined));
    return this;
  }
  start(): void {
    this.port.start();
  }
  close(): void {
    this.port.close();
  }
}

class MessageChannelMain {
  readonly port1: PortMain;
  readonly port2: PortMain;
  constructor() {
    const { port1, port2 } = new MessageChannel();
    this.port1 = new PortMain(port1);
    this.port2 = new PortMain(port2);
  }
}

/** A page's window: what the preload and the page share. */
class PageWindow extends EventTarget {
  postMessage(data: unknown, _origin: string, transfer: MessagePort[] = []): void {
    setTimeout(() => {
      const event = new Event('message');
      Object.assign(event, { data: structuredClone(data), source: this, ports: transfer });
      this.dispatchEvent(event);
    }, 0);
  }
}

const ipcMain = new EventEmitter();

/** A window of the application: its contents, its top frame, its page. */
class Window {
  static next = 1;
  readonly contents = Object.assign(new EventEmitter(), { id: Window.next++ });
  readonly renderer = new EventEmitter();
  readonly page = new PageWindow();
  readonly frame: { url: string; parent: unknown; postMessage: (channel: string, message: unknown, transfer?: MessagePortMainLike[]) => void };

  constructor(url = 'file:///app/index.html', parent: unknown = null) {
    this.frame = {
      url,
      parent,
      postMessage: (channel, message, transfer = []) => {
        const ports = transfer.map((port) => (port as PortMain).port);
        setTimeout(() => this.renderer.emit(channel, { ports }, message), 0);
      },
    };
    // The preload, as a sandboxed renderer runs it: Electron's require and the page's window.
    const ipcRenderer = {
      send: (channel: string, ...args: unknown[]) =>
        setTimeout(() => ipcMain.emit(channel, { sender: this.contents, senderFrame: this.frame }, ...args), 0),
      on: (channel: string, listener: (...args: any[]) => void) => this.renderer.on(channel, listener),
    };
    const require = (name: string) => {
      if (name !== 'electron') throw new Error(`a sandboxed preload cannot require ${name}`);
      return { ipcRenderer };
    };
    new Function('require', 'window', PRELOAD)(require, this.page);
  }

  /** A client made in the page. */
  client(options: Parameters<typeof createElectronClient>[0] = {}): BridgeClient {
    vi.stubGlobal('window', this.page);
    try {
      return createElectronClient(options);
    } finally {
      vi.unstubAllGlobals();
      installFakeWebSocket();
    }
  }
}

let owners: WSEOwner[] = [];
let clients: BridgeClient[] = [];

function owner(options: Partial<Parameters<typeof createWSEOwner>[0]> = {}): WSEOwner {
  const made = createWSEOwner({
    electron: { ipcMain: ipcMain as never, MessageChannelMain },
    endpoints: [URL],
    offline: { enabled: false },
    ...options,
  });
  owners.push(made);
  return made;
}

function track(client: BridgeClient): BridgeClient {
  clients.push(client);
  return client;
}

async function serve(ws: FakeWebSocket): Promise<void> {
  ws.serverOpen();
  ws.serverSend(serverReadyMessage({ user_id: 'ann' }));
  ws.serverSend({ c: 'WSE', t: 'subscription_update', p: { action: 'subscribe', success: true, success_topics: ['prices'], failed_topics: [] }, v: 1 });
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  for (const c of clients) c.destroy();
  for (const o of owners) o.destroy();
  clients = [];
  owners = [];
  ipcMain.removeAllListeners();
  vi.unstubAllGlobals();
});

describe('Electron', () => {
  it('gives a served window’s page the main process’s connection through the preload', async () => {
    const o = owner();
    const win = new Window();
    o.serve(win.contents);
    const client = track(win.client());
    const heard: number[] = [];
    client.onMessage('price_update', (m) => heard.push(m.p.n));
    client.retain();
    client.subscribe(['prices']);
    await vi.waitFor(() => expect(client.bridge.phase).toBe('attached'));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    ws.serverSend(stamped('prices', 1, '0000000a', { n: 4 }));
    await vi.waitFor(() => expect(heard).toEqual([4]));
    expect(client.state.getSnapshot()).toMatchObject({ ready: true, connection: 'connected', endpoint: URL });
    expect(o.attached).toBe(1);
  });

  it('sends the token from the main process alone, a refreshed one at each attempt', async () => {
    let token = 'first';
    const o = owner({ token: () => token });
    o.client.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    expect(FakeWebSocket.last().init).toEqual({ headers: { Authorization: 'Bearer first' } });
    token = 'second';
    o.client.reconnect();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    expect(FakeWebSocket.last().init).toEqual({ headers: { Authorization: 'Bearer second' } });
  });

  it('answers a page that asked before its window was served, once', async () => {
    const o = owner();
    const win = new Window();
    const client = track(win.client());
    await new Promise((resolve) => setTimeout(resolve, 600));
    expect(client.bridge.phase).toBe('attaching');
    o.serve(win.contents);
    await vi.waitFor(() => expect(client.bridge.phase).toBe('attached'));
    await new Promise((resolve) => setTimeout(resolve, 600));
    expect(o.attached).toBe(1);
  });

  it('answers no page of the web, no frame inside a page, and no window it does not serve', async () => {
    const o = owner();
    const web = new Window('https://example.com/');
    const inner = new Window('file:///app/index.html', {});
    const stranger = new Window();
    o.serve(web.contents);
    o.serve(inner.contents);
    const clients = [web, inner, stranger].map((w) => track(w.client({ attachTimeoutMs: 700 })));
    await vi.waitFor(() => expect(clients.map((c) => c.bridge.phase)).toEqual(['closed', 'closed', 'closed']), {
      timeout: 2_000,
    });
    expect(clients[0].bridge.reason).toMatch(/no port from the main process/);
    expect(o.attached).toBe(0);
  });

  it('lets go of a page that went, and of every page of a window that closed', async () => {
    const o = owner();
    const win = new Window('app://main/index.html');
    o.serve(win.contents);
    const first = track(win.client());
    const second = track(win.client());
    first.retain();
    await vi.waitFor(() => expect(o.attached).toBe(2));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    // The first page's port goes with it, as when its document unloads.
    first.destroy();
    await vi.waitFor(() => expect(o.attached).toBe(1));
    await vi.waitFor(() => expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0));
    // The window closes: its contents are destroyed.
    win.contents.emit('destroyed');
    await vi.waitFor(() => expect(o.attached).toBe(0));
    await vi.waitFor(() => expect(second.bridge.phase).toBe('closed'));
  });

  it('ships its preload beside the main process’s module', () => {
    expect(wsePreloadPath.endsWith('preload.cjs')).toBe(true);
    expect(readFileSync(wsePreloadPath, 'utf8')).toBe(PRELOAD);
  });
});
