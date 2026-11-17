// =============================================================================
// WebSocket Engine - An Electron window's client (wse-client/electron/renderer)
// =============================================================================
//
// The client a page of an Electron window holds: the connection is the main
// process's (createWSEOwner from wse-client/electron), and this client talks
// to it over a port the preload hands the page. It is a WSEClient: the
// adapters for React, Vue, Svelte, Angular and Solid take it unchanged.

import { createBridgeClient, type BridgeClient, type BridgeClientOptions } from '../bridge/client.js';
import { messagePortChannel, type PortLike, type WindowChannel } from '../bridge/channel.js';
import type { OwnerFrame, WindowFrame } from '../bridge/protocol.js';
import { generateId } from '../utils/id.js';

export interface ElectronClientOptions extends BridgeClientOptions {
  /**
   * How long the page waits for the main process's port (default 15 s): the
   * window is served (owner.serve(webContents)) and its preload set, or the
   * client ends and says why.
   */
  attachTimeoutMs?: number;
}

/** The page asks again this often until the main process answers: it may serve the window a moment later. */
const ASK_EVERY_MS = 250;

function attach(timeoutMs: number): Promise<WindowChannel> {
  if (typeof window === 'undefined') {
    return Promise.reject(new Error('createElectronClient runs in a page of an Electron window'));
  }
  const page = window;
  const id = generateId();
  return new Promise((resolve, reject) => {
    let ask: ReturnType<typeof setInterval> | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const done = () => {
      page.removeEventListener('message', answer);
      if (ask) clearInterval(ask);
      if (timer) clearTimeout(timer);
    };
    const answer = (event: MessageEvent) => {
      const data = event.data;
      if (event.source !== page || !data || data.wse !== 'port' || data.id !== id || !event.ports[0]) return;
      done();
      resolve(messagePortChannel<WindowFrame, OwnerFrame>(event.ports[0] as unknown as PortLike));
    };
    page.addEventListener('message', answer);
    const post = () => page.postMessage({ wse: 'attach', id }, '*');
    post();
    ask = setInterval(post, ASK_EVERY_MS);
    timer = setTimeout(() => {
      done();
      reject(
        new Error(
          `no port from the main process in ${timeoutMs} ms: serve the window (owner.serve(webContents)) and give it the preload (wsePreloadPath)`,
        ),
      );
    }, timeoutMs);
  });
}

/** The client of this window's page, over the main process's connection. */
export function createElectronClient(options: ElectronClientOptions = {}): BridgeClient {
  const { attachTimeoutMs = 15_000, ...rest } = options;
  return createBridgeClient(() => attach(attachTimeoutMs), { agent: 'electron', ...rest });
}

export type { BridgeClient, BridgeClientOptions, BridgePhase } from '../bridge/client.js';
