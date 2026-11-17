// =============================================================================
// WebSocket Engine - Electron's main process (wse-client/electron)
// =============================================================================
//
// One connection for every window of an Electron application: the owner
// lives in the main process with the token, the keys and the outbox (a file,
// with fileOutbox from wse-client/node), and the page of each window it
// serves gets a port to it through the preload (wsePreloadPath, or
// wse-client/electron/preload imported by the application's own preload) and
// makes its client with createElectronClient (wse-client/electron/renderer).
// A page attaches only from its window's top frame, at a URL the application
// allows. Nothing of Electron is imported here: the application passes
// ipcMain and MessageChannelMain in.

import { fileURLToPath } from 'node:url';
import { createBridgeOwner, type BridgeOwner, type BridgeOwnerOptions } from '../bridge/owner.js';
import type { OwnerChannel } from '../bridge/channel.js';
import type { OwnerFrame, WindowFrame } from '../bridge/protocol.js';
import { logger } from '../utils/logger.js';

/**
 * The preload's file, for a window's `webPreferences.preload` or a session's
 * preload scripts. A main process the application bundles imports
 * wse-client/electron/preload in its own preload instead.
 */
export const wsePreloadPath: string = fileURLToPath(new URL('./preload.cjs', import.meta.url));

/** Electron's MessagePortMain, as far as the owner uses it. */
export interface MessagePortMainLike {
  postMessage(message: unknown): void;
  on(event: 'message', listener: (event: { data: unknown }) => void): unknown;
  on(event: 'close', listener: () => void): unknown;
  start(): void;
  close(): void;
}

/** A frame of a page (WebFrameMain), as far as the owner looks at it. */
export interface WebFrameMainLike {
  readonly url: string;
  /** null for a window's top frame. */
  readonly parent: unknown;
  postMessage(channel: string, message: unknown, transfer?: MessagePortMainLike[]): void;
}

/** A window's web contents, as far as the owner uses them. */
export interface WebContentsLike {
  readonly id: number;
  on(event: 'destroyed', listener: () => void): unknown;
  removeListener(event: 'destroyed', listener: () => void): unknown;
}

/** An IPC message's event (IpcMainEvent), as far as the owner looks at it. */
export interface IpcEventLike {
  readonly sender: WebContentsLike;
  readonly senderFrame: WebFrameMainLike | null;
}

/** What the application hands in from 'electron'. */
export interface ElectronMain {
  ipcMain: {
    on(channel: 'wse:attach', listener: (event: IpcEventLike, id: unknown) => void): unknown;
    removeListener(channel: 'wse:attach', listener: (event: IpcEventLike, id: unknown) => void): unknown;
  };
  MessageChannelMain: new () => { port1: MessagePortMainLike; port2: MessagePortMainLike };
}

export interface WSEOwnerOptions extends BridgeOwnerOptions {
  /** ipcMain and MessageChannelMain, from 'electron'. */
  electron: ElectronMain;
  /**
   * The token, sent as `Authorization: Bearer` with each connection attempt;
   * a function is asked each time (a token it refreshed). It never leaves
   * the main process. Without it the application's `webSocket` factory
   * authenticates the connection (a Cookie header, say).
   */
  token?: string | (() => string | null | undefined);
  /**
   * Whether a page at `url` may attach. Default: the application's own
   * pages: a file, a scheme of its own (app://), or a development server on
   * this machine (http://localhost); never a page of the web.
   */
  allow?: (url: string) => boolean;
}

export interface WSEOwner extends BridgeOwner {
  /**
   * The window of `contents` may attach: its page asks through the preload
   * and gets a port. The returned function stops serving it; a page already
   * attached is let go.
   */
  serve(contents: WebContentsLike): () => void;
}

const LOOPBACK = new Set(['localhost', '127.0.0.1', '[::1]']);
const WEB = new Set(['data:', 'blob:', 'about:', 'javascript:', 'ws:', 'wss:', 'ftp:']);

/** The application's own pages: a file, a scheme of its own, or a server on this machine. */
function own(url: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (parsed.protocol === 'file:') return true;
  if (parsed.protocol === 'http:' || parsed.protocol === 'https:') return LOOPBACK.has(parsed.hostname);
  return !WEB.has(parsed.protocol);
}

/** The owner's end of a page's port; `gone` once either end closed it. */
function portChannel(port: MessagePortMainLike, gone: () => void): OwnerChannel {
  let closed = false;
  let sink: { frame(frame: WindowFrame): void; close(): void } | null = null;
  port.on('message', (event) => {
    if (!closed) sink?.frame(event.data as WindowFrame);
  });
  port.on('close', () => {
    if (closed) return;
    closed = true;
    gone();
    sink?.close();
  });
  return {
    post(frame: OwnerFrame) {
      if (!closed) port.postMessage(frame);
    },
    listen(next) {
      sink = next;
      port.start();
    },
    close() {
      if (closed) return;
      closed = true;
      gone();
      port.close();
    },
  };
}

/** A window served: the pages attached in it, and the asks answered. */
interface Served {
  attached: Map<string, () => void>;
}

export function createWSEOwner(options: WSEOwnerOptions): WSEOwner {
  const { electron, token, allow = own, ...ownerOptions } = options;
  const bearer = () => (typeof token === 'function' ? token() : token) ?? null;
  const owner = createBridgeOwner({
    ...ownerOptions,
    auth: ownerOptions.auth ?? (token !== undefined ? 'token' : undefined),
    webSocket:
      ownerOptions.webSocket ??
      (token !== undefined
        ? (url: string) => {
            const value = bearer();
            // The main process's WebSocket is Node's, which takes headers.
            const NodeWebSocket = WebSocket as unknown as new (url: string, init?: { headers: Record<string, string> }) => WebSocket;
            return value ? new NodeWebSocket(url, { headers: { Authorization: `Bearer ${value}` } }) : new NodeWebSocket(url);
          }
        : undefined),
  });
  const served = new Map<number, Served>();

  const attach = (event: IpcEventLike, id: unknown) => {
    const window = served.get(event.sender.id);
    const frame = event.senderFrame;
    // A page asks until it is answered: each ask once, from its window's top frame.
    if (!window || !frame || frame.parent !== null || typeof id !== 'string' || window.attached.has(id)) return;
    if (!allow(frame.url)) {
      logger.warn(`[wse] a page at ${frame.url} asked for the connection: not one the application allows`);
      return;
    }
    const { port1, port2 } = new electron.MessageChannelMain();
    window.attached.set(id, () => {});
    const detach = owner.attach(portChannel(port1, () => window.attached.delete(id)));
    if (window.attached.has(id)) window.attached.set(id, detach);
    try {
      frame.postMessage('wse:port', { id }, [port2]);
    } catch (error) {
      // The frame went meanwhile.
      logger.debug('[wse] the page went before its port reached it:', error);
      detach();
    }
  };
  electron.ipcMain.on('wse:attach', attach);

  const letGo = (window: Served) => {
    for (const detach of [...window.attached.values()]) detach();
  };
  const end = () => {
    electron.ipcMain.removeListener('wse:attach', attach);
    for (const window of served.values()) letGo(window);
    served.clear();
  };
  const destroy = owner.destroy.bind(owner);
  const signOut = owner.signOut.bind(owner);

  return Object.assign(owner, {
    serve(contents: WebContentsLike) {
      if (served.has(contents.id)) return () => {};
      const window: Served = { attached: new Map() };
      served.set(contents.id, window);
      const gone = () => {
        contents.removeListener('destroyed', gone);
        if (served.get(contents.id) !== window) return;
        served.delete(contents.id);
        letGo(window);
      };
      contents.on('destroyed', gone);
      return gone;
    },
    destroy() {
      end();
      destroy();
    },
    signOut() {
      end();
      signOut();
    },
  });
}
