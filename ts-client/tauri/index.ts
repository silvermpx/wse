// =============================================================================
// WebSocket Engine - A Tauri window's client (wse-client/tauri)
// =============================================================================
//
// The client a page of a Tauri window holds: the connection is the Rust
// side's (the tauri-plugin-wse crate, an owner of wse_client::bridge), and
// this client talks to it through the plugin's commands and a Channel. It is
// a WSEClient: the adapters for React, Vue, Svelte, Angular and Solid take it
// unchanged. Nothing of Tauri is imported here: the application passes
// invoke and Channel in (from @tauri-apps/api/core, or window.__TAURI__.core).

import { createBridgeClient, type BridgeClient, type BridgeClientOptions } from '../bridge/client.js';
import type { WindowChannel } from '../bridge/channel.js';
import type { OwnerFrame, WindowFrame } from '../bridge/protocol.js';
import { logger } from '../utils/logger.js';

/** What the application hands in from Tauri's core API. */
export interface TauriCore {
  invoke<T>(command: string, args?: Record<string, unknown>): Promise<T>;
  Channel: new () => { onmessage: (message: any) => void };
}

export interface TauriClientOptions extends BridgeClientOptions {
  /** The plugin's name, as the application registered it (default 'wse'). */
  plugin?: string;
}

/**
 * The page's channel to the Rust side: frames it sends go one command at a
 * time, in order, those posted meanwhile together in the next; frames the
 * owner sends come on a Channel.
 */
async function attach(tauri: TauriCore, plugin: string): Promise<WindowChannel> {
  const channel = new tauri.Channel();
  let sink: { frame(frame: OwnerFrame): void; close(): void } | null = null;
  let early: OwnerFrame[] = [];
  let closed = false;
  channel.onmessage = (frame: OwnerFrame) => {
    if (closed) return;
    if (sink) sink.frame(frame);
    else early.push(frame);
  };
  const att = await tauri.invoke<number>(`plugin:${plugin}|attach`, { onFrame: channel });
  let pending: WindowFrame[] = [];
  let sending = false;
  const gone = (error: unknown) => {
    if (closed) return;
    closed = true;
    logger.error('[wse] the Rust side took no more frames:', error);
    sink?.close();
  };
  const flush = () => {
    if (sending || closed || pending.length === 0) return;
    sending = true;
    const frames = pending;
    pending = [];
    tauri.invoke(`plugin:${plugin}|frames`, { att, frames }).then(
      () => {
        sending = false;
        flush();
      },
      (error) => {
        sending = false;
        gone(error);
      },
    );
  };
  return {
    post(frame) {
      if (closed) return;
      pending.push(frame);
      flush();
    },
    listen(next) {
      sink = next;
      const queued = early;
      early = [];
      for (const frame of queued) next.frame(frame);
    },
    close() {
      if (closed) return;
      // What was posted goes first (a bye among it), then the window is let go.
      const last = pending;
      pending = [];
      closed = true;
      const detach = () => tauri.invoke(`plugin:${plugin}|detach`, { att }).catch(() => {});
      if (last.length > 0) void tauri.invoke(`plugin:${plugin}|frames`, { att, frames: last }).finally(detach);
      else void detach();
    },
  };
}

/** The client of this window's page, over the Rust side's connection. */
export function createTauriClient(tauri: TauriCore, options: TauriClientOptions = {}): BridgeClient {
  const { plugin = 'wse', ...rest } = options;
  return createBridgeClient(() => attach(tauri, plugin), { agent: 'tauri', ...rest });
}

export type { BridgeClient, BridgeClientOptions, BridgePhase } from '../bridge/client.js';
