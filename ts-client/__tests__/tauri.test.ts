// Tauri's package, on a Tauri of the test's: invoke and Channel as Tauri's
// core API gives them, the plugin's commands answered by a bridge owner in
// place of the Rust side's.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage, stamped } from './helpers.js';
import { createBridgeOwner, type BridgeOwner, type OwnerFrame, type WindowFrame } from '../bridge/index.js';
import { createTauriClient, type BridgeClient, type TauriCore } from '../tauri/index.js';

const URL = 'ws://tauri.test/wse';

/** Tauri's core API over `owner`: each command and frame crosses as JSON, a turn later. */
function tauri(owner: BridgeOwner, plugin = 'wse') {
  const windows = new Map<number, { sink: ((frame: WindowFrame) => void) | null; detach: () => void }>();
  const commands: Array<{ command: string; frames?: string[] }> = [];
  let next = 0;
  let gone = false;
  class Channel {
    onmessage: (message: any) => void = () => {};
  }
  const invoke = async <T,>(command: string, args: Record<string, any> = {}): Promise<T> => {
    commands.push({ command, frames: args.frames?.map((f: WindowFrame) => f.k) });
    await new Promise((resolve) => setTimeout(resolve, 1));
    if (gone) throw new Error('no such plugin');
    switch (command) {
      case `plugin:${plugin}|attach`: {
        const channel = args.onFrame as Channel;
        const att = ++next;
        const window: { sink: ((frame: WindowFrame) => void) | null; detach: () => void } = { sink: null, detach: () => {} };
        windows.set(att, window);
        window.detach = owner.attach({
          post: (frame: OwnerFrame) => setTimeout(() => channel.onmessage(JSON.parse(JSON.stringify(frame))), 0),
          listen: (sink) => {
            window.sink = (frame) => sink.frame(frame);
          },
          close: () => {},
        });
        return att as T;
      }
      case `plugin:${plugin}|frames`:
        for (const frame of args.frames as WindowFrame[]) windows.get(args.att)?.sink?.(JSON.parse(JSON.stringify(frame)));
        return undefined as T;
      case `plugin:${plugin}|detach`:
        windows.get(args.att)?.detach();
        windows.delete(args.att);
        return undefined as T;
      default:
        throw new Error(`no command ${command}`);
    }
  };
  const core: TauriCore = { invoke, Channel };
  return {
    core,
    commands,
    goes: () => {
      gone = true;
    },
  };
}

let owners: BridgeOwner[] = [];
let clients: BridgeClient[] = [];

function owner(): BridgeOwner {
  const made = createBridgeOwner({ endpoints: [URL], offline: { enabled: false } });
  owners.push(made);
  return made;
}

function track(client: BridgeClient): BridgeClient {
  clients.push(client);
  return client;
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
  vi.unstubAllGlobals();
});

describe('Tauri', () => {
  it('gives a page the Rust side’s connection through the plugin', async () => {
    const o = owner();
    const { core } = tauri(o);
    const client = track(createTauriClient(core));
    const heard: number[] = [];
    client.onMessage('price_update', (m) => heard.push(m.p.n));
    client.retain();
    client.subscribe(['prices']);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage({ user_id: 'ann' }));
    ws.serverSend({ c: 'WSE', t: 'subscription_update', p: { action: 'subscribe', success: true, success_topics: ['prices'], failed_topics: [] }, v: 1 });
    ws.serverSend(stamped('prices', 1, '0000000a', { n: 9 }));
    await vi.waitFor(() => expect(heard).toEqual([9]));
    expect(client.state.getSnapshot()).toMatchObject({ ready: true, connection: 'connected' });
    expect(client.bridge.phase).toBe('attached');
  });

  it('sends the page’s frames one command at a time, in order, those of a turn together', async () => {
    const o = owner();
    const { core, commands } = tauri(o, 'realtime');
    const client = track(createTauriClient(core, { plugin: 'realtime' }));
    client.onMessage('price_update', () => {});
    client.retain();
    client.subscribe(['prices']);
    await vi.waitFor(() => expect(client.bridge.phase).toBe('attached'));
    const frames = commands.filter((c) => c.command === 'plugin:realtime|frames').flatMap((c) => c.frames);
    expect(frames).toEqual(['hello', 'listen', 'share', 'hold']);
    // The first command carries hello; what came meanwhile goes in the next.
    expect(commands.filter((c) => c.command === 'plugin:realtime|frames').length).toBeLessThanOrEqual(2);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
  });

  it('ends the client when the Rust side takes no more frames', async () => {
    const o = owner();
    const { core, goes } = tauri(o);
    const client = track(createTauriClient(core));
    await vi.waitFor(() => expect(client.bridge.phase).toBe('attached'));
    goes();
    client.retain();
    await vi.waitFor(() => expect(client.bridge.phase).toBe('closed'));
    expect(client.state.getSnapshot().ready).toBe(false);
  });

  it('lets the window go when the page’s client goes', async () => {
    const o = owner();
    const { core, commands } = tauri(o);
    const client = createTauriClient(core);
    client.retain();
    await vi.waitFor(() => expect(o.attached).toBe(1));
    client.destroy();
    await vi.waitFor(() => expect(o.attached).toBe(0));
    expect(commands.some((c) => c.command === 'plugin:wse|detach')).toBe(true);
  });
});

describe('Tauri, its commands in flight', () => {
  it('lets the window go only after the frames it sent, a sign-out among them, went through', async () => {
    const calls: string[] = [];
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    let handler: { onmessage: (message: any) => void } | null = null;
    class Channel {
      onmessage: (message: any) => void = () => {};
      constructor() {
        handler = this;
      }
    }
    const conn = { connection: 'connected', ready: true, paused: [], endpoint: 'ws://localhost/', lastError: null, storage: 'memory' };
    const core: TauriCore = {
      Channel,
      invoke: async <T,>(command: string, args: Record<string, any> = {}): Promise<T> => {
        const frames = (args.frames ?? []) as WindowFrame[];
        calls.push(command.endsWith('|frames') ? `frames:${frames.map((f) => f.k).join(',')}` : command.split('|')[1]);
        if (command.endsWith('|attach')) return 1 as T;
        if (frames.some((f) => f.k === 'hello')) {
          handler!.onmessage({ k: 'welcome', v: 1, att: 1, limits: {}, realm: 'test', account: { epoch: 0, user: 'ann' }, conn, gen: 1 });
        }
        if (frames.some((f) => f.k === 'signOut')) await held;
        return undefined as T;
      },
    };
    const client = createTauriClient(core);
    await vi.waitFor(() => expect(client.bridge.phase).toBe('attached'));
    client.signOut();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(calls).not.toContain('detach');
    release();
    await vi.waitFor(() => expect(calls).toContain('detach'));
    const signOut = calls.findIndex((c) => c.includes('signOut'));
    expect(signOut).toBeGreaterThanOrEqual(0);
    expect(calls.indexOf('detach')).toBeGreaterThan(signOut);
  });
});
