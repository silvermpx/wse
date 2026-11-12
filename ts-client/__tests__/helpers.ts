// Test harness: a controllable WebSocket double and helpers that play the
// server side of the WSE protocol (including the real ECDH + AES-GCM key
// exchange, so encryption tests exercise the production crypto code).

import { vi } from 'vitest';
import { destroyCurrentSession } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';

export const ENDPOINT = 'ws://wse.test/wse';

type Frame = string | ArrayBuffer;

export class FakeWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  static instances: FakeWebSocket[] = [];
  /** Answer the client's heartbeat ping with a PONG, like the Rust server. */
  static autoPong = true;

  readonly url: string;
  readyState = FakeWebSocket.CONNECTING;
  binaryType = 'blob';
  sent: Frame[] = [];
  closeCalls: Array<{ code?: number; reason?: string }> = [];

  onopen: ((ev: any) => void) | null = null;
  onmessage: ((ev: any) => void) | null = null;
  onerror: ((ev: any) => void) | null = null;
  onclose: ((ev: any) => void) | null = null;

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  static reset(): void {
    FakeWebSocket.instances = [];
    FakeWebSocket.autoPong = true;
  }

  static last(): FakeWebSocket {
    const ws = FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
    if (!ws) throw new Error('no WebSocket was created');
    return ws;
  }

  send(data: Frame): void {
    if (this.readyState !== FakeWebSocket.OPEN) {
      throw new Error('InvalidStateError: socket not open');
    }
    this.sent.push(data);
    if (FakeWebSocket.autoPong && typeof data === 'string') {
      try {
        const msg = JSON.parse(data);
        if (msg.t === 'ping') {
          queueMicrotask(() => {
            if (this.readyState === FakeWebSocket.OPEN) {
              this.serverSend({
                c: 'WSE',
                t: 'PONG',
                p: { client_timestamp: msg.p?.timestamp ?? 0, server_timestamp: Date.now() },
                v: 1,
              });
            }
          });
        }
      } catch {
        // not JSON
      }
    }
  }

  /** Client-initiated close: like a browser, the close event comes later (finishClose()). */
  close(code?: number, reason?: string): void {
    this.closeCalls.push({ code, reason });
    if (this.readyState === FakeWebSocket.CLOSED) return;
    this.readyState = FakeWebSocket.CLOSING;
  }

  /** Deliver the close event of a client-initiated close to whatever handler is attached now. */
  finishClose(code = 1000, reason = ''): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.({ code, reason, wasClean: true });
  }

  // ---- server side ---------------------------------------------------------

  serverOpen(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.({});
  }

  serverSend(data: Frame | Record<string, unknown>): void {
    const payload = typeof data === 'string' || data instanceof ArrayBuffer ? data : JSON.stringify(data);
    this.onmessage?.({ data: payload });
  }

  serverClose(code = 1006, reason = ''): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.({ code, reason, wasClean: code === 1000 });
  }

  serverError(): void {
    this.onerror?.({});
  }

  /** Text frames the client sent, parsed. */
  textMessages(): any[] {
    return this.sent
      .filter((f): f is string => typeof f === 'string')
      .map((f) => {
        try {
          return JSON.parse(f);
        } catch {
          return { raw: f };
        }
      });
  }

  /** Parsed text frames of type `t`. */
  messagesOfType(t: string): any[] {
    return this.textMessages().filter((m) => m.t === t);
  }

  /** Application frames: everything except protocol control frames. */
  appTextMessages(): any[] {
    return this.textMessages().filter((m) => !['ping', 'PONG', 'client_hello'].includes(m.t));
  }

  binaryFrames(): ArrayBuffer[] {
    return this.sent.filter((f): f is ArrayBuffer => typeof f !== 'string');
  }
}

export function serverReadyMessage(details: Record<string, unknown> = {}) {
  return {
    c: 'WSE',
    t: 'server_ready',
    id: `sr-${Math.random()}`,
    p: {
      message: 'Connection established (Rust transport)',
      details: {
        version: 1,
        features: { recovery: true, encryption: false },
        connection_id: 'conn-1',
        user_id: 'user-1',
        ...details,
      },
    },
    v: 1,
  };
}

export function serverHelloMessage(extra: Record<string, unknown> = {}) {
  return {
    c: 'WSE',
    t: 'server_hello',
    p: {
      server_version: 'test',
      protocol_version: 1,
      features: { compression: true, recovery: true, encryption: false },
      limits: { max_message_size: 1048576, rate_limit_capacity: 100000, rate_limit_refill: 10000 },
      ping_interval: 25000,
      connection_id: 'conn-1',
      ...extra,
    },
    v: 1,
  };
}

/** A stamped topic publication as the server fans it out. */
export function stamped(topic: string, offset: number, epoch = '0000000a', payload: Record<string, unknown> = {}) {
  return { c: 'U', t: 'price_update', tp: topic, e: epoch, o: offset, p: { offset, ...payload }, v: 1 };
}

/** Let pending promise chains (and zero-delay timers) run. */
export async function tick(times = 3): Promise<void> {
  for (let i = 0; i < times; i++) {
    if (vi.isFakeTimers()) {
      await vi.advanceTimersByTimeAsync(0);
    } else {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }
}

/** Open the socket and complete server_ready (and client_hello -> server_hello). */
export async function connectServer(ws: FakeWebSocket, readyDetails: Record<string, unknown> = {}): Promise<void> {
  ws.serverOpen();
  await tick();
  ws.serverSend(serverReadyMessage(readyDetails));
  await tick();
}

export function resetClientState(): void {
  destroyCurrentSession({ clearUserData: true });
  useWSEStore.getState().reset();
  useMessageQueueStore.getState().clearQueue();
  useMessageQueueStore.getState().setCapacity(10000);
  FakeWebSocket.reset();
}

export function installFakeWebSocket(): void {
  vi.stubGlobal('WebSocket', FakeWebSocket as unknown as typeof WebSocket);
}

// ---- server-side crypto (mirrors server.rs derive_connection_key) ---------

function b64(bytes: ArrayBuffer): string {
  let s = '';
  const view = new Uint8Array(bytes);
  for (let i = 0; i < view.length; i++) s += String.fromCharCode(view[i]);
  return btoa(s);
}

function unb64(text: string): Uint8Array {
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export interface ServerCipher {
  publicKeyB64: string;
  encrypt(text: string): Promise<ArrayBuffer>;
  decrypt(frame: ArrayBuffer): Promise<string>;
}

/** Derive the AES key the way the server does from the client's client_hello key. */
export async function serverKeyExchange(clientPublicKeyB64: string): Promise<ServerCipher> {
  const subtle = globalThis.crypto.subtle;
  const pair = await subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']);
  const clientKey = await subtle.importKey(
    'raw', unb64(clientPublicKeyB64) as BufferSource, { name: 'ECDH', namedCurve: 'P-256' }, false, []
  );
  const bits = await subtle.deriveBits({ name: 'ECDH', public: clientKey }, pair.privateKey, 256);
  const hkdf = await subtle.importKey('raw', bits, 'HKDF', false, ['deriveKey']);
  const aes = await subtle.deriveKey(
    { name: 'HKDF', hash: 'SHA-256', salt: new Uint8Array(0), info: new TextEncoder().encode('wse-encryption/aes-gcm-key') },
    hkdf,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  );
  const publicKeyB64 = b64(await subtle.exportKey('raw', pair.publicKey));

  return {
    publicKeyB64,
    async encrypt(text: string) {
      const iv = globalThis.crypto.getRandomValues(new Uint8Array(12));
      const ct = await subtle.encrypt({ name: 'AES-GCM', iv }, aes, new TextEncoder().encode(text));
      const out = new Uint8Array(2 + 12 + ct.byteLength);
      out.set([69, 58], 0);
      out.set(iv, 2);
      out.set(new Uint8Array(ct), 14);
      return out.buffer;
    },
    async decrypt(frame: ArrayBuffer) {
      const view = new Uint8Array(frame);
      if (view[0] !== 69 || view[1] !== 58) throw new Error('not an E: frame');
      const pt = await subtle.decrypt({ name: 'AES-GCM', iv: view.slice(2, 14) }, aes, view.slice(14));
      return new TextDecoder().decode(pt);
    },
  };
}
