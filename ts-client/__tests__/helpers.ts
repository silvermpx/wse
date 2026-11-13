// Test harness: a controllable WebSocket double and helpers that play the
// server side of the WSE protocol (including the real ECDH + AES-GCM key
// exchange, so encryption tests exercise the production crypto code).

import { vi } from 'vitest';
import { destroyCurrentSession } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { E2E_SUITE, E2eReceiver, E2eSender, keyId, transcript } from '../utils/e2eSession.js';

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

// ---- the server side of an E2E session (mirrors e2e_session.rs) ------------

function raw(text: string): Uint8Array<ArrayBuffer> {
  const bin = atob(text);
  const out = new Uint8Array(new ArrayBuffer(bin.length));
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function hkdf(th: Uint8Array<ArrayBuffer>, ikm: Uint8Array<ArrayBuffer>, label: string): Promise<Uint8Array<ArrayBuffer>> {
  const subtle = globalThis.crypto.subtle;
  const key = await subtle.importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
  const bits = await subtle.deriveBits({ name: 'HKDF', hash: 'SHA-256', salt: th, info: new TextEncoder().encode(label) }, key, 256);
  return new Uint8Array(bits);
}

/** A server identity key pair: what e2e_identity_key holds on the server. */
export interface SessionIdentity {
  publicKeyB64: string;
  publicKey: Uint8Array<ArrayBuffer>;
  sign(data: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>>;
}

export async function sessionIdentity(): Promise<SessionIdentity> {
  const subtle = globalThis.crypto.subtle;
  const pair = await subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']);
  const publicKey = new Uint8Array(await subtle.exportKey('raw', pair.publicKey));
  return {
    publicKeyB64: b64(publicKey.buffer),
    publicKey,
    // WebCrypto signs ECDSA as raw r||s: the wire format.
    sign: async (data) => new Uint8Array(await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, pair.privateKey, data)),
  };
}

export interface SessionServer {
  /** server_hello.p.e2e */
  hello: Record<string, unknown>;
  sender: E2eSender;
  /** A sealed binary frame carrying `data` (JSON text, or raw bytes). */
  seal(data: string | Uint8Array<ArrayBuffer>): Promise<ArrayBuffer>;
  /** The client's next sealed frame, opened (null: its close notice). */
  open(frame: ArrayBuffer): Promise<string | null>;
}

/** Answer the client's offer (client_hello.p.e2e) like the server; `identity` null: unauthenticated. */
export async function sessionServer(offer: any, connectionId: string, identity: SessionIdentity | null): Promise<SessionServer> {
  const subtle = globalThis.crypto.subtle;
  const pair = await subtle.generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']);
  const serverPublic = new Uint8Array(await subtle.exportKey('raw', pair.publicKey));
  const clientPublic = raw(offer.client_public_key);
  const kidHex = identity ? await keyId(identity.publicKey) : '0000000000000000';
  const kid = new Uint8Array(new ArrayBuffer(8));
  for (let i = 0; i < 8; i++) kid[i] = parseInt(kidHex.slice(2 * i, 2 * i + 2), 16);
  const t = transcript(offer.suites, E2E_SUITE, raw(offer.client_random), clientPublic, serverPublic, connectionId, kid);
  const peer = await subtle.importKey('raw', clientPublic, { name: 'ECDH', namedCurve: 'P-256' }, false, []);
  const z = new Uint8Array(await subtle.deriveBits({ name: 'ECDH', public: peer }, pair.privateKey, 256));
  const th = new Uint8Array(await subtle.digest('SHA-256', t));
  const sender = await new E2eSender(th, 0x02, await hkdf(th, z, 'wse/e2e s2c')).init();
  const receiver = await new E2eReceiver(th, 0x01, await hkdf(th, z, 'wse/e2e c2s')).init();
  return {
    hello: {
      suite: E2E_SUITE,
      server_public_key: b64(serverPublic.buffer),
      key_id: kidHex,
      alg: identity ? 'ES256' : null,
      signature: identity ? b64((await identity.sign(t)).buffer) : null,
    },
    sender,
    async seal(data) {
      const body = typeof data === 'string' ? new TextEncoder().encode(data) : data;
      return (await sender.seal(body as Uint8Array<ArrayBuffer>)).buffer;
    },
    async open(frame) {
      const plaintext = await receiver.open(new Uint8Array(frame));
      return plaintext === null ? null : new TextDecoder().decode(plaintext);
    },
  };
}
