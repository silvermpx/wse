// =============================================================================
// WSE end-to-end encryption with an authenticated server: the client's side
// =============================================================================
//
// The client offers its suites, an ephemeral P-256 key and 32 random bytes in
// client_hello.p.e2e. The server answers in server_hello.p.e2e with its own
// ephemeral key and an ECDSA P-256 signature over the exchange, made with its
// identity key; a client pinning that key knows no one in between answered.
// Both sides derive one key schedule per direction, bound to the exchange,
// and every frame carries its direction's epoch and sequence number: only the
// next frame opens, so replayed, reordered, dropped or foreign frames end the
// connection.
//
// Frame: "E:" | 0x01 | type | epoch u32be | seq u64be | ciphertext | tag.
// =============================================================================

export const E2E_SUITE = 'p256-aes256gcm';
export const E2E_SIGNATURE_ALG = 'ES256';

export const E2E_CLOSE_UNSUPPORTED = 4800;
export const E2E_CLOSE_SERVER_REJECTED = 4801;
export const E2E_CLOSE_BAD_FRAME = 4802;
export const E2E_CLOSE_EXHAUSTED = 4803;

const LABEL = new TextEncoder().encode('WSE-E2E server_hello\0');
const FORMAT = 0x01;
const FRAME_DATA = 0;
const FRAME_CLOSE = 1;
const HEADER_LEN = 14;
const TAG_LEN = 16;
const SEQ_MAX = Number.MAX_SAFE_INTEGER; // 2^53 - 1
const REKEY_FRAMES = 2 ** 20;
const REKEY_AFTER_MS = 600_000;
const MAX_EPOCH_FRAMES = 2 ** 24;
const C2S = 0x01;
const S2C = 0x02;

export class E2eError extends Error {
  constructor(message: string, readonly closeCode: number) {
    super(message);
    this.name = 'E2eError';
  }
}

const subtle = (): SubtleCrypto => {
  const s = globalThis.crypto?.subtle;
  if (!s) throw new E2eError('WebCrypto is not available (HTTPS or localhost only)', E2E_CLOSE_UNSUPPORTED);
  return s;
};

const bytes = (n: number): Uint8Array<ArrayBuffer> => new Uint8Array(new ArrayBuffer(n));

function concat(...parts: Uint8Array[]): Uint8Array<ArrayBuffer> {
  const out = bytes(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function b64decode(s: string): Uint8Array<ArrayBuffer> {
  const bin = atob(s);
  const out = bytes(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function b64encode(b: Uint8Array): string {
  let s = '';
  for (const x of b) s += String.fromCharCode(x);
  return btoa(s);
}

function hex(b: Uint8Array): string {
  return Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
}

function header(type: number, epoch: number, seq: number): Uint8Array<ArrayBuffer> {
  const h = bytes(HEADER_LEN);
  const v = new DataView(h.buffer);
  v.setUint8(0, FORMAT);
  v.setUint8(1, type);
  v.setUint32(2, epoch);
  v.setUint32(6, Math.floor(seq / 2 ** 32));
  v.setUint32(10, seq >>> 0);
  return h;
}

function nonce(iv: Uint8Array, seq: number): Uint8Array<ArrayBuffer> {
  const n = bytes(12);
  n.set(iv, 0);
  const v = new DataView(n.buffer);
  v.setUint32(4, Math.floor(seq / 2 ** 32));
  v.setUint32(8, seq >>> 0);
  return n;
}

async function sha256(data: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
  return new Uint8Array(await subtle().digest('SHA-256', data));
}

/** The id of an identity public key (65-byte P-256 point): 8 bytes of its SHA-256, hex. */
export async function keyId(publicKey: Uint8Array<ArrayBuffer>): Promise<string> {
  return hex((await sha256(publicKey)).subarray(0, 8));
}

/** The bytes the server signs and both sides hash into the session. */
export function transcript(
  suites: string[],
  chosen: string,
  clientRandom: Uint8Array,
  clientPublic: Uint8Array,
  serverPublic: Uint8Array,
  connectionId: string,
  kid: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const enc = new TextEncoder();
  const parts: Uint8Array[] = [LABEL, Uint8Array.of(suites.length)];
  for (const s of suites) {
    const b = enc.encode(s);
    parts.push(Uint8Array.of(b.length), b);
  }
  const c = enc.encode(chosen);
  const id = enc.encode(connectionId).subarray(0, 255);
  parts.push(Uint8Array.of(c.length), c, clientRandom, clientPublic, serverPublic, Uint8Array.of(id.length), id, kid);
  return concat(...parts);
}

async function derive(th: Uint8Array<ArrayBuffer>, ikm: Uint8Array<ArrayBuffer>, label: string, length: number): Promise<Uint8Array<ArrayBuffer>> {
  const key = await subtle().importKey('raw', ikm, 'HKDF', false, ['deriveBits']);
  const out = await subtle().deriveBits(
    { name: 'HKDF', hash: 'SHA-256', salt: th, info: new TextEncoder().encode(label) },
    key,
    8 * length,
  );
  return new Uint8Array(out);
}

interface EpochKeys {
  key: CryptoKey;
  iv: Uint8Array<ArrayBuffer>;
}

async function epochKeys(th: Uint8Array<ArrayBuffer>, secret: Uint8Array<ArrayBuffer>): Promise<EpochKeys> {
  const raw = await derive(th, secret, 'wse/e2e key', 32);
  const key = await subtle().importKey('raw', raw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
  return { key, iv: await derive(th, secret, 'wse/e2e iv', 4) };
}

function aad(direction: number, th: Uint8Array, hdr: Uint8Array): Uint8Array<ArrayBuffer> {
  return concat(Uint8Array.of(direction), th, hdr);
}

/** One direction's key schedule. */
class Direction {
  epoch = 0;
  keys!: EpochKeys;

  constructor(readonly th: Uint8Array<ArrayBuffer>, readonly direction: number, public secret: Uint8Array<ArrayBuffer>) {}

  async init(): Promise<this> {
    this.keys = await epochKeys(this.th, this.secret);
    return this;
  }

  nextSecret(): Promise<Uint8Array<ArrayBuffer>> {
    return derive(this.th, this.secret, 'wse/e2e next', 32);
  }
}

export class E2eSender extends Direction {
  private seq = 0;
  private epochFrames = 0;
  private epochStarted = Date.now();
  private chain: Promise<unknown> = Promise.resolve();

  /** `plaintext` as the body of a binary frame. Calls seal in call order. */
  seal(plaintext: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
    const next = this.chain.then(() => this.sealNow(plaintext));
    this.chain = next.catch(() => undefined);
    return next;
  }

  /** Force the next frame into a new epoch (tests). */
  rekeyNext(): void {
    this.epochFrames = REKEY_FRAMES;
  }

  private async sealNow(plaintext: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
    if (this.seq > SEQ_MAX) throw new E2eError('e2e frame counter used up', E2E_CLOSE_EXHAUSTED);
    if (this.epochFrames >= REKEY_FRAMES || Date.now() - this.epochStarted >= REKEY_AFTER_MS) {
      this.secret = await this.nextSecret();
      this.epoch += 1;
      this.keys = await epochKeys(this.th, this.secret);
      this.epochFrames = 0;
      this.epochStarted = Date.now();
    }
    const hdr = header(FRAME_DATA, this.epoch, this.seq);
    const sealed = await subtle().encrypt(
      { name: 'AES-GCM', iv: nonce(this.keys.iv, this.seq), additionalData: aad(this.direction, this.th, hdr), tagLength: 128 },
      this.keys.key,
      plaintext,
    );
    this.seq += 1;
    this.epochFrames += 1;
    return concat(new TextEncoder().encode('E:'), hdr, new Uint8Array(sealed));
  }
}

export class E2eReceiver extends Direction {
  private expected = 0;
  private epochFrames = 0;
  private chain: Promise<unknown> = Promise.resolve();

  /**
   * The plaintext of the next frame (null for the peer's close notice).
   * Frames open in call order; anything but the next frame rejects with an
   * E2eError that must end the connection.
   */
  open(frame: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer> | null> {
    const next = this.chain.then(() => this.openNow(frame));
    this.chain = next.catch(() => undefined);
    return next;
  }

  private async openNow(frame: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer> | null> {
    const bad = () => new E2eError('e2e frame refused', E2E_CLOSE_BAD_FRAME);
    if (frame.length < 2 + HEADER_LEN + TAG_LEN || frame[0] !== 0x45 || frame[1] !== 0x3a) throw bad();
    const hdr = frame.slice(2, 2 + HEADER_LEN);
    const v = new DataView(hdr.buffer);
    const type = v.getUint8(1);
    const epoch = v.getUint32(2);
    const seq = v.getUint32(6) * 2 ** 32 + v.getUint32(10);
    if (v.getUint8(0) !== FORMAT || (type !== FRAME_DATA && type !== FRAME_CLOSE) || seq !== this.expected) throw bad();
    let keys = this.keys;
    let rekeyed: { secret: Uint8Array<ArrayBuffer>; keys: EpochKeys } | null = null;
    if (epoch === this.epoch) {
      if (this.epochFrames >= MAX_EPOCH_FRAMES) throw bad();
    } else if (epoch === this.epoch + 1) {
      // Derived on trial: kept only if the tag holds.
      const secret = await this.nextSecret();
      rekeyed = { secret, keys: await epochKeys(this.th, secret) };
      keys = rekeyed.keys;
    } else {
      throw bad();
    }
    let plaintext: ArrayBuffer;
    try {
      plaintext = await subtle().decrypt(
        { name: 'AES-GCM', iv: nonce(keys.iv, seq), additionalData: aad(this.direction, this.th, hdr), tagLength: 128 },
        keys.key,
        frame.slice(2 + HEADER_LEN),
      );
    } catch {
      throw bad();
    }
    if (rekeyed) {
      this.secret = rekeyed.secret;
      this.keys = rekeyed.keys;
      this.epoch = epoch;
      this.epochFrames = 0;
    }
    this.expected += 1;
    this.epochFrames += 1;
    return type === FRAME_DATA ? new Uint8Array(plaintext) : null;
  }
}

/** An established session. */
export class E2eSession {
  constructor(readonly sender: E2eSender, readonly receiver: E2eReceiver) {}

  seal(plaintext: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer>> {
    return this.sender.seal(plaintext);
  }

  open(frame: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer> | null> {
    return this.receiver.open(frame);
  }
}

export interface E2eTrust {
  /** The server's identity public keys (base64 P-256 points) one of which must sign. */
  serverKeys?: string[];
  /** Accept an unsigned server: protects only against passive observers. */
  unauthenticated?: boolean;
}

/** A fresh offer, made for each connection attempt. */
export class E2eOffer {
  readonly suites = [E2E_SUITE];

  private constructor(
    private readonly pair: CryptoKeyPair,
    readonly publicKey: Uint8Array<ArrayBuffer>,
    readonly random: Uint8Array<ArrayBuffer>,
  ) {}

  static async create(): Promise<E2eOffer> {
    const pair = (await subtle().generateKey({ name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits'])) as CryptoKeyPair;
    const publicKey = new Uint8Array(await subtle().exportKey('raw', pair.publicKey));
    const random = bytes(32);
    globalThis.crypto.getRandomValues(random);
    return new E2eOffer(pair, publicKey, random);
  }

  /** client_hello.p.e2e */
  message(): Record<string, unknown> {
    return {
      suites: [...this.suites],
      client_public_key: b64encode(this.publicKey),
      client_random: b64encode(this.random),
    };
  }

  /** Check server_hello.p.e2e and start the session. */
  async accept(hello: any, connectionId: string, trust: E2eTrust): Promise<E2eSession> {
    if (!this.suites.includes(hello?.suite)) throw new E2eError('no e2e suite in common', E2E_CLOSE_UNSUPPORTED);
    let serverPublic: Uint8Array<ArrayBuffer>;
    let kid: Uint8Array<ArrayBuffer>;
    try {
      serverPublic = b64decode(hello.server_public_key);
      kid = Uint8Array.from((hello.key_id as string).match(/../g)!.map((h) => parseInt(h, 16))) as Uint8Array<ArrayBuffer>;
    } catch {
      throw new E2eError('malformed server e2e answer', E2E_CLOSE_SERVER_REJECTED);
    }
    if (serverPublic.length !== 65 || kid.length !== 8) throw new E2eError('malformed server e2e answer', E2E_CLOSE_SERVER_REJECTED);
    const t = transcript(this.suites, E2E_SUITE, this.random, this.publicKey, serverPublic, connectionId, kid);
    if (trust.serverKeys && trust.serverKeys.length > 0) {
      let signer: Uint8Array<ArrayBuffer> | null = null;
      for (const k of trust.serverKeys) {
        const raw = b64decode(k);
        if ((await keyId(raw)) === hex(kid)) signer = raw;
      }
      if (!signer || hello.alg !== E2E_SIGNATURE_ALG || typeof hello.signature !== 'string') {
        throw new E2eError('server identity not pinned', E2E_CLOSE_SERVER_REJECTED);
      }
      const key = await subtle().importKey('raw', signer, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
      const ok = await subtle().verify({ name: 'ECDSA', hash: 'SHA-256' }, key, b64decode(hello.signature), t);
      if (!ok) throw new E2eError('server identity signature invalid', E2E_CLOSE_SERVER_REJECTED);
    } else if (!trust.unauthenticated) {
      throw new E2eError("pin the server's e2e identity key (serverKeys) or allow unauthenticated", E2E_CLOSE_SERVER_REJECTED);
    }
    const peer = await subtle().importKey('raw', serverPublic, { name: 'ECDH', namedCurve: 'P-256' }, false, []);
    const z = new Uint8Array(await subtle().deriveBits({ name: 'ECDH', public: peer }, this.pair.privateKey, 256));
    const th = await sha256(t);
    const sender = await new E2eSender(th, C2S, await derive(th, z, 'wse/e2e c2s', 32)).init();
    const receiver = await new E2eReceiver(th, S2C, await derive(th, z, 'wse/e2e s2c', 32)).init();
    return new E2eSession(sender, receiver);
  }
}
