// =============================================================================
// WebSocket Engine - Signed messages
// =============================================================================
//
// A signed message travels as the exact text its sender signed, with the
// signature and the id of the signing key, so whoever keeps it can show later
// who sent what: `{"t": "signed", "sb": body, "sig": ..., "kid": ...}`. The
// signature is ECDSA P-256 / SHA-256, `r || s` in base64, over a label naming
// the signer's side and the body. See docs/PROTOCOL.md (Signed messages).

import { keyId } from './e2eSession.js';

export const CLIENT_LABEL = 'WSE-SIG client\0';
export const SERVER_LABEL = 'WSE-SIG server\0';

const ECDSA = { name: 'ECDSA', namedCurve: 'P-256' } as const;
const SIGN = { name: 'ECDSA', hash: 'SHA-256' } as const;
const encoder = new TextEncoder();

/** What proves who sent a signed message: keep body, sig and kid together. */
export interface SignatureProof {
  /** The exact text that was signed (a JSON object). */
  body: string;
  /** The signature, `r || s` in base64. */
  sig: string;
  /** The signing key's id: the first 8 bytes of SHA-256 of its public key, hex. */
  kid: string;
  /** Checked against a pinned server key (false when none are pinned). */
  verified: boolean;
}

export class SignatureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SignatureError';
  }
}

const subtle = (): SubtleCrypto => {
  const s = globalThis.crypto?.subtle;
  if (!s) throw new SignatureError('WebCrypto is not available (HTTPS or localhost only)');
  return s;
};

function b64decode(s: string): Uint8Array<ArrayBuffer> {
  const bin = atob(s);
  const out = new Uint8Array(new ArrayBuffer(bin.length));
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function b64encode(b: Uint8Array): string {
  let s = '';
  for (const x of b) s += String.fromCharCode(x);
  return btoa(s);
}

function input(label: string, body: string): Uint8Array<ArrayBuffer> {
  const bytes = encoder.encode(label + body);
  const out = new Uint8Array(new ArrayBuffer(bytes.length));
  out.set(bytes);
  return out;
}

/** This client's key for signed messages, and what the server needs of it. */
export interface SigningKey {
  /** The key pair: its private key signs (non-extractable when made here). */
  keyPair: CryptoKeyPair;
  /** The public key as the server takes it: base64 of the uncompressed point. */
  publicKey: string;
  /** The public key as a JWK, for a token's `cnf` claim: `{"jwk": ...}`. */
  jwk: JsonWebKey;
  /** The key id. */
  kid: string;
}

/** A SigningKey from a P-256 ECDSA key pair (e.g. one kept in IndexedDB). */
export async function signingKeyFrom(keyPair: CryptoKeyPair): Promise<SigningKey> {
  const point = new Uint8Array(await subtle().exportKey('raw', keyPair.publicKey));
  const jwk = await subtle().exportKey('jwk', keyPair.publicKey);
  return {
    keyPair,
    publicKey: b64encode(point),
    jwk: { kty: jwk.kty, crv: jwk.crv, x: jwk.x, y: jwk.y },
    kid: await keyId(point),
  };
}

/**
 * A new P-256 key pair for this client's signed messages. The private key
 * cannot be exported; keep the CryptoKeyPair (IndexedDB stores it as is) and
 * register `publicKey` (or `jwk` in the token's `cnf` claim) with the server.
 */
export async function generateSigningKey(): Promise<SigningKey> {
  const keyPair = await subtle().generateKey(ECDSA, false, ['sign', 'verify']);
  return signingKeyFrom(keyPair);
}

/**
 * A signed message as JSON text, for the server whose signing key id is
 * `audience`. `nonce` is unique per message (an order id, say).
 */
export async function signMessage(
  key: SigningKey,
  type: string,
  payload: unknown,
  audience: string,
  nonce: string,
  nowMs: number = Date.now(),
): Promise<{ t: 'signed'; sb: string; sig: string; kid: string }> {
  const body: Record<string, unknown> = { t: type };
  if (payload !== undefined) body.p = payload;
  body.aud = audience;
  body.ts = nowMs;
  body.n = nonce;
  const text = JSON.stringify(body);
  const sig = new Uint8Array(
    await subtle().sign(SIGN, key.keyPair.privateKey, input(CLIENT_LABEL, text)),
  );
  return { t: 'signed', sb: text, sig: b64encode(sig), kid: key.kid };
}

/** The server signing keys a client trusts, by key id. */
export class ServerSigningKeys {
  private constructor(private readonly keys: Map<string, CryptoKey>) {}

  static async create(publicKeys: string[] | undefined): Promise<ServerSigningKeys> {
    const keys = new Map<string, CryptoKey>();
    for (const publicKey of publicKeys ?? []) {
      const point = b64decode(publicKey);
      if (point.length !== 65 || point[0] !== 0x04) {
        throw new SignatureError('a server signing key is base64 of an uncompressed P-256 point');
      }
      keys.set(await keyId(point), await subtle().importKey('raw', point, ECDSA, false, ['verify']));
    }
    return new ServerSigningKeys(keys);
  }

  get pinned(): boolean {
    return this.keys.size > 0;
  }

  /**
   * The body of a server-signed message and its proof. Throws SignatureError
   * when it is malformed or, with pinned keys, when no pinned key has its id
   * or the signature does not verify. Without pinned keys the body comes
   * unchecked (`verified: false`).
   */
  async open(envelope: Record<string, unknown>): Promise<{ message: Record<string, any>; proof: SignatureProof }> {
    const { sb: body, sig, kid } = envelope;
    if (typeof body !== 'string' || typeof sig !== 'string' || typeof kid !== 'string') {
      throw new SignatureError('signed message malformed');
    }
    let verified = false;
    if (this.keys.size > 0) {
      const key = this.keys.get(kid);
      if (!key) throw new SignatureError(`no pinned server signing key ${kid}`);
      let raw: Uint8Array<ArrayBuffer>;
      try {
        raw = b64decode(sig);
      } catch {
        throw new SignatureError('signature is not base64');
      }
      if (raw.length !== 64 || !(await subtle().verify(SIGN, key, raw, input(SERVER_LABEL, body)))) {
        throw new SignatureError('server signature does not verify');
      }
      verified = true;
    }
    let message: unknown;
    try {
      message = JSON.parse(body);
    } catch {
      throw new SignatureError('signed body is not JSON');
    }
    if (!message || typeof message !== 'object' || typeof (message as any).t !== 'string') {
      throw new SignatureError('signed body malformed');
    }
    return { message: message as Record<string, any>, proof: { body, sig, kid, verified } };
  }
}
