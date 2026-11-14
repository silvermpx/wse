// Signed messages on the client side: the shared test vectors (made with an
// independent implementation: tests/vectors/make_sign_vectors.py), signing
// with a WebCrypto key, and both directions through a session.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ENDPOINT, FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { acquireSession, getCurrentSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import type { WSMessage } from '../types.js';
import { keyId } from '../utils/e2eSession.js';
import {
  CLIENT_LABEL,
  SERVER_LABEL,
  ServerSigningKeys,
  SignatureError,
  generateSigningKey,
  signMessage,
} from '../utils/signing.js';

const v = JSON.parse(
  readFileSync(fileURLToPath(new URL('../../tests/vectors/sign.json', import.meta.url)), 'utf8'),
);
const b64 = (s: string) => new Uint8Array(Buffer.from(s, 'base64'));
const SIGN = { name: 'ECDSA', hash: 'SHA-256' } as const;
const encode = (label: string, body: string) => new TextEncoder().encode(label + body);

async function verifies(publicKey: CryptoKey, label: string, body: string, sig: string): Promise<boolean> {
  return crypto.subtle.verify(SIGN, publicKey, b64(sig), encode(label, body));
}

async function importPoint(point: string): Promise<CryptoKey> {
  return crypto.subtle.importKey('raw', b64(point), { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
}

describe('signed message vectors', () => {
  it('uses the labels and key ids of the vectors', async () => {
    expect(CLIENT_LABEL).toBe(v.client_label);
    expect(SERVER_LABEL).toBe(v.server_label);
    for (const side of ['client', 'server']) {
      expect(await keyId(b64(v[side].public_key))).toBe(v[side].kid);
    }
  });

  it('verifies the client vector as the server would', async () => {
    const key = await importPoint(v.client.public_key);
    expect(await verifies(key, CLIENT_LABEL, v.client.body, v.client.sig)).toBe(true);
    expect(await verifies(key, CLIENT_LABEL, v.client.tampered_body, v.client.sig)).toBe(false);
  });

  it('opens the server vector with its proof', async () => {
    const keys = await ServerSigningKeys.create([v.server.public_key]);
    const { message, proof } = await keys.open(JSON.parse(v.server.envelope));
    expect(message.t).toBe('fill');
    expect(message.p).toEqual({ px: '1.25', qty: '3' });
    expect(proof).toEqual({ body: v.server.body, sig: v.server.sig, kid: v.server.kid, verified: true });
  });

  it('refuses a tampered body and a key it does not pin', async () => {
    const envelope = JSON.parse(v.server.envelope);
    const pinned = await ServerSigningKeys.create([v.server.public_key]);
    await expect(pinned.open({ ...envelope, sb: v.server.tampered_body })).rejects.toBeInstanceOf(SignatureError);
    const other = await ServerSigningKeys.create([v.client.public_key]);
    await expect(other.open(envelope)).rejects.toBeInstanceOf(SignatureError);
    await expect(ServerSigningKeys.create(['AAAA'])).rejects.toBeInstanceOf(SignatureError);
  });

  it('opens unchecked without pinned keys', async () => {
    const keys = await ServerSigningKeys.create(undefined);
    const { proof } = await keys.open(JSON.parse(v.server.envelope));
    expect(proof.verified).toBe(false);
  });
});

describe('signing with a WebCrypto key', () => {
  it('signs what its public key verifies, for one server and one nonce', async () => {
    const key = await generateSigningKey();
    expect(key.kid).toBe(await keyId(b64(key.publicKey)));
    expect(key.jwk).toMatchObject({ kty: 'EC', crv: 'P-256' });
    const envelope = await signMessage(key, 'order', { qty: '1.5', note: 'café' }, 'ed94c7318b47fe94', 'o-1');
    expect(envelope.t).toBe('signed');
    expect(envelope.kid).toBe(key.kid);
    const body = JSON.parse(envelope.sb);
    expect(body).toMatchObject({ t: 'order', aud: 'ed94c7318b47fe94', n: 'o-1' });
    expect(typeof body.ts).toBe('number');
    expect(await verifies(key.keyPair.publicKey, CLIENT_LABEL, envelope.sb, envelope.sig)).toBe(true);
  });
});

describe('signed messages through a session', () => {
  beforeEach(() => {
    installFakeWebSocket();
    resetClientState();
  });

  afterEach(() => {
    resetClientState();
  });

  function start(config: UseWSEConfig = {}) {
    return acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT], offline: { enabled: false }, ...config },
    });
  }

  it('signs for the server named in server_ready and delivers what the server signed', async () => {
    const key = await generateSigningKey();
    start({ security: { encryptionEnabled: false, signingKey: key.keyPair, serverSigningKeys: [v.server.public_key] } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { signing: { kid: v.server.kid, public_key: v.server.public_key } });

    expect(await wseActions.sendSigned('order', { qty: '1.5' }, { nonce: 'o-1' })).toBe('sent');
    const sent = ws.appTextMessages().find((m) => m.t === 'signed');
    expect(sent.kid).toBe(key.kid);
    expect(JSON.parse(sent.sb)).toMatchObject({ t: 'order', p: { qty: '1.5' }, aud: v.server.kid, n: 'o-1' });
    expect(await verifies(key.keyPair.publicKey, CLIENT_LABEL, sent.sb, sent.sig)).toBe(true);

    const got: WSMessage[] = [];
    getCurrentSession()!.processor.registerHandler('fill', (m) => got.push(m));
    ws.serverSend(JSON.parse(v.server.envelope));
    await tick(6);
    expect(got).toHaveLength(1);
    expect(got[0].p).toEqual({ px: '1.25', qty: '3' });
    expect(got[0].signature?.verified).toBe(true);

    // One the pinned key does not verify never reaches the handler.
    ws.serverSend({ ...JSON.parse(v.server.envelope), sb: v.server.tampered_body });
    await tick(6);
    expect(got).toHaveLength(1);
  });

  it('refuses to sign without a key or before the server names its key', async () => {
    const key = await generateSigningKey();
    start({ security: { encryptionEnabled: false, signingKey: key.keyPair } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    expect(await wseActions.sendSigned('order', {})).toBe('dropped');
    resetClientState();
    installFakeWebSocket();
    start();
    await tick();
    await connectServer(FakeWebSocket.last(), { signing: { kid: v.server.kid } });
    expect(await wseActions.sendSigned('order', {})).toBe('dropped');
  });
});
