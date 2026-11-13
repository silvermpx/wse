// The client side of E2E sessions against the shared test vectors (made with
// an independent implementation: tests/vectors/make_e2e_vectors.py).

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { E2eError, E2eReceiver, E2eSender, keyId, transcript } from '../utils/e2eSession.js';

const v = JSON.parse(
  readFileSync(fileURLToPath(new URL('../../tests/vectors/e2e.json', import.meta.url)), 'utf8'),
);
const b64 = (s: string) => new Uint8Array(Buffer.from(s, 'base64'));
const unhex = (s: string) => new Uint8Array(Buffer.from(s, 'hex'));
const toHex = (b: Uint8Array) => Buffer.from(b).toString('hex');

describe('E2E session vectors', () => {
  it('builds the transcript and verifies the server signature', async () => {
    const identity = b64(v.identity_public_key);
    expect(await keyId(identity)).toBe(v.key_id);
    const t = transcript(
      v.suites,
      v.suite,
      b64(v.client_random),
      b64(v.client_public_key),
      b64(v.server_public_key),
      v.connection_id,
      unhex(v.key_id),
    );
    expect(toHex(t)).toBe(v.transcript);
    const key = await crypto.subtle.importKey('raw', identity, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
    expect(await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, key, b64(v.signature), t)).toBe(true);
  });

  for (const [name, direction] of [['client_to_server', 1], ['server_to_client', 2]] as const) {
    it(`seals and opens the ${name} frames`, async () => {
      const th = unhex(v.th);
      const secret = unhex(v[`${name}_secret`]);
      const sender = await new E2eSender(th, direction, secret).init();
      const receiver = await new E2eReceiver(th, direction, secret).init();
      for (const [i, f] of (v[name] as any[]).entries()) {
        if (i === 2) sender.rekeyNext(); // the vectors rekey at the third frame
        const text = new TextEncoder().encode(f.plaintext);
        const frame = await sender.seal(text);
        expect(toHex(frame)).toBe(f.frame);
        expect(new TextDecoder().decode((await receiver.open(frame))!)).toBe(f.plaintext);
      }
    });
  }

  it('refuses a replayed or tampered frame', async () => {
    const th = unhex(v.th);
    const secret = unhex(v.server_to_client_secret);
    const sender = await new E2eSender(th, 2, secret).init();
    const receiver = await new E2eReceiver(th, 2, secret).init();
    const one = await sender.seal(new TextEncoder().encode('one'));
    await receiver.open(one);
    await expect(receiver.open(one)).rejects.toBeInstanceOf(E2eError);
    const fresh = await new E2eReceiver(th, 2, secret).init();
    const tampered = one.slice();
    tampered[tampered.length - 1] ^= 1;
    await expect(fresh.open(tampered)).rejects.toMatchObject({ closeCode: 4802 });
  });
});
