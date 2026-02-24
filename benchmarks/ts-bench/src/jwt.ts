import { createHmac } from 'node:crypto';

function base64url(buf: Buffer): string {
  return buf.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

export function generateBenchToken(secret: Buffer, subject: string): string {
  const header = base64url(Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })));
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    sub: subject,
    iss: 'wse-bench',
    aud: 'wse-bench',
    exp: now + 3600,
  };
  const payload = base64url(Buffer.from(JSON.stringify(claims)));
  const sig = base64url(
    createHmac('sha256', secret).update(`${header}.${payload}`).digest()
  );
  return `${header}.${payload}.${sig}`;
}
