// What the framework checks share: the Rust client's example server, a door
// in front of it that counts the connections made through it, free ports,
// and failing with a reason.

import { spawn } from 'node:child_process';
import { connect, createServer } from 'node:net';

/** Stops the check with `reason`. */
export function fail(reason) {
  throw new Error(reason);
}

/** The example server on a free port: its port, and the process. */
export async function wseServer(executable, ms = 30_000) {
  const port = await freePort();
  const child = spawn(executable, [], { env: { ...process.env, WSE_PORT: String(port) }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`the example server did not start in ${ms} ms:\n${output}`)), ms);
    const hear = (chunk) => {
      output += chunk;
      if (output.includes('signing_public_key')) {
        clearTimeout(timer);
        resolve();
      }
    };
    child.stdout.on('data', hear);
    child.stderr.on('data', hear);
    child.on('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`the example server ended (${code}):\n${output}`));
    });
  });
  return { child, port };
}

/** A door to 127.0.0.1:`port` that counts the connections made through it. */
export async function countingDoor(port) {
  const door = { connections: 0, port: 0, server: null };
  door.server = createServer((client) => {
    door.connections++;
    const upstream = connect(port, '127.0.0.1');
    client.pipe(upstream).pipe(client);
    const end = () => {
      client.destroy();
      upstream.destroy();
    };
    client.on('error', end);
    upstream.on('error', end);
    client.on('close', end);
    upstream.on('close', end);
  });
  await new Promise((resolve) => door.server.listen(0, '127.0.0.1', resolve));
  door.port = door.server.address().port;
  return door;
}

export function freePort() {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.on('error', reject);
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address();
      probe.close(() => resolve(port));
    });
  });
}

/** Waits until `check` returns true, or fails with `reason` after `ms`. */
export async function until(check, reason, ms = 20_000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  fail(reason);
}
