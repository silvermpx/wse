// Checks a framework's application of tests/clients, built against the
// packed wse-client: its server renders the page for two requests at once
// without a connection, Chromium hydrates it without a mismatch and takes
// exactly one connection, the ticks come, and the page switches between
// German and English with no word of the other language left.
//
//   node page.mjs <angular|solid1|solid2> <built application> <the Rust client's example server>

import { realpathSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { countingDoor, fail, freePort, until, wseServer } from './common.mjs';
import { spawn } from 'node:child_process';

// How each framework's built server starts, and what marks a page it
// rendered for hydration.
const FRAMEWORKS = {
  angular: { server: 'dist/fixture/server/server.mjs', hydration: /ngh=/, complaint: /NG05\d\d|hydrat|mismatch/i },
  solid1: { server: '.output/server/index.mjs', hydration: /data-hk=/, complaint: /hydrat|mismatch/i },
  solid2: { server: 'dist/server/node.js', hydration: /data-hk=|_\$HY/, complaint: /hydrat|mismatch/i },
};

const [kind, given, executable] = process.argv.slice(2);
const framework = FRAMEWORKS[kind];
if (!framework || !given || !executable) {
  fail('usage: node page.mjs <angular|solid1|solid2> <built application> <example server>');
}
// The real path: a server takes itself for the main module only by it
// (on macOS a temporary folder is a link into /private).
const app = realpathSync(given);

// What the connected page shows in each language (the button names the other
// language, by its own name, in both).
const GERMAN = ['WSE-Prüfung', 'Verbindung', 'Nachrichten empfangen', 'verbunden'];
const ENGLISH = ['WSE check', 'Connection', 'Messages received', 'connected'];

const cleanup = [];
try {
  const wse = await wseServer(executable);
  cleanup.push(() => wse.child.kill());
  const door = await countingDoor(wse.port);
  cleanup.push(() => door.server.close());
  const port = await freePort();
  const ssr = spawn(process.execPath, [join(app, framework.server)], {
    cwd: app,
    env: { ...process.env, PORT: String(port), NITRO_PORT: String(port), NODE_ENV: 'production' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let ssrOutput = '';
  ssr.stdout.on('data', (chunk) => (ssrOutput += chunk));
  ssr.stderr.on('data', (chunk) => (ssrOutput += chunk));
  cleanup.push(() => ssr.kill());
  const origin = `http://localhost:${port}`;
  await until(
    async () => {
      if (ssr.exitCode !== null) fail(`the application's server ended (${ssr.exitCode}):\n${ssrOutput}`);
      return fetch(origin).then(
        () => true,
        () => false,
      );
    },
    `the application's server did not answer at ${origin}:\n${ssrOutput}`,
    30_000,
  );
  const page = `${origin}/?wse=${encodeURIComponent(`ws://127.0.0.1:${door.port}/wse`)}`;

  // The server: two requests at once, each rendered with the client's first
  // state, in German, ready to hydrate, and no connection taken.
  const rendered = await Promise.all([fetch(page), fetch(page)]);
  for (const response of rendered) {
    const html = await response.text();
    if (response.status !== 200) fail(`the server answered ${response.status}:\n${html}\n${ssrOutput}`);
    for (const word of ['WSE-Prüfung', 'wartet', 'data-state="pending"']) {
      if (!html.includes(word)) fail(`the server's page lacks ${word}:\n${html}\n${ssrOutput}`);
    }
    if (!framework.hydration.test(html)) fail(`the server rendered nothing to hydrate (${framework.hydration}):\n${html}`);
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
  if (door.connections !== 0) fail(`the server's rendering connected ${door.connections} time(s)`);

  // The browser: hydration without a mismatch, then one connection and ticks.
  const browser = await chromium.launch();
  cleanup.push(() => browser.close());
  const tab = await browser.newPage();
  const complaints = [];
  tab.on('console', (message) => {
    if (message.type() === 'error' || message.type() === 'warning') complaints.push(message.text());
  });
  tab.on('pageerror', (error) => complaints.push(String(error)));
  await tab.goto(page);
  const state = tab.locator('[data-test=connection]');
  await until(async () => (await state.getAttribute('data-state')) === 'connected', 'the page never connected');
  await until(async () => Number(await tab.locator('[data-test=ticks]').textContent()) > 0, 'no tick came');
  if (door.connections !== 1) fail(`the page took ${door.connections} connections, not one`);
  const hydration = complaints.filter((text) => framework.complaint.test(text));
  if (hydration.length > 0) fail(`hydration complained:\n${hydration.join('\n')}`);

  // German first, then English, then German again: each whole.
  const shows = async (words, others) => {
    const text = await tab.locator('main').innerText();
    return words.every((word) => text.includes(word)) && !others.some((word) => text.includes(word));
  };
  await until(() => shows(GERMAN, ENGLISH), 'the page is not all German');
  await tab.locator('[data-test=language]').click();
  await until(() => shows(ENGLISH, GERMAN), 'the page is not all English after the switch');
  await tab.locator('[data-test=language]').click();
  await until(() => shows(GERMAN, ENGLISH), 'the page is not all German after switching back');
  if (complaints.length > 0) fail(`the page complained:\n${complaints.join('\n')}`);
  console.log(`ok (${kind}): rendered twice with no connection, hydrated, one connection, ticks, German and English`);
} finally {
  for (const close of cleanup.reverse()) {
    try {
      await close();
    } catch {
      // gone already
    }
  }
}
