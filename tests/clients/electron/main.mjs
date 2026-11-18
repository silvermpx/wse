// An Electron application with the packed wse-client, as an application
// builds it: the connection in this main process (wse-client/electron, the
// outbox in a file with wse-client/node), two windows, one with the
// package's preload, the other with the application's own, which imports it
// (built with the application's bundler). It checks itself against the
// server WSE_URL names: both windows connected and taking ticks on one
// connection; then one closes, and the other goes on. Exit 0 when it all
// holds.

import { app, BrowserWindow, ipcMain, MessageChannelMain } from 'electron';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createWSEOwner, wsePreloadPath } from 'wse-client/electron';
import { fileOutbox } from 'wse-client/node';

const here = fileURLToPath(new URL('.', import.meta.url));
const url = process.env.WSE_URL;
// What it keeps goes with the run.
app.setPath('userData', mkdtempSync(join(tmpdir(), 'wse-electron-check-')));

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const fail = (why) => {
  console.error(`electron check: ${why}`);
  app.exit(1);
};

async function until(what, test, ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await test()) return;
    await sleep(100);
  }
  throw new Error(`${what}: not within ${ms} ms`);
}

app.whenReady().then(async () => {
  if (!url) return fail('WSE_URL names no server');
  const owner = createWSEOwner({
    electron: { ipcMain, MessageChannelMain },
    endpoints: [url],
    offline: { store: fileOutbox(join(app.getPath('userData'), 'wse-outbox')) },
  });
  const preloads = [wsePreloadPath, join(here, 'dist', 'preload.cjs')];
  const windows = [1, 2].map((n) => {
    const win = new BrowserWindow({ show: false, webPreferences: { preload: preloads[n - 1] } });
    owner.serve(win.webContents);
    void win.loadFile(join(here, 'dist', 'index.html'), { hash: `window=${n}` });
    return win;
  });
  const read = (win) =>
    win.webContents.executeJavaScript(
      '({ connection: document.body.dataset.connection, ticks: Number(document.body.dataset.ticks ?? 0) })',
    );
  try {
    await until(
      'both windows connected, taking ticks',
      async () => (await Promise.all(windows.map(read))).every((s) => s.connection === 'connected' && s.ticks >= 3),
      30_000,
    );
    if (owner.attached !== 2) throw new Error(`${owner.attached} windows attached, not 2`);
    // One window closes: the other keeps the connection, and its ticks.
    windows[0].destroy();
    const before = (await read(windows[1])).ticks;
    await until(
      'the other window going on',
      async () => {
        const state = await read(windows[1]);
        return state.connection === 'connected' && state.ticks >= before + 5;
      },
      10_000,
    );
    if (owner.attached !== 1) throw new Error(`${owner.attached} windows attached after one closed, not 1`);
  } catch (error) {
    return fail(error instanceof Error ? error.message : String(error));
  }
  console.log('electron check: two windows on one connection, one closed, the other went on');
  owner.destroy();
  app.exit(0);
});
