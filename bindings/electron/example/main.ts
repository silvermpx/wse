// The example application: one connection for both of its windows, held in
// this main process with the outbox in a file. Against the example server:
//
//   cargo run -p wse-client --example server
//   node bindings/electron/example/build.mjs
//   bindings/electron/node_modules/.bin/electron bindings/electron/example/out/main.mjs
//
// WSE_URL names another server. With WSE_EXAMPLE_CHECK=1 it checks itself:
// each window connects, takes ticks and has an echo delivered, then it ends
// (exit 0, or 1 after 30 s); WSE_EXAMPLE_SHOT=<dir> also saves each window,
// in German and in English, as PNG.

import { app, BrowserWindow, ipcMain, MessageChannelMain } from 'electron';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createWSEOwner, wsePreloadPath } from '../../../ts-client/electron/main.js';
import { fileOutbox } from '../../../ts-client/node/fileOutbox.js';

const here = fileURLToPath(new URL('.', import.meta.url));
const url = process.env.WSE_URL ?? 'ws://127.0.0.1:5007/wse';
const shots = process.env.WSE_EXAMPLE_SHOT;
const check = process.env.WSE_EXAMPLE_CHECK === '1' || Boolean(shots);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Its own folder for what it keeps (the outbox among it); a check keeps
// nothing past its run.
if (check) app.setPath('userData', mkdtempSync(join(tmpdir(), 'wse-example-')));
else app.setName('wse-example');

/** Each window works, and its picture in each language, when asked for. */
async function selfCheck(windows: BrowserWindow[]): Promise<number> {
  const until = Date.now() + 30_000;
  const working = async (win: BrowserWindow) =>
    (await win.webContents.executeJavaScript('document.body.dataset.working === "1"')) === true;
  while (Date.now() < until) {
    const all = await Promise.all(windows.map(working));
    if (all.every(Boolean)) break;
    await sleep(100);
  }
  const all = await Promise.all(windows.map(working));
  if (!all.every(Boolean)) {
    console.error(`electron example: windows ${all.map((ok, at) => (ok ? '' : at + 1)).filter(Boolean).join(', ')} did not work`);
    return 1;
  }
  if (shots) {
    mkdirSync(shots, { recursive: true });
    for (const [at, win] of windows.entries()) {
      for (const language of ['de', 'en']) {
        await win.webContents.executeJavaScript(
          `document.documentElement.lang === '${language}' || document.querySelector('button.language').click()`,
        );
        await sleep(150);
        const image = await win.webContents.capturePage();
        writeFileSync(join(shots, `electron-window-${at + 1}-${language}.png`), image.toPNG());
      }
    }
  }
  console.log('electron example: every window works');
  return 0;
}

app.whenReady().then(async () => {
  const owner = createWSEOwner({
    electron: { ipcMain, MessageChannelMain },
    endpoints: [url],
    offline: { store: fileOutbox(join(app.getPath('userData'), 'wse-outbox')) },
  });
  const windows = [1, 2].map((n) => {
    const win = new BrowserWindow({
      width: 460,
      height: 620,
      x: 60 + (n - 1) * 480,
      y: 60,
      show: !check,
      webPreferences: { preload: wsePreloadPath },
    });
    owner.serve(win.webContents);
    const hash = new URLSearchParams({ window: String(n) });
    if (check) hash.set('check', '1');
    void win.loadFile(join(here, 'index.html'), { hash: hash.toString() });
    return win;
  });
  app.on('window-all-closed', () => {
    owner.destroy();
    app.quit();
  });
  if (check) {
    const code = await selfCheck(windows);
    owner.destroy();
    app.exit(code);
  }
});
