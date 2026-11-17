// The conformance cases in a real Electron: the owner of the connection in
// this main process (wse-client/electron), and a hidden window whose page
// runs the cases with its own client, through the package's preload. The
// page asks for an owner as each case wants it, and says how the cases went;
// this process ends with that. build.mjs bundles it, from the package's
// source, into out/ beside the page and the preload:
//
//   node bindings/electron/conformance/build.mjs
//   bindings/electron/node_modules/.bin/electron bindings/electron/conformance/out/main.mjs ws://127.0.0.1:P/control [case,...]

import { app, BrowserWindow, ipcMain, MessageChannelMain, net, protocol } from 'electron';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createWSEOwner, wsePreloadPath, type WSEOwner } from '../../../ts-client/electron/main.js';
import { generateSigningKey } from '../../../ts-client/utils/signing.js';
import { logger, LogLevel } from '../../../ts-client/utils/logger.js';
import type { Asked } from '../../../ts-client/__tests__/conformance/driver.js';

const here = fileURLToPath(new URL('.', import.meta.url));
const files: Record<string, string> = { '/page.html': `${here}page.html`, '/page.js': `${here}page.js` };
const [control, cases] = process.argv.slice(2).filter((arg) => !arg.startsWith('-') && !arg.endsWith('main.mjs'));

// The page's own scheme: an application's page, which the owner serves by default.
protocol.registerSchemesAsPrivileged([
  { scheme: 'app', privileges: { standard: true, secure: true, supportFetchAPI: true, corsEnabled: true } },
]);

// The cases break connections on purpose: the owner's warnings are expected.
logger.setLevel(LogLevel.ERROR);

const deadline = setTimeout(() => {
  console.error('electron: the cases did not end within 10 minutes');
  app.exit(2);
}, 600_000);

/** The owner's connection as the case asks for it, and its signing key's public key. */
async function connection(asked: Omit<Asked, 'application'>) {
  const security: Record<string, unknown> = { encryptionEnabled: asked.e2eKey !== null };
  if (asked.e2eKey) security.serverKeys = [asked.e2eKey];
  let publicKey: string | null = null;
  if (asked.signing) {
    const signing = await generateSigningKey();
    publicKey = signing.publicKey;
    security.signingKey = signing.keyPair;
    security.serverSigningKeys = asked.serverSigningKey ? [asked.serverSigningKey] : [];
    // The application binds the key (bind_key): the token names none.
    security.bindSession = false;
  }
  return {
    publicKey,
    options: {
      endpoints: [asked.url],
      token: asked.token,
      reconnection: { baseDelay: 50, maxDelay: 300, jitter: false },
      wireFormat: asked.format,
      security,
    },
  };
}

app.whenReady().then(() => {
  if (!control) {
    console.error('usage: electron main.js ws://127.0.0.1:P/control [case,...]');
    app.exit(2);
    return;
  }
  const win = new BrowserWindow({ show: false, webPreferences: { preload: wsePreloadPath } });
  win.webContents.on('console-message', (event, ...legacy) => {
    console.log(`[page] ${event.message ?? legacy[1]}`);
  });
  win.webContents.on('render-process-gone', (_event, details) => {
    console.error(`electron: the page's process went: ${details.reason}`);
    app.exit(3);
  });
  let owner: WSEOwner | null = null;
  protocol.handle('app', async (request) => {
    const { pathname } = new URL(request.url);
    if (pathname === '/owner') {
      // One owner serves the window at a time: the last case's goes first.
      owner?.destroy();
      owner = null;
      try {
        const { options, publicKey } = await connection(await request.json());
        owner = createWSEOwner({ electron: { ipcMain, MessageChannelMain }, ...options });
        owner.serve(win.webContents);
        return Response.json({ publicKey });
      } catch (error) {
        return Response.json({ error: String(error) });
      }
    }
    if (pathname === '/done') {
      const { failed } = await request.json();
      for (const failure of failed as string[]) console.error(`failed: ${failure}`);
      console.log(failed.length ? `electron: ${failed.length} case(s) failed` : 'electron: every case passed');
      clearTimeout(deadline);
      owner?.destroy();
      setImmediate(() => app.exit(failed.length ? 1 : 0));
      return new Response('ok');
    }
    const file = files[pathname];
    return file ? net.fetch(pathToFileURL(file).href) : new Response('not found', { status: 404 });
  });
  const given = new URLSearchParams({
    control,
    runtime: `electron ${process.versions.electron}`,
    os: process.platform,
    arch: process.arch,
  });
  if (cases) given.set('cases', cases);
  void win.loadURL(`app://harness/page.html#${given}`);
});
