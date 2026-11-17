// Bundles the example into out/: its main process (main.ts, with the
// package's source), its page's script (page.ts and the page every example
// shows), the page with its content security policy, and the preload.

import { build } from 'vite';
import { copyFileSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const here = fileURLToPath(new URL('.', import.meta.url));
const out = `${here}out`;
await build({
  configFile: false,
  root: here,
  logLevel: 'warn',
  build: {
    outDir: out,
    emptyOutDir: true,
    minify: false,
    target: 'es2022',
    lib: { entry: `${here}page.ts`, formats: ['iife'], name: 'wseExample', fileName: () => 'page.js' },
  },
});
await build({
  configFile: false,
  root: here,
  logLevel: 'warn',
  build: {
    ssr: `${here}main.ts`,
    outDir: out,
    emptyOutDir: false,
    minify: false,
    target: 'node22',
    rollupOptions: { external: ['electron'], output: { format: 'es', entryFileNames: 'main.mjs' } },
  },
});
const page = readFileSync(`${here}../../example-ui/index.html`, 'utf8').replace(
  '<!-- csp -->',
  `<meta http-equiv="Content-Security-Policy" content="default-src 'self'; style-src 'self' 'unsafe-inline'" />`,
);
writeFileSync(`${out}/index.html`, page);
copyFileSync(`${here}../../../ts-client/electron/preload.cjs`, `${out}/preload.cjs`);
