// Bundles the Electron app of the conformance cases into out/: its main
// process (main.ts, with the package's source; Electron and Node's own
// modules left to the runtime), its page's script (page.ts, one classic
// script), the page and the package's preload.

import { build } from 'vite';
import { copyFileSync } from 'node:fs';
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
    lib: { entry: `${here}page.ts`, formats: ['iife'], name: 'wseConformance', fileName: () => 'page.js' },
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
copyFileSync(`${here}page.html`, `${out}/page.html`);
copyFileSync(`${here}../../../ts-client/electron/preload.cjs`, `${out}/preload.cjs`);
