// Bundles the example's page into out/ (page.ts and the page every example
// shows): the application embeds out/ as it builds, so this goes first.

import { build } from 'vite';
import { copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const here = fileURLToPath(new URL('.', import.meta.url));
await build({
  configFile: false,
  root: here,
  logLevel: 'warn',
  build: {
    outDir: `${here}out`,
    emptyOutDir: true,
    minify: false,
    target: 'es2020',
    lib: { entry: `${here}page.ts`, formats: ['iife'], name: 'wseExample', fileName: () => 'page.js' },
  },
});
copyFileSync(`${here}../../example-ui/index.html`, `${here}out/index.html`);
