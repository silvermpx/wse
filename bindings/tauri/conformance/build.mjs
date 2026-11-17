// Bundles the page's script (page.ts and what it imports) into one classic
// script, beside the page, in out/: the application embeds out/ as it
// builds, so this goes first.

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
    lib: { entry: `${here}page.ts`, formats: ['iife'], name: 'wseConformance', fileName: () => 'page.js' },
  },
});
copyFileSync(`${here}index.html`, `${here}out/index.html`);
