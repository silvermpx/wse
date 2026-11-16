// Solid 2.0 as its template sets a server-rendered application up: the
// plugin makes the entries around src/App.tsx, wrapped in src/Document.tsx,
// and a Node server (dist/server/node.js).
import { defineConfig } from 'vite';
import solid from '@solidjs/vite-plugin';

export default defineConfig({
  plugins: [solid({ start: { nodeServer: true }, ssr: true })],
  build: { target: 'esnext' },
});
