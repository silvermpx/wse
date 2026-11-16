// SolidStart 2 on Solid 1.9, as its template sets an application up: the
// server rendering through Nitro's Node server.
import { defineConfig } from 'vite';
import { nitro } from 'nitro/vite';
import { solidStart } from '@solidjs/start/config';

export default defineConfig({
  plugins: [solidStart(), nitro()],
});
