// A Svelte 5 page over a client of its own, compiled for the server and for
// the browser: the files go next to this one (git ignores them), where
// `svelte` resolves.

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { compile } from 'svelte/compiler';

const here = dirname(fileURLToPath(import.meta.url));

const source = `<script>
  import { wseMessage, wseState } from '../../../svelte/index.js';
  let { client } = $props();
  const status = wseState(client);
  const news = wseMessage(client, 'news_update', ['news']);
</script>
<p id="status">connection: {$status.connection}{$news ? '' : ''}</p>
`;

/** The page's module for `generate` ('server' or 'client'), as a path to import. */
export function compiledStatus(generate: 'server' | 'client'): string {
  const dir = join(here, '.compiled');
  mkdirSync(dir, { recursive: true });
  const file = join(dir, `Status.${generate}.js`);
  writeFileSync(file, compile(source, { generate, filename: 'Status.svelte' }).js.code);
  return file;
}
