// The example's page in a Tauri window: its client over the connection the
// application's Rust code holds, and the page every example shows.

import { createTauriClient, type TauriCore } from '../../../ts-client/tauri/index.js';
import { mountExample } from '../../example-ui/ui.js';

const core = (window as unknown as { __TAURI__: { core: TauriCore } }).__TAURI__.core;
// Which window this is, and whether the application checks itself: set by
// the Rust side before the page runs.
const given = (window as unknown as { __WSE_EXAMPLE__?: { window: number; check: boolean } }).__WSE_EXAMPLE__ ?? {
  window: 1,
  check: false,
};
const root = document.getElementById('app');
if (root) {
  mountExample(root, createTauriClient(core), {
    platform: 'Tauri',
    holder: { de: 'Rust-Code der Anwendung', en: "application's Rust code" },
    window: given.window,
    onWorking: given.check ? () => void core.invoke('example_working', { window: given.window }) : undefined,
  });
}
