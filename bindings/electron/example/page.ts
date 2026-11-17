// The example's page in an Electron window: its client over the main
// process's connection, and the page every example shows.

import { createElectronClient } from '../../../ts-client/electron/renderer.js';
import { mountExample } from '../../example-ui/ui.js';

const given = new URLSearchParams(location.hash.slice(1));
const root = document.getElementById('app');
if (root) {
  mountExample(root, createElectronClient(), {
    platform: 'Electron',
    holder: { de: 'Hauptprozess', en: 'main process' },
    window: Number(given.get('window') ?? 1),
    onWorking: given.get('check')
      ? () => {
          document.body.dataset.working = '1';
        }
      : undefined,
  });
}
