'use strict';
// =============================================================================
// WebSocket Engine - Electron's preload (wse-client/electron/preload)
// =============================================================================
//
// Hands the page a port to the WSE owner of the main process: the page asks
// (createElectronClient posts { wse: 'attach', id } to its own window), the
// preload passes the ask on, and posts the port the main process answers
// with back to the page, which alone holds it. It runs in a sandboxed
// preload, as a preload's file (wsePreloadPath) or imported by the
// application's own preload, and needs nothing but Electron's ipcRenderer.
{
  const { ipcRenderer } = require('electron');
  window.addEventListener('message', (event) => {
    const data = event.data;
    if (event.source !== window || !data || data.wse !== 'attach' || typeof data.id !== 'string') return;
    ipcRenderer.send('wse:attach', data.id);
  });
  ipcRenderer.on('wse:port', (event, data) => {
    const port = event.ports && event.ports[0];
    if (!port || !data || typeof data.id !== 'string') return;
    window.postMessage({ wse: 'port', id: data.id }, '*', [port]);
  });
}
