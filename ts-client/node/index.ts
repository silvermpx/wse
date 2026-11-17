// =============================================================================
// WebSocket Engine - Node (wse-client/node)
// =============================================================================
//
// What only Node has (Electron's main process included): the outbox in a
// file, so that what waits outlives the process.

export { fileOutbox } from './fileOutbox.js';
export type { OutboxStore } from '../services/Outbox.js';
