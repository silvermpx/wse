// =============================================================================
// WebSocket Engine - The window bridge (wse-client/bridge)
// =============================================================================
//
// One connection for every window of an application: an owner holds it, and
// each window a client with the WSEClient interface over a channel to it.
// Electron's package (wse-client/electron) and Tauri's (wse-client/tauri)
// are made of these; a worker or a host of its own may use them as they are.

export { BRIDGE_PROTOCOL, DEFAULT_BRIDGE_LIMITS } from './protocol.js';
export type { BridgeItem, BridgeLimits, ConnState, OwnerFrame, SendFrameOptions, WindowFrame } from './protocol.js';
export { messagePortChannel } from './channel.js';
export type { Channel, OwnerChannel, PortLike, WindowChannel } from './channel.js';
export { createBridgeClient } from './client.js';
export type { BridgeClient, BridgeClientOptions, BridgePhase } from './client.js';
export { createBridgeOwner } from './owner.js';
export type { BridgeOwner, BridgeOwnerOptions } from './owner.js';
