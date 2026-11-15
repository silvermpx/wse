// =============================================================================
// WebSocket Engine - The root package's service classes
// =============================================================================
//
// The services as the root package has always exported them: built without a
// context, they are the default client's (the page's stores, keys and window
// events). The core entry exports the same classes, with a context of their
// own by default.

import { ConnectionManager as CoreConnectionManager } from '../services/ConnectionManager.js';
import { MessageProcessor as CoreMessageProcessor } from '../services/MessageProcessor.js';
import { NetworkMonitor as CoreNetworkMonitor } from '../services/NetworkMonitor.js';
import { legacyContext } from './context.js';

export class ConnectionManager extends CoreConnectionManager {
  constructor(...args: ConstructorParameters<typeof CoreConnectionManager>) {
    const [endpoints, reconnection, onMessage, onStateChange, onServerReady, config] = args;
    super(endpoints, reconnection, onMessage, onStateChange, onServerReady, {
      ...config,
      context: config?.context ?? legacyContext(),
    });
  }
}

export class MessageProcessor extends CoreMessageProcessor {
  constructor(...args: ConstructorParameters<typeof CoreMessageProcessor>) {
    const [batchSize, batchTimeout, options] = args;
    super(batchSize, batchTimeout, { ...options, context: options?.context ?? legacyContext() });
  }
}

export class NetworkMonitor extends CoreNetworkMonitor {
  constructor(...args: ConstructorParameters<typeof CoreNetworkMonitor>) {
    const [maxHistorySize, diagnosticsIntervalMs, autoAnalyze, context] = args;
    super(maxHistorySize, diagnosticsIntervalMs, autoAnalyze, context ?? legacyContext());
  }
}
