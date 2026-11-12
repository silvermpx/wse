// =============================================================================
// WebSocket Engine - Event Handler Registry
// =============================================================================
//
// This is an empty registry shell. Applications register their own domain-specific
// handlers by calling messageProcessor.registerHandler() directly.
//
// Example (in your application code):
//
//   useWSE('cookie', ['orders'], {
//     registerHandlers: (processor) => {
//       processor.registerHandler('order_update', (msg) => { ... });
//     },
//   });

import { logger } from '../utils/logger.js';

/**
 * Register all application-specific event handlers with the message processor.
 *
 * Override this function or call messageProcessor.registerHandler() directly
 * to add domain-specific handlers for your application.
 *
 * The MessageProcessor already registers system-level handlers (server_ready,
 * subscription_update, error, health_check, etc.) by default.
 */
export function registerAllHandlers(messageProcessor: {
  registerHandler: (type: string, handler: (msg: any) => void) => void;
  getRegisteredHandlers?: () => string[];
}): void {
  // No domain-specific handlers in standalone mode.
  // Applications register their own through the `registerHandlers` option of
  // useWSE() or messageProcessor.registerHandler() calls.

  if (messageProcessor.getRegisteredHandlers) {
    const registeredHandlers = messageProcessor.getRegisteredHandlers();
    logger.debug('Registered handlers:', registeredHandlers);
  }
}
