// =============================================================================
// WebSocket Event System - Event Handler Registry
// =============================================================================
//
// This is an empty registry shell. Applications register their own domain-specific
// handlers by calling messageProcessor.registerHandler() directly.
//
// Example (in your application code):
//
//   import { MessageProcessor } from 'wse-client/services/MessageProcessor';
//
//   function registerMyHandlers(processor: MessageProcessor) {
//     processor.registerHandler('my_event', (msg) => { ... });
//     processor.registerHandler('another_event', (msg) => { ... });
//   }

import { logger } from '../utils/logger';

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
  logger.info('=== REGISTERING EVENT HANDLERS ===');

  // No domain-specific handlers in standalone mode.
  // Applications should register their own handlers here or via
  // messageProcessor.registerHandler() calls.

  logger.info('Handler registration complete (no domain handlers in standalone mode)');

  if (messageProcessor.getRegisteredHandlers) {
    const registeredHandlers = messageProcessor.getRegisteredHandlers();
    logger.debug('Registered handlers:', registeredHandlers);
  }
}
