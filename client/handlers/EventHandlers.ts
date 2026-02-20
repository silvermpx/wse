// =============================================================================
// WebSocket Event System - Event Handlers Wrapper
// =============================================================================

import { registerAllHandlers } from './index';

/**
 * EventHandlers static class for compatibility with useWSE hook.
 * Delegates to registerAllHandlers which is a no-op in standalone mode.
 * Applications should override or extend this to register domain-specific handlers.
 */
export class EventHandlers {
  /**
   * Register all event handlers with the message processor.
   * Called by useWSE hook during initialization.
   */
  static registerAll(messageProcessor: {
    registerHandler: (type: string, handler: (msg: unknown) => void) => void;
    getRegisteredHandlers?: () => string[];
  }): void {
    registerAllHandlers(messageProcessor);
  }
}

export default EventHandlers;
