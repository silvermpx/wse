/**
 * Example: chat handlers for WSE.
 *
 * Messages reach the application through handlers registered by message
 * type (the `t` the server publishes). Pass the result of createChatHandlers
 * as `registerHandlers` in the useWSE config:
 *
 *   const handlers = useMemo(() => createChatHandlers({ onMessage: addMessage }), []);
 *   useWSE('cookie', ['chat:general'], { registerHandlers: handlers });
 */

import type { HandlerRegistry } from '../services/WSESession.js';
import type { WSMessage } from '../types.js';

export interface ChatMessage {
  id: string;
  channelId: string;
  author: string;
  text: string;
  timestamp: string;
}

export interface ChatCallbacks {
  onMessage: (message: ChatMessage) => void;
  onTyping?: (event: { userId: string; channelId: string }) => void;
  onPresence?: (event: { user_id: string; data?: Record<string, unknown> }) => void;
}

export function createChatHandlers(callbacks: ChatCallbacks) {
  return (processor: HandlerRegistry): void => {
    processor.registerHandler('chat_message', (msg: WSMessage) => {
      callbacks.onMessage(msg.p as ChatMessage);
    });

    processor.registerHandler('typing_start', (msg: WSMessage) => {
      callbacks.onTyping?.(msg.p);
    });

    // The server's presence events are `presence_join`, `presence_leave` and
    // `presence_update`. Registering a handler replaces the default one,
    // which dispatches the window events presenceJoin/presenceLeave/presenceUpdate.
    processor.registerHandler('presence_update', (msg: WSMessage) => {
      callbacks.onPresence?.(msg.p);
    });
  };
}
