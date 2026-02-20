/**
 * Example: Chat application event handlers for WSE.
 *
 * Register these handlers via the useWSE config to handle
 * domain-specific events from your server.
 */

export interface ChatMessage {
  id: string;
  channelId: string;
  author: string;
  text: string;
  timestamp: string;
}

export function registerChatHandlers() {
  // Register handlers for chat events
  window.addEventListener('chat_message', ((e: CustomEvent<ChatMessage>) => {
    console.log('New message:', e.detail);
    // Update your UI state here
  }) as EventListener);

  window.addEventListener('typing_start', ((e: CustomEvent<{ userId: string; channelId: string }>) => {
    console.log('User typing:', e.detail.userId);
  }) as EventListener);

  window.addEventListener('presence_update', ((e: CustomEvent<{ userId: string; status: string }>) => {
    console.log('Presence:', e.detail);
  }) as EventListener);
}
