/**
 * React client for WSE: a dashboard of prices and notifications, and a chat
 * room, in German and English.
 *
 *     npm install wse-client react
 *
 * One client for the page, its connection held while a component that needs
 * it is mounted:
 *
 *     import { createWSEClient } from 'wse-client/core';
 *     import { WSEProvider } from 'wse-client/react';
 *     import { Dashboard, ChatRoom } from './client_react';
 *
 *     const client = createWSEClient({ endpoints: ['ws://localhost:5007/wse'] });
 *
 *     <WSEProvider client={client}>
 *       <Dashboard lang="de" />
 *       <ChatRoom channelId="general" lang="de" />
 *     </WSEProvider>
 *
 * The server authenticates a browser by its cookie (`jwt_cookie_name`):
 * a page's WebSocket cannot carry an Authorization header.
 */

import { useCallback, useState } from 'react';
import { useWSEClient, useWSEMessage, useWSEState, useWSETopics } from 'wse-client/react';

// -- The page's words, in German and English ----------------------------------

export type Lang = 'de' | 'en';

const WORDS = {
  de: {
    dashboard: 'WSE-Übersicht',
    status: 'Verbindung',
    connected: 'verbunden',
    connecting: 'verbindet',
    disconnected: 'getrennt',
    topics: 'Themen',
    none: 'keine',
    prices: 'Kurse',
    notifications: 'Mitteilungen',
    chat: 'Chat',
    placeholder: 'Nachricht schreiben …',
    send: 'Senden',
  },
  en: {
    dashboard: 'WSE dashboard',
    status: 'Connection',
    connected: 'connected',
    connecting: 'connecting',
    disconnected: 'disconnected',
    topics: 'Topics',
    none: 'none',
    prices: 'Prices',
    notifications: 'Notifications',
    chat: 'Chat',
    placeholder: 'Write a message …',
    send: 'Send',
  },
} as const;

/** How the client's state reads on the page. */
function connection(ready: boolean, state: string, lang: Lang): string {
  const words = WORDS[lang];
  if (ready) return words.connected;
  return state === 'connecting' || state === 'reconnecting' || state === 'pending'
    ? words.connecting
    : words.disconnected;
}

// -- A dashboard: topics held while mounted, messages by type -----------------

export function Dashboard({ lang = 'en' }: { lang?: Lang }) {
  const words = WORDS[lang];
  const state = useWSEState();
  useWSETopics(['prices', 'notifications']);

  const [prices, setPrices] = useState<Record<string, number>>({});
  const [notifications, setNotifications] = useState<string[]>([]);

  useWSEMessage('price_update', (message) => {
    const { symbol, price } = message.p as { symbol: string; price: number };
    setPrices((previous) => ({ ...previous, [symbol]: price }));
  });
  useWSEMessage('notification', (message) => {
    const { text } = message.p as { text: string };
    setNotifications((previous) => [text, ...previous].slice(0, 50));
  });

  return (
    <div>
      <h2>{words.dashboard}</h2>
      <p>
        <strong>{words.status}:</strong> {connection(state.ready, state.connection, lang)}
        {' | '}
        <strong>{words.topics}:</strong> {state.topics.length > 0 ? state.topics.join(', ') : words.none}
      </p>

      <h3>{words.prices}</h3>
      <ul>
        {Object.entries(prices).map(([symbol, price]) => (
          <li key={symbol}>
            {symbol}: {price.toLocaleString(lang, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </li>
        ))}
      </ul>

      <h3>{words.notifications}</h3>
      <ul>
        {notifications.map((text, i) => (
          <li key={i}>{text}</li>
        ))}
      </ul>
    </div>
  );
}

// -- A chat room: its topic follows the channel -------------------------------

export function ChatRoom({ channelId, lang = 'en' }: { channelId: string; lang?: Lang }) {
  const words = WORDS[lang];
  const client = useWSEClient();
  // Held while mounted; another channel, another topic.
  useWSETopics([`chat:${channelId}`]);

  const [messages, setMessages] = useState<Array<{ author: string; text: string }>>([]);
  const [input, setInput] = useState('');

  useWSEMessage('chat_message', (message) => {
    if (message.tp !== `chat:${channelId}`) return;
    const { author, text } = message.p as { author: string; text: string };
    setMessages((previous) => [...previous, { author, text }]);
  });

  const send = useCallback(() => {
    const text = input.trim();
    if (!text) return;
    client.send('chat_message', { text, channel: channelId });
    setInput('');
  }, [client, input, channelId]);

  return (
    <div>
      <h3>
        {words.chat}: #{channelId}
      </h3>
      <div style={{ height: 300, overflow: 'auto' }}>
        {messages.map((message, i) => (
          <div key={i}>
            <strong>{message.author}:</strong> {message.text}
          </div>
        ))}
      </div>
      <input
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={(e) => e.key === 'Enter' && send()}
        placeholder={words.placeholder}
      />
      <button onClick={send}>{words.send}</button>
    </div>
  );
}
