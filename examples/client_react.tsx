/**
 * React client for WSE.
 *
 * Connects to a WSE server, subscribes to topics,
 * displays real-time events in a dashboard.
 *
 *     npm install wse-client
 *
 * Usage in your React app:
 *
 *     import { Dashboard } from './client_react';
 *     // ... render <Dashboard /> in your app
 */

import { useEffect, useState, useCallback } from 'react';
import { useWSE } from 'wse-client';

// -- Basic usage: one hook, real-time data ------------------------------------

export function Dashboard() {
  const {
    connectionHealth,
    activeTopics,
    stats,
    subscribe,
    unsubscribe,
  } = useWSE(
    'your-jwt-token',             // auth token
    ['prices', 'notifications'],  // initial topics
    {
      endpoints: ['ws://localhost:5007/wse'],
    }
  );

  const [prices, setPrices] = useState<Record<string, number>>({});
  const [notifications, setNotifications] = useState<string[]>([]);

  // listen for price updates
  useEffect(() => {
    const handler = (e: Event) => {
      const { symbol, price } = (e as CustomEvent).detail;
      setPrices(prev => ({ ...prev, [symbol]: price }));
    };
    window.addEventListener('price_update', handler);
    return () => window.removeEventListener('price_update', handler);
  }, []);

  // listen for notifications
  useEffect(() => {
    const handler = (e: Event) => {
      const { text } = (e as CustomEvent).detail;
      setNotifications(prev => [text, ...prev].slice(0, 50));
    };
    window.addEventListener('notification', handler);
    return () => window.removeEventListener('notification', handler);
  }, []);

  return (
    <div>
      <h2>WSE Dashboard</h2>

      <div>
        <strong>Status:</strong> {connectionHealth}
        {' | '}
        <strong>Latency:</strong> {stats.averageLatency.toFixed(1)}ms
        {' | '}
        <strong>Topics:</strong> {activeTopics.join(', ')}
      </div>

      <h3>Prices</h3>
      <ul>
        {Object.entries(prices).map(([symbol, price]) => (
          <li key={symbol}>{symbol}: ${price.toFixed(2)}</li>
        ))}
      </ul>

      <h3>Notifications</h3>
      <ul>
        {notifications.map((text, i) => (
          <li key={i}>{text}</li>
        ))}
      </ul>
    </div>
  );
}

// -- Dynamic subscriptions ----------------------------------------------------

export function ChatRoom({ channelId }: { channelId: string }) {
  const { subscribe, unsubscribe, sendMessage } = useWSE(
    'your-jwt-token',
    [],  // no initial topics -- subscribe dynamically
    { endpoints: ['ws://localhost:5007/wse'] }
  );

  const [messages, setMessages] = useState<Array<{ author: string; text: string }>>([]);
  const [input, setInput] = useState('');

  // subscribe to the channel topic when channelId changes
  useEffect(() => {
    const topic = `chat:${channelId}`;
    subscribe([topic]);
    return () => unsubscribe([topic]);
  }, [channelId, subscribe, unsubscribe]);

  // listen for chat messages
  useEffect(() => {
    const handler = (e: Event) => {
      const msg = (e as CustomEvent).detail;
      setMessages(prev => [...prev, msg]);
    };
    window.addEventListener('message_sent', handler);
    return () => window.removeEventListener('message_sent', handler);
  }, []);

  const send = useCallback(() => {
    if (!input.trim()) return;
    sendMessage('chat_message', { text: input, channel: channelId });
    setInput('');
  }, [input, channelId, sendMessage]);

  return (
    <div>
      <h3>Chat: #{channelId}</h3>
      <div style={{ height: 300, overflow: 'auto' }}>
        {messages.map((msg, i) => (
          <div key={i}><strong>{msg.author}:</strong> {msg.text}</div>
        ))}
      </div>
      <input
        value={input}
        onChange={e => setInput(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && send()}
        placeholder="Type a message..."
      />
      <button onClick={send}>Send</button>
    </div>
  );
}
