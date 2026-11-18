// @vitest-environment happy-dom
//
// The React example (examples/client_react.tsx), rendered as an application
// renders it: against a fake server, in both of its languages.

import { afterEach, beforeEach, expect, it } from 'vitest';
import { createElement, type ReactElement } from 'react';
import { act, cleanup, fireEvent, render } from '@testing-library/react';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { createWSEClient } from '../core/index.js';
import { WSEProvider } from '../react/index.js';
import { ChatRoom, Dashboard } from '../../examples/client_react';

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  cleanup();
  resetClientState();
});

function app(element: ReactElement) {
  const client = createWSEClient({ endpoints: ['ws://example.test/wse'], offline: { enabled: false } });
  const page = render(createElement(WSEProvider, { client }, element));
  return { client, page };
}

it('shows the prices and notifications that come, from its first render on', async () => {
  const { client, page } = app(createElement(Dashboard, { lang: 'en' }));
  expect(page.container.textContent).toContain('WSE dashboard');
  await act(async () => {
    await tick();
    await connectServer(FakeWebSocket.last());
  });
  expect(page.container.textContent).toContain('Connection: connected');
  await act(async () => {
    const ws = FakeWebSocket.last();
    ws.serverSend({ c: 'U', t: 'price_update', tp: 'prices', p: { symbol: 'EUR', price: 1.5 }, v: 1 });
    ws.serverSend({ c: 'U', t: 'notification', tp: 'notifications', p: { text: 'hello' }, v: 1 });
    await tick();
  });
  expect(page.container.textContent).toContain('EUR: 1.50');
  expect(page.container.textContent).toContain('hello');
  client.destroy();
});

it('shows a message of its channel, and sends what is typed', async () => {
  const { client, page } = app(createElement(ChatRoom, { channelId: 'general', lang: 'en' }));
  await act(async () => {
    await tick();
    await connectServer(FakeWebSocket.last());
  });
  const ws = FakeWebSocket.last();
  await act(async () => {
    ws.serverSend({ c: 'U', t: 'chat_message', tp: 'chat:general', p: { author: 'ann', text: 'hi' }, v: 1 });
    ws.serverSend({ c: 'U', t: 'chat_message', tp: 'chat:other', p: { author: 'bob', text: 'elsewhere' }, v: 1 });
    await tick();
  });
  expect(page.container.textContent).toContain('ann: hi');
  expect(page.container.textContent).not.toContain('elsewhere');
  const input = page.container.querySelector('input')!;
  await act(async () => {
    fireEvent.change(input, { target: { value: 'hello there' } });
    fireEvent.click(page.container.querySelector('button')!);
    await tick();
  });
  expect(ws.messagesOfType('chat_message').map((m) => m.p)).toEqual([{ text: 'hello there', channel: 'general' }]);
  client.destroy();
});

it('speaks German or English, and never a word of the other', () => {
  const words = {
    de: ['WSE-Übersicht', 'Verbindung', 'Themen', 'Kurse', 'Mitteilungen', 'Senden', 'Nachricht schreiben'],
    en: ['WSE dashboard', 'Connection', 'Topics', 'Prices', 'Notifications', 'Send', 'Write a message'],
  };
  for (const [lang, other] of [['de', 'en'], ['en', 'de']] as const) {
    const { client, page } = app(
      createElement('div', null, createElement(Dashboard, { lang }), createElement(ChatRoom, { channelId: 'x', lang })),
    );
    // Each text of the page apart, and the input's placeholder.
    const texts: string[] = [page.container.querySelector('input')!.placeholder];
    const walker = document.createTreeWalker(page.container, NodeFilter.SHOW_TEXT);
    while (walker.nextNode()) texts.push(walker.currentNode.textContent ?? '');
    const shown = texts.join(' ');
    // A whole word, not part of one (`Send` is in `Senden`).
    const has = (word: string) => new RegExp(`(^|[^\\p{L}])${word}($|[^\\p{L}])`, 'u').test(shown);
    for (const word of words[lang]) expect(has(word), word).toBe(true);
    for (const word of words[other]) expect(has(word), word).toBe(false);
    cleanup();
    client.destroy();
  }
});
