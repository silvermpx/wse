// A window of an Electron application with the packed wse-client: its client
// over the main process's connection, rendered with wse-client/react's hooks
// as they are. What it shows is German and English; what it saw is on the
// body for the main process to read.

import { StrictMode, createElement as h, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createElectronClient } from 'wse-client/electron/renderer';
import { WSEProvider, useWSEMessage, useWSEState, useWSETopics } from 'wse-client/react';

type Language = 'de' | 'en';

const TEXT = {
  de: {
    title: 'Fenster',
    other: 'English',
    states: { connected: 'Verbunden', connecting: 'Verbindet …', reconnecting: 'Verbindet erneut …' } as Record<string, string>,
    waiting: 'Wartet',
    tick: 'Letzter Takt',
    noTick: 'Noch kein Takt',
  },
  en: {
    title: 'Window',
    other: 'Deutsch',
    states: { connected: 'Connected', connecting: 'Connecting …', reconnecting: 'Connecting again …' } as Record<string, string>,
    waiting: 'Waiting',
    tick: 'Last tick',
    noTick: 'No tick yet',
  },
};

function Ticks({ window: number }: { window: string }) {
  const [language, setLanguage] = useState<Language>('de');
  const state = useWSEState();
  useWSETopics(['ticks']);
  const [last, setLast] = useState<number | null>(null);
  const [count, setCount] = useState(0);
  useWSEMessage('tick', (message) => {
    setLast(Number(message.p?.n));
    setCount((n) => n + 1);
  });
  useEffect(() => {
    document.documentElement.lang = language;
    document.body.dataset.connection = state.connection;
    document.body.dataset.ticks = String(count);
  }, [language, state.connection, count]);
  const t = TEXT[language];
  return h(
    'section',
    null,
    h('h1', null, `WSE: ${t.title} ${number}`),
    h('p', null, t.states[state.connection] ?? t.waiting),
    h('p', null, last === null ? t.noTick : `${t.tick}: ${last}`),
    h('button', { type: 'button', onClick: () => setLanguage(language === 'de' ? 'en' : 'de') }, t.other),
  );
}

const window = new URLSearchParams(location.hash.slice(1)).get('window') ?? '1';
const client = createElectronClient();
const root = document.getElementById('app');
if (root) {
  createRoot(root).render(h(StrictMode, null, h(WSEProvider, { client }, h(Ticks, { window }))));
}
