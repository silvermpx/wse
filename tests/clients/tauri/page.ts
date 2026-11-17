// A window of a Tauri application with the packed wse-client: its client
// over the connection tauri-plugin-wse holds on the Rust side, rendered with
// wse-client/react's hooks as they are. What it shows is German and
// English; what it saw it tells the Rust side.

import { StrictMode, createElement as h, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createTauriClient, type TauriCore } from 'wse-client/tauri';
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

const core = (window as unknown as { __TAURI__: { core: TauriCore } }).__TAURI__.core;
const number = (window as unknown as { __WSE_CHECK__?: { window: number } }).__WSE_CHECK__?.window ?? 1;

function Ticks() {
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
  }, [language]);
  useEffect(() => {
    void core.invoke('check_seen', { connection: state.connection, ticks: count });
  }, [state.connection, count]);
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

const client = createTauriClient(core);
const root = document.getElementById('app');
if (root) createRoot(root).render(h(StrictMode, null, h(WSEProvider, { client }, h(Ticks))));
