// The example applications' page, for Electron and for Tauri: one window's
// view of the connection every window shares. It shows the connection's
// state, the ticks of the example server's topic `ticks`, and sends echoes;
// each window may pause itself. Every word on it is German and English.

import type { WSEClient } from '../../ts-client/core/client.js';
import type { SendStatus } from '../../ts-client/types.js';

export type Language = 'de' | 'en';

type Outcome = SendStatus | 'delivered';

export interface Texts {
  title: string;
  /** The other language, by its own name. */
  language: string;
  connection: string;
  shared: string;
  states: Record<string, string>;
  paused: string;
  ticks: string;
  subscribe: string;
  unsubscribe: string;
  lastTick: string;
  noTick: string;
  echo: string;
  placeholder: string;
  send: string;
  replies: string;
  noReply: string;
  outcomes: Record<Outcome, string>;
  pause: string;
  resume: string;
}

export const TEXTS: Record<Language, Texts> = {
  de: {
    title: 'WSE in {platform}: Fenster {window}',
    language: 'English',
    connection: 'Verbindung',
    shared: 'Alle Fenster teilen eine Verbindung, gehalten im {holder}.',
    states: {
      pending: 'Noch nicht verbunden',
      connecting: 'Verbindet …',
      connected: 'Verbunden',
      reconnecting: 'Verbindet erneut …',
      disconnected: 'Getrennt',
      error: 'Fehler',
      degraded: 'Eingeschränkt',
    },
    paused: 'Dieses Fenster ist pausiert.',
    ticks: 'Takte (Thema ticks)',
    subscribe: 'Abonnieren',
    unsubscribe: 'Abbestellen',
    lastTick: 'Letzter Takt: {n}',
    noTick: 'Noch kein Takt',
    echo: 'Echo senden',
    placeholder: 'Text eingeben',
    send: 'Senden',
    replies: 'Antworten',
    noReply: 'Noch keine Antwort',
    outcomes: {
      queued: 'Wartet auf die Verbindung',
      sent: 'Gesendet',
      delivered: 'Zugestellt',
      dropped: 'Nicht gesendet',
    },
    pause: 'Pausieren',
    resume: 'Fortsetzen',
  },
  en: {
    title: 'WSE in {platform}: window {window}',
    language: 'Deutsch',
    connection: 'Connection',
    shared: 'Every window shares one connection, held in the {holder}.',
    states: {
      pending: 'Not connected yet',
      connecting: 'Connecting …',
      connected: 'Connected',
      reconnecting: 'Connecting again …',
      disconnected: 'Disconnected',
      error: 'Error',
      degraded: 'Degraded',
    },
    paused: 'This window is paused.',
    ticks: 'Ticks (topic ticks)',
    subscribe: 'Subscribe',
    unsubscribe: 'Unsubscribe',
    lastTick: 'Last tick: {n}',
    noTick: 'No tick yet',
    echo: 'Send an echo',
    placeholder: 'Type a text',
    send: 'Send',
    replies: 'Replies',
    noReply: 'No reply yet',
    outcomes: {
      queued: 'Waiting for the connection',
      sent: 'Sent',
      delivered: 'Delivered',
      dropped: 'Not sent',
    },
    pause: 'Pause',
    resume: 'Resume',
  },
};

export interface ExampleOptions {
  /** Electron or Tauri. */
  platform: string;
  /** Where the connection is held, in each language. */
  holder: Record<Language, string>;
  /** The window's number. */
  window: number;
  /** The language to start in (default: the one chosen last, else German). */
  language?: Language;
  /** Heard once the window is connected, took three ticks and had an echo delivered. */
  onWorking?: () => void;
}

function fill(text: string, values: Record<string, string | number>): string {
  return text.replace(/\{(\w+)\}/g, (_, key: string) => String(values[key] ?? ''));
}

function remembered(): Language | null {
  try {
    const language = localStorage.getItem('wse-example-language');
    return language === 'de' || language === 'en' ? language : null;
  } catch {
    return null;
  }
}

function remember(language: Language): void {
  try {
    localStorage.setItem('wse-example-language', language);
  } catch {
    // Not kept: the page starts in German next time.
  }
}

function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attributes: Record<string, string> = {},
  ...children: Node[]
): HTMLElementTagNameMap[K] {
  const made = document.createElement(tag);
  for (const [name, value] of Object.entries(attributes)) made.setAttribute(name, value);
  made.append(...children);
  return made;
}

/** Puts the example's page into `root`, over `client`. */
export function mountExample(root: HTMLElement, client: WSEClient, options: ExampleOptions): () => void {
  let language: Language = options.language ?? remembered() ?? 'de';
  let lastTick: number | null = null;
  let ticksTaken = 0;
  let replies: string[] = [];
  let outcome: { id: string; status: Outcome } | null = null;
  let delivered = false;
  let working = false;
  let ticks: { release(): void } | null = client.subscribe(['ticks']);
  const share = client.retain();

  const heading = element('h1');
  const switcher = element('button', { type: 'button', class: 'language' });
  const connectionTitle = element('h2');
  const state = element('p', { class: 'state', role: 'status' });
  const shared = element('p', { class: 'note' });
  const pausedNote = element('p', { class: 'note paused' });
  const pause = element('button', { type: 'button' });
  const ticksTitle = element('h2');
  const tick = element('p', { class: 'tick' });
  const subscribe = element('button', { type: 'button' });
  const echoTitle = element('h2');
  const input = element('input', { type: 'text', autocomplete: 'off' });
  const send = element('button', { type: 'submit' });
  const form = element('form', {}, input, send);
  const sent = element('p', { class: 'note', role: 'status' });
  const repliesTitle = element('h3');
  const list = element('ul', { class: 'replies' });

  root.replaceChildren(
    element('header', {}, heading, switcher),
    element('section', {}, connectionTitle, state, shared, pausedNote, pause),
    element('section', {}, ticksTitle, tick, subscribe),
    element('section', {}, echoTitle, form, sent, repliesTitle, list),
  );

  const render = () => {
    const t = TEXTS[language];
    const snapshot = client.state.getSnapshot();
    const paused = snapshot.paused.includes('user');
    document.documentElement.lang = language;
    document.title = fill(t.title, { platform: options.platform, window: options.window });
    heading.textContent = document.title;
    switcher.textContent = t.language;
    switcher.setAttribute('lang', language === 'de' ? 'en' : 'de');
    connectionTitle.textContent = t.connection;
    state.textContent = t.states[snapshot.connection] ?? t.states.error;
    state.dataset.state = snapshot.connection;
    shared.textContent = fill(t.shared, { holder: options.holder[language] });
    pausedNote.textContent = paused ? t.paused : '';
    pausedNote.hidden = !paused;
    pause.textContent = paused ? t.resume : t.pause;
    ticksTitle.textContent = t.ticks;
    tick.textContent = lastTick === null ? t.noTick : fill(t.lastTick, { n: lastTick });
    subscribe.textContent = ticks ? t.unsubscribe : t.subscribe;
    echoTitle.textContent = t.echo;
    input.placeholder = t.placeholder;
    send.textContent = t.send;
    sent.textContent = outcome ? t.outcomes[outcome.status] : '';
    repliesTitle.textContent = t.replies;
    list.replaceChildren(
      ...(replies.length > 0 ? replies : [t.noReply]).map((text) => element('li', {}, document.createTextNode(text))),
    );
    if (!working && options.onWorking && snapshot.ready && ticksTaken >= 3 && delivered) {
      working = true;
      options.onWorking();
    }
  };

  const echo = (text: string) => {
    const id = `echo-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    outcome = { id, status: client.send('echo', { text }, { id }) };
    render();
  };

  const stops = [
    client.state.listen(render),
    client.onMessage('tick', (message) => {
      lastTick = message.p?.n ?? null;
      ticksTaken++;
      render();
    }),
    client.onMessage('echoed', (message) => {
      replies = [String(message.p?.text ?? ''), ...replies].slice(0, 5);
      render();
    }),
    client.on('wseDelivered', (detail: { ids: string[] }) => {
      if (outcome && detail.ids.includes(outcome.id)) {
        outcome = { ...outcome, status: 'delivered' };
        delivered = true;
        render();
      }
    }),
    client.on('wseNotDelivered', (detail: { ids: string[] }) => {
      if (outcome && detail.ids.includes(outcome.id)) {
        outcome = { ...outcome, status: 'dropped' };
        render();
      }
    }),
  ];

  switcher.addEventListener('click', () => {
    language = language === 'de' ? 'en' : 'de';
    remember(language);
    render();
  });
  pause.addEventListener('click', () => {
    if (client.state.getSnapshot().paused.includes('user')) client.resume('user');
    else client.pause('user');
    render();
  });
  subscribe.addEventListener('click', () => {
    if (ticks) {
      ticks.release();
      ticks = null;
    } else {
      ticks = client.subscribe(['ticks']);
    }
    render();
  });
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const text = input.value.trim();
    if (text === '') return;
    input.value = '';
    echo(text);
  });

  // An application that checks itself sends an echo of its own.
  if (options.onWorking) {
    const first = client.state.listen((snapshot) => {
      if (!snapshot.ready) return;
      first();
      echo(`${options.platform} ${options.window}`);
    });
    stops.push(first);
  }

  render();
  return () => {
    for (const stop of stops) stop();
    ticks?.release();
    share.release();
  };
}
