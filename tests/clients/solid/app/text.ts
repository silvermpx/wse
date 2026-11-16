// The page's words, in German and in English.
import type { WSEClientSnapshot } from 'wse-client/solid';

export type Language = 'de' | 'en';

export const TEXT: Record<
  Language,
  { title: string; connection: string; received: string; other: string; states: Record<WSEClientSnapshot['connection'], string> }
> = {
  de: {
    title: 'WSE-Prüfung',
    connection: 'Verbindung',
    received: 'Nachrichten empfangen',
    other: 'English',
    states: {
      pending: 'wartet',
      connecting: 'wird aufgebaut',
      connected: 'verbunden',
      reconnecting: 'wird neu aufgebaut',
      disconnected: 'getrennt',
      error: 'Fehler',
      degraded: 'gestört',
    },
  },
  en: {
    title: 'WSE check',
    connection: 'Connection',
    received: 'Messages received',
    other: 'Deutsch',
    states: {
      pending: 'waiting',
      connecting: 'connecting',
      connected: 'connected',
      reconnecting: 'reconnecting',
      disconnected: 'disconnected',
      error: 'error',
      degraded: 'degraded',
    },
  },
};
