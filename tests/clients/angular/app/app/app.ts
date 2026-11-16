// The page of the check: the connection's state and the ticks heard, in
// German and in English. On the server it renders the client's first state
// and takes no connection; in the browser the connection, the ticks topic and
// the tick messages are taken after the first render.
import { Component, computed, signal } from '@angular/core';
import { injectWSEMessage, injectWSEState, injectWSETopics, type WSEClientSnapshot } from 'wse-client/angular';

type Language = 'de' | 'en';

const TEXT: Record<
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

@Component({
  selector: 'app-root',
  template: `
    <main [attr.lang]="language()">
      <h1>{{ text().title }}</h1>
      <p>
        {{ text().connection }}:
        <span data-test="connection" [attr.data-state]="state().connection">{{ text().states[state().connection] }}</span>
      </p>
      <p>
        {{ text().received }}: <span data-test="ticks">{{ ticks() }}</span>
      </p>
      <button type="button" data-test="language" (click)="switchLanguage()">{{ text().other }}</button>
    </main>
  `,
})
export class App {
  protected readonly language = signal<Language>('de');
  protected readonly text = computed(() => TEXT[this.language()]);
  protected readonly state = injectWSEState();
  protected readonly ticks = signal(0);

  constructor() {
    injectWSETopics(['ticks']);
    injectWSEMessage('tick', () => this.ticks.update((n) => n + 1));
  }

  protected switchLanguage(): void {
    this.language.update((language) => (language === 'de' ? 'en' : 'de'));
  }
}
