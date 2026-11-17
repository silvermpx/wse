// @vitest-environment happy-dom
//
// The example applications' page speaks German and English, whole: every
// word has both, and a page switched to one shows no word of the other.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/client.js';
import { TEXTS, mountExample, type Language } from '../../bindings/example-ui/ui.js';

/** Every text of a language, by its path. */
function leaves(value: unknown, path = ''): Record<string, string> {
  if (typeof value === 'string') return { [path]: value };
  const out: Record<string, string> = {};
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    Object.assign(out, leaves(child, path ? `${path}.${key}` : key));
  }
  return out;
}

const placeholders = (text: string) => [...text.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();

/** What the page shows: its text, each field's placeholder, and its title. */
function shown(): string[] {
  const texts: string[] = [document.title];
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    const text = node.textContent?.trim();
    if (text) texts.push(text);
  }
  for (const input of document.querySelectorAll('input')) texts.push(input.placeholder);
  return texts;
}

let client: WSEClient;

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  client = createWSEClient({ endpoints: ['ws://example.test/wse'], offline: { enabled: false } });
  localStorage.clear();
});

afterEach(() => {
  client.destroy();
  document.body.replaceChildren();
  vi.unstubAllGlobals();
});

describe('the example page', () => {
  it('has every text in German and in English, and the two differ', () => {
    const de = leaves(TEXTS.de);
    const en = leaves(TEXTS.en);
    expect(Object.keys(de).sort()).toEqual(Object.keys(en).sort());
    for (const key of Object.keys(de)) {
      expect(de[key].trim(), key).not.toBe('');
      expect(en[key].trim(), key).not.toBe('');
      expect(de[key], key).not.toBe(en[key]);
      expect(placeholders(de[key]), key).toEqual(placeholders(en[key]));
    }
  });

  it('shows no word of the other language, whichever it is switched to', async () => {
    const root = document.createElement('main');
    document.body.append(root);
    const stop = mountExample(root, client, {
      platform: 'Electron',
      holder: { de: 'Hauptprozess', en: 'main process' },
      window: 1,
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await connectServer(FakeWebSocket.last());
    const other = (language: Language) => new Set(Object.values(leaves(TEXTS[language === 'de' ? 'en' : 'de'])));
    for (const language of ['de', 'en', 'de'] as Language[]) {
      expect(document.documentElement.lang).toBe(language);
      const foreign = other(language);
      for (const text of shown()) expect(foreign.has(text), `"${text}" on the ${language} page`).toBe(false);
      expect(shown()).toContain(TEXTS[language].connection);
      (root.querySelector('button.language') as HTMLButtonElement).click();
    }
    expect(localStorage.getItem('wse-example-language')).toBe('en');
    stop();
  });
});
