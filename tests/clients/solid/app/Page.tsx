// The page of the check: the connection's state and the ticks heard, in
// German and in English. A client of its own per render: on the server it
// renders the client's first state and takes no connection; in the browser
// the connection, the ticks topic and the tick messages are taken after the
// first render.
import { createMemo, createSignal, onCleanup } from 'solid-js';
import { createWSEClient, createWSEState, createWSETopics, onWSEMessage } from 'wse-client/solid';
import { wseEndpoint } from './endpoint';
import { TEXT, type Language } from './text';

export default function Page() {
  const client = createWSEClient({ endpoints: [wseEndpoint()] });
  onCleanup(() => client.destroy());
  const [language, setLanguage] = createSignal<Language>('de');
  const text = createMemo(() => TEXT[language()]);
  const state = createWSEState(client);
  const [ticks, setTicks] = createSignal(0);
  createWSETopics(['ticks'], client);
  onWSEMessage('tick', () => setTicks((n) => n + 1), client);
  return (
    <main lang={language()}>
      <h1>{text().title}</h1>
      <p>
        {text().connection}:{' '}
        <span data-test="connection" data-state={state().connection}>
          {text().states[state().connection]}
        </span>
      </p>
      <p>
        {text().received}: <span data-test="ticks">{ticks()}</span>
      </p>
      <button type="button" data-test="language" onClick={() => setLanguage((l) => (l === 'de' ? 'en' : 'de'))}>
        {text().other}
      </button>
    </main>
  );
}
