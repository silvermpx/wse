// The same React and Vue pages for the server render and the hydration
// tests: a status line over a client of its own.

import { createElement } from 'react';
import { defineComponent, h, createSSRApp } from 'vue';
import { createWSEClient, type WSEClient } from '../../core/index.js';
import { WSEProvider, useWSEState, useWSETopics } from '../../react/index.js';
import { useWSEState as useVueState, useWSETopics as useVueTopics, wsePlugin } from '../../vue/index.js';

export const URL = 'ws://wse.test/wse';

export function newClient(): WSEClient {
  return createWSEClient({ endpoints: [URL], offline: { enabled: false }, topics: ['prices'] });
}

function Status() {
  const state = useWSEState();
  useWSETopics(['news']);
  return createElement('p', { id: 'status' }, `connection: ${state.connection}`);
}

export function reactPage(client: WSEClient) {
  return createElement(WSEProvider, { client }, createElement(Status));
}

const VueStatus = defineComponent({
  setup() {
    const state = useVueState();
    useVueTopics(['news']);
    return () => h('p', { id: 'status' }, `connection: ${state.value.connection}`);
  },
});

export function vuePage(client: WSEClient) {
  return createSSRApp(VueStatus).use(wsePlugin(client));
}
