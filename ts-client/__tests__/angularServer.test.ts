// The server render of Angular, in Node: a platform per request, each with a
// client of its own that shows its first state, opens nothing, starts no
// timer and is destroyed with the request.

import '@angular/compiler';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Component, PendingTasks, inject, signal } from '@angular/core';
import { bootstrapApplication, provideClientHydration, type BootstrapContext } from '@angular/platform-browser';
import { provideServerRendering, renderApplication } from '@angular/platform-server';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { injectWSEEvent, injectWSEMessage, injectWSEState, injectWSETopics, provideWSEClient } from '../angular/index.js';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('the Angular server render', () => {
  it('gives each request a client of its own and leaves nothing running', async () => {
    expect(typeof window).toBe('undefined');
    const opened = vi.fn();
    vi.stubGlobal('WebSocket', opened);
    const made: WSEClient[] = [];
    const destroyed: WSEClient[] = [];
    const App = Component({ selector: 'app-root', template: '<p>connection: {{ state().connection }}</p>' })(
      class {
        state = injectWSEState();
        constructor() {
          injectWSETopics(['news']);
          injectWSEMessage('news', () => {});
          injectWSEEvent('wseGap', () => {});
        }
      },
    );
    const bootstrap = (context: BootstrapContext) =>
      bootstrapApplication(
        App,
        {
          providers: [
            provideServerRendering(),
            provideClientHydration(),
            provideWSEClient(() => {
              const client = createWSEClient({ endpoints: ['ws://ssr.test/wse'], offline: { enabled: false } });
              const destroy = client.destroy.bind(client);
              client.destroy = () => {
                destroyed.push(client);
                destroy();
              };
              made.push(client);
              return client;
            }),
          ],
        },
        context,
      );
    const [a, b] = await Promise.all([
      renderApplication(bootstrap, { document: '<app-root></app-root>', url: '/a' }),
      renderApplication(bootstrap, { document: '<app-root></app-root>', url: '/b' }),
    ]);
    expect(a).toContain('connection: pending');
    expect(b).toContain('connection: pending');
    expect(made).toHaveLength(2);
    expect(made[0]).not.toBe(made[1]);
    expect(destroyed).toEqual(made);
    expect(opened).not.toHaveBeenCalled();
  });

  it('takes nothing on the server even while another request is rendering', async () => {
    const opened = vi.fn();
    vi.stubGlobal('WebSocket', opened);
    const client = createWSEClient({ endpoints: ['ws://ssr.test/wse'], offline: { enabled: false } });
    const retain = vi.spyOn(client, 'retain');
    const Late = Component({ selector: 'app-late', template: 'late' })(
      class {
        constructor() {
          injectWSETopics(['news'], { client });
          injectWSEState({ client });
        }
      },
    );
    const Slow = Component({ selector: 'app-root', imports: [Late], template: '@if (ready()) { <app-late /> }' })(
      class {
        ready = signal(false);
        constructor() {
          inject(PendingTasks).run(
            () =>
              new Promise<void>((done) =>
                setTimeout(() => {
                  this.ready.set(true);
                  done();
                }, 50),
              ),
          );
        }
      },
    );
    const Fast = Component({ selector: 'app-root', template: 'fast' })(class {});
    const boot = (root: any) => (context: BootstrapContext) =>
      bootstrapApplication(root, { providers: [provideServerRendering()] }, context);
    const fast = renderApplication(boot(Fast), { document: '<app-root></app-root>', url: '/fast' });
    const slow = renderApplication(boot(Slow), { document: '<app-root></app-root>', url: '/slow' });
    await fast;
    expect(await slow).toContain('late');
    expect(retain).not.toHaveBeenCalled();
    expect(opened).not.toHaveBeenCalled();
    client.destroy();
  });
});
