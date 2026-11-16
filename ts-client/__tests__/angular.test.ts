// @vitest-environment happy-dom
//
// Angular over a client of its own: the helpers take the client's
// connection, topics and listeners after the first render and let them go
// with their injector; a handler gets the client's own message object, and
// its failure goes to the application's ErrorHandler.

import '@angular/compiler';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  Component,
  EnvironmentInjector,
  ErrorHandler,
  createEnvironmentInjector,
  runInInjectionContext,
  signal,
} from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import type { WSMessage } from '../types.js';
import {
  WSE_CLIENT,
  injectWSEEvent,
  injectWSEMessage,
  injectWSEState,
  injectWSETopics,
  provideWSEClient,
} from '../angular/index.js';

const URL = 'ws://angular.test/wse';
let client: WSEClient;

beforeAll(() => {
  TestBed.initTestEnvironment(BrowserTestingModule, platformBrowserTesting());
});

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  client = createWSEClient({ endpoints: [URL], offline: { enabled: false } });
});

afterEach(() => {
  TestBed.resetTestingModule();
  client.destroy();
  resetClientState();
  vi.unstubAllGlobals();
});

const updates = () =>
  FakeWebSocket.last()
    .messagesOfType('subscription_update')
    .map((m) => `${m.p.action}:${[...m.p.topics].sort().join(',')}`);

describe('angular', () => {
  it('connects after the first render and lets go with its injector', async () => {
    TestBed.configureTestingModule({ providers: [{ provide: WSE_CLIENT, useValue: client }] });
    const child = createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
    const state = runInInjectionContext(child, () => injectWSEState());
    // Nothing while injecting: a server render stops here.
    expect(FakeWebSocket.instances).toHaveLength(0);
    expect(state().connection).toBe('pending');
    TestBed.tick();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    expect(state().connection).toBe('connected');
    child.destroy();
    await tick();
    expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0);
  });

  it('an injector ended before its first render never connects', async () => {
    TestBed.configureTestingModule({ providers: [{ provide: WSE_CLIENT, useValue: client }] });
    const child = createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
    runInInjectionContext(child, () => {
      injectWSEState();
      injectWSETopics(['a']);
      injectWSEMessage('price_update', () => {});
    });
    child.destroy();
    TestBed.tick();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it('renders the state, and a changed topic list keeps the topics it still holds', async () => {
    const topic = signal('b');
    const Status = Component({ selector: 'wse-status', template: '<p>{{ state().connection }}</p>' })(
      class {
        state = injectWSEState();
        constructor() {
          injectWSETopics(() => ['a', topic()]);
        }
      },
    );
    TestBed.configureTestingModule({ providers: [{ provide: WSE_CLIENT, useValue: client }] });
    const fixture = TestBed.createComponent(Status);
    await fixture.whenStable();
    // Connecting once rendered; the subscription goes with the connection.
    expect(fixture.nativeElement.textContent).toBe('connecting');
    await tick();
    await connectServer(FakeWebSocket.last());
    await fixture.whenStable();
    expect(fixture.nativeElement.textContent).toBe('connected');
    topic.set('c');
    await fixture.whenStable();
    await tick();
    // 'a' is held throughout: the new list is taken before the old one goes.
    expect(updates()).toEqual(['subscribe:a,b', 'subscribe:c', 'unsubscribe:b']);
    fixture.destroy();
  });

  it('a hundred mounts leave no lease and no listener behind', async () => {
    // What the client holds for the helpers, counted as it is taken and let go.
    let held = 0;
    const counted = <T extends (...args: any[]) => any>(take: T) =>
      ((...args: Parameters<T>) => {
        const taken = take(...args);
        held++;
        let gone = false;
        const letGo = () => {
          if (!gone) held--;
          gone = true;
        };
        return typeof taken === 'function'
          ? () => (letGo(), taken())
          : { release: () => (letGo(), taken.release()) };
      }) as T;
    const own = {
      retain: client.retain.bind(client),
      subscribe: client.subscribe.bind(client),
      onMessage: client.onMessage.bind(client),
      listen: client.state.listen.bind(client.state),
    };
    vi.spyOn(client, 'retain').mockImplementation(counted(own.retain));
    vi.spyOn(client, 'subscribe').mockImplementation(counted(own.subscribe));
    vi.spyOn(client, 'onMessage').mockImplementation(counted(own.onMessage));
    vi.spyOn(client.state, 'listen').mockImplementation(counted(own.listen));
    const Status = Component({ selector: 'wse-status', template: '{{ state().connection }}' })(
      class {
        state = injectWSEState();
        constructor() {
          injectWSETopics(['news']);
          injectWSEMessage('news', () => {});
        }
      },
    );
    TestBed.configureTestingModule({ providers: [{ provide: WSE_CLIENT, useValue: client }] });
    let most = 0;
    for (let i = 0; i < 100; i++) {
      const fixture = TestBed.createComponent(Status);
      await fixture.whenStable();
      most = Math.max(most, held);
      fixture.destroy();
    }
    // Each mount took a state listener, a topic lease, a message listener and
    // three connection leases; nothing is left once it is gone.
    expect(most).toBe(6);
    expect(held).toBe(0);
  });

  it("a handler gets the client's own message; its failure goes to the ErrorHandler", async () => {
    const seen: WSMessage[] = [];
    const failures: unknown[] = [];
    const Listener = Component({ selector: 'wse-listener', template: '' })(
      class {
        constructor() {
          injectWSEMessage('price_update', (message) => {
            seen.push(message);
            if (seen.length === 1) throw new Error('a broken handler');
          });
        }
      },
    );
    TestBed.configureTestingModule({
      providers: [
        { provide: WSE_CLIENT, useValue: client },
        { provide: ErrorHandler, useValue: { handleError: (error: unknown) => failures.push(error) } },
      ],
    });
    const fixture = TestBed.createComponent(Listener);
    await fixture.whenStable();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 1 }, v: 1 });
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 2 }, v: 1 });
    await tick();
    expect(seen.map((m) => m.p)).toEqual([{ n: 1 }, { n: 2 }]);
    expect(failures).toHaveLength(1);
    fixture.destroy();
  });

  it("hears the client's events while its injector lives, holding nothing", async () => {
    const heard: unknown[] = [];
    TestBed.configureTestingModule({ providers: [{ provide: WSE_CLIENT, useValue: client }] });
    const child = createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
    runInInjectionContext(child, () => injectWSEEvent('presenceJoin', (detail) => heard.push(detail)));
    TestBed.tick();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(0);
    const lease = client.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    ws.serverSend({ c: 'WSE', t: 'presence_join', p: { user: 'bob' }, v: 1 });
    await tick();
    child.destroy();
    ws.serverSend({ c: 'WSE', t: 'presence_join', p: { user: 'eve' }, v: 1 });
    await tick();
    expect(heard).toEqual([{ user: 'bob' }]);
    lease.release();
  });

  it('provideWSEClient: a client per injector, destroyed with it; one owned elsewhere is refused', async () => {
    const made: WSEClient[] = [];
    const Owner = Component({
      selector: 'wse-owner',
      template: '',
      providers: [
        provideWSEClient(() => {
          const c = createWSEClient({ endpoints: [URL], offline: { enabled: false } });
          made.push(c);
          return c;
        }),
      ],
    })(
      class {
        state = injectWSEState();
      },
    );
    const a = TestBed.createComponent(Owner);
    const b = TestBed.createComponent(Owner);
    await a.whenStable();
    await b.whenStable();
    expect(made).toHaveLength(2);
    expect(made[0]).not.toBe(made[1]);
    const destroyed = made.map((c) => vi.spyOn(c, 'destroy'));
    a.destroy();
    b.destroy();
    expect(destroyed.map((d) => d.mock.calls.length)).toEqual([1, 1]);

    const Shared = Component({ selector: 'wse-shared', template: '', providers: [provideWSEClient(() => client)] })(
      class {
        state = injectWSEState();
      },
    );
    TestBed.createComponent(Shared).destroy();
    expect(() => TestBed.createComponent(Shared)).toThrow(/another injector/);
  });

  it('outside an injection context, without an injector, it says so', () => {
    expect(() => injectWSEState({ client })).toThrow();
  });
});
