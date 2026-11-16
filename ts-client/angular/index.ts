// =============================================================================
// WebSocket Engine - Angular (wse-client/angular)
// =============================================================================
//
// Angular over a client of its own (createWSEClient): an injection token, a
// provider that owns a client per injector, and functions for an injection
// context that hold the client's connection, topics and listeners from the
// first render in a browser to the end of their injector. On a server they
// take nothing, and the page shows the client's first state. Plain functions
// over @angular/core and @angular/common: no decorators, no RxJS.

import { isPlatformBrowser } from '@angular/common';
import {
  DestroyRef,
  ErrorHandler,
  InjectionToken,
  Injector,
  NgZone,
  PLATFORM_ID,
  afterNextRender,
  assertInInjectionContext,
  computed,
  effect,
  inject,
  signal,
  untracked,
  type Provider,
  type Signal,
} from '@angular/core';
import type { Lease, WSEClient, WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';

export * from '../core/index.js';

/** The client of an injector. */
export const WSE_CLIENT = new InjectionToken<WSEClient>('WSE_CLIENT');

const owned = new WeakSet<WSEClient>();

/**
 * A client per injector: `factory` runs in that injector's context the first
 * time something injects WSE_CLIENT (on a server, once per request), and the
 * client is destroyed with the injector. A factory that returns a client
 * another injector owns is refused.
 */
export function provideWSEClient(factory: () => WSEClient): Provider {
  return {
    provide: WSE_CLIENT,
    useFactory: () => {
      const client = factory();
      if (owned.has(client)) {
        throw new Error('provideWSEClient: the factory returned a client another injector owns');
      }
      owned.add(client);
      inject(DestroyRef).onDestroy(() => client.destroy());
      return client;
    },
  };
}

export interface WSEInjectOptions {
  /** This client instead of the injector's WSE_CLIENT. */
  client?: WSEClient;
  /** For a call outside an injection context, as effect() and toSignal() take one. */
  injector?: Injector;
}

export interface WSEHoldOptions extends WSEInjectOptions {
  /** Hold the client's connection while the injector lives (default true). */
  hold?: boolean;
}

function injectorOf(fn: (...args: any[]) => unknown, options?: { injector?: Injector }): Injector {
  if (options?.injector) return options.injector;
  assertInInjectionContext(fn);
  return inject(Injector);
}

/** The injector's client. */
export function injectWSEClient(options?: { injector?: Injector }): WSEClient {
  const client = injectorOf(injectWSEClient, options).get(WSE_CLIENT, null);
  if (!client) throw new Error('injectWSEClient: no client here; provideWSEClient() in a provider above gives one');
  return client;
}

interface Place {
  injector: Injector;
  browser: boolean;
  destroyRef: DestroyRef;
}

function placeOf(fn: (...args: any[]) => unknown, options?: WSEInjectOptions): Place {
  const injector = injectorOf(fn, options);
  return {
    injector,
    browser: isPlatformBrowser(injector.get(PLATFORM_ID)),
    destroyRef: injector.get(DestroyRef),
  };
}

function clientOf(place: Place, options?: WSEInjectOptions): WSEClient {
  return options?.client ?? injectWSEClient({ injector: place.injector });
}

/**
 * After the first render, outside Angular's zone: the socket's own timers and
 * reads keep no zone-based application from becoming stable. An injector
 * ended before its first render runs nothing.
 */
function onMounted(place: Place, run: () => void): void {
  const zone = place.injector.get(NgZone);
  afterNextRender(() => zone.runOutsideAngular(run), { injector: place.injector });
}

/** The client's state: its first snapshot until the first render (what a server rendered), then each change. */
export function injectWSEState(options?: WSEHoldOptions): Signal<WSEClientSnapshot> {
  const place = placeOf(injectWSEState, options);
  const client = clientOf(place, options);
  const state = signal(client.state.getSnapshot());
  if (!place.browser) return state.asReadonly();
  let stop: (() => void) | null = null;
  let connection: Lease | null = null;
  place.destroyRef.onDestroy(() => {
    stop?.();
    connection?.release();
    stop = connection = null;
  });
  onMounted(place, () => {
    // Listening first, then reading: no change between the two is missed.
    stop = client.state.listen((snapshot) => state.set(snapshot));
    state.set(client.state.getSnapshot());
    if (options?.hold !== false) connection = client.retain();
  });
  return state.asReadonly();
}

const listOf = (topics: readonly string[]): string[] => [...new Set(topics)].sort();
const sameList = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((t, i) => t === b[i]);

/**
 * Hold `topics` (a list, or a signal or computation of one) while the
 * injector lives. A changed list is taken before the old one is let go: a
 * topic in both stays subscribed, with its recovery position.
 */
export function injectWSETopics(
  topics: readonly string[] | (() => readonly string[]),
  options?: WSEHoldOptions & { recover?: boolean },
): void {
  const place = placeOf(injectWSETopics, options);
  if (!place.browser) return;
  const client = clientOf(place, options);
  const how = options?.recover === undefined ? undefined : { recover: options.recover };
  let lease: Lease | null = null;
  let connection: Lease | null = null;
  let mounted = false;
  place.destroyRef.onDestroy(() => {
    lease?.release();
    connection?.release();
    lease = connection = null;
  });
  const wanted = typeof topics === 'function' ? computed(() => listOf(topics()), { equal: sameList }) : null;
  const fixed = wanted ? null : listOf(topics as readonly string[]);
  onMounted(place, () => {
    mounted = true;
    lease = client.subscribe(wanted ? untracked(wanted) : fixed!, how);
    if (options?.hold !== false) connection = client.retain();
  });
  if (wanted) {
    const zone = place.injector.get(NgZone);
    effect(
      () => {
        const next = wanted();
        if (!mounted) return;
        untracked(() =>
          zone.runOutsideAngular(() => {
            const previous = lease;
            lease = client.subscribe(next, how);
            previous?.release();
          }),
        );
      },
      { injector: place.injector },
    );
  }
}

/** Hear the messages of `type` while the injector lives: in order, the client's own message objects; the application commits. */
export function injectWSEMessage(
  type: string,
  handler: (message: WSMessage) => void,
  options?: WSEHoldOptions,
): void {
  const place = placeOf(injectWSEMessage, options);
  if (!place.browser) return;
  const client = clientOf(place, options);
  const errors = place.injector.get(ErrorHandler);
  const hear = (message: WSMessage) => {
    try {
      untracked(() => handler(message));
    } catch (error) {
      errors.handleError(error);
    }
  };
  let stop: (() => void) | null = null;
  let connection: Lease | null = null;
  place.destroyRef.onDestroy(() => {
    stop?.();
    connection?.release();
    stop = connection = null;
  });
  onMounted(place, () => {
    stop = client.onMessage(type, hear);
    if (options?.hold !== false) connection = client.retain();
  });
}

/** Hear the client's events (`wseGap`, `wseEncryptionFailed`, ...) while the injector lives; holds nothing. */
export function injectWSEEvent(event: string, handler: (detail: any) => void, options?: WSEInjectOptions): void {
  const place = placeOf(injectWSEEvent, options);
  if (!place.browser) return;
  const client = clientOf(place, options);
  const errors = place.injector.get(ErrorHandler);
  let stop: (() => void) | null = null;
  place.destroyRef.onDestroy(() => {
    stop?.();
    stop = null;
  });
  onMounted(place, () => {
    stop = client.on(event, (detail) => {
      try {
        untracked(() => handler(detail));
      } catch (error) {
        errors.handleError(error);
      }
    });
  });
}
