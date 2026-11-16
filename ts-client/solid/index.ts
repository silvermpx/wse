// =============================================================================
// WebSocket Engine - Solid (wse-client/solid)
// =============================================================================
//
// Solid over a client of its own (createWSEClient), for Solid 1.9 and 2.0:
// functions for a component (or any owner) that hold the client's
// connection, topics and listeners from the first render in a browser to the
// owner's disposal. On a server, and while hydrating, they take nothing, and
// the page shows the client's first state. No JSX in the library. The client
// comes as an argument, as an accessor of one (a new client after a
// sign-in), or from WSEContext.

import {
  createContext,
  createEffect,
  createSignal,
  getOwner,
  onCleanup,
  untrack,
  useContext,
  type Accessor,
  type Context,
} from 'solid-js';
import type { Lease, WSEClient, WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';
import { logger } from '../utils/logger.js';

export * from '../core/index.js';

/** A client, or an accessor of one (a new client after a sign-in). */
export type WSEClientSource = WSEClient | Accessor<WSEClient>;

/** The client below it: Solid 1, `<WSEContext.Provider value={client}>`; Solid 2, `<WSEContext value={client}>`. */
export const WSEContext: Context<WSEClientSource | null> = /* @__PURE__ */ createContext<WSEClientSource | null>(
  null,
);

function sourceOf(name: string, client?: WSEClientSource): Accessor<WSEClient> {
  if (!getOwner()) throw new Error(`${name}: call it in a component, or under createRoot (no owner here)`);
  const given = client ?? useContext(WSEContext);
  if (!given) throw new Error(`${name}: no client given, and no WSEContext above`);
  return typeof given === 'function' ? given : () => given;
}

/** The client of the nearest WSEContext, as an accessor. */
export function useWSEClient(): Accessor<WSEClient> {
  return sourceOf('useWSEClient');
}

// A failure (a client already signed out) is logged: in Solid 2 an error
// thrown from an effect stops every effect without an <Errored> above.
function attempt<T>(act: (value: T) => void, value: T): void {
  try {
    act(value);
  } catch (error) {
    logger.error('wse-client/solid:', error);
  }
}

// Not on a server: Solid 2 runs no compute there, Solid 1 no effect.
const CLIENT_ONLY = { name: 'wse', ssrSource: 'client' } as const;

/**
 * Runs `act` with what `read` returns after the first render, and again
 * when it changes. Solid 1 hands an effect's first run its initial value
 * (here `apply`) and runs the effect itself after rendering; Solid 2 runs a
 * pure compute and then the effect function after the flush.
 */
function afterRender<T>(read: () => T, act: (value: T) => void): void {
  let solid1: boolean | undefined;
  const apply = (value: T): void => attempt(act, value);
  createEffect(
    (previous: unknown) => {
      const value = read();
      solid1 ??= previous === apply;
      if (solid1) untrack(() => attempt(act, value));
      return value;
    },
    apply as never,
    CLIENT_ONLY as never,
  );
}

// Each change is written, a snapshot after hydration too; a write from a
// client's listener is no component's own write (Solid 2 checks that).
const MIRROR = { equals: false, ownedWrite: true } as const;

/** The client's state: its first snapshot until the first render, then each change; holds the connection. */
export function createWSEState(client?: WSEClientSource): Accessor<WSEClientSnapshot> {
  const source = sourceOf('createWSEState', client);
  const [snapshot, set] = createSignal<WSEClientSnapshot>(untrack(source).state.getSnapshot(), MIRROR as never);
  const write = (next: WSEClientSnapshot) => set(() => next);
  let held: { client: WSEClient; connection: Lease; stop: () => void } | null = null;
  afterRender(source, (c) => {
    if (held?.client === c) return;
    const connection = c.retain();
    const stop = c.state.listen(write);
    write(c.state.getSnapshot());
    const previous = held;
    held = { client: c, connection, stop };
    if (previous) {
      previous.stop();
      previous.connection.release();
    }
  });
  onCleanup(() => {
    held?.stop();
    held?.connection.release();
    held = null;
  });
  return snapshot;
}

const listOf = (topics: readonly string[]): string[] => [...new Set(topics.filter((t) => t !== ''))].sort();
const sameList = (a: readonly string[], b: readonly string[]) => a.length === b.length && a.every((t, i) => t === b[i]);

/**
 * Hold `topics` (a list, or an accessor of topics that change). A new list
 * or a new client is taken before the old is let go: a topic in both stays
 * subscribed, with its recovery position.
 */
export function createWSETopics(topics: readonly string[] | Accessor<readonly string[]>, client?: WSEClientSource): void {
  const source = sourceOf('createWSETopics', client);
  const wanted = typeof topics === 'function' ? topics : () => topics;
  let held: { client: WSEClient; topics: string[]; connection: Lease; lease: Lease } | null = null;
  afterRender(
    () => [source(), listOf(wanted())] as const,
    ([c, next]) => {
      if (held && held.client === c && sameList(held.topics, next)) return;
      const connection = c.retain();
      const lease = c.subscribe(next);
      const previous = held;
      held = { client: c, topics: next, connection, lease };
      if (previous) {
        previous.lease.release();
        previous.connection.release();
      }
    },
  );
  onCleanup(() => {
    held?.lease.release();
    held?.connection.release();
    held = null;
  });
}

/** Every message of `type`, in order, the client's own object; the application commits. Holds the connection. */
export function onWSEMessage(type: string, handler: (message: WSMessage) => void, client?: WSEClientSource): void {
  const source = sourceOf('onWSEMessage', client);
  let held: { client: WSEClient; connection: Lease; stop: () => void } | null = null;
  afterRender(source, (c) => {
    if (held?.client === c) return;
    const connection = c.retain();
    const stop = c.onMessage(type, handler);
    const previous = held;
    held = { client: c, connection, stop };
    if (previous) {
      previous.stop();
      previous.connection.release();
    }
  });
  onCleanup(() => {
    held?.stop();
    held?.connection.release();
    held = null;
  });
}

/** The client's events (`wseGap`, ...); listens without holding the connection. */
export function onWSEEvent(event: string, handler: (detail: any) => void, client?: WSEClientSource): void {
  const source = sourceOf('onWSEEvent', client);
  let held: { client: WSEClient; stop: () => void } | null = null;
  afterRender(source, (c) => {
    if (held?.client === c) return;
    const stop = c.on(event, handler);
    const previous = held;
    held = { client: c, stop };
    previous?.stop();
  });
  onCleanup(() => {
    held?.stop();
    held = null;
  });
}
