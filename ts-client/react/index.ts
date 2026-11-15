'use client';
// =============================================================================
// WebSocket Engine - React (wse-client/react)
// =============================================================================
//
// React over a client of its own (createWSEClient): a provider, and hooks
// that read its state through useSyncExternalStore and hold its connection,
// topics and listeners while a component is mounted. Effects never run while
// rendering on a server, so server rendering opens no socket. Needs React,
// not zustand; the page-wide useWSE() stays in the root package.

import { createContext, createElement, useContext, useEffect, useRef, useSyncExternalStore } from 'react';
import type { ReactNode } from 'react';
import type { WSEClient, WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';

export * from '../core/index.js';

const ClientContext = createContext<WSEClient | null>(null);

export interface WSEProviderProps {
  client: WSEClient;
  children?: ReactNode;
}

/** Gives the components below it `client`, and holds a share of it while mounted. */
export function WSEProvider({ client, children }: WSEProviderProps) {
  useEffect(() => {
    const lease = client.retain();
    return () => lease.release();
  }, [client]);
  return createElement(ClientContext.Provider, { value: client }, children);
}

function useClient(client: WSEClient | undefined, hook: string): WSEClient {
  const provided = useContext(ClientContext);
  const found = client ?? provided;
  if (!found) throw new Error(`${hook}: no client given, and no <WSEProvider> above`);
  return found;
}

/** The client of the nearest <WSEProvider>. */
export function useWSEClient(): WSEClient {
  return useClient(undefined, 'useWSEClient');
}

/** The client's state; the component renders again when it changes. */
export function useWSEState(client?: WSEClient): WSEClientSnapshot {
  const c = useClient(client, 'useWSEState');
  return useSyncExternalStore(c.state.listen, c.state.getSnapshot, c.state.getSnapshot);
}

/** Hold `topics` while the component is mounted. */
export function useWSETopics(topics: string[], client?: WSEClient): void {
  const c = useClient(client, 'useWSETopics');
  const key = JSON.stringify([...new Set(topics)].sort());
  useEffect(() => {
    const lease = c.subscribe(JSON.parse(key) as string[]);
    return () => lease.release();
  }, [c, key]);
}

/** Hear the messages of `type` while the component is mounted; `handler` may change each render. */
export function useWSEMessage(type: string, handler: (message: WSMessage) => void, client?: WSEClient): void {
  const c = useClient(client, 'useWSEMessage');
  const latest = useRef(handler);
  useEffect(() => {
    latest.current = handler;
  });
  useEffect(() => c.onMessage(type, (message) => latest.current(message)), [c, type]);
}

/** Hear the client's events of `event` (`wseGap`, ...) while the component is mounted. */
export function useWSEEvent(event: string, handler: (detail: any) => void, client?: WSEClient): void {
  const c = useClient(client, 'useWSEEvent');
  const latest = useRef(handler);
  useEffect(() => {
    latest.current = handler;
  });
  useEffect(() => c.on(event, (detail) => latest.current(detail)), [c, event]);
}
