// =============================================================================
// WebSocket Engine - Store
// =============================================================================
//
// A small store with zustand's vanilla contract: each client of the core keeps
// its state in one, and the React entry builds its zustand hooks from the same
// state creators, so both read and write one object.

/** Merges `partial` into the state, or with `replace` puts `state` in its place. */
export interface SetState<T> {
  (partial: T | Partial<T> | ((state: T) => T | Partial<T>), replace?: false): void;
  (state: T | ((state: T) => T), replace: true): void;
}

export type GetState<T> = () => T;

export interface StoreApi<T> {
  getState: GetState<T>;
  getInitialState: GetState<T>;
  setState: SetState<T>;
  /** Called after every change with the new state and the one before; returns the unsubscribe. */
  subscribe: (listener: (state: T, previous: T) => void) => () => void;
}

/** A store's contents and actions: zustand's `(set, get, api) => state`. */
export type StateCreator<T> = (set: SetState<T>, get: GetState<T>, api: StoreApi<T>) => T;

export function createStore<T>(creator: StateCreator<T>): StoreApi<T> {
  let state: T;
  const listeners = new Set<(state: T, previous: T) => void>();

  const setState = ((partial: unknown, replace?: boolean) => {
    const next = typeof partial === 'function' ? (partial as (s: T) => unknown)(state) : partial;
    if (Object.is(next, state)) return;
    const previous = state;
    state =
      (replace ?? (typeof next !== 'object' || next === null))
        ? (next as T)
        : Object.assign({}, state, next);
    for (const listener of listeners) listener(state, previous);
  }) as SetState<T>;

  const getState: GetState<T> = () => state;
  const api: StoreApi<T> = {
    getState,
    getInitialState: () => initial,
    setState,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
  const initial = (state = creator(setState, getState, api));
  return api;
}
