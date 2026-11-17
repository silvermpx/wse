// =============================================================================
// WebSocket Engine - A channel of the window bridge
// =============================================================================

import type { OwnerFrame, WindowFrame } from './protocol.js';

/** One end of a window's channel: frames in order, one way and the other. */
export interface Channel<Send, Receive> {
  post(frame: Send): void;
  /** Its frames, and its end (the other side went); one listener. */
  listen(sink: { frame(frame: Receive): void; close(): void }): void;
  close(): void;
}

/** The window's end. */
export type WindowChannel = Channel<WindowFrame, OwnerFrame>;
/** The owner's end of one window's channel. */
export type OwnerChannel = Channel<OwnerFrame, WindowFrame>;

/** A MessagePort of a page, a worker or Node: each frame is copied once (structured clone). */
export interface PortLike {
  postMessage(data: unknown): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'close', listener: () => void): void;
  start?(): void;
  close(): void;
}

/** A channel over `port`. */
export function messagePortChannel<Send, Receive>(port: PortLike): Channel<Send, Receive> {
  let closed = false;
  let sink: { frame(frame: Receive): void; close(): void } | null = null;
  const end = () => {
    if (closed) return;
    closed = true;
    sink?.close();
  };
  port.addEventListener('message', (event) => {
    if (!closed) sink?.frame(event.data as Receive);
  });
  // A port whose other end went (Chrome and Node say so; a page that is
  // gone takes its port with it).
  port.addEventListener('close', end);
  return {
    post(frame) {
      if (closed) return;
      port.postMessage(frame);
    },
    listen(next) {
      sink = next;
      port.start?.();
    },
    close() {
      if (closed) return;
      closed = true;
      port.close();
    },
  };
}
