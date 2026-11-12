// =============================================================================
// WebSocket Engine - Message Processing
// =============================================================================
//
// Inbound: frames are processed strictly one after another in arrival order
// (an encrypted frame that waits for its key never lets a later frame
// overtake it). Recovery-stamped topic messages (tp/e/o) are deduplicated by
// (epoch, offset); a gap never blacks a topic out -- the message is delivered
// and a `wseGap` event reports what is missing. Only while a requested replay
// can still fill the hole (right after a reconnect) are out-of-order messages
// held, for at most recoveryTimeoutMs.
//
// Outbound: this processor is the only consumer of the outgoing queue. A
// message that cannot go out yet stays queued (in priority/FIFO order) and is
// flushed when the connection becomes ready; nothing is dropped because the
// socket was busy, reconnecting or rate limited.

import {
  WSMessage,
  QueuedMessage,
  MessagePriority,
  ConnectionQuality,
  ConnectionState,
  CircuitBreakerState,
  SendStatus,
  WSEGapEvent,
} from '../types.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { EventSequencer } from './EventSequencer.js';
import { CompressionManager, DecompressedSizeLimitError } from '../protocols/compression.js';
import { logger } from '../utils/logger.js';
import {
  WS_CLIENT_VERSION,
  WS_PROTOCOL_VERSION,
  DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
  DEFAULT_RECOVERY_TIMEOUT_MS,
} from '../constants.js';
import { securityManager } from '../utils/security.js';
import { generateId } from '../utils/id.js';
import { dispatchWSEEvent } from '../utils/events.js';
import type { SendResult } from './ConnectionManager.js';
import type { OfflineQueue } from './OfflineQueue.js';

/** What the processor needs from the connection (ConnectionManager implements it). */
export interface ProcessorConnection {
  trySend(message: WSMessage): SendResult;
  sendControl(data: string | ArrayBuffer): boolean;
  isTransportReady(): boolean;
  isAwaitingEncryptionKey(): boolean;
  getRetryAfterMs(): number;
  handleServerReady(details: any): void;
  handleServerHello(payload: any): Promise<void>;
  failEncryption(reason: string): void;
  markAuthRejected(): void;
}

export interface MessageProcessorOptions {
  /** Inbound size limit in bytes (characters for text frames); checked while inflating. */
  maxMessageSize?: number;
  /** Hold window for out-of-order messages while a requested replay may fill a gap. */
  recoveryTimeoutMs?: number;
  /** Opt-in: drop application messages of one type arriving faster than this (ms). */
  eventThrottleMs?: number;
  /** Shared sequencer (keeps dedup/recovery positions across sessions). */
  sequencer?: EventSequencer;
  /** IndexedDB queue replayed when the connection becomes ready. */
  offlineQueue?: OfflineQueue | null;
}

interface RecoveryWindow {
  epoch: string;
  buffer: Map<number, WSMessage>;
  timer: ReturnType<typeof setTimeout> | null;
  confirmed: boolean;
}

// Protocol/system message types: never throttled.
const SYSTEM_TYPES = new Set([
  'server_ready',
  'server_hello',
  'client_hello_ack',
  'subscription_update',
  'error',
  'ping',
  'PING',
  'pong',
  'PONG',
  'connection_state_change',
  'health_check',
  'health_check_response',
  'rate_limit_warning',
  'connection_quality',
  'snapshot_complete',
  'heartbeat',
  'batch',
]);

const MAX_RECOVERY_BUFFER = 1000;
const MAX_EARLY_ENCRYPTED_FRAMES = 1000;
const MAX_LOGGED_UNHANDLED_TYPES = 1000;
const PARKED = Symbol('parked');

function isSystemType(type: string): boolean {
  return SYSTEM_TYPES.has(type) || type.includes('snapshot');
}

function stripCategoryPrefix(text: string): { category: 'WSE' | 'S' | 'U' | null; json: string } {
  if (text.startsWith('WSE{') || text.startsWith('WSE[')) return { category: 'WSE', json: text.slice(3) };
  if (text.startsWith('S{') || text.startsWith('S[')) return { category: 'S', json: text.slice(1) };
  if (text.startsWith('U{') || text.startsWith('U[')) return { category: 'U', json: text.slice(1) };
  return { category: null, json: text };
}

function asArrayBuffer(data: unknown): ArrayBuffer | null {
  if (data instanceof ArrayBuffer) return data;
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength) as ArrayBuffer;
  }
  // Cross-realm ArrayBuffer (e.g. from another frame or test environment)
  if (data && typeof (data as ArrayBuffer).byteLength === 'number' && typeof (data as ArrayBuffer).slice === 'function') {
    return data as ArrayBuffer;
  }
  return null;
}

export class MessageProcessor {
  private sequencer: EventSequencer;
  private readonly ownsSequencer: boolean;
  private compression: CompressionManager;
  private messageHandlers: Map<string, (message: WSMessage) => void>;
  private connectionManager: ProcessorConnection | null = null;
  private isReady = false;
  private destroyed = false;
  private waitForHandlersInterval: ReturnType<typeof setInterval> | null = null;

  // Optional sink for measured round-trip latency (ms), set by the host. The
  // PONG handling is the single place RTT is computed, so anything that wants
  // latency (e.g. the NetworkMonitor's jitter/quality window) subscribes here.
  private onLatency: ((latencyMs: number) => void) | null = null;
  private onGap: ((gap: WSEGapEvent) => void) | null = null;

  // Inbound
  private inboundChain: Promise<void> = Promise.resolve();
  private maxMessageSize: number;
  private recoveryTimeoutMs: number;
  private eventThrottleMs: number;
  private lastEventTime = new Map<string, number>();
  private recoveryWindows = new Map<string, RecoveryWindow>();
  private earlyEncryptedFrames: ArrayBuffer[] = [];
  private loggedEventVersions = new Set<string>();
  private unhandledTypesLogged = new Set<string>();

  // Outbound
  private offlineQueue: OfflineQueue | null;
  private flushing = false;
  private flushRequested = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private flushDue = 0;

  // Message types logged at debug level (kept for API compatibility)
  private highFrequencyTypes: Set<string> = new Set([
    'heartbeat',
    'health_check',
    'health_check_response',
    'metrics_response',
    'PONG',
  ]);

  constructor(
    private batchSize: number = 10,
    private batchTimeout: number = 100,
    options: MessageProcessorOptions = {},
  ) {
    this.ownsSequencer = !options.sequencer;
    this.sequencer = options.sequencer ?? new EventSequencer();
    this.maxMessageSize = options.maxMessageSize && options.maxMessageSize > 0
      ? options.maxMessageSize
      : DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    this.compression = new CompressionManager(1024, 6, this.maxMessageSize);
    this.recoveryTimeoutMs = options.recoveryTimeoutMs ?? DEFAULT_RECOVERY_TIMEOUT_MS;
    this.eventThrottleMs = options.eventThrottleMs ?? 0;
    this.offlineQueue = options.offlineQueue ?? null;
    this.messageHandlers = new Map();

    this.registerDefaultHandlers();
  }

  setLatencyListener(fn: ((latencyMs: number) => void) | null): void {
    this.onLatency = fn;
  }

  /** Called for every detected gap (also dispatched as the `wseGap` window event). */
  setGapListener(fn: ((gap: WSEGapEvent) => void) | null): void {
    this.onGap = fn;
  }

  setConnectionManager(manager: ProcessorConnection | null): void {
    this.connectionManager = manager;
  }

  setOfflineQueue(queue: OfflineQueue | null): void {
    this.offlineQueue = queue;
  }

  setReady(ready: boolean): void {
    this.isReady = ready;
  }

  getSequencer(): EventSequencer {
    return this.sequencer;
  }

  // ---------------------------------------------------------------------------
  // Debug
  // ---------------------------------------------------------------------------

  static enableDebugMode(enabled: boolean = true): void {
    if (typeof window === 'undefined') return;
    (window as any).WSE_DEBUG = enabled;
    logger.info(`WSE debug mode ${enabled ? 'enabled' : 'disabled'}`);
  }

  // ---------------------------------------------------------------------------
  // Event Versioning
  // ---------------------------------------------------------------------------

  private handleEventVersion(message: WSMessage): void {
    // Client supports event version 1
    const CLIENT_SUPPORTED_VERSION = 1;

    if (!message.event_version) return;

    const key = `${message.t}_v${message.event_version}`;
    if (this.loggedEventVersions.has(key)) return;
    if (this.loggedEventVersions.size < MAX_LOGGED_UNHANDLED_TYPES) this.loggedEventVersions.add(key);

    if (message.event_version > CLIENT_SUPPORTED_VERSION) {
      logger.warn(
        `Received event with version ${message.event_version} (type: ${message.t}), ` +
        `but client only supports up to version ${CLIENT_SUPPORTED_VERSION}. ` +
        `Some fields may not be handled correctly. Consider upgrading the client.`
      );
    } else {
      logger.debug(`Event type '${message.t}' using schema version ${message.event_version}`);
    }
  }

  private logPerformanceMetrics(message: WSMessage): void {
    if (message.latency_ms !== undefined && message.latency_ms > 1000) {
      logger.debug(`High event latency: ${message.latency_ms}ms for '${message.t}'`);
    }
    if (message.wse_processing_ms !== undefined && message.wse_processing_ms > 150) {
      logger.debug(`High backend WSE processing time: ${message.wse_processing_ms}ms for '${message.t}'`);
    }
  }

  // ---------------------------------------------------------------------------
  // Inbound processing
  // ---------------------------------------------------------------------------

  /**
   * Queue one inbound frame. Frames are processed one after another in
   * arrival order; the returned promise settles when this frame is done
   * (errors are logged, never thrown).
   */
  processIncoming(data: string | ArrayBuffer): Promise<void> {
    if (this.destroyed) return Promise.resolve();
    return this.enqueueTask(() => this.processFrame(data));
  }

  private enqueueTask(task: () => Promise<void> | void): Promise<void> {
    const run = this.inboundChain.then(() => {
      if (!this.destroyed) return task();
    });
    this.inboundChain = run.catch((error) => {
      logger.error('Error processing message:', error);
      try {
        useWSEStore.getState().setLastError('Message processing error');
      } catch {
        // ignore
      }
    });
    return this.inboundChain;
  }

  private recordInbound(bytes: number, position?: { topic: string; epoch: string; offset: number }): void {
    useWSEStore.getState().recordInbound(bytes, position);
  }

  private countDropped(count: number): void {
    const store = useWSEStore.getState();
    store.updateMetrics({ messagesDropped: store.metrics.messagesDropped + count });
  }

  private async processFrame(data: string | ArrayBuffer): Promise<void> {
    let message: WSMessage | null | typeof PARKED;
    let size: number;

    if (typeof data === 'string') {
      size = data.length;
      if (size > this.maxMessageSize) {
        logger.warn(`Inbound message of ${size} characters exceeds maxMessageSize (${this.maxMessageSize}); dropped`);
        this.countDropped(1);
        return;
      }
      message = this.processTextMessage(data);
    } else {
      const buffer = asArrayBuffer(data);
      if (!buffer) {
        logger.warn('Unsupported inbound frame type; dropped');
        return;
      }
      size = buffer.byteLength;
      if (size > this.maxMessageSize) {
        logger.warn(`Inbound message of ${size} bytes exceeds maxMessageSize (${this.maxMessageSize}); dropped`);
        this.countDropped(1);
        return;
      }
      message = await this.processBinaryMessage(buffer);
    }

    if (message === PARKED) return;
    if (!message) {
      this.recordInbound(size);
      return;
    }
    await this.dispatchMessage(message, size);
  }

  private async dispatchMessage(message: WSMessage, size: number): Promise<void> {
    if (!message || typeof message !== 'object' || typeof message.t !== 'string') {
      this.recordInbound(size);
      logger.debug('Ignoring a frame without a message type');
      return;
    }

    logger.debug(`Received message type: ${message.t}`);
    this.handleEventVersion(message);
    this.logPerformanceMetrics(message);

    // Idempotency: stamped topic messages (tp/e/o) are deduped and gap-detected
    // by per-topic (epoch, offset) -- authoritative and reconnect-safe. The
    // global `seq` is NOT a per-topic order, so it must not drive dedup here.
    // Unstamped messages fall back to id-based dedup.
    if (message.tp && message.e && message.o !== undefined && message.o !== null) {
      await this.handleStamped(message, size);
      return;
    }

    if (message.id && this.sequencer.isDuplicate(message.id)) {
      this.recordInbound(size);
      logger.debug(`Duplicate message ignored: ${message.id}`);
      return;
    }

    this.recordInbound(size);
    await this.deliver(message);
  }

  private async deliver(message: WSMessage): Promise<void> {
    const consumed = await this.runProtocolHook(message);
    if (consumed) return;
    this.routeMessage(message);
  }

  /**
   * Protocol processing that must happen whatever handlers the application
   * registered. Returns true when the message must not be routed further.
   */
  private async runProtocolHook(message: WSMessage): Promise<boolean> {
    switch (message.t) {
      case 'ping':
      case 'PING':
        this.replyPong(message);
        return true;
      case 'PONG':
      case 'pong':
        this.recordPong(message);
        return false;
      case 'server_ready':
        this.processServerReady(message);
        return false;
      case 'server_hello':
        await this.processServerHello(message);
        return false;
      case 'subscription_update':
        await this.processSubscriptionUpdate(message);
        return false;
      case 'error':
        this.processErrorSignals(message);
        return false;
      case 'batch':
        await this.processBatchMessage(message);
        return true;
      default:
        return false;
    }
  }

  private replyPong(message: WSMessage): void {
    const cm = this.connectionManager;
    if (!cm) return;
    const serverTimestamp = message.p?.server_time ?? message.p?.timestamp ?? Date.now();
    cm.sendControl(JSON.stringify({
      c: 'WSE',
      t: 'PONG',
      p: { server_timestamp: serverTimestamp, client_timestamp: Date.now() },
      v: WS_PROTOCOL_VERSION,
    }));
  }

  private recordPong(message: WSMessage): void {
    const clientTs = message.p?.client_timestamp;
    if (typeof clientTs === 'number' && clientTs > 0) {
      const latency = Date.now() - clientTs;
      if (latency >= 0) {
        useWSEStore.getState().recordLatency(latency);
        this.onLatency?.(latency);
      }
    }
  }

  private processServerReady(message: WSMessage): void {
    const payload = message.p || {};
    const details = payload.details || payload;
    const features = details.features || {};
    if (typeof features.recovery === 'boolean') {
      useWSEStore.getState().setServerRecoveryEnabled(features.recovery);
    }
    this.connectionManager?.handleServerReady(details);
  }

  private async processServerHello(message: WSMessage): Promise<void> {
    const payload = message.p || {};
    const store = useWSEStore.getState();

    if (payload.features) {
      // server_ready only exists on the Rust-JWT path; server_hello always
      // reports whether recovery is on.
      if (typeof payload.features.recovery === 'boolean') {
        store.setServerRecoveryEnabled(payload.features.recovery);
      }
      store.updateConfig({
        compressionEnabled: payload.features.compression ?? true,
        batchingEnabled: payload.features.batching ?? true,
        offlineModeEnabled: payload.features.offline_queue ?? true,
      });
    }

    if (payload.limits?.max_queue_size) {
      useMessageQueueStore.getState().setCapacity(payload.limits.max_queue_size);
    }

    const cm = this.connectionManager;
    if (cm) {
      await cm.handleServerHello(payload);
    }
    await this.drainEarlyEncryptedFrames();
  }

  private processErrorSignals(message: WSMessage): void {
    const code = message.p?.code;
    if (code === 'KEY_EXCHANGE_FAILED') {
      this.connectionManager?.failEncryption('the server reported KEY_EXCHANGE_FAILED');
    } else if (code === 'AUTH_FAILED' || code === 'AUTH_REQUIRED') {
      this.connectionManager?.markAuthRejected();
    }
  }

  private async processBatchMessage(message: WSMessage): Promise<void> {
    const messages = message.p?.messages;
    if (!Array.isArray(messages)) return;
    for (const inner of messages) {
      await this.dispatchMessage(inner, 0);
    }
  }

  private processTextMessage(data: string): WSMessage | null {
    // Legacy text heartbeats ("PING:<ts>", "WSE:PING:<ts>", "PONG:<ts>").
    // Only the first characters are inspected -- upper-casing a whole
    // multi-megabyte frame per message was wasted work.
    const head = data.length > 16 ? data.slice(0, 16).toUpperCase() : data.toUpperCase();
    if (head.startsWith('WSE:PING') || head.startsWith('PING')) {
      const parts = data.split(':');
      const serverTimestamp = parts.length > 1 ? parseInt(parts[parts.length - 1], 10) : Date.now();
      this.connectionManager?.sendControl(JSON.stringify({
        c: 'WSE',
        t: 'PONG',
        p: { server_timestamp: serverTimestamp, client_timestamp: Date.now() },
        v: WS_PROTOCOL_VERSION,
      }));
      return null;
    }

    if (head.startsWith('WSE:PONG:') || head.startsWith('PONG:')) {
      const timestamp = parseInt(data.split(':').pop() || '0', 10);
      const latency = Date.now() - timestamp;
      if (timestamp > 0 && latency >= 0) {
        useWSEStore.getState().recordLatency(latency);
        this.onLatency?.(latency);
      }
      return null;
    }

    const { category, json } = stripCategoryPrefix(data);
    try {
      const parsed = JSON.parse(json);
      if (category && parsed && typeof parsed === 'object') {
        parsed.c = category;
      }
      return parsed;
    } catch (error) {
      logger.warn('Invalid JSON message dropped:', error instanceof Error ? error.message : error);
      return null;
    }
  }

  private async processBinaryMessage(data: ArrayBuffer): Promise<WSMessage | null | typeof PARKED> {
    const view = new Uint8Array(data);

    // 1. Compression header 'C:'
    if (view.length >= 2 && view[0] === 67 && view[1] === 58) {
      try {
        const decompressed = this.compression.decompress(data.slice(2), this.maxMessageSize);
        const store = useWSEStore.getState();
        store.incrementMetric('compressionHits');
        const text = new TextDecoder().decode(decompressed);
        const { json } = stripCategoryPrefix(text);
        return JSON.parse(json);
      } catch (error) {
        if (error instanceof DecompressedSizeLimitError) {
          this.countDropped(1);
        } else {
          logger.warn('Failed to process compressed message with C: header:', error instanceof Error ? error.message : error);
        }
        return null;
      }
    }

    // 2. Encryption header 'E:'
    if (view.length >= 2 && view[0] === 69 && view[1] === 58) {
      return this.processEncryptedFrame(data);
    }

    // 3. MessagePack header 'M:'
    if (view.length >= 2 && view[0] === 77 && view[1] === 58) {
      try {
        return this.compression.unpackMsgPack(data.slice(2));
      } catch {
        logger.warn('Failed to unpack MessagePack message');
        return null;
      }
    }

    // 4. Raw zlib data (0x78 header)
    if (view.length > 2 && view[0] === 0x78 && [0x01, 0x5e, 0x9c, 0xda].includes(view[1])) {
      try {
        const decompressed = this.compression.decompress(data, this.maxMessageSize);
        useWSEStore.getState().incrementMetric('compressionHits');
        const { json } = stripCategoryPrefix(new TextDecoder().decode(decompressed));
        return JSON.parse(json);
      } catch (error) {
        if (error instanceof DecompressedSizeLimitError) {
          this.countDropped(1);
          return null;
        }
        // Not zlib after all -- try the other formats below
      }
    }

    // 5. JSON in a binary frame (with or without a WSE/S/U prefix)
    try {
      const { json } = stripCategoryPrefix(new TextDecoder().decode(data));
      if (json.startsWith('{') || json.startsWith('[')) {
        return JSON.parse(json);
      }
    } catch {
      // Not JSON, continue to other methods
    }

    // 6. Raw MessagePack
    try {
      return this.compression.unpackMsgPack(data);
    } catch {
      // Not MessagePack either
    }

    logger.warn(`Failed to parse a ${data.byteLength}-byte binary message in any known format`);
    return null;
  }

  private async processEncryptedFrame(data: ArrayBuffer): Promise<WSMessage | null | typeof PARKED> {
    const cm = this.connectionManager;
    if (!securityManager.isTransportEncryptionReady()) {
      if (securityManager.hasPendingKeyDerivation()) {
        // The key is being derived right now: wait for it instead of dropping.
        const ok = await securityManager.waitForTransportKey();
        if (!ok) {
          logger.warn('Encrypted message dropped: the key exchange failed');
          return null;
        }
      } else if (cm?.isAwaitingEncryptionKey()) {
        // The server may start encrypting just before its server_hello
        // reaches us: keep the frame and decrypt it once the key exists.
        if (this.earlyEncryptedFrames.length < MAX_EARLY_ENCRYPTED_FRAMES) {
          this.earlyEncryptedFrames.push(data);
          return PARKED;
        }
        logger.warn('Too many encrypted frames before the key exchange completed; frame dropped');
        return null;
      } else {
        logger.warn(`Encrypted message received but no encryption key exists; dropped (${data.byteLength} bytes)`);
        return null;
      }
    }

    try {
      const decrypted = await securityManager.decryptFromTransport(data);
      if (!decrypted) {
        logger.warn('Decryption returned nothing for an encrypted message');
        return null;
      }
      return this.processTextMessage(decrypted);
    } catch (error) {
      logger.error('Failed to decrypt message:', error);
      return null;
    }
  }

  private async drainEarlyEncryptedFrames(): Promise<void> {
    if (this.earlyEncryptedFrames.length === 0) return;
    const frames = this.earlyEncryptedFrames;
    this.earlyEncryptedFrames = [];
    if (!securityManager.isTransportEncryptionReady()) {
      logger.warn(`Dropping ${frames.length} encrypted frame(s): the key exchange did not complete`);
      return;
    }
    for (const frame of frames) {
      await this.processFrame(frame);
    }
  }

  // ---------------------------------------------------------------------------
  // Recovery-stamped messages: dedup, gaps, replay windows
  // ---------------------------------------------------------------------------

  private async handleStamped(message: WSMessage, size: number): Promise<void> {
    const topic = message.tp!;
    const epoch = String(message.e);
    const offset = Number(message.o);

    if (!Number.isFinite(offset)) {
      this.recordInbound(size);
      await this.deliver(message);
      return;
    }

    const window = this.recoveryWindows.get(topic);
    if (window && window.epoch !== epoch) {
      // New epoch (server restarted): release what the old epoch's window held first.
      await this.flushRecoveryWindow(topic);
    }

    const previous = this.sequencer.getTopicPosition(topic);
    const verdict = this.sequencer.checkTopicStamp(topic, epoch, offset);

    if (verdict === 'duplicate') {
      this.recordInbound(size);
      logger.debug(`Duplicate (offset) ignored: ${topic}#${offset}`);
      return;
    }

    if (verdict === 'gap') {
      const open = this.recoveryWindows.get(topic);
      if (open && open.epoch === epoch) {
        if (open.buffer.size < MAX_RECOVERY_BUFFER) {
          // A replay was requested and may still fill the hole: hold it.
          open.buffer.set(offset, message);
          this.recordInbound(size);
          return;
        }
        await this.flushRecoveryWindow(topic);
        // The flush may have advanced the position; re-evaluate this message.
        await this.handleStamped(message, size);
        return;
      }

      // No replay pending: deliver now and report what is missing.
      const last = previous!.offset;
      this.sequencer.setTopicPosition(topic, epoch, offset);
      this.reportGap({
        topic,
        epoch,
        fromOffset: last + 1,
        toOffset: offset - 1,
        missed: offset - last - 1,
        reason: 'offset_gap',
      });
    } else if (previous && previous.epoch !== epoch) {
      this.reportGap({
        topic,
        epoch,
        fromOffset: null,
        toOffset: null,
        missed: null,
        reason: 'epoch_changed',
      });
    }

    // The recovery anchor is the last DELIVERED position.
    this.recordInbound(size, { topic, epoch, offset });
    await this.deliver(message);

    if (this.recoveryWindows.has(topic)) {
      await this.drainRecoveryBuffer(topic);
    }
  }

  /** Deliver held messages that are now in order. */
  private async drainRecoveryBuffer(topic: string): Promise<void> {
    const window = this.recoveryWindows.get(topic);
    if (!window) return;

    let position = this.sequencer.getTopicPosition(topic);
    while (position && position.epoch === window.epoch) {
      for (const held of [...window.buffer.keys()]) {
        if (held <= position.offset) window.buffer.delete(held);
      }
      const next = window.buffer.get(position.offset + 1);
      if (!next) break;
      window.buffer.delete(position.offset + 1);
      await this.deliverHeld(topic, window.epoch, position.offset + 1, next);
      position = this.sequencer.getTopicPosition(topic);
    }

    if (window.confirmed && window.buffer.size === 0) {
      this.closeRecoveryWindow(topic);
    }
  }

  private async deliverHeld(topic: string, epoch: string, offset: number, message: WSMessage): Promise<void> {
    this.sequencer.setTopicPosition(topic, epoch, offset);
    useWSEStore.getState().updateRecoveryState({ [topic]: { epoch, offset } });
    await this.deliver(message);
  }

  /** End a window: deliver everything held, in offset order, reporting holes. */
  private async flushRecoveryWindow(topic: string): Promise<void> {
    const window = this.recoveryWindows.get(topic);
    if (!window) return;
    this.closeRecoveryWindow(topic);

    const offsets = [...window.buffer.keys()].sort((a, b) => a - b);
    for (const offset of offsets) {
      const message = window.buffer.get(offset)!;
      const position = this.sequencer.getTopicPosition(topic);
      if (position && position.epoch === window.epoch) {
        if (offset <= position.offset) continue;
        if (offset > position.offset + 1) {
          this.reportGap({
            topic,
            epoch: window.epoch,
            fromOffset: position.offset + 1,
            toOffset: offset - 1,
            missed: offset - position.offset - 1,
            reason: 'offset_gap',
          });
        }
      }
      await this.deliverHeld(topic, window.epoch, offset, message);
    }
  }

  private closeRecoveryWindow(topic: string): void {
    const window = this.recoveryWindows.get(topic);
    if (!window) return;
    if (window.timer) clearTimeout(window.timer);
    this.recoveryWindows.delete(topic);
  }

  /**
   * A resubscribe asked the server to replay from these positions: hold
   * out-of-order messages of these topics until the replay fills the hole,
   * the server says it cannot, or recoveryTimeoutMs passes.
   */
  openRecoveryWindows(positions: Record<string, { epoch: string; offset: number }>): void {
    if (this.destroyed || !(this.recoveryTimeoutMs > 0)) return;
    for (const [topic, position] of Object.entries(positions)) {
      this.closeRecoveryWindow(topic);
      const window: RecoveryWindow = {
        epoch: position.epoch,
        buffer: new Map(),
        confirmed: false,
        timer: null,
      };
      window.timer = setTimeout(() => {
        window.timer = null;
        void this.enqueueTask(() => {
          if (this.recoveryWindows.get(topic) === window) {
            if (window.buffer.size > 0) {
              logger.warn(`Recovery for topic ${topic} did not fill the gap within ${this.recoveryTimeoutMs}ms`);
            }
            return this.flushRecoveryWindow(topic);
          }
        });
      }, this.recoveryTimeoutMs);
      this.recoveryWindows.set(topic, window);
    }
  }

  private reportGap(gap: WSEGapEvent): void {
    logger.warn(
      `Gap on topic ${gap.topic}: ${gap.missed ?? 'an unknown number of'} message(s) missing (${gap.reason})`
    );
    const store = useWSEStore.getState();
    store.updateMetrics({
      gapsDetected: store.metrics.gapsDetected + 1,
      messagesMissed: store.metrics.messagesMissed + (gap.missed ?? 0),
    });
    try {
      this.onGap?.(gap);
    } catch (error) {
      logger.error('Gap listener failed:', error);
    }
    dispatchWSEEvent('wseGap', gap);
  }

  /** Recovery bookkeeping for a subscription_update reply (runs before any handler). */
  private async processSubscriptionUpdate(message: WSMessage): Promise<void> {
    const payload = message.p || {};
    const recovery = payload.recovery;
    if (!recovery || typeof recovery !== 'object') return;

    for (const [topic, info] of Object.entries(recovery as Record<string, any>)) {
      if (!info || info.epoch == null || info.offset == null) continue;
      const serverOffset = Number(info.offset);
      if (!Number.isFinite(serverOffset)) continue;
      const serverEpoch = String(info.epoch);
      const window = this.recoveryWindows.get(topic);

      if (info.recovered === false) {
        // The server cannot replay (history evicted, epoch changed, no
        // recovery requested): release what is held, then move the anchor
        // forward to the server's position -- never backwards -- and report
        // what was lost. The post-subscribe snapshot covers the state.
        if (window) await this.flushRecoveryWindow(topic);
        const position = this.sequencer.getTopicPosition(topic);
        if (!position) {
          this.setBaseline(topic, serverEpoch, serverOffset);
        } else if (position.epoch !== serverEpoch) {
          this.reportGap({
            topic,
            epoch: serverEpoch,
            fromOffset: null,
            toOffset: null,
            missed: null,
            reason: 'not_recovered',
          });
          this.setBaseline(topic, serverEpoch, serverOffset);
        } else if (serverOffset > position.offset) {
          this.reportGap({
            topic,
            epoch: serverEpoch,
            fromOffset: position.offset + 1,
            toOffset: serverOffset,
            missed: serverOffset - position.offset,
            reason: 'not_recovered',
          });
          this.setBaseline(topic, serverEpoch, serverOffset);
        }
        continue;
      }

      // Recovered (or no flag): the replayed frames advance the anchor as
      // they are delivered. The reply's offset is the server head, which may
      // be past what was actually replayed, so it never moves the anchor.
      if (window) {
        window.confirmed = true;
        await this.drainRecoveryBuffer(topic);
      }
    }
  }

  private setBaseline(topic: string, epoch: string, offset: number): void {
    this.sequencer.setTopicPosition(topic, epoch, offset);
    useWSEStore.getState().updateRecoveryState({ [topic]: { epoch, offset } });
  }

  /** Last delivered (epoch, offset) of the given topics -- the recovery anchor. */
  getTopicPositions(topics?: string[]): Record<string, { epoch: string; offset: number }> {
    return this.sequencer.getTopicPositions(topics);
  }

  /** Forget everything about topics the client unsubscribed from. */
  forgetTopics(topics: string[]): void {
    for (const topic of topics) {
      this.closeRecoveryWindow(topic);
    }
    this.sequencer.clearTopics(topics);
    useWSEStore.getState().removeRecoveryTopics(topics);
  }

  /** The socket is gone: release held messages and drop undecryptable early frames. */
  handleConnectionClosed(): void {
    if (this.destroyed) return;
    void this.enqueueTask(async () => {
      this.earlyEncryptedFrames = [];
      for (const topic of [...this.recoveryWindows.keys()]) {
        await this.flushRecoveryWindow(topic);
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Message Routing
  // ---------------------------------------------------------------------------

  private routeMessage(message: WSMessage): void {
    const type = message.t;

    if (this.eventThrottleMs > 0 && !isSystemType(type)) {
      const now = Date.now();
      const last = this.lastEventTime.get(type);
      if (last !== undefined && now - last < this.eventThrottleMs) {
        logger.debug(`Throttling ${type} event (eventThrottleMs=${this.eventThrottleMs})`);
        return;
      }
      this.lastEventTime.set(type, now);
    }

    const handler = this.messageHandlers.get(type);
    if (handler) {
      try {
        const result: unknown = handler(message);
        if (result && typeof (result as Promise<unknown>).then === 'function') {
          (result as Promise<unknown>).catch((error) => {
            logger.error(`Error in handler for ${type}:`, error);
          });
        }
      } catch (error) {
        logger.error(`Error in handler for ${type}:`, error);
      }
      return;
    }

    if (!this.unhandledTypesLogged.has(type) && this.unhandledTypesLogged.size < MAX_LOGGED_UNHANDLED_TYPES) {
      this.unhandledTypesLogged.add(type);
      logger.debug(`No handler registered for message type "${type}"; dispatching the wseMessage window event`);
    }
    // Catch-all for messages no handler claimed: one fixed event name, so a
    // server message type can never collide with a real DOM event.
    dispatchWSEEvent('wseMessage', message);
  }

  // ---------------------------------------------------------------------------
  // Message Handlers
  // ---------------------------------------------------------------------------

  private registerDefaultHandlers(): void {
    // Protocol work for these types happens in runProtocolHook(); the
    // defaults below only publish state/events and can be replaced.
    this.messageHandlers.set('server_ready', () => {});
    this.messageHandlers.set('server_hello', () => {});
    this.messageHandlers.set('PONG', () => {});
    this.messageHandlers.set('subscription_update', (msg) => this.handleSubscriptionUpdate(msg));
    this.messageHandlers.set('error', (msg) => this.handleError(msg));
    this.messageHandlers.set('connection_state_change', (msg) => this.handleConnectionStateChange(msg));
    this.messageHandlers.set('health_check', () => this.handleHealthCheck());
    this.messageHandlers.set('health_check_response', (msg) => this.handleHealthCheckResponse(msg));
    this.messageHandlers.set('rate_limit_warning', (msg) => this.handleRateLimitWarning(msg));
    this.messageHandlers.set('connection_quality', (msg) => this.handleConnectionQuality(msg));
    this.messageHandlers.set('snapshot_complete', (msg) => dispatchWSEEvent('snapshotComplete', msg.p));
    this.messageHandlers.set('heartbeat', () => this.handleHeartbeat());

    // Presence handlers
    this.messageHandlers.set('presence_join', (msg) => dispatchWSEEvent('presenceJoin', msg.p));
    this.messageHandlers.set('presence_leave', (msg) => dispatchWSEEvent('presenceLeave', msg.p));
    this.messageHandlers.set('presence_update', (msg) => dispatchWSEEvent('presenceUpdate', msg.p));

    // Debug / configuration / encryption responses
    this.messageHandlers.set('debug_response', (msg) => dispatchWSEEvent('debugResponse', msg.p));
    this.messageHandlers.set('sequence_stats_response', (msg) => dispatchWSEEvent('sequenceStatsResponse', msg.p));
    this.messageHandlers.set('config_response', (msg) => dispatchWSEEvent('configResponse', msg.p));
    this.messageHandlers.set('config_update_response', (msg) => dispatchWSEEvent('configUpdateResponse', msg.p));
    this.messageHandlers.set('encryption_response', (msg) => this.handleEncryptionResponse(msg));
    this.messageHandlers.set('key_rotation_response', (msg) => this.handleKeyRotationResponse(msg));
    this.messageHandlers.set('batch_message_result', (msg) => dispatchWSEEvent('batchMessageResult', msg.p));
    this.messageHandlers.set('metrics_response', (msg) => dispatchWSEEvent('metricsResponse', msg.p));
    this.messageHandlers.set('connection_state_response', (msg) => this.handleConnectionStateResponse(msg));

    // Sync request handler
    this.messageHandlers.set('sync_request', () => {
      const store = useWSEStore.getState();
      this.queueOutgoing({
        t: 'sync_response',
        p: {
          client_version: WS_CLIENT_VERSION,
          protocol_version: WS_PROTOCOL_VERSION,
          sequence: this.sequencer.getCurrentSequence(),
          subscriptions: store.activeTopics,
          last_update: Date.now(),
        }
      }, MessagePriority.HIGH);
    });

    // Config update handler
    this.messageHandlers.set('config_update', (msg) => {
      const config = msg.p || {};
      const store = useWSEStore.getState();

      if (config.compression_enabled !== undefined) {
        store.updateConfig({ compressionEnabled: config.compression_enabled });
      }
      if (config.batching_enabled !== undefined) {
        store.updateConfig({ batchingEnabled: config.batching_enabled });
        if (config.batch_size) {
          this.batchSize = config.batch_size;
        }
        if (config.batch_timeout) {
          this.batchTimeout = config.batch_timeout;
        }
      }
      if (config.max_queue_size !== undefined) {
        useMessageQueueStore.getState().setCapacity(config.max_queue_size);
      }

      logger.debug('Configuration updated:', config);
    });

    // Metrics request handler
    this.messageHandlers.set('metrics_request', () => {
      const store = useWSEStore.getState();
      const queueStore = useMessageQueueStore.getState();

      this.queueOutgoing({
        t: 'metrics_response',
        p: {
          client_version: WS_CLIENT_VERSION,
          connection_stats: store.metrics,
          queue_stats: queueStore.stats,
          diagnostics: store.diagnostics,
          circuit_breaker: store.circuitBreaker,
          security: store.security,
          subscriptions: {
            active: store.activeTopics,
            pending: store.subscriptions.pendingSubscriptions,
            failed: store.subscriptions.failedSubscriptions,
          },
          event_sequencer: this.sequencer.getStats(),
          timestamp: new Date().toISOString(),
        }
      }, MessagePriority.HIGH);
    });

    // Priority message handler
    this.messageHandlers.set('priority_message', (msg) => {
      const payload = msg.p || {};
      if (payload.type && payload.type !== 'priority_message' && this.messageHandlers.has(payload.type)) {
        const handler = this.messageHandlers.get(payload.type);
        handler!({ ...msg, t: payload.type, p: payload.content || payload });
      }
    });
  }

  private handleSubscriptionUpdate(message: WSMessage): void {
    const payload = message.p || {};
    const store = useWSEStore.getState();

    if (payload.success) {
      payload.success_topics?.forEach((topic: string) => {
        store.confirmSubscription(topic);
      });
    }

    if (payload.failed_topics) {
      payload.failed_topics.forEach((topic: string) => {
        store.failSubscription(topic);
      });
    }

    dispatchWSEEvent('subscriptionUpdate', payload);
  }

  private handleError(message: WSMessage): void {
    const store = useWSEStore.getState();

    const errorData = message.p || {};
    const errorMessage = errorData.message || 'Unknown error';
    const errorCode = errorData.code || 'UNKNOWN_ERROR';
    const recoverable = errorData.recoverable !== false;
    const details = errorData.details || {};

    logger.warn(`Server error: ${errorCode}: ${errorMessage}`);

    store.setLastError(errorMessage,
      typeof errorCode === 'number' ? errorCode :
      errorCode === 'AUTH_FAILED' || errorCode === 'AUTH_REQUIRED' ? 401 : 500
    );

    if (errorCode === 'INIT_ERROR' && !recoverable) {
      dispatchWSEEvent('wseInitializationError', { message: errorMessage, code: errorCode, recoverable: false, details });
    }

    if (errorCode === 'SERVER_ERROR') {
      dispatchWSEEvent('wseServerError', { message: errorMessage, code: errorCode, details });
    }

    if (errorCode === 'CIRCUIT_BREAKER_OPEN') {
      store.updateCircuitBreaker({
        state: CircuitBreakerState.OPEN,
        lastFailureTime: Date.now()
      });
    }

    if (errorCode === 'RATE_LIMITED' || errorCode === 'RATE_LIMIT_EXCEEDED' || String(errorMessage).includes('Rate limit')) {
      dispatchWSEEvent('rateLimitExceeded', {
        message: errorMessage,
        retryAfter: errorData.retry_after || errorData.retryAfter,
        ...details
      });
    }

    if (errorCode === 'SUBSCRIPTION_FAILED') {
      dispatchWSEEvent('subscriptionFailed', {
        message: errorMessage,
        topics: errorData.topics || [],
        ...details
      });
    }

    dispatchWSEEvent('serverError', {
      message: errorMessage,
      code: errorCode,
      details: errorData,
      recoverable,
      timestamp: errorData.timestamp || new Date().toISOString(),
      severity: errorData.severity || 'error',
      context: {
        messageType: message.t,
        messageId: message.id,
        sequence: message.seq
      }
    });

    if (!recoverable || errorCode === 'PROTOCOL_ERROR' || errorCode === 'INVALID_MESSAGE') {
      dispatchWSEEvent('criticalError', {
        code: errorCode,
        message: errorMessage,
        shouldReconnect: errorData.shouldReconnect !== false
      });
    }
  }

  private handleConnectionStateChange(message: WSMessage): void {
    const payload = message.p || {};
    const store = useWSEStore.getState();

    if (payload.new_state) {
      const stateMap: Record<string, ConnectionState> = {
        'pending': ConnectionState.PENDING,
        'connecting': ConnectionState.CONNECTING,
        'connected': ConnectionState.CONNECTED,
        'reconnecting': ConnectionState.RECONNECTING,
        'disconnected': ConnectionState.DISCONNECTED,
        'error': ConnectionState.ERROR,
        'degraded': ConnectionState.DEGRADED,
      };

      const newState = stateMap[String(payload.new_state).toLowerCase()];
      if (newState) {
        store.setConnectionState(newState);
      }
    }

    dispatchWSEEvent('connectionStateChange', {
      oldState: payload.old_state,
      newState: payload.new_state,
      connectionId: payload.connection_id,
      timestamp: payload.timestamp || new Date().toISOString(),
    });
  }

  private handleHealthCheck(): void {
    const store = useWSEStore.getState();
    store.updateMetrics({ lastHealthCheck: Date.now() });

    this.queueOutgoing({
      t: 'health_check_response',
      p: {
        client_version: WS_CLIENT_VERSION,
        stats: store.metrics,
        diagnostics: store.diagnostics,
        queue_size: useMessageQueueStore.getState().size,
      }
    }, MessagePriority.CRITICAL);
  }

  private handleHealthCheckResponse(message: WSMessage): void {
    const payload = message.p || {};
    if (payload.diagnostics) {
      useWSEStore.getState().updateDiagnostics(payload.diagnostics);
    }
  }

  private handleRateLimitWarning(message: WSMessage): void {
    const warning = message.p || {};
    logger.warn('Rate limit warning:', warning);
    useWSEStore.getState().setLastError(`Rate limit: remaining=${warning.remaining}, limit=${warning.limit}`, 429);
  }

  private handleConnectionQuality(message: WSMessage): void {
    const payload = message.p || {};
    const store = useWSEStore.getState();

    if (payload.suggestions && payload.suggestions.length > 0) {
      const currentDiagnostics = store.diagnostics || {
        quality: ConnectionQuality.UNKNOWN,
        stability: 100,
        jitter: 0,
        packetLoss: 0,
        roundTripTime: 0,
        suggestions: [],
        lastAnalysis: null,
      };

      store.updateDiagnostics({
        ...currentDiagnostics,
        suggestions: payload.suggestions,
        quality: payload.quality || currentDiagnostics.quality,
        lastAnalysis: Date.now(),
      });
    }

    if (payload.recommended_settings) {
      const settings = payload.recommended_settings;

      if (settings.compression !== undefined) {
        store.updateConfig({ compressionEnabled: settings.compression });
      }
      if (settings.batch_size !== undefined) {
        this.batchSize = settings.batch_size;
      }
      if (settings.batch_timeout !== undefined) {
        this.batchTimeout = settings.batch_timeout;
      }
    }
  }

  private handleHeartbeat(): void {
    useWSEStore.getState().updateMetrics({ lastHealthCheck: Date.now() });
  }

  private handleEncryptionResponse(message: WSMessage): void {
    const payload = message.p || {};
    if (payload.enabled !== undefined) {
      useWSEStore.getState().updateSecurity({
        encryptionEnabled: payload.enabled,
        encryptionAlgorithm: payload.algorithms?.encryption || null
      });
    }
    dispatchWSEEvent('encryptionResponse', payload);
  }

  private handleKeyRotationResponse(message: WSMessage): void {
    const payload = message.p || {};
    if (payload.success) {
      useWSEStore.getState().updateSecurity({ lastKeyRotation: Date.now() });
    }
    dispatchWSEEvent('keyRotationResponse', payload);
  }

  private handleConnectionStateResponse(message: WSMessage): void {
    const payload = message.p || {};
    if (payload.metrics) {
      useWSEStore.getState().updateMetrics(payload.metrics);
    }
    dispatchWSEEvent('connectionStateResponse', payload);
  }

  // ---------------------------------------------------------------------------
  // Outgoing messages
  // ---------------------------------------------------------------------------

  /**
   * Send `message` now when possible, otherwise keep it until the connection
   * is ready. With `offline: true` and IndexedDB available, a message that
   * cannot go out now is persisted and replayed after the next connect --
   * even across page reloads.
   */
  enqueueOutgoing(
    message: WSMessage,
    priority: number = MessagePriority.NORMAL,
    options: { offline?: boolean } = {},
  ): SendStatus {
    if (this.destroyed) return 'dropped';

    const cm = this.connectionManager;
    const queueEmpty = useMessageQueueStore.getState().size === 0;
    const offlinePending = (this.offlineQueue?.getStats().size ?? 0) > 0;

    // Fast path keeps latency low; it is skipped while anything older is
    // still waiting so the original order is preserved.
    if (cm && queueEmpty && !offlinePending && !this.flushing && cm.isTransportReady()) {
      const result = cm.trySend(message);
      if (result === 'sent') return 'sent';
      if (result === 'error') {
        this.countDropped(1);
        return 'dropped';
      }
    }

    if (options.offline && this.offlineQueue?.isAvailable() && !cm?.isTransportReady()) {
      const queue = this.offlineQueue;
      queue.enqueue(message).then(
        (stored) => {
          if (!stored) this.enqueueInMemory(message, priority);
          else this.scheduleFlush(0);
        },
        (error) => {
          logger.error('Failed to persist offline message, keeping it in memory:', error);
          this.enqueueInMemory(message, priority);
        },
      );
      return 'queued';
    }

    return this.enqueueInMemory(message, priority) ? 'queued' : 'dropped';
  }

  private enqueueInMemory(message: WSMessage, priority: number): boolean {
    if (this.destroyed) return false;
    const queued: QueuedMessage = {
      id: message.id || generateId(),
      type: message.t,
      payload: message.p,
      priority,
      timestamp: Date.now(),
      retries: 0,
      encrypted: message.enc,
      correlation_id: message.cid,
      message,
    };

    if (!useMessageQueueStore.getState().enqueue(queued)) {
      this.countDropped(1);
      logger.warn(`Outgoing queue full; message ${message.t} dropped`);
      return false;
    }

    useWSEStore.getState().incrementMetric('messagesQueued');
    if (this.connectionManager?.isTransportReady()) {
      this.scheduleFlush(0);
    }
    return true;
  }

  /** Re-queue a message that was accepted but whose socket vanished before it went out. */
  requeue(message: WSMessage, priority: number = message.pri ?? MessagePriority.NORMAL): void {
    if (this.destroyed) return;
    this.enqueueInMemory(message, priority);
  }

  /**
   * Queue a message for sending (compatibility wrapper around
   * enqueueOutgoing). Returns false when the queue is full.
   */
  queueOutgoing(
    message: Partial<WSMessage>,
    priority: MessagePriority = MessagePriority.NORMAL
  ): boolean {
    const full: WSMessage = {
      ...message,
      t: message.t || 'unknown',
      p: message.p ?? {},
      id: message.id || generateId(),
      ts: message.ts || new Date().toISOString(),
      v: message.v ?? WS_PROTOCOL_VERSION,
      pri: message.pri ?? priority,
    };
    return this.enqueueOutgoing(full, priority) !== 'dropped';
  }

  private scheduleFlush(delayMs: number): void {
    if (this.destroyed) return;
    const due = Date.now() + Math.max(0, delayMs);
    if (this.flushTimer) {
      if (this.flushDue <= due) return;
      clearTimeout(this.flushTimer);
    }
    this.flushDue = due;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flush();
    }, Math.max(0, delayMs));
  }

  /**
   * Send everything that is waiting: first the IndexedDB offline queue
   * (oldest first), then the in-memory queue (priority, then FIFO). Stops
   * early -- keeping the rest queued -- when the connection is not ready or
   * the rate limit is exhausted.
   */
  async flush(): Promise<void> {
    if (this.destroyed) return;
    if (this.flushing) {
      this.flushRequested = true;
      return;
    }

    this.flushing = true;
    const queueStore = useMessageQueueStore.getState();
    try {
      do {
        this.flushRequested = false;
        const stalled = await this.flushOnce();
        if (stalled) break;
      } while (this.flushRequested && !this.destroyed);
    } catch (error) {
      logger.error('Flushing the outgoing queue failed:', error);
    } finally {
      this.flushing = false;
      queueStore.setProcessing(false);
    }
  }

  private toWire(queued: QueuedMessage): WSMessage {
    if (queued.message) return queued.message;
    const message: WSMessage = {
      id: queued.id,
      t: queued.type,
      p: queued.payload,
      v: WS_PROTOCOL_VERSION,
      ts: new Date(queued.timestamp).toISOString(),
      pri: queued.priority,
    };
    if (queued.correlation_id) message.cid = queued.correlation_id;
    if (queued.encrypted) message.enc = true;
    return message;
  }

  /** One pass over both queues; true when it stopped before they were empty. */
  private async flushOnce(): Promise<boolean> {
    const cm = this.connectionManager;
    if (!cm || !cm.isTransportReady()) return true;

    if (this.offlineQueue && this.offlineQueue.getStats().size > 0) {
      const stalled = await this.replayOfflineQueue(cm);
      if (stalled) return true;
    }

    const step = Math.max(1, this.batchSize);
    while (!this.destroyed) {
      const queueStore = useMessageQueueStore.getState();
      if (queueStore.size === 0) return false;
      queueStore.setProcessing(true);
      const batch = queueStore.dequeue(step);
      if (batch.length === 0) return false;

      for (let i = 0; i < batch.length; i++) {
        const result = cm.trySend(this.toWire(batch[i]));
        if (result === 'sent') continue;
        if (result === 'error') {
          this.countDropped(1);
          logger.error(`Dropping message ${batch[i].type}: it cannot be serialised`);
          continue;
        }
        // Not sent: everything from here on goes back, in order.
        useMessageQueueStore.getState().requeue(batch.slice(i));
        if (result === 'rate_limited') {
          this.scheduleFlush(cm.getRetryAfterMs());
        }
        return true;
      }
    }
    return true;
  }

  private async replayOfflineQueue(cm: ProcessorConnection): Promise<boolean> {
    const queue = this.offlineQueue;
    if (!queue) return false;

    let records;
    try {
      records = await queue.getAll();
    } catch (error) {
      logger.error('Reading the offline queue failed:', error);
      return false;
    }

    const done: string[] = [];
    try {
      for (const record of records) {
        if (this.destroyed || !cm.isTransportReady()) return true;
        const result = cm.trySend(record.message);
        if (result === 'sent') {
          done.push(record.id);
          continue;
        }
        if (result === 'error') {
          this.countDropped(1);
          done.push(record.id);
          continue;
        }
        if (result === 'rate_limited') {
          this.scheduleFlush(cm.getRetryAfterMs());
        }
        return true;
      }
      if (records.length > 0) {
        logger.info(`Replayed ${records.length} offline message(s)`);
      }
      return false;
    } finally {
      if (done.length > 0) {
        // Only the messages that actually went out are deleted.
        await queue.remove(done).catch((error) => {
          logger.error('Removing sent offline messages failed:', error);
        });
      }
    }
  }

  /**
   * Dequeue up to batchSize messages as wire messages without sending them
   * (kept for API compatibility; the processor flushes its queue itself).
   */
  async processBatch(): Promise<WSMessage[]> {
    if (this.destroyed) return [];
    const messages = useMessageQueueStore.getState().dequeue(this.batchSize);
    return messages.map(msg => ({
      ...this.toWire(msg),
      seq: this.sequencer.getNextSequence(),
    }));
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Register an event type as high-frequency (kept for API compatibility;
   * per-message routing is always logged at debug level now).
   */
  registerHighFrequencyType(type: string): void {
    this.highFrequencyTypes.add(type);
  }

  registerHandler(type: string, handler: (message: WSMessage) => void): void {
    logger.debug(`Registering handler for message type: ${type}`);
    this.messageHandlers.set(type, handler);
  }

  unregisterHandler(type: string): void {
    this.messageHandlers.delete(type);
  }

  clearHandlers(): void {
    this.messageHandlers.clear();
    this.registerDefaultHandlers();
  }

  getRegisteredHandlers(): string[] {
    return Array.from(this.messageHandlers.keys());
  }

  isHandlerRegistered(type: string): boolean {
    return this.messageHandlers.has(type);
  }

  waitForHandlers(requiredHandlers: string[], timeout: number = 5000): Promise<boolean> {
    return new Promise((resolve) => {
      const checkHandlers = (): boolean => {
        const allRegistered = requiredHandlers.every(h => this.isHandlerRegistered(h));
        if (allRegistered) {
          resolve(true);
          return true;
        }
        return false;
      };

      if (checkHandlers()) return;

      const startTime = Date.now();
      this.waitForHandlersInterval = setInterval(() => {
        if (this.destroyed || checkHandlers()) {
          if (this.waitForHandlersInterval) {
            clearInterval(this.waitForHandlersInterval);
            this.waitForHandlersInterval = null;
          }
          if (this.destroyed) resolve(false);
        } else if (Date.now() - startTime > timeout) {
          if (this.waitForHandlersInterval) {
            clearInterval(this.waitForHandlersInterval);
            this.waitForHandlersInterval = null;
          }
          resolve(requiredHandlers.every(h => this.isHandlerRegistered(h)));
        }
      }, 100);
    });
  }

  destroy(): void {
    this.destroyed = true;

    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    if (this.waitForHandlersInterval) {
      clearInterval(this.waitForHandlersInterval);
      this.waitForHandlersInterval = null;
    }

    for (const window of this.recoveryWindows.values()) {
      if (window.timer) clearTimeout(window.timer);
    }
    this.recoveryWindows.clear();
    this.earlyEncryptedFrames = [];
    this.clearHandlers();
    this.connectionManager = null;

    if (this.ownsSequencer) {
      this.sequencer.destroy();
    }
  }
}
