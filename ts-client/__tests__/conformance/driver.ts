// The TypeScript drivers for the conformance server (crates/wse-conformance):
// they run each case they can, step by step, and record what their client
// saw and did; the server judges. One driver for every runtime: the client
// of a case is made by the runtime's own maker (a client of its own in Node,
// a bridge window in Node, a page of Electron or Tauri).

import type { WSEClient } from '../../core/client.js';
import type { DeliveryOptions, ResyncRequest } from '../../core/delivery.js';

/** What the TypeScript client has, by the catalog's names. */
export const FEATURES = ['outbox', 'start', 'live', 'msgpack', 'e2e', 'signing', 'recover_or_resync'];

/** What a case's connect step asks for. */
export interface Asked {
  /** The case's server. */
  url: string;
  /** The user's token, sent as the bearer. */
  token: string;
  format: 'json' | 'msgpack';
  /** The server's identity key, to pin, when the case asks for end-to-end encryption. */
  e2eKey: string | null;
  /** Whether the client signs messages, with a key of its own the server learns by bind_key. */
  signing: boolean;
  /** The server's signing key, that signed messages are checked against. */
  serverSigningKey: string | null;
  /** The application's own options (recover-or-resync among them). */
  application: { consumer?: string; delivery?: DeliveryOptions };
}

/** A case's client, as the runtime made it. */
export interface Made {
  client: WSEClient;
  /** The public key of the signing key, when the case asked for one. */
  publicKey: string | null;
  /** Ends what the runtime made beside the client (a bridge's owner). */
  close?: () => void;
}

export interface DriverOptions {
  control: string;
  agent: string;
  runtime: string;
  os: string;
  arch: string;
  make: (asked: Asked) => Promise<Made>;
  /**
   * The connection is another's (a bridge's owner): a paused window is not
   * ready at once, and the connection ends a moment later.
   */
  bridged: boolean;
  /** Only these cases (all by default). */
  only?: string[];
  /** Hears each case's trace. */
  trace?: (name: string, trace: unknown[]) => void;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The control channel: requests and their answers, by id. */
class Control {
  private next = 1;
  private readonly answers = new Map<number, (answer: any) => void>();

  private constructor(private readonly ws: WebSocket) {
    ws.onmessage = (event) => {
      const answer = JSON.parse(String(event.data));
      this.answers.get(answer.id)?.(answer);
      this.answers.delete(answer.id);
    };
  }

  static open(url: string): Promise<Control> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.onopen = () => resolve(new Control(ws));
      ws.onerror = () => reject(new Error(`no conformance server at ${url}`));
    });
  }

  call(op: string, body: Record<string, unknown> = {}): Promise<any> {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      this.answers.set(id, (answer) => (answer.ok ? resolve(answer) : reject(new Error(`${op}: ${answer.error}`))));
      this.ws.send(JSON.stringify({ id, op, ...body }));
    });
  }

  close(): void {
    this.ws.close();
  }
}

/** One case: the client, what it saw, and the steps' doings. */
class Run {
  readonly trace: any[] = [];
  private made: Made | null = null;
  private release: Array<() => void> = [];
  private ready = false;
  private readonly published = new Map<string, number[]>();
  private readonly resyncs: Array<{ topics: string[]; done: () => void }> = [];
  private bound = false;
  private readonly began = Date.now();

  constructor(
    private readonly begun: any,
    private readonly control: Control,
    private readonly options: DriverOptions,
  ) {}

  private get client(): WSEClient | null {
    return this.made?.client ?? null;
  }

  private record(event: Record<string, unknown>): void {
    this.trace.push({ ...event, t: Date.now() - this.began });
  }

  private messages(where: (event: any) => boolean): any[] {
    return this.trace.filter((e) => e.ev === 'message' && where(e));
  }

  /** Whether `test` holds within `ms`. */
  private async until(test: () => boolean, ms: number): Promise<boolean> {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (test()) return true;
      await sleep(10);
    }
    return test();
  }

  async go(): Promise<void> {
    const steps: any[] = this.begun.steps;
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      try {
        if (step.server) {
          this.record({ ev: 'step', server: step.server });
          const done = await this.control.call('act', { step });
          if (step.server === 'publish') {
            this.published.set(done.topic, [...(this.published.get(done.topic) ?? []), ...done.n]);
          }
          continue;
        }
        if (!(await this.clientStep(step))) {
          this.record({ ev: 'timeout', step: i, waiting: step });
          break;
        }
      } catch (error) {
        this.record({ ev: 'error', message: `step ${i}: ${error instanceof Error ? error.message : String(error)}` });
        break;
      }
    }
    this.close();
  }

  /** A step of the client's: false when what it waits for did not come. */
  private async clientStep(step: any): Promise<boolean> {
    switch (step.client) {
      case 'connect':
        await this.connect(step);
        return true;
      case 'wait':
        return this.until(() => this.holds(step), step.timeout_ms ?? 10_000);
      case 'send':
        await this.send(step);
        return true;
      case 'pause':
        this.client?.pause('test');
        return true;
      case 'resume':
        this.client?.resume('test');
        return true;
      case 'resynced': {
        const topics: string[] = step.topics;
        this.record({ ev: 'resynced', topics });
        for (const resync of this.resyncs.splice(0)) {
          if (resync.topics.some((t) => topics.includes(t))) resync.done();
          else this.resyncs.push(resync);
        }
        return true;
      }
      case 'sleep':
        await sleep(step.ms ?? 0);
        return true;
      case 'close':
        this.close();
        return true;
      default:
        throw new Error(`no such client step: ${step.client}`);
    }
  }

  private holds(step: any): boolean {
    switch (step.for) {
      case 'ready':
        return this.ready;
      case 'not_ready':
        // A window is not ready once paused; the connection, its owner's,
        // ends a moment later, when its share was the last.
        return (
          !this.ready && (!this.options.bridged || this.client?.state.getSnapshot().connection !== 'connected')
        );
      case 'messages':
        return this.messages((e) => e.topic === step.topic).length >= step.count;
      case 'message':
        return this.messages((e) => e.type === step.type).length > 0;
      case 'published': {
        const wanted = (this.published.get(step.topic) ?? []).slice(-step.last);
        const got = new Set(this.messages((e) => e.topic === step.topic).map((e) => e.n));
        return wanted.length === step.last && wanted.every((n) => got.has(n));
      }
      case 'break':
        return this.trace.some(
          (e) => (e.ev === 'break' && e.topic === step.topic) || (e.ev === 'resync' && e.topics.includes(step.topic)),
        );
      case 'resync':
        return this.trace.some((e) => e.ev === 'resync' && !e.auto && e.topics.includes(step.topic));
      case 'delivered': {
        const delivered = new Set(this.trace.filter((e) => e.ev === 'delivered').flatMap((e) => e.ids));
        const ids: string[] = step.all
          ? this.trace.filter((e) => e.ev === 'sent' && e.ok && !e.live && !e.signed).map((e) => e.id)
          : step.ids;
        return ids.every((id) => delivered.has(id));
      }
      default:
        throw new Error(`no such wait: ${step.for}`);
    }
  }

  private async connect(step: any): Promise<void> {
    const rr = !!step.recover_or_resync;
    const made = await this.options.make({
      url: this.begun.url,
      token: this.begun.tokens[step.user ?? 'ann'],
      format: step.format === 'msgpack' ? 'msgpack' : 'json',
      e2eKey: step.e2e ? this.begun.identity_key : null,
      signing: !!step.signing,
      serverSigningKey: this.begun.signing_key ?? null,
      application: rr
        ? {
            consumer: 'conformance',
            delivery: { mode: 'recover-or-resync', resync: (request: ResyncRequest) => this.resync(request) },
          }
        : {},
    });
    this.made = made;
    const client = made.client;
    this.release.push(
      client.state.listen((snapshot) => {
        if (snapshot.ready && !this.ready) this.record({ ev: 'ready' });
        this.ready = snapshot.ready;
      }),
    );
    for (const type of ['tick', 'echoed', 'signed_taken', 'key_bound']) {
      this.release.push(
        client.onMessage(type, (message) => {
          this.record({
            ev: 'message',
            type,
            topic: message.tp ?? null,
            n: typeof message.p?.n === 'number' ? message.p.n : null,
            payload: message.p,
          });
          // Applied: the checkpoint moves on.
          if (rr && message.tp) client.commit(message);
        }),
      );
    }
    this.release.push(
      client.on('wseGap', (gap) => this.record({ ev: 'break', topic: gap.topic, kind: 'gap' })),
      client.on('wseContinuity', (detail) => {
        if (detail.continuity !== 'continuous') {
          this.record({ ev: 'break', topic: detail.topic, kind: 'continuity', reason: detail.reason });
        }
      }),
      client.on('wseDelivered', (detail) => this.record({ ev: 'delivered', ids: detail.ids })),
      client.on('wseNotDelivered', (detail) =>
        this.record({ ev: 'not_delivered', reason: detail.reason, ids: detail.ids }),
      ),
    );
    if (step.topics?.length) {
      const lease = client.subscribe(step.topics);
      this.release.push(() => lease.release());
    }
    const lease = client.retain();
    this.release.push(() => lease.release());
  }

  /** The application's resync: at a cold start at once; else once a step says it is done. */
  private resync(request: ResyncRequest): Promise<void> | void {
    const topics = [...request.topics];
    const cold = Object.values(request.reasons).every((reason) => reason === 'cold_start');
    this.record({ ev: 'resync', topics, auto: cold, reasons: request.reasons });
    if (cold) {
      this.record({ ev: 'resynced', topics, auto: true });
      return;
    }
    return new Promise((done) => this.resyncs.push({ topics, done }));
  }

  private async send(step: any): Promise<void> {
    const client = this.client;
    if (!client) throw new Error('send before connect');
    const count: number = step.count ?? 1;
    for (let k = 1; k <= count; k++) {
      const id: string = step.id ?? `${step.type}-${k}`;
      const payload = step.payload ?? { n: k };
      if (step.signed) {
        if (!this.bound) {
          const bind = client.send('bind_key', { public_key: this.made?.publicKey }, { id: 'bind-key' });
          this.record({ ev: 'sent', id: 'bind-key', type: 'bind_key', live: false, ok: bind !== 'dropped' });
          if (!(await this.until(() => this.messages((e) => e.type === 'key_bound').length > 0, 10_000))) {
            throw new Error('the server never bound the key');
          }
          this.bound = true;
        }
        const status = await client.sendSigned(step.type, payload, { nonce: id });
        this.record({ ev: 'sent', id, type: step.type, signed: true, live: false, ok: status !== 'dropped' });
        continue;
      }
      const status = client.send(step.type, payload, { id, live: !!step.live });
      this.record({ ev: 'sent', id, type: step.type, live: !!step.live, ok: status !== 'dropped', status });
    }
  }

  close(): void {
    for (const release of this.release.splice(0).reverse()) release();
    this.made?.client.destroy();
    this.made?.close?.();
    this.made = null;
    this.ready = false;
  }
}

/** Runs every case the agent can, each judged by the server: what failed, and why. */
export async function runCases(options: DriverOptions): Promise<string[]> {
  const control = await Control.open(options.control);
  const { cases } = await control.call('hello', {
    agent: options.agent,
    version: '3.2.0',
    runtime: options.runtime,
    os: options.os,
    arch: options.arch,
    features: FEATURES,
  });
  const failed: string[] = [];
  for (const { case: name } of cases) {
    if (options.only && !options.only.includes(name)) continue;
    const begun = await control.call('begin', { case: name });
    const run = new Run(begun, control, options);
    await run.go();
    for (let at = 0; at < run.trace.length; at += 500) {
      await control.call('trace', { events: run.trace.slice(at, at + 500) });
    }
    const judged = await control.call('end');
    if (judged.verdict !== 'pass') failed.push(`${name}: ${judged.failures.join('; ')}`);
    options.trace?.(name, run.trace);
  }
  await control.call('bye');
  control.close();
  return failed;
}
