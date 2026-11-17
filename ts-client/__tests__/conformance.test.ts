// The TypeScript client's driver for the conformance server
// (crates/wse-conformance): it runs each case it can, step by step, and
// records what its client saw and did; the server judges. Runs when
// WSE_CONFORMANCE holds the server's control URL:
//
//   cargo run -p wse-conformance -- serve        # prints the control URL
//   WSE_CONFORMANCE=ws://127.0.0.1:P/control npx vitest run --project client ts-client/__tests__/conformance.test.ts

import { describe, expect, it } from 'vitest';
import { createWSEClient, type WSEClient } from '../core/client.js';
import type { ResyncRequest } from '../core/delivery.js';
import { logger, LogLevel } from '../utils/logger.js';
import { generateSigningKey, type SigningKey } from '../utils/signing.js';

const CONTROL = process.env.WSE_CONFORMANCE;
if (process.env.WSE_CONFORMANCE_REQUIRED === '1' && !CONTROL) {
  throw new Error('WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not');
}

/** What the TypeScript client has, by the catalog's names. */
const FEATURES = ['outbox', 'start', 'live', 'msgpack', 'e2e', 'signing', 'recover_or_resync'];

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
  private client: WSEClient | null = null;
  private release: Array<() => void> = [];
  private ready = false;
  private readonly published = new Map<string, number[]>();
  private readonly resyncs: Array<{ topics: string[]; done: () => void }> = [];
  private signing: SigningKey | null = null;
  private bound = false;

  constructor(
    private readonly begun: any,
    private readonly control: Control,
  ) {}

  private readonly began = Date.now();

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
        if (!(await this.client_(step))) {
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
  private async client_(step: any): Promise<boolean> {
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
        return !this.ready;
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
    const token: string = this.begun.tokens[step.user ?? 'ann'];
    const security: Record<string, unknown> = { encryptionEnabled: !!step.e2e };
    if (step.e2e) security.serverKeys = [this.begun.identity_key];
    if (step.signing) {
      this.signing = await generateSigningKey();
      security.signingKey = this.signing.keyPair;
      security.serverSigningKeys = [this.begun.signing_key];
      // The application binds the key (bind_key): the token names none.
      security.bindSession = false;
    }
    const rr = !!step.recover_or_resync;
    const client = createWSEClient({
      endpoints: [this.begun.url],
      auth: 'token',
      // Node's WebSocket takes headers: the token goes as the bearer.
      webSocket: (url) => new WebSocket(url, { headers: { Authorization: `Bearer ${token}` } } as any),
      reconnection: { baseDelay: 50, maxDelay: 300, jitter: false },
      wireFormat: step.format === 'msgpack' ? 'msgpack' : 'json',
      security: security as any,
      ...(rr
        ? {
            consumer: 'conformance',
            delivery: {
              mode: 'recover-or-resync' as const,
              resync: (request: ResyncRequest) => this.resync(request),
            },
          }
        : {}),
    });
    this.client = client;
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
          const bind = client.send('bind_key', { public_key: this.signing!.publicKey }, { id: 'bind-key' });
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
    this.client?.destroy();
    this.client = null;
    this.ready = false;
  }
}

describe.skipIf(!CONTROL)('conformance', () => {
  it('runs every case the TypeScript client can, and the server judges each', async () => {
    logger.setLevel(process.env.WSE_CONFORMANCE_DEBUG === '1' ? LogLevel.DEBUG : LogLevel.ERROR);
    const control = await Control.open(CONTROL!);
    const { cases } = await control.call('hello', {
      agent: 'ts-core',
      version: '3.2.0',
      runtime: `node ${process.versions.node}`,
      os: process.platform,
      arch: process.arch,
      features: FEATURES,
    });
    const failed: string[] = [];
    // WSE_CONFORMANCE_CASES=a,b runs those alone.
    const only = process.env.WSE_CONFORMANCE_CASES?.split(',');
    for (const { case: name } of cases) {
      if (only && !only.includes(name)) continue;
      const begun = await control.call('begin', { case: name });
      const run = new Run(begun, control);
      await run.go();
      for (let at = 0; at < run.trace.length; at += 500) {
        await control.call('trace', { events: run.trace.slice(at, at + 500) });
      }
      const judged = await control.call('end');
      if (judged.verdict !== 'pass') failed.push(`${name}: ${judged.failures.join('; ')}`);
      if (process.env.WSE_CONFORMANCE_TRACE === '1') console.log(name, JSON.stringify(run.trace));
    }
    await control.call('bye');
    control.close();
    expect(failed).toEqual([]);
  }, 600_000);
});
