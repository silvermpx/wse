// =============================================================================
// WebSocket Engine - The outbox in a file (Node, Electron's main process)
// =============================================================================
//
// The file the Rust and Python clients write (crates/wse-client/tests/vectors/
// outbox.json holds it): a log of lines, each `<crc32 of the salt and the
// JSON, 8 hex digits> <JSON>`, read up to the first that does not check. The
// first line names the format, the salt of the lines after it, the producer
// and the next number.
//
// The event loop never waits for the disk: a batch of lines goes to the file
// at once and to the disk right after (on macOS past the drive's cache), one
// write at a time, the next batch taking what came meanwhile. The file is
// written anew beside the old one and put in its place. When the process
// ends, what is not written yet is written at once.
//
// The file is the client's alone while the process runs, held by the
// system: on macOS and the BSDs a lock on `<path>.lock` (the one the Rust
// and Python clients take), on Windows that file opened for this process
// alone, on Linux a socket of the abstract namespace named after the file.
// The system lets go of each when the process ends, however it ends.

import {
  close,
  closeSync,
  constants,
  fdatasync,
  fdatasyncSync,
  fsync,
  fsyncSync,
  open,
  openSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  unlinkSync,
  write,
  writeSync,
} from 'node:fs';
import { createHash, randomBytes } from 'node:crypto';
import { createServer } from 'node:net';
import { basename, dirname, join } from 'node:path';
import * as zlib from 'node:zlib';
import type { WSMessage } from '../types.js';
import {
  numbered,
  unnumbered,
  type OutboxLine,
  type OutboxLog,
  type OutboxStore,
  type ReadOutbox,
  type StoredMessage,
  type StoredOutbox,
} from '../services/Outbox.js';

// -- The lines ----------------------------------------------------------------

type Crc = (data: string, value: number) => number;

/** CRC-32 of a string's UTF-8, from `value` (the CRC of what came before). */
const crc32: Crc =
  (zlib as unknown as { crc32?: Crc }).crc32 ??
  (() => {
    // Node before 20.15 and 22.2: the table of the same polynomial.
    const table = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      table[n] = c >>> 0;
    }
    return (data: string, value: number): number => {
      const bytes = Buffer.from(data, 'utf8');
      let c = ~value;
      for (let i = 0; i < bytes.length; i++) c = table[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
      return ~c >>> 0;
    };
  })();

/** A line: its check (from the salt's CRC), a space, its JSON, a newline. */
export function encodeLine(seed: number, body: string): string {
  return `${crc32(body, seed).toString(16).padStart(8, '0')} ${body}\n`;
}

/** A message kept, its text as it goes (a JSON object, numbered). */
export function addBody(record: StoredMessage): string {
  return `${addPrefix(record)}${numbered(record.s, record.json)}}}`;
}

/** A kept message's line up to its text, as every client writes it. */
function addPrefix(record: Omit<StoredMessage, 'json'>): string {
  const requires = record.requires && record.requires.length > 0 ? `"requires":${JSON.stringify(record.requires)},` : '';
  return (
    `{"add":{"s":${record.s},"id":${JSON.stringify(record.id)},"at":${record.at},"until":${record.until},` +
    `"owned":${record.owned},${requires}"text":`
  );
}

/** The salt's CRC: where each line's check starts from. */
export function saltSeed(salt: string): number {
  return crc32(salt, 0);
}

/** The whole file for `state`, under `salt`: its text and how many lines. */
export function encodeOutbox(state: StoredOutbox, salt: string): { data: string; lines: number } {
  const seed = saltSeed(salt);
  const out = [
    encodeLine(0, JSON.stringify({ wse_outbox: 1, salt, producer: state.producer, next: state.next })),
  ];
  if (state.node !== null) out.push(encodeLine(seed, JSON.stringify({ node: state.node })));
  if (state.written > 0) out.push(encodeLine(seed, JSON.stringify({ written: state.written })));
  if (state.user !== null) out.push(encodeLine(seed, JSON.stringify({ user: state.user })));
  for (const record of state.records) out.push(encodeLine(seed, addBody(record)));
  return { data: out.join(''), lines: out.length };
}

const HEX8 = /^[0-9a-fA-F]{8}$/;

/** One line's JSON, when its check holds. */
function checked(line: string, seed: number): string | null {
  if (line.length < 10 || line.charCodeAt(8) !== 32) return null;
  const crc = line.slice(0, 8);
  if (!HEX8.test(crc)) return null;
  const body = line.slice(9);
  return crc32(body, seed) === parseInt(crc, 16) ? body : null;
}

/** A number of the file: a whole number, not below zero. */
function isCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

/** Topics a kept message requires: a list of names, none empty. */
function isTopics(value: unknown): value is string[] {
  return Array.isArray(value) && value.length > 0 && value.every((t) => typeof t === 'string' && t.length > 0);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** A message kept, as read: its text taken as it was written. */
function readAdd(body: string, add: unknown): (StoredMessage & { message: WSMessage }) | null {
  if (!isObject(add)) return null;
  const { s, id, at, until, owned, requires, text } = add;
  if (!isCount(s) || typeof id !== 'string' || !isCount(at) || !isCount(until)) return null;
  if (typeof owned !== 'boolean' || !isObject(text)) return null;
  if (requires !== undefined && !isTopics(requires)) return null;
  const prefix = addPrefix({ s, id, at, until, owned, requires });
  if (!body.startsWith(prefix) || !body.endsWith('}}')) return null;
  const { s: _number, ...message } = text;
  return {
    s,
    id,
    at,
    until,
    owned,
    requires,
    json: unnumbered(body.slice(prefix.length, -2)),
    message: message as unknown as WSMessage,
  };
}

/**
 * What a file holds, read up to the first line that does not check: a tail
 * cut short, or left from an older file, is not read. A line of a kind a
 * later version writes is skipped. Null: not a file of this format.
 */
export function readOutbox(data: string): ReadOutbox | null {
  let end = data.indexOf('\n');
  const headBody = checked(end < 0 ? data : data.slice(0, end), 0);
  if (headBody === null) return null;
  let head: unknown;
  try {
    head = JSON.parse(headBody);
  } catch {
    return null;
  }
  if (!isObject(head) || head.wse_outbox !== 1) return null;
  const seed = saltSeed(typeof head.salt === 'string' ? head.salt : '');
  const read: ReadOutbox = {
    producer: typeof head.producer === 'string' ? head.producer : '',
    node: null,
    written: 0,
    next: isCount(head.next) ? Math.max(1, head.next) : 1,
    user: null,
    records: [],
  };
  // The records are in the order of their numbers: what an answer took is
  // at the front, before `first`.
  let records = read.records;
  let first = 0;
  while (end >= 0) {
    const start = end + 1;
    end = data.indexOf('\n', start);
    const line = end < 0 ? data.slice(start) : data.slice(start, end);
    if (line.length === 0) continue;
    const body = checked(line, seed);
    let item: unknown;
    try {
      item = body === null ? null : JSON.parse(body);
    } catch {
      item = null;
    }
    const keys = isObject(item) ? Object.keys(item) : [];
    if (body === null || keys.length !== 1) break;
    const kind = keys[0];
    const value = (item as Record<string, unknown>)[kind];
    if (kind === 'add') {
      const record = readAdd(body, value);
      if (!record) break;
      read.next = Math.max(read.next, record.s + 1);
      records.push(record);
    } else if (kind === 'written') {
      if (!isCount(value)) break;
      read.written = Math.max(read.written, value);
      read.next = Math.max(read.next, value + 1);
    } else if (kind === 'ack') {
      if (!isCount(value)) break;
      while (first < records.length && records[first].s <= value) first++;
    } else if (kind === 'drop') {
      if (!Array.isArray(value) || !value.every(isCount)) break;
      const gone = new Set(value as number[]);
      records = records.slice(first).filter((r) => !gone.has(r.s));
      first = 0;
    } else if (kind === 'user') {
      if (value !== null && typeof value !== 'string') break;
      read.user = value as string | null;
    } else if (kind === 'node') {
      if (typeof value !== 'string') break;
      read.node = value;
    }
    // A line of a kind a later version writes is skipped: a change it must
    // not skip is a new format (`wse_outbox`).
  }
  read.records = first > 0 ? records.slice(first) : records;
  return read;
}

// -- The lock -------------------------------------------------------------------

interface Lock {
  release(): void;
}

/** O_EXLOCK of macOS and the BSDs: the lock flock takes, taken with the open. */
const O_EXLOCK = 0x20;
/** UV_FS_O_EXLOCK of libuv on Windows: the file shared with no other open. */
const WINDOWS_EXCLUSIVE = 0x10000000;

function anotherClients(path: string): Error {
  return new Error(`${path} is another client's`);
}

/** The file, this process's alone until it lets go or ends. */
/**
 * The name of the lock every client takes of the file at `path` on Linux,
 * a socket in the abstract namespace beside the lock file the Rust and
 * Python clients lock too: `wse-outbox-` and 40 hex digits of the SHA-256 of
 * its path, its directory's resolved.
 */
export function lockName(path: string): string {
  const where = join(realpathSync(dirname(path)), basename(path));
  return `wse-outbox-${createHash('sha256').update(where).digest('hex').slice(0, 40)}`;
}

/**
 * Whether Node takes on `platform` a lock of the file the clients of the
 * other runtimes see: on Linux a Node before 22 binds the socket's name
 * padded to the whole address, a name no other client meets.
 */
export function lockSeen(platform: string, node: string): boolean {
  if (platform !== 'linux' && platform !== 'android') return true;
  return Number(node.split('.')[0]) >= 22;
}

function takeLock(path: string): Lock {
  const platform = process.platform;
  if (platform === 'linux' || platform === 'android') {
    if (!lockSeen(platform, process.versions.node)) {
      throw new Error(
        `the outbox's file needs Node 22 or newer on ${platform}: Node ${process.versions.node} takes a lock the other clients do not see`,
      );
    }
    const name = `\0${lockName(path)}`;
    const server = createServer();
    server.on('error', () => {});
    // Bound at once, or not: exclusive, so a cluster worker binds it itself.
    server.listen({ path: name, exclusive: true });
    if (!server.listening) {
      server.close();
      throw anotherClients(path);
    }
    server.unref();
    return { release: () => server.close() };
  }
  let flags = constants.O_RDWR | constants.O_CREAT;
  if (platform === 'win32') flags |= WINDOWS_EXCLUSIVE;
  else if (platform === 'darwin' || platform === 'freebsd' || platform === 'openbsd' || platform === 'netbsd') {
    flags |= O_EXLOCK | constants.O_NONBLOCK;
  } else {
    throw new Error(`no lock for the outbox's file on ${platform}`);
  }
  let fd: number;
  try {
    fd = openSync(`${path}.lock`, flags, 0o600);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'EAGAIN' || code === 'EWOULDBLOCK' || code === 'EBUSY') throw anotherClients(path);
    throw error;
  }
  return { release: () => closeSync(fd) };
}

// -- Writing ------------------------------------------------------------------------

/** All of `bytes` to `fd`, at once. */
function writeAllSync(fd: number, bytes: Buffer): void {
  let at = 0;
  while (at < bytes.length) at += writeSync(fd, bytes, at, bytes.length - at);
}

/** The directory's entry for the file, to the disk (a rename is in it). */
function syncDirSync(path: string): void {
  if (process.platform === 'win32') return;
  try {
    const fd = openSync(dirname(path), constants.O_RDONLY);
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  } catch {
    // A directory that cannot be synced: the rename stands as the system keeps it.
  }
}

/** The logs open in this process: written at once when it ends. */
const openLogs = new Set<FileLog>();
let exitHooked = false;

function hookExit(): void {
  if (exitHooked) return;
  exitHooked = true;
  process.on('exit', () => {
    for (const log of [...openLogs]) log.close();
  });
}

const APPEND = constants.O_WRONLY | constants.O_CREAT | constants.O_APPEND;

class FileLog implements OutboxLog {
  lines = 0;
  /** The CRC of the salt of the lines after the head. */
  private seed = 0;
  /** The lines since the last flush. */
  private batch: string[] = [];
  /** Lines handed to be appended, not written yet. */
  private queued: string[] = [];
  /** The file anew, handed and not started. */
  private rewriteQueued: string | null = null;
  /** A write runs: an append (on `fd`), or the file anew. */
  private busy: 'append' | 'rewrite' | null = null;
  /**
   * Who hears that what was handed is written: with `queued`, with the
   * rewrite handed, and with the write that runs.
   */
  private queuedWritten: Array<() => void> = [];
  private rewriteWritten: Array<() => void> = [];
  /** Null once the write that runs was told (or failed). */
  private running: Array<() => void> | null = null;
  /** A write failed and the outbox was not told yet. */
  private broken = false;
  /** The file lacks what a failed write held: it is written anew before anything is appended. */
  private stale = false;
  /** A failure was said; the next is said after a success. */
  private failing = false;
  private closed = false;
  private fd: number;
  /**
   * The file anew is written beside it under a name of this log's own: a
   * write that runs on once the log was let go never touches the next one's.
   */
  private readonly tmp: string;

  constructor(
    private readonly path: string,
    fd: number,
    private readonly lock: Lock,
    private readonly state: () => StoredOutbox,
    private readonly report: (operation: string, error: unknown) => void,
  ) {
    this.fd = fd;
    this.tmp = `${path}${TMP}${randomBytes(8).toString('hex')}`;
    openLogs.add(this);
    hookExit();
  }

  add(record: StoredMessage): void {
    this.batch.push(encodeLine(this.seed, addBody(record)));
    this.lines++;
  }

  put(line: OutboxLine): void {
    this.batch.push(encodeLine(this.seed, JSON.stringify(line)));
    this.lines++;
  }

  flush(written?: () => void): void {
    if (this.batch.length > 0) {
      for (const line of this.batch) this.queued.push(line);
      this.batch = [];
    }
    if (written) {
      // With what is handed last: the appends go after a rewrite that
      // waits, and that after the write that runs.
      if (this.queued.length > 0) this.queuedWritten.push(written);
      else if (this.rewriteQueued !== null) this.rewriteWritten.push(written);
      else if (this.busy && this.running) this.running.push(written);
      else written();
    }
    this.kick();
  }

  forget(): void {
    this.batch = [];
  }

  takeBroken(): boolean {
    const broken = this.broken;
    this.broken = false;
    return broken;
  }

  rewrite(state: StoredOutbox, written?: () => void): void {
    const salt = randomBytes(16).toString('hex');
    const { data, lines } = encodeOutbox(state, salt);
    this.seed = saltSeed(salt);
    this.lines = lines;
    // What waits to be appended is in it.
    this.batch = [];
    this.queued = [];
    this.rewriteWritten.push(...this.queuedWritten);
    this.queuedWritten = [];
    if (written) this.rewriteWritten.push(written);
    this.rewriteQueued = data;
    this.kick();
  }

  /** What was handed with the write that runs is written: who waited for it hears so. */
  private told(): void {
    const running = this.running;
    this.running = null;
    for (const written of running ?? []) written();
  }

  /** The next write, once none runs. */
  private kick(): void {
    if (this.busy || this.closed) return;
    const data = this.rewriteQueued;
    if (data !== null) {
      this.rewriteQueued = null;
      this.running = this.rewriteWritten;
      this.rewriteWritten = [];
      this.busy = 'rewrite';
      this.writeAnew(data);
      return;
    }
    if (this.queued.length === 0) return;
    if (this.stale) {
      // In what is written anew.
      this.queued = [];
      this.queuedWritten = [];
      return;
    }
    const bytes = Buffer.from(this.queued.join(''), 'utf8');
    this.queued = [];
    this.running = this.queuedWritten;
    this.queuedWritten = [];
    this.busy = 'append';
    const fd = this.fd;
    write(fd, bytes, 0, bytes.length, null, (error, written) => {
      if (error || written !== bytes.length) {
        this.running = null;
        this.appended(fd, error ?? new Error(`wrote ${written} of ${bytes.length} bytes`));
        return;
      }
      // Written, past a crash of the process: who waited for it hears so;
      // the disk has it once synced, at once after.
      if (!this.closed) this.told();
      fdatasync(fd, (error) => this.appended(fd, error));
    });
  }

  private appended(fd: number, error: Error | null): void {
    this.busy = null;
    if (this.closed) {
      // Let go when it ended: the fd was left for this write.
      close(fd, () => {});
      return;
    }
    if (error) this.fail(error);
    else this.failing = false;
    this.kick();
  }

  /** The new file beside the old, on the disk, then in its place. */
  private writeAnew(data: string): void {
    const tmp = this.tmp;
    const bytes = Buffer.from(data, 'utf8');
    const done = (error: Error | null): void => {
      this.busy = null;
      if (error || this.closed) {
        try {
          unlinkSync(tmp);
        } catch {
          // Not made, or gone.
        }
      }
      if (this.closed) return;
      if (error) {
        this.running = null;
        this.fail(error);
      } else {
        this.stale = false;
        this.failing = false;
        this.told();
      }
      this.kick();
    };
    open(tmp, 'w', 0o600, (error, tfd) => {
      if (error) return done(error);
      write(tfd, bytes, 0, bytes.length, null, (error, written) => {
        const failed = error ?? (written !== bytes.length ? new Error(`wrote ${written} of ${bytes.length} bytes`) : null);
        const finish = (error: Error | null): void =>
          close(tfd, () => {
            // Written at once as it ended: this one goes.
            if (error || this.closed) return done(error);
            try {
              renameSync(tmp, this.path);
            } catch (error) {
              return done(error as Error);
            }
            try {
              closeSync(this.fd);
            } catch {
              // Closed by a failed open before.
            }
            try {
              this.fd = openSync(this.path, APPEND, 0o600);
            } catch (error) {
              this.fd = -1;
              return done(error as Error);
            }
            if (process.platform === 'win32') return done(null);
            open(dirname(this.path), 'r', (error, dfd) => {
              if (error) return done(null);
              fsync(dfd, () => close(dfd, () => done(null)));
            });
          });
        if (failed) return finish(failed);
        fdatasync(tfd, (error) => finish(error));
      });
    });
  }

  private fail(error: unknown): void {
    this.broken = true;
    this.stale = true;
    if (this.failing) return;
    this.failing = true;
    this.report('write', error);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    openLogs.delete(this);
    try {
      if (this.stale || this.busy || this.rewriteQueued !== null) {
        // What runs is behind what the outbox holds: the file anew, at once.
        const { data } = encodeOutbox(this.state(), randomBytes(16).toString('hex'));
        const tmp = `${this.tmp}-closing`;
        const fd = openSync(tmp, 'w', 0o600);
        try {
          writeAllSync(fd, Buffer.from(data, 'utf8'));
          fdatasyncSync(fd);
        } finally {
          closeSync(fd);
        }
        renameSync(tmp, this.path);
        syncDirSync(this.path);
      } else if (this.queued.length > 0 || this.batch.length > 0) {
        writeAllSync(this.fd, Buffer.from(this.queued.join('') + this.batch.join(''), 'utf8'));
        fdatasyncSync(this.fd);
      }
    } catch (error) {
      this.report('write', error);
    }
    this.queued = [];
    this.batch = [];
    // An append that runs closes its fd when it ends.
    if (this.busy !== 'append') {
      try {
        closeSync(this.fd);
      } catch {
        // Closed.
      }
    }
    this.lock.release();
  }
}

/**
 * Keeps the outbox in the file at `path`, so that what waits outlives the
 * process: `createWSEClient({ offline: { store: fileOutbox(path) } })`. The
 * file is the client's alone while the process runs; one another client has
 * is a `wseStorageError` event (`open`), and the outbox goes on in memory.
 */
/** What a log's file anew is named beside it: the file's name, this, and the log's own part. */
const TMP = '.tmp-';

/**
 * The files anew that logs let go without finishing (a crash, or a write
 * that ran on past the close): gone, once the lock is this log's.
 */
function removeLeftovers(path: string): void {
  const prefix = `${basename(path)}${TMP}`;
  let names: string[];
  try {
    names = readdirSync(dirname(path));
  } catch {
    return;
  }
  for (const name of names) {
    if (!name.startsWith(prefix)) continue;
    try {
      unlinkSync(join(dirname(path), name));
    } catch {
      // Gone already.
    }
  }
}

export function fileOutbox(path: string): OutboxStore {
  return {
    open(state, report) {
      const lock = takeLock(path);
      try {
        let data: string | null = null;
        try {
          data = readFileSync(path, 'utf8');
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }
        const read = data === null ? null : readOutbox(data);
        removeLeftovers(path);
        const fd = openSync(path, APPEND, 0o600);
        const log = new FileLog(path, fd, lock, state, report);
        // Written anew at once: a tail a crash left goes, under a new salt.
        log.rewrite(read ?? state());
        return { read, log };
      } catch (error) {
        lock.release();
        throw error;
      }
    },
  };
}
