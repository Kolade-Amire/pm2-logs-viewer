import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import { getLogPaths, Pm2UnavailableError } from './pm2Client';
import { LogLine } from './types';

const BACKLOG_LINES = 50;
/** Stat-poll safety net: fs.watch semantics differ between macOS (dev) and Linux (prod). */
const STAT_POLL_INTERVAL_MS = 2_000;
/** Re-resolve pm2 log paths every N polls (~10s) to pick up scaled/restarted instances. */
const PATH_REFRESH_EVERY_TICKS = 5;
const BACKLOG_READ_BYTES = 64 * 1024;
const READ_CHUNK_BYTES = 64 * 1024;
/** A "line" longer than this is flushed as-is rather than buffered forever. */
const MAX_REMAINDER_BYTES = 1024 * 1024;

const NEWLINE = 0x0a;

export type LineListener = (line: LogLine) => void;
export type Unsubscribe = () => void;

function isErrnoException(err: unknown): err is NodeJS.ErrnoException {
  return err instanceof Error && typeof (err as NodeJS.ErrnoException).code === 'string';
}

/** Expected filesystem races around rotation — everything else is a bug and propagates. */
function isExpectedFsError(err: unknown): boolean {
  return isErrnoException(err) && (err.code === 'ENOENT' || err.code === 'EACCES' || err.code === 'EPERM');
}

interface TailTarget {
  filePath: string;
  pm_id: number;
  stream: 'out' | 'err';
}

/**
 * Tails one log file: tracks an open handle + byte offset, detects rotation
 * (inode change / disappearance) and truncation (size < offset), and emits
 * complete lines. Byte-accurate: partial lines and multi-byte UTF-8 sequences
 * are carried across reads as a Buffer remainder.
 */
class FileTail {
  private handle: fsp.FileHandle | null = null;
  /**
   * Per-file watcher for write latency: macOS kqueue dir-watches don't report
   * content writes (Linux inotify does). Rotation correctness never depends on
   * this — the directory watcher + stat poll remain authoritative.
   */
  private watcher: fs.FSWatcher | null = null;
  private ino: bigint | null = null;
  private offset = 0;
  private remainder: Buffer = Buffer.alloc(0);
  /** First successful open seeks to EOF (backlog is sent separately); reopens read from 0. */
  private firstOpenAtEnd = true;
  private checking = false;
  private recheck = false;
  private closed = false;

  constructor(
    readonly target: TailTarget,
    private readonly emit: LineListener,
  ) {}

  /** Serialized: concurrent triggers (watch event + poll) coalesce into one re-check. */
  async check(): Promise<void> {
    if (this.closed) return;
    if (this.checking) {
      this.recheck = true;
      return;
    }
    this.checking = true;
    try {
      do {
        this.recheck = false;
        await this.checkOnce();
      } while (this.recheck && !this.closed);
    } catch (err) {
      if (!isExpectedFsError(err)) throw err;
    } finally {
      this.checking = false;
    }
  }

  private async checkOnce(): Promise<void> {
    let stat: fs.BigIntStats;
    try {
      stat = await fsp.stat(this.target.filePath, { bigint: true });
    } catch (err) {
      if (isErrnoException(err) && err.code === 'ENOENT') {
        // Rotated away (renamed) and not yet recreated. pm2 keeps writing to
        // the renamed file until it reopens (e.g. reloadLogs), so keep the old
        // descriptor and keep draining it; it is closed with a final drain the
        // moment the path reappears (inode-change branch below). If the file
        // never returns, the path refresh retires this tail entirely.
        await this.readNew();
        return;
      }
      throw err;
    }

    if (this.handle && this.ino !== null && stat.ino !== this.ino) {
      // Recreated under the same name (rename + create rotation).
      await this.drainAndClose();
    }

    if (!this.handle) {
      try {
        this.handle = await fsp.open(this.target.filePath, 'r');
      } catch (err) {
        if (isErrnoException(err) && err.code === 'ENOENT') return; // lost the race; next event retries
        throw err;
      }
      const opened = await this.handle.stat({ bigint: true });
      this.ino = opened.ino;
      this.offset = this.firstOpenAtEnd ? Number(opened.size) : 0;
      this.firstOpenAtEnd = false;
      this.remainder = Buffer.alloc(0);
      this.watchFile();
    } else if (Number(stat.size) < this.offset) {
      // Truncated in place (copytruncate-style rotation).
      this.offset = 0;
      this.remainder = Buffer.alloc(0);
    }

    await this.readNew();
  }

  private async readNew(): Promise<void> {
    if (!this.handle) return;
    const chunk = Buffer.alloc(READ_CHUNK_BYTES);
    for (;;) {
      const { bytesRead } = await this.handle.read(chunk, 0, chunk.length, this.offset);
      if (bytesRead === 0) return;
      this.offset += bytesRead;
      this.consume(chunk.subarray(0, bytesRead));
    }
  }

  private consume(data: Buffer): void {
    let buf = this.remainder.length > 0 ? Buffer.concat([this.remainder, data]) : Buffer.from(data);
    let start = 0;
    let nl: number;
    while ((nl = buf.indexOf(NEWLINE, start)) !== -1) {
      this.emitLine(buf.subarray(start, nl));
      start = nl + 1;
    }
    this.remainder = buf.subarray(start);
    if (this.remainder.length > MAX_REMAINDER_BYTES) {
      this.emitLine(this.remainder);
      this.remainder = Buffer.alloc(0);
    }
  }

  private emitLine(bytes: Buffer): void {
    let line = bytes.toString('utf8');
    if (line.endsWith('\r')) line = line.slice(0, -1);
    this.emit({
      stream: this.target.stream,
      line,
      ts: new Date().toISOString(),
      pm_id: this.target.pm_id,
    });
  }

  private watchFile(): void {
    this.unwatchFile();
    try {
      this.watcher = fs.watch(this.target.filePath);
    } catch (err) {
      if (isExpectedFsError(err)) return; // poll still covers this file
      throw err;
    }
    this.watcher.on('change', () => void this.check());
    this.watcher.on('error', () => this.unwatchFile());
  }

  private unwatchFile(): void {
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }
  }

  /** Read whatever the (possibly renamed) file still holds via the open fd, then close it. */
  private async drainAndClose(): Promise<void> {
    this.unwatchFile();
    if (this.handle) {
      try {
        await this.readNew();
      } catch (err) {
        if (!isExpectedFsError(err)) throw err;
      }
      if (this.remainder.length > 0) {
        // Final partial line of the rotated-away file would otherwise be lost.
        this.emitLine(this.remainder);
      }
      const handle = this.handle;
      this.handle = null;
      await handle.close().catch(() => undefined);
    }
    this.ino = null;
    this.offset = 0;
    this.remainder = Buffer.alloc(0);
  }

  async close(): Promise<void> {
    this.closed = true;
    this.unwatchFile();
    const handle = this.handle;
    this.handle = null;
    if (handle) await handle.close().catch(() => undefined);
  }
}

/**
 * One shared tailer per process name, refcounted by subscribers. Tails all
 * instance files (out + err per pm_id) simultaneously and fans merged lines
 * out to every listener.
 */
class ProcessTailer {
  private readonly listeners = new Set<LineListener>();
  private readonly tails = new Map<string, FileTail>();
  private readonly watchers = new Map<string, fs.FSWatcher>();
  private pollTimer: NodeJS.Timeout | null = null;
  private tick = 0;
  private disposed = false;
  private startPromise: Promise<void> | null = null;

  constructor(private readonly processName: string) {}

  ensureStarted(): Promise<void> {
    if (!this.startPromise) {
      this.startPromise = this.start();
    }
    return this.startPromise;
  }

  private async start(): Promise<void> {
    await this.refreshTargets();
    await Promise.all([...this.tails.values()].map((tail) => tail.check()));
    this.pollTimer = setInterval(() => {
      void this.poll();
    }, STAT_POLL_INTERVAL_MS);
    this.pollTimer.unref();
  }

  private async poll(): Promise<void> {
    if (this.disposed) return;
    this.tick += 1;
    if (this.tick % PATH_REFRESH_EVERY_TICKS === 0) {
      try {
        await this.refreshTargets();
      } catch (err) {
        if (!(err instanceof Pm2UnavailableError)) throw err;
        // Daemon outage: keep tailing the files we already know about.
      }
    }
    for (const tail of this.tails.values()) {
      await tail.check();
    }
  }

  /** Resolve current instance log paths from pm2 and reconcile tails/watchers. */
  private async refreshTargets(): Promise<void> {
    const instances = await getLogPaths(this.processName);
    if (this.disposed) return;

    const desired = new Map<string, TailTarget>();
    for (const inst of instances) {
      if (inst.outPath) desired.set(inst.outPath, { filePath: inst.outPath, pm_id: inst.pm_id, stream: 'out' });
      if (inst.errPath) desired.set(inst.errPath, { filePath: inst.errPath, pm_id: inst.pm_id, stream: 'err' });
    }

    for (const [filePath, tail] of this.tails) {
      if (!desired.has(filePath)) {
        this.tails.delete(filePath);
        void tail.close();
      }
    }
    for (const [filePath, target] of desired) {
      if (!this.tails.has(filePath)) {
        this.tails.set(filePath, new FileTail(target, this.fanOut));
      }
    }

    const neededDirs = new Set([...desired.keys()].map((p) => path.dirname(p)));
    for (const [dir, watcher] of this.watchers) {
      if (!neededDirs.has(dir)) {
        this.watchers.delete(dir);
        watcher.close();
      }
    }
    for (const dir of neededDirs) {
      this.ensureWatcher(dir);
    }
  }

  private ensureWatcher(dir: string): void {
    if (this.watchers.has(dir)) return;
    let watcher: fs.FSWatcher;
    try {
      watcher = fs.watch(dir);
    } catch (err) {
      if (isExpectedFsError(err)) return; // dir missing; retried on next path refresh
      throw err;
    }
    watcher.on('change', (_event, filename) => {
      this.onDirEvent(dir, typeof filename === 'string' ? filename : null);
    });
    watcher.on('error', () => {
      // Watched dir vanished or watcher broke; the stat poll still covers us
      // and the next path refresh re-creates the watcher.
      this.watchers.delete(dir);
      watcher.close();
    });
    this.watchers.set(dir, watcher);
  }

  private onDirEvent(dir: string, filename: string | null): void {
    if (this.disposed) return;
    for (const tail of this.tails.values()) {
      const { filePath } = tail.target;
      if (path.dirname(filePath) !== dir) continue;
      if (filename === null || path.basename(filePath) === filename) {
        void tail.check();
      }
    }
  }

  private fanOut: LineListener = (line) => {
    for (const listener of this.listeners) listener(line);
  };

  /**
   * Last BACKLOG_LINES across all instance files: per spec, file tails are
   * ordered by file mtime (oldest first), concatenated, and the trailing
   * BACKLOG_LINES overall are delivered to this listener only.
   */
  async sendBacklog(listener: LineListener): Promise<void> {
    const perFile: { mtimeMs: number; lines: LogLine[] }[] = [];
    for (const tail of this.tails.values()) {
      const result = await readLastLines(tail.target.filePath, BACKLOG_LINES);
      if (!result) continue;
      const ts = new Date(result.mtimeMs).toISOString();
      perFile.push({
        mtimeMs: result.mtimeMs,
        lines: result.lines.map((line) => ({
          stream: tail.target.stream,
          line,
          ts,
          pm_id: tail.target.pm_id,
        })),
      });
    }
    perFile.sort((a, b) => a.mtimeMs - b.mtimeMs);
    const merged = perFile.flatMap((f) => f.lines).slice(-BACKLOG_LINES);
    for (const line of merged) listener(line);
  }

  addListener(listener: LineListener): void {
    this.listeners.add(listener);
  }

  removeListener(listener: LineListener): boolean {
    this.listeners.delete(listener);
    return this.listeners.size === 0;
  }

  get hasListeners(): boolean {
    return this.listeners.size > 0;
  }

  dispose(): void {
    this.disposed = true;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    for (const watcher of this.watchers.values()) watcher.close();
    this.watchers.clear();
    for (const tail of this.tails.values()) void tail.close();
    this.tails.clear();
    this.listeners.clear();
  }
}

/** Tail of a file without touching tailer state: last `maxLines` lines + mtime. */
async function readLastLines(
  filePath: string,
  maxLines: number,
): Promise<{ lines: string[]; mtimeMs: number } | null> {
  let handle: fsp.FileHandle;
  try {
    handle = await fsp.open(filePath, 'r');
  } catch (err) {
    if (isExpectedFsError(err)) return null; // not written yet — graceful empty backlog
    throw err;
  }
  try {
    const stat = await handle.stat();
    const size = stat.size;
    const start = Math.max(0, size - BACKLOG_READ_BYTES);
    const length = size - start;
    if (length === 0) return { lines: [], mtimeMs: stat.mtimeMs };
    const buf = Buffer.alloc(length);
    await handle.read(buf, 0, length, start);
    let lines = buf.toString('utf8').split('\n');
    if (lines[lines.length - 1] === '') lines.pop(); // trailing newline
    if (start > 0) lines.shift(); // first line is partial when we cut into the file
    lines = lines.map((l) => (l.endsWith('\r') ? l.slice(0, -1) : l));
    return { lines: lines.slice(-maxLines), mtimeMs: stat.mtimeMs };
  } finally {
    await handle.close().catch(() => undefined);
  }
}

const tailers = new Map<string, ProcessTailer>();

/**
 * Subscribe to a process's merged log stream. Sends the backlog to `listener`
 * first, then live lines. Returns an unsubscribe function; the underlying
 * tailer (watchers, fds, timers) is torn down when the last subscriber leaves.
 */
export async function subscribe(processName: string, listener: LineListener): Promise<Unsubscribe> {
  let tailer = tailers.get(processName);
  if (!tailer) {
    tailer = new ProcessTailer(processName);
    tailers.set(processName, tailer);
  }
  try {
    await tailer.ensureStarted();
  } catch (err) {
    if (!tailer.hasListeners && tailers.get(processName) === tailer) {
      tailer.dispose();
      tailers.delete(processName);
    }
    throw err;
  }
  await tailer.sendBacklog(listener);
  tailer.addListener(listener);

  let unsubscribed = false;
  return () => {
    if (unsubscribed) return;
    unsubscribed = true;
    if (tailer.removeListener(listener) && tailers.get(processName) === tailer) {
      tailer.dispose();
      tailers.delete(processName);
    }
  };
}

/** Tear down every tailer — graceful shutdown path. */
export function disposeAll(): void {
  for (const tailer of tailers.values()) tailer.dispose();
  tailers.clear();
}
