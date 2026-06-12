import pm2 from 'pm2';
import type { ProcessDescription } from 'pm2';
import { InstanceLogPaths, Pm2Instance, Pm2Status, ProcessGroup } from './types';

const BACKOFF_INITIAL_MS = 1_000;
const BACKOFF_MAX_MS = 30_000;
/**
 * pm2's RPC layer (axon) queues calls forever while the daemon socket is gone
 * instead of erroring — without a deadline, every caller would hang.
 */
const RPC_TIMEOUT_MS = 5_000;
/** Connect may legitimately spawn a fresh daemon, which takes a few seconds. */
const CONNECT_TIMEOUT_MS = 10_000;

/** Thrown for every PM2 RPC failure; callers translate it (503, skipped tick) — never crash. */
export class Pm2UnavailableError extends Error {
  constructor(message = 'PM2 daemon is unavailable') {
    super(message);
    this.name = 'Pm2UnavailableError';
  }
}

/**
 * The single typed view over pm2_env — pm2's bundled types leave it loose,
 * so every field we consume is declared (and defaulted) here once.
 */
interface RawPm2Env {
  status?: string;
  restart_time?: number;
  pm_uptime?: number;
  pm_out_log_path?: string;
  pm_err_log_path?: string;
}

/**
 * Full set of statuses pm2 emits (pm2/constants.js v7), mapped onto the spec's
 * wire union. The bundled .d.ts spells WAITING_RESTART with an underscore, the
 * runtime constant uses a space — both are handled. Unrecognized → 'stopped'.
 */
const STATUS_MAP: Record<string, Pm2Status> = {
  online: 'online',
  stopped: 'stopped',
  stopping: 'stopped',
  'waiting restart': 'launching',
  waiting_restart: 'launching',
  launching: 'launching',
  errored: 'errored',
  'one-launch-status': 'online',
};

function normalizeStatus(raw: string | undefined): Pm2Status {
  return (raw !== undefined && STATUS_MAP[raw]) || 'stopped';
}

/** pm2 reports '/dev/null' (or empty) when a log side is disabled. */
function normalizeLogPath(raw: string | undefined): string | null {
  return raw && raw !== '/dev/null' ? raw : null;
}

type ConnState = 'disconnected' | 'connecting' | 'connected';

let state: ConnState = 'disconnected';
let connecting: Promise<void> | null = null;
let backoffMs = BACKOFF_INITIAL_MS;
let retryTimer: NodeJS.Timeout | null = null;
let shutDown = false;

function connectOnce(): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Pm2UnavailableError('PM2 connect timed out'));
    }, CONNECT_TIMEOUT_MS);
    timer.unref();
    pm2.connect((err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (err) reject(err);
      else resolve();
    });
  });
}

function ensureConnected(): Promise<void> {
  if (state === 'connected') return Promise.resolve();
  if (!connecting) {
    state = 'connecting';
    connecting = connectOnce()
      .then(() => {
        state = 'connected';
        backoffMs = BACKOFF_INITIAL_MS;
      })
      .catch((err: Error) => {
        state = 'disconnected';
        scheduleReconnect();
        throw err;
      })
      .finally(() => {
        connecting = null;
      });
  }
  return connecting;
}

/** Background retry with exponential backoff so the link heals even without traffic. */
function scheduleReconnect(): void {
  if (shutDown || retryTimer) return;
  const delay = backoffMs;
  backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
  retryTimer = setTimeout(() => {
    retryTimer = null;
    ensureConnected().catch(() => {
      /* scheduleReconnect already re-armed by ensureConnected's catch */
    });
  }, delay);
  retryTimer.unref();
}

function markDisconnected(): void {
  state = 'disconnected';
  try {
    pm2.disconnect();
  } catch {
    // disconnect on a dead client can throw; the client is being discarded anyway
  }
  scheduleReconnect();
}

async function listRaw(): Promise<ProcessDescription[]> {
  try {
    await ensureConnected();
  } catch {
    throw new Pm2UnavailableError();
  }
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      markDisconnected();
      reject(new Pm2UnavailableError('PM2 RPC timed out'));
    }, RPC_TIMEOUT_MS);
    timer.unref();
    pm2.list((err, procs) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (err) {
        markDisconnected();
        reject(new Pm2UnavailableError());
      } else {
        resolve(procs);
      }
    });
  });
}

function toInstance(proc: ProcessDescription): Pm2Instance {
  const env = (proc.pm2_env ?? {}) as RawPm2Env;
  const status = normalizeStatus(env.status);
  const uptime =
    status === 'online' && typeof env.pm_uptime === 'number'
      ? Math.max(0, Date.now() - env.pm_uptime)
      : 0;
  return {
    pm_id: proc.pm_id ?? -1,
    status,
    cpu: proc.monit?.cpu ?? 0,
    memory: proc.monit?.memory ?? 0,
    restarts: env.restart_time ?? 0,
    uptime,
  };
}

export async function listProcesses(): Promise<ProcessGroup[]> {
  const procs = await listRaw();
  const groups = new Map<string, Pm2Instance[]>();
  for (const proc of procs) {
    const name = proc.name ?? `pm_id-${proc.pm_id ?? '?'}`;
    const instances = groups.get(name) ?? [];
    instances.push(toInstance(proc));
    groups.set(name, instances);
  }
  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, instances]) => ({
      name,
      instances: instances.sort((a, b) => a.pm_id - b.pm_id),
    }));
}

export async function getLogPaths(processName: string): Promise<InstanceLogPaths[]> {
  const procs = await listRaw();
  return procs
    .filter((proc) => proc.name === processName)
    .map((proc) => {
      const env = (proc.pm2_env ?? {}) as RawPm2Env;
      return {
        pm_id: proc.pm_id ?? -1,
        outPath: normalizeLogPath(env.pm_out_log_path),
        errPath: normalizeLogPath(env.pm_err_log_path),
      };
    })
    .sort((a, b) => a.pm_id - b.pm_id);
}

export function disconnect(): void {
  shutDown = true;
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = null;
  }
  if (state !== 'disconnected') {
    state = 'disconnected';
    try {
      pm2.disconnect();
    } catch {
      // already gone — nothing to clean up
    }
  }
}
