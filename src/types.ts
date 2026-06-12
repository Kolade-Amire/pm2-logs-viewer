// Shared contracts used across pm2Client, logTailer, server, and the frontend wire format.

export type Pm2Status = 'online' | 'stopped' | 'errored' | 'launching';

export interface Pm2Instance {
  pm_id: number;
  status: Pm2Status;
  /** CPU usage in percent, as reported by pm2 monit. */
  cpu: number;
  /** Resident memory in bytes, as reported by pm2 monit. */
  memory: number;
  restarts: number;
  /** Milliseconds since the instance started; 0 when not online. */
  uptime: number;
}

export interface ProcessGroup {
  name: string;
  instances: Pm2Instance[];
}

/** One log line on the wire: WS /ws/logs/:name sends these as JSON. */
export interface LogLine {
  stream: 'out' | 'err';
  line: string;
  /** ISO timestamp of when the server read the line (not parsed from log content). */
  ts: string;
  pm_id: number;
}

/**
 * Internal contract between pm2Client and logTailer: per-instance log file
 * locations exactly as PM2 reports them (pm_out_log_path / pm_err_log_path).
 * Paths are never constructed from process names. A side is null when PM2
 * has logging disabled for it (reported as '/dev/null' or empty).
 */
export interface InstanceLogPaths {
  pm_id: number;
  outPath: string | null;
  errPath: string | null;
}
