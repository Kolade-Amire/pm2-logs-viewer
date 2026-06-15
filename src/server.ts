import * as crypto from 'crypto';
import * as fs from 'fs';
import * as http from 'http';
import * as path from 'path';
import { WebSocket, WebSocketServer } from 'ws';
import * as logTailer from './logTailer';
import { disconnect as pm2Disconnect, listProcesses, Pm2UnavailableError } from './pm2Client';

const PORT = Number(process.env.PORT) || 4000;
const VIEWER_TOKEN = process.env.VIEWER_TOKEN ?? '';
const PROCESS_PUSH_INTERVAL_MS = 5_000;
const SHUTDOWN_GRACE_MS = 5_000;
/** Slow consumers get disconnected rather than buffering log lines unboundedly. */
const MAX_WS_BUFFERED_BYTES = 8 * 1024 * 1024;

const INDEX_PATH = path.join(__dirname, '..', 'public', 'index.html');

if (!VIEWER_TOKEN) {
  console.error('VIEWER_TOKEN must be set (see .env.example)');
  process.exit(1);
}

function tokenOk(candidate: string | null | undefined): boolean {
  if (!candidate) return false;
  // Hash both sides so timingSafeEqual gets equal lengths without leaking length info.
  const a = crypto.createHash('sha256').update(candidate).digest();
  const b = crypto.createHash('sha256').update(VIEWER_TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}

function httpAuthOk(req: http.IncomingMessage): boolean {
  const header = req.headers.authorization;
  if (!header || !header.startsWith('Bearer ')) return false;
  return tokenOk(header.slice('Bearer '.length));
}

/**
 * Behind nginx "Option B" (location /logs without URI rewrite) requests arrive
 * prefixed with /logs — strip it so the same routes work for both deployments.
 */
function routePath(rawUrl: string): { pathname: string; searchParams: URLSearchParams } {
  const url = new URL(rawUrl, 'http://internal');
  const pathname = url.pathname.replace(/^\/logs(?=\/|$)/, '') || '/';
  return { pathname, searchParams: url.searchParams };
}

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(body));
}

function serveIndex(res: http.ServerResponse): void {
  const stream = fs.createReadStream(INDEX_PATH);
  stream.on('error', () => {
    sendJson(res, 500, { error: 'index.html not found' });
  });
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  stream.pipe(res);
}

const server = http.createServer((req, res) => {
  const { pathname } = routePath(req.url ?? '/');

  if (req.method === 'GET' && (pathname === '/' || pathname === '/index.html')) {
    // Static shell, no secrets — served without auth; the UI collects the
    // token and every /api + WS endpoint enforces it.
    serveIndex(res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/processes') {
    if (!httpAuthOk(req)) {
      sendJson(res, 401, { error: 'unauthorized' });
      return;
    }
    listProcesses()
      .then((groups) => sendJson(res, 200, groups))
      .catch((err: unknown) => {
        if (err instanceof Pm2UnavailableError) {
          sendJson(res, 503, { error: 'pm2 daemon unavailable' });
        } else {
          // HTTP edge is a true boundary: translate unexpected failures to 500
          // so one bad request can't take the server down.
          console.error('GET /api/processes failed:', err);
          sendJson(res, 500, { error: 'internal error' });
        }
      });
    return;
  }

  sendJson(res, 404, { error: 'not found' });
});

const wss = new WebSocketServer({ noServer: true });

// --- /ws/processes: shared 5s broadcast ---------------------------------

const processClients = new Set<WebSocket>();
let processInterval: NodeJS.Timeout | null = null;

function sendTo(ws: WebSocket, payload: string): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  if (ws.bufferedAmount > MAX_WS_BUFFERED_BYTES) {
    ws.terminate();
    return;
  }
  ws.send(payload);
}

async function pushProcessSnapshot(targets: Iterable<WebSocket>): Promise<void> {
  let payload: string;
  try {
    payload = JSON.stringify(await listProcesses());
  } catch (err) {
    if (err instanceof Pm2UnavailableError) return; // skip tick; clients keep last state
    throw err;
  }
  for (const ws of targets) sendTo(ws, payload);
}

function handleProcessesSocket(ws: WebSocket): void {
  processClients.add(ws);
  if (!processInterval) {
    processInterval = setInterval(() => {
      void pushProcessSnapshot(processClients).catch((err: unknown) => {
        console.error('process broadcast failed:', err);
      });
    }, PROCESS_PUSH_INTERVAL_MS);
  }
  void pushProcessSnapshot([ws]).catch((err: unknown) => {
    console.error('initial process snapshot failed:', err);
  });
  ws.on('close', () => {
    processClients.delete(ws);
    if (processClients.size === 0 && processInterval) {
      clearInterval(processInterval);
      processInterval = null;
    }
  });
}

// --- /ws/logs/:name ------------------------------------------------------

function handleLogsSocket(ws: WebSocket, processName: string): void {
  logTailer
    .subscribe(processName, (line) => sendTo(ws, JSON.stringify(line)))
    .then((unsubscribe) => {
      if (ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
        unsubscribe();
        return;
      }
      ws.on('close', unsubscribe);
    })
    .catch((err: unknown) => {
      if (err instanceof Pm2UnavailableError) {
        ws.close(1011, 'pm2 daemon unavailable');
      } else {
        console.error(`log subscribe failed for ${processName}:`, err);
        ws.close(1011, 'internal error');
      }
    });
}

// --- upgrade routing -----------------------------------------------------

function rejectUpgrade(socket: import('stream').Duplex, status: number, reason: string): void {
  socket.write(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

server.on('upgrade', (req, socket, head) => {
  const { pathname, searchParams } = routePath(req.url ?? '/');

  if (!tokenOk(searchParams.get('token'))) {
    rejectUpgrade(socket, 401, 'Unauthorized');
    return;
  }

  if (pathname === '/ws/processes') {
    wss.handleUpgrade(req, socket, head, handleProcessesSocket);
    return;
  }

  const logsMatch = pathname.match(/^\/ws\/logs\/([^/]+)$/);
  if (logsMatch) {
    let processName: string;
    try {
      processName = decodeURIComponent(logsMatch[1]);
    } catch {
      rejectUpgrade(socket, 400, 'Bad Request');
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => handleLogsSocket(ws, processName));
    return;
  }

  rejectUpgrade(socket, 404, 'Not Found');
});

// --- lifecycle -----------------------------------------------------------

let shuttingDown = false;

function shutdown(signal: string): void {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received, shutting down`);
  if (processInterval) {
    clearInterval(processInterval);
    processInterval = null;
  }
  for (const ws of wss.clients) ws.close(1001, 'server shutting down');
  logTailer.disposeAll();
  pm2Disconnect();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), SHUTDOWN_GRACE_MS).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

server.listen(PORT, '127.0.0.1', () => {
  console.log(`pm2-log-viewer listening on :${PORT}`);
});
