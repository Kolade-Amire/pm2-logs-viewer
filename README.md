# PM2 Log Viewer

A lightweight internal web tool for viewing live PM2 process status and streaming logs in real time — no server access needed. Runs as a PM2 process itself on the same machine, read-only (no restart/stop buttons, no file writes).

## Features

- Live process table (status, CPU, memory, restarts, uptime), grouped by name, pushed over WebSocket every 5s
- Real-time log streaming with the last 50 lines as backlog, merged across all instances of a clustered process (`[id:N]` tags per line)
- stdout/stderr distinction, per-line timestamps, case-insensitive live filter, auto-scroll that pauses when you scroll up
- Survives `pm2-logrotate`: rename and truncate rotations are detected via inode tracking — no missed lines, no stale file descriptors
- Survives PM2 daemon outages: exponential-backoff reconnect, API returns 503 instead of hanging
- Static bearer-token auth on every API request and WebSocket upgrade

## Stack

Node.js + TypeScript backend (plain `http` + `ws`, no framework) · single-file vanilla-JS frontend (no build step) · PM2 programmatic API.

## Quick start (dev)

```bash
npm install
VIEWER_TOKEN=dev-token npm run dev     # http://localhost:4000, paste the token in the UI
```

## Deployment

```bash
npm install && npm run build
cp .env.example .env                   # set a real token: openssl rand -hex 32
pm2 start ecosystem.config.js
pm2 save
```

The token lives only in the gitignored `.env` (or shell env, which takes precedence). The server refuses to start without one. After rotating the token, re-apply with `pm2 restart ecosystem.config.js --update-env`.

Must run as the **same user** as the PM2 apps it monitors (it talks to that user's daemon and reads `~/.pm2/logs`).

### Nginx

WebSockets need the upgrade headers:

```nginx
location / {                # or: location /logs — the app handles the prefix
    proxy_pass http://localhost:4000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
}
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `VIEWER_TOKEN` | — (required) | Bearer token; UI prompts for it once and stores it in localStorage |
| `PORT` | `4000` | HTTP/WebSocket listen port |

## API

| Endpoint | Auth | Description |
|---|---|---|
| `GET /` | none | Frontend (static shell, no secrets) |
| `GET /api/processes` | `Authorization: Bearer <token>` | Current `ProcessGroup[]`, never cached |
| `WS /ws/processes?token=<token>` | query param | `ProcessGroup[]` push every 5s |
| `WS /ws/logs/:name?token=<token>` | query param | Last 50 lines, then live `LogLine` stream |
