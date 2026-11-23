# sse-watchdog

An OpenCode V2 plugin that aborts a stalled SSE response. It watches real SSE
field lines, ignores pure comment heartbeats (`: keepalive`), and surfaces a
transport error after `stallMs` without a data event, so OpenCode's normal retry
logic can take over.

- Per-request timer, cleared when the stream ends.
- Heartbeats do not keep a dead generation alive.
- Aborts are appended to a log file; logging never affects the request.

## Files

| File | Role |
|---|---|
| `index.ts` | Stream wrapper and plugin registration |
