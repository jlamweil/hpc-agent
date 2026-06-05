Status: `ready-for-agent`

# Fetch proxy v1 — internet for air-gapped compute nodes

## What to build

A file-based internet proxy that lets HPC compute nodes (network-isolated) fetch URLs, search the web, and call APIs via BeeGFS-backed request/response files.

This is the single highest-value addition — it retrofits internet access onto air-gapped compute nodes using only shared filesystem I/O.

### Proxy server (`proxy/server.py`)

A long-running process on the local machine that:

- Polls `~/hpc-agent/fetch-requests/` for new request files (via rsync or direct if on BeeGFS)
- For each request file, dispatches to the appropriate handler based on `type`
- Writes the response to `~/hpc-agent/fetch-responses/{task_id}.json`
- Cleans up the request file after processing (or moves to a `processed/` subdirectory)
- Writes a heartbeat file (`proxy-heartbeat.json`) every 10 seconds with timestamp + status

Default poll interval: 5 seconds (configurable via `--poll` flag).

### Request/response format (`proxy/schema.py`)

```python
# Request (written by compute node)
{
    "id": "task-uuid",           # unique per request
    "type": "http",              # http | search | github | arxiv
    "url": "https://...",        # for http type
    "method": "GET",             # GET or POST
    "headers": {},               # optional
    "body": null,                # for POST
    "timeout": 30,               # seconds
    "created_at": 1234567890.0   # unix timestamp
}

# Response (written by local proxy)
{
    "id": "task-uuid",           # matches request id
    "status": 200,               # HTTP status or 0 for errors
    "body": "...",               # response body as string
    "headers": {...},            # response headers
    "error": null,               # error message if failed
    "fetched_at": 1234567895.0   # unix timestamp
}
```

### Proxy handlers (`proxy/handlers/`)

- `proxy/handlers/http.py` — `requests.get/post`, handles redirects, timeouts, size limits
- `proxy/handlers/search.py` — web search via configured provider (DuckDuckGo)
- `proxy/handlers/github.py` — GitHub API proxy (attaches GITHUB_TOKEN from local env, initiates requests from local)
- `proxy/handlers/arxiv.py` — arXiv API proxy (fetch abstracts, download PDFs as text)

Each handler is a standalone async function that takes a request dict and returns a response dict.

### Security

- **URL allowlist** — default allows all `https://` URLs. Configurable in `config.yaml` to restrict to specific domains
- **SSRF protection** — blocks private IP ranges (10.x, 172.16-31.x, 192.168.x, 127.x). DNS-resolves hostnames before allowing
- **Request size limit** — reject request files > 10 KB
- **Response size limit** — truncate responses > 5 MB with `"truncated": true` header
- **Cleanup** — stale request files (>10 min) are deleted; stale response files (>1 hour) are cleaned up

### Heartbeat + readiness

- Heartbeat file: `~/hpc-agent/proxy-heartbeat.json` with `{"status": "running", "timestamp": ..., "requests_processed": N, "uptime_seconds": N}`
- HPC tasks check heartbeat before first fetch attempt. If no heartbeat file or timestamp >60s old, the task skips all proxy calls and proceeds without internet (graceful degradation)
- Heartbeat is the only health indicator — no HTTP server, no dashboard

### Supervisor script

`proxy/run.sh` — simple bash supervisor:

```bash
#!/bin/bash
while true; do
    python3 -m proxy.server --poll 5
    echo "Proxy crashed at $(date), restarting in 2s..." >> proxy.log
    sleep 2
done
```

For production: wrap in systemd or tmux.

### dsv4_batch.slurm integration

Two changes to the production SLURM batch script:

1. **Tool definition injection** — add `fetch_url` to the tool list sent to vLLM:

```python
FETCH_TOOL = {
    "type": "function",
    "function": {
        "name": "fetch_url",
        "description": "Fetch a URL from the internet",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "reason": {"type": "string", "description": "Why this fetch is needed"}
            },
            "required": ["url"]
        }
    }
}
```

2. **Tool call handler** — after vLLM returns a `fetch_url` tool call:

```python
def handle_fetch(task_id: str, args: dict) -> dict:
    """Write request file, poll for response, return result."""
    request = {"id": task_id, "type": "http", "url": args["url"], "timeout": 30}
    write_json(fetch_requests_dir / f"{task_id}.json", request)

    # Poll for response with backoff: 4s, 8s, 16s... up to ~2 min
    for attempt in range(8):
        response_path = fetch_responses_dir / f"{task_id}.json"
        if response_path.exists():
            return read_json(response_path)
        time.sleep(4 * (2 ** attempt))
    
    return {"error": "proxy timeout", "id": task_id}
```

### Proxy configuration

```yaml
proxy:
  poll_interval: 5              # seconds
  request_timeout: 30           # default timeout for HTTP requests
  max_response_size: 5242880    # 5 MB
  allowlist: ["https://*"]      # URL patterns to allow
  heartbeat_interval: 10        # seconds
  stale_request_cleanup: 600    # 10 min
  hpc_dir: "~/hpc-agent"       # BeeGFS path (or rsync path)
```

## Acceptance criteria

- [ ] Proxy server starts, polls, fulfills HTTP GET requests via temp directory
- [ ] All handler types work (http, search, github, arxiv)
- [ ] Request/response file format matches schema
- [ ] Heartbeat file is written every 10s
- [ ] SSRF protection blocks private IP ranges
- [ ] Response size limit truncates large responses gracefully
- [ ] Stale request files are cleaned up on startup + periodically
- [ ] Supervisor script restarts on crash
- [ ] `dsv4_batch.slurm` `fetch_url` tool works end-to-end: vLLM returns tool call → proxy fetches → result injected back to vLLM
- [ ] Graceful degradation: when proxy is down, tasks skip internet calls and complete without error

## Blocked by

- Issue #1 (Collector framework + arXiv) — the proxy complements the collector flow; both are independent systems but need the task infrastructure for context
