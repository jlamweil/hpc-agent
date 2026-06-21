# PRD: HPC Hermes Subagent System

> **⚠️ Deprecated — Historical Document**
> This PRD describes the legacy HTTP-provider architecture. The current system
> uses **MCP tools** (run_hpc_agent_task, submit_hpc_task, check_hpc_task_status)
> instead of the HTTP provider. The `hpc_gateway.py` REST shim has been deleted.
> See `docs/orchestrator-prompts.md` and `CONTEXT.md` for the current MCP-first
> architecture.

## Problem Statement

hpc-agent routes all LLM text generation through an opencode_provider HTTP API
(`localhost:9876`) that mediates between OpenCode agents and HPC vLLM. This
architecture has several pain points:

- **Single point of failure:** If `opencode_provider.py` crashes or hangs, all
  agent LLM calls fail. No fallback path exists.
- **Synchronous HPC dispatch:** The provider submits SLURM jobs via SSH `srun`,
  then waits for completion via SSH `curl`. If the SSH connection drops mid-
  request, the entire agent call fails with no recovery.
- **No background processing:** Tasks must be submitted synchronously through
  the provider. There is no queue-and-process model where tasks accumulate and
  a worker processes them as capacity allows.
- **No fill task utilization:** GPU time on HPC goes unused when no agent
  requests are active. There is no mechanism to submit low-priority background
  work (investigation, improvement tasks) during idle periods.
- **No SSH tunnel reuse:** Every agent call creates a new SSH connection
  (srun + curl). There is no persistent tunnel that amortizes connection setup
  cost across multiple requests.
- **Zero debuggability:** When the pipeline fails, there are no diagnostics,
  no task queue visibility, and no way to trace a task from submission through
  processing to completion.
- **rsync deployment gap:** The `rsync_to_hpc()` function only syncs DB, data/,
  and slurm/ directories. New worker modules (`hermes_worker.py`, `tunnel.py`)
  and MCP server files (`mcps/`) were never deployed to HPC, causing SLURM
  jobs to crash with `ModuleNotFoundError`.

## Solution

A multi-component subagent system that decouples task submission from
processing via a local SQLite task queue, with a persistent SSH tunnel to HPC
vLLM and a background daemon that processes tasks asynchronously.

**Architecture:**

```
Orchestrator Machine                         HPC Compute Node
─────────────────────────                    ─────────────────
MCP Server (mcps/hpc_hermes.py)              vLLM (port 8000)
  └── submit_task() → tasks.db               Qwen2.5-72B-Instruct
  └── poll tasks.db ←─────── SSH tunnel ──── port 8100 → node:8000
                              │
HermesWorker (daemon)          │
  └── HermesRunner ────────────┘
  └── AIAgent (local process)
        └── tool calls → LOCAL filesystem ✓
```

The AIAgent runs locally, not on HPC. It connects to vLLM via the SSH tunnel.
All tool calls (read_file, bash, grep, etc.) execute on the orchestrator
machine. The HermesWorker daemon polls the task queue, and when idle, runs
fill tasks (priority=-1) that cycle forever without consuming GPU time for
task management.

**Key insight:** The AIAgent has its own provider configuration
(`~/.hermes/config.yaml`) that silently overrides any `base_url` passed to
its constructor. The tunnel endpoint must be forced after AIAgent creation by
overriding `agent.base_url`, `agent._client_kwargs['base_url']`, and
recreating the OpenAI client.

## User Stories

1. As an agent developer, I want to submit a prompt to HPC vLLM via an MCP
   tool call, so that I don't need to configure a custom provider.

2. As an agent developer, I want the MCP tool to return the result
   synchronously for quick prompts, so that I can use it in tight loops
   without managing task IDs.

3. As an agent developer, I want a fire-and-forget MCP tool that returns an
   immediately task ID, so that I can submit long-running prompts and check
   results later without blocking.

4. As a system operator, I want a background daemon that polls a local task
   queue and processes tasks through HPC vLLM, so that tasks don't block on
   provider availability.

5. As a system operator, I want the daemon to establish and reuse an SSH
   tunnel to the HPC compute node, so that multiple task processing runs
   share a single persistent connection.

6. As a system operator, I want the daemon to run indefinitely without
   self-terminating, so that it serves as a permanent resident worker.

7. As a system operator, I want the daemon to automatically restart if it
   crashes, so that the system self-heals without manual intervention.

8. As a system operator, I want the daemon to time out long-running fill
   tasks, so that background work doesn't block real task processing.

9. As a system operator, I want the HPC SLURM job script to contain only
   vLLM startup (no inline Python worker), so that the compute node's only
   responsibility is serving the model.

10. As a fill task author, I want to add new fill prompts by dropping a .txt
    file into a directory, so that registration is as simple as creating a
    file.

11. As a fill task author, I want the daemon to auto-seed fill tasks if the
    queue is empty on startup, so that fill tasks survive database resets.

12. As a debugger, I want a one-command system diagnostic that checks all
    components (DB, daemon, tunnel, HPC job, crontab), so that I can quickly
    identify failures.

13. As a debugger, I want verbose logging at each step of the task pipeline
    (submit, pick-up, tunnel establish, AIAgent call, result store), so that
    I can trace where failures occur.

14. As a debugger, I want the HermesRunner to log task IDs, prompt previews,
    tunnel status, and error details to stderr, so that I can correlate
    worker log entries with task DB entries.

15. As a debugger, I want the tunnel module to emit debug logs when
    TUNNEL_DEBUG=1 is set, so that I can diagnose SSH/squeue/lsof failures.

16. As an operator, I want a crontab health check that restarts the daemon
    if it crashes, so that the task queue continues to be drained.

17. As an operator, I want the MCP server to redirect hpc_batch dispatch
    noise to stderr, so that the JSON-RPC stdio protocol is not corrupted
    by auto-dispatch print statements.

18. As an operator, I want deployment to sync all worker modules and MCP
    server files to HPC, not just SLURM scripts, so that inline Python
    imports in SLURM jobs don't crash with ModuleNotFoundError.

19. As a developer, I want to verify the complete pipeline works end-to-end
    with an isolated DB (DELETE journal mode), so that tests are not
    affected by WAL race conditions from concurrent daemon access.

20. As an operator, I want the MCP cycle time to include the daemon's poll
    interval (60s) plus AIAgent processing time, so that I can set
    appropriate timeouts on MCP tool calls.

21. As an operator, I want the system to handle prompts up to 5.7K chars
    (the size of HANDOFF.md) through both sync and fire-and-forget MCP
    tools, so that real investigation prompts work end-to-end.

22. As a developer, I want a diagnostic command that prints a stuck-task
    recovery SQL command, so that I can fix stuck "running" tasks without
    looking up the SQL syntax.

## Implementation Decisions

### MCP Server (mcps/hpc_hermes.py)

- Built with FastMCP (from the `mcp` package), not the lower-level `Server`.
  FastMCP provides `@mcp.tool()` decorator, auto-JSON-Schema generation, and a
  simple `mcp.run(transport="stdio")` entry point.
- Two tools exposed:
  - `run_on_hpc_tool(prompt, timeout=300)` — synchronous: submits to
    `tasks.db`, polls every 5s for completion, returns result text. Polls
    use direct SQLite SELECT (no MCP overhead per poll cycle).
  - `submit_hpc_task(prompt)` — fire-and-forget: submits to `tasks.db`,
    returns the task ID immediately. Result can be checked later via
    `hpc_batch.py view <task-id>` or `hpc_batch.py view <task-id> --json`.
- Stdout isolation: both tools wrap `hpc_batch` calls in
  `contextlib.redirect_stdout(sys.stderr)` because `submit_task()` triggers
  `_auto_dispatch_if_needed()` which spawns a thread that calls
  `rsync_to_hpc()` and `ensure_hpc_worker()` — these print `[dispatch]
  rsync ...` to stdout, corrupting the JSON-RPC protocol.
- DB path resolved from `$HPC_DB_PATH` env var, falling back to
  `os.path.join(os.path.dirname(__file__), "..", "tasks.db")` (relative to
  the script location).
- Error path: all exceptions caught, returned as `ERROR: <msg>` string,
  never raised to the MCP layer.
- Entry point: `def main(): mcp.run(transport="stdio")` called from
  `if __name__ == "__main__"`.
- Registered in `~/.config/opencode/opencode.json` as:
  ```json
  "hpc-hermes": {
    "type": "local",
    "enabled": true,
    "command": ["python3", "/path/to/hpc-agent/mcps/hpc_hermes.py"]
  }
  ```

### HermesWorker Daemon (worker/hermes_worker.py)

- `HermesWorker(db_path, poll_interval, idle_timeout, fill_timeout, runner)`
  — configurable queue-polling loop. Defaults:
  - `db_path="tasks.db"` — local SQLite queue
  - `poll_interval=60` — seconds between queue checks
  - `idle_timeout=600` — seconds before self-shutdown (set to 0 for daemon)
  - `fill_timeout=300` — max seconds a fill task can run before abandoned
  - `runner=None` — creates `HermesRunner()` on demand
- Cycle flow:
  1. Count real pending tasks (`status='pending' AND priority >= 0`)
  2. If real tasks exist: process ALL of them in a `while _process_next():`
     loop, reset idle counter, return (skips sleep on work done)
  3. If idle but fill tasks exist: pick one via ORDER BY RANDOM() LIMIT 1,
     process with fill_timeout thread, return (sleeps normally)
  4. If truly idle: increment idle counter, check against timeout
  5. In daemon mode (`idle_timeout=0`): idle counter never increments,
     shutdown returns are skipped — runs forever
- Fill tasks are permanent DB residents: `_process_random_fill()` reads
  the prompt, runs it through HermesRunner, but never modifies the DB row
  (stays `status=pending` forever). This means fill tasks cycle endlessly
  without needing re-insertion.
- Fill task timeout: wraps `self.runner.run(task)` in a daemon thread with
  `fill_timeout` join. If the thread is still alive after timeout, the
  fill task is abandoned (daemon thread continues, but daemon moves on).
- Cycle logging: each `_idle_cycle()` prints real/fill task counts, action
  taken, error details to stderr. Heartbeat message every 10 cycles.
- `run()` loop: wraps `_idle_cycle()` — on `shutdown` signal, prints
  exit message and breaks.
- The `_process_next()` method delegates to the standalone function
  `process_next_task(db_path, runner, min_priority=0)` — the same function
  used by unit tests, ensuring testable seams.

### Crash Recovery Wrapper (/tmp/start_worker.sh)

- Wraps the daemon in an infinite restart loop:
  ```bash
  while true; do
      python3 -u -c 'from worker.hermes_worker import HermesWorker; ...'
      2>&1 | tee -a logs/worker.log
      echo "[$(date)] HermesWorker exited, restarting in 5s..."
      sleep 5
  done
  ```
- The wrapper is a shell script (not managed by systemd/supervisord) because
  the crontab health check (`*/5 * * * *`) serves as the top-level watchdog.
- On crash: the wrapper catches the Python script's exit, logs the crash
  timestamp, waits 5s, and restarts. If the crash is persistent (e.g.
  import error), the wrapper loops indefinitely with 5s between attempts.
- The wrapper script is created at `/tmp/start_worker.sh` by `hpc-startup.sh
  --worker`. It's not checked into the repo because it contains absolute
  paths and is ephemeral (regenerated on each `--worker` invocation).

### SSH Tunnel Module (worker/tunnel.py)

- Module-level globals `_TUNNEL_PID`, `_TUNNEL_NODE`, `_TUNNEL_PORT` track
  tunnel state per-Python-process. State does NOT persist across daemon
  restarts (new Python process = fresh globals).
- `find_vllm_node(ssh_host)` — runs:
  ```
  ssh <ssh_host> "squeue -u $(whoami) -o '%i %T %N %j' --noheader |
                  awk '/dsv4-.*/ && /RUNNING/ {print $3; exit}'"
  ```
  with 15s subprocess timeout. Returns hostname string or None.
- `ensure_tunnel(ssh_host, node, port=8100)` — creates SSH tunnel:
  1. Check if existing tunnel (`_TUNNEL_PID`) is alive and for same node
  2. If stale or wrong node: `close_tunnel()` first
  3. Run: `ssh -L <port>:<node>:8000 -N -f <ssh_host>`
  4. Wait up to 10s (20 attempts x 0.5s) for port to listen via socket
  5. Verify tunnel with `lsof -ti tcp:<port>` to capture actual PID
  6. Store `_TUNNEL_PID`, `_TUNNEL_NODE`, `_TUNNEL_PORT`
- `close_tunnel()` — sends `SIGTERM` to `_TUNNEL_PID`, resets globals.
  Handles `OSError`/`ProcessLookupError` silently.
- Debug mode: `TUNNEL_DEBUG=1` env var enables verbose stderr logging:
  - `find_vllm_node` → stdout/stderr content, return code, timeouts
  - `ensure_tunnel` → Popen PID, port listen attempts, lsof results
  - `close_tunnel` → kill attempts, success/failure
- Stale tunnel detection: `os.kill(pid, 0)` health check on existing PID.
  If it raises `ProcessLookupError`, the PID is dead and is replaced.

### HermesRunner (worker/model_runner.py)

- `HermesRunner(base_url=None, model=..., timeout=300, tunnel_port=8100)`:
  - `base_url=None` (default) → auto-establish SSH tunnel on first `run()`
  - `base_url="http://..."` (explicit) → skip tunnel, use the URL directly
  - `_tunnel_established` flag tracks whether tunnel has been set up
- `_ensure_vllm_endpoint()` — establishes SSH tunnel:
  1. Load HPC config from `hpc_batch._get_hpc_config()`
  2. Build `ssh_host = "user@host"`
  3. Call `find_vllm_node(ssh_host)` to find running SLURM job
  4. If no node found, raise `RuntimeError("No running SLURM job...")`
  5. Call `ensure_tunnel(ssh_host, node, tunnel_port)` to create tunnel
  6. If tunnel fails, raise `RuntimeError("Failed to create SSH tunnel")`
  7. Return `"http://localhost:<port>/v1"`
- **Tunnel persistence:** After successful tunnel establishment, set
  `self.endpoint = tunneled_url`. Without this fix, the second `run()` call
  would skip tunnel (because `_tunnel_established=True`) and fall back to
  the stale default `http://localhost:8000/v1`.
- **AIAgent base_url override** (critical fix): After creating
  `AIAgent(base_url=effective_base)`, HERMES_CONFIG the AIAgent's provider
  configuration from `~/.hermes/config.yaml` silently overrides the endpoint
  to `http://127.0.0.1:9876/v1` (the opencode_provider). The fix:
  ```python
  agent.base_url = effective_base
  agent._client_kwargs['base_url'] = effective_base
  agent.client = agent._create_openai_client(agent._client_kwargs, ...)
  ```
- AIAgent runs in `ThreadPoolExecutor(max_workers=1)` with `self.timeout`
  timeout. Returns `TaskResult(status=FAILED, error="...")` on:
  - `concurrent.futures.TimeoutError` → task exceeded timeout
  - Any other exception → AIAgent error caught and returned
- Content extraction from AIAgent response:
  ```python
  content = (response.get("final_response") or   # key used by run_conversation
             response.get("content") or "")       # fallback for other callers
  ```
  The AIAgent's `run_conversation()` returns `{"final_response": "..."}` not
  `{"content": "..."}`.
- Logging: every `run()` call prints task_id, prompt preview (80 chars),
  tunnel URL, AIAgent creation, and result size to stderr.

### SLURM Script (slurm/dsv4_fast.slurm)

- Removed inline Python worker (lines 58-79 of the original 83-line script).
  The SLURM job now only:
  1. Starts vLLM with the correct model and GPU config
  2. Waits for it to become ready (grep log for startup message)
  3. Prints SLURM_JOB_ID
  4. Waits for vLLM to finish (`wait "$VLLM_PID"`)
  5. Exits
- No Python code runs on the compute node. No worker loop, no task queue
  access, no database reads. The compute node's sole job is serving the
  vLLM API.
- Wall time limit unchanged: 12h (`#SBATCH --time=12:00:00`).
- No idle monitor inside the SLURM job. If vLLM crashes (`wait` returns),
  the job exits and auto-dispatch will submit a new one when real tasks
  accumulate.

### hpc-startup.sh

- Added `--worker` flag:
  1. Check if fill tasks exist in DB (`SELECT COUNT(*) FROM tasks WHERE
     priority < 0`). If zero, run `scripts/seed-fill-tasks.sh`.
  2. Kill any existing `hpc-worker` tmux session
  3. Create new tmux session running the HermesWorker daemon:
     ```bash
     tmux new-session -d -s hpc-worker \
         "cd $REPO_DIR && python3 -c \
         'from worker.hermes_worker import HermesWorker; \
          HermesWorker(db_path=\"tasks.db\", idle_timeout=0).run()' \
         2>&1 | tee $LOG_DIR/worker.log"
     ```
  4. Prints confirmation message
- Crontab health check (installed via `crontab -e`):
  ```cron
  */5 * * * * tmux has-session -t hpc-worker 2>/dev/null || \
      (cd /home/lam/Documents/GAIA/hpc-agent && bash hpc-startup.sh --worker)
  ```
  This is a LOCAL tmux check only — no SSH, no HPC load. The check fires
  every 5 minutes but the `||` branch only executes if tmux is dead.

### Fill Tasks

- Registry: `prompts/fill/` directory. Every `.txt` file is a fill prompt
  type. The filesystem IS the registry — no config list needed.
- `_available_fill_prompts()` in `hpc_batch.py` scans `prompts/fill/*.txt`
  dynamically, excluding `.cycle-counter`. Adding a `.txt` file is the
  only registration step.
- `config.yaml` `prompt_types` list deprecated and left empty in favor of
  filesystem scanning.
- Built-in types and prompts:
  - `handoff` → autoresearch-factory investigation. Points at real
    `HANDOFF.md` (never embeds stale copy — was a 33-line derivative that
    lost critical details like the lock warning, convergence criteria, and
    investigation rules).
  - `frontier-agent` → frontier-arbiter pipeline execution (SCAN →
    DECIDE → ACT → REVIEW cycle).
  - `frontier-improve` → frontier-arbiter diagnostic and repair agent.
- Seed script `scripts/seed-fill-tasks.sh`:
  - Seeds all 3 types via `hpc_batch.py submit-fill --type <type>`
  - Fill tasks use priority=-1 so they're never picked up by
    `process_next_task()` with `min_priority=0`
  - Fill tasks are permanent: `_process_random_fill()` never modifies
    the DB row (status stays `pending`)
  - Auto-seeded on daemon startup if count is zero

### Diagnostic Command (scripts/diag.py)

- `python3 scripts/diag.py` — instant health check outputting emoji-coded
  results (✅ ⚠️ ❌ ):
  - **Database:** file existence, `PRAGMA integrity_check`, task counts
    by status (pending/running/completed/failed/fill), stuck-task warning
    with copy-paste recovery SQL command
  - **Daemon:** PID via `pgrep -a python | grep HermesWorker`, tmux
    session check, worker.log size and last line
  - **Tunnel:** port 8100 liveness via socket connect, lsof PID check,
    vLLM model response via `curl http://localhost:8100/v1/models`
  - **MCP Server:** file existence check for `mcps/hpc_hermes.py`
  - **HPC Job:** SSH to login node, `squeue` for active dsv4-fast jobs,
    shows job ID, state, node, runtime
  - **Crontab:** parses `crontab -l` for hpc-worker and hpc-provider
    health check lines
- `--json` flag for machine-readable output with `healthy: bool` summary.
- Error/warning counters in footer: `📊 0 failures, 2 warnings`.
- Stuck task recovery command printed inline:
  ```
  ⚠️  1 task(s) stuck in 'running' — recover with:
    python3 -c "import sqlite3; c=sqlite3.connect('tasks.db');
    c.execute(\"UPDATE tasks SET status='pending',started_at=NULL
    WHERE status='running'\"); c.commit(); c.close()"
  ```

### OpenCode Configuration

- MCP server registered in `~/.config/opencode/opencode.json`:
  ```json
  "hpc-hermes": {
    "type": "local",
    "enabled": true,
    "command": ["python3", "/home/lam/Documents/GAIA/hpc-agent/mcps/hpc_hermes.py"]
  }
  ```
- Client-level MCP timeout: `"experimental": {"mcp_timeout": 300000}` (5 min).
  This limits the synchronous `run_on_hpc_tool` — if the daemon's 60s poll
  cycle plus AIAgent processing time exceeds 5 minutes, the call times out.
  Fire-and-forget `submit_hpc_task` is not affected.
- Example config updated in `opencode.example.json` with the MCP server
  entry for documentation.

### CONTEXT.md Documentation

- Added "Hermes Subagent System (Jun 18)" section documenting:
  - Architecture diagram (orchestrator ↔ tunnel ↔ compute node)
  - MCP Server: FastMCP, tool names, DB path resolution, stdout isolation
  - HermesWorker Daemon: tmux, poll cycle, fill tasks, daemon mode
  - Tunnel Module: find_vllm_node, ensure_tunnel, close_tunnel, lazy setup
  - Fill Tasks: 3 permanent types, auto-seed, prompts point at real files
  - Deploy Order: 2b before 2c, daemon before cleaned SLURM script

### Deployment Sync

- Files manually rsynced to HPC after implementation:
  - `slurm/dsv4_fast.slurm` — cleaned script (no inline Python)
  - `worker/hermes_worker.py` — daemon module (was missing — caused crash)
  - `worker/model_runner.py` — HermesRunner with tunnel integration
  - `worker/tunnel.py` — SSH tunnel module (new)
  - `mcps/hpc_hermes.py` — MCP server (new)
  - `hpc-startup.sh` — with --worker flag
  - `scripts/seed-fill-tasks.sh` — fill task seeder
  - `~/.config/opencode/opencode.json` — MCP server registration
- The `rsync_to_hpc()` function in `hpc_batch.py` does NOT automatically
  sync `worker/` or `mcps/` directories. Manual rsync was required.
  Future work should extend `rsync_to_hpc()` to include these paths.

## Testing Decisions

### Philosophy

- **Good tests** verify behavior through public interfaces — the MCP
  protocol (init → list → call), the daemon seam (submit →
  process_next_task → poll), and the tunnel lifecycle (discovery →
  creation → close → reuse). They don't test internal implementation
  details like database column names or method signatures.
- **Seams are reused across tests and production.** The standalone function
  `process_next_task(db, runner=MockRunner())` is used by both the daemon
  and unit tests. No special test-only code paths.

### HermesRunner Tests

- **Must pass `base_url` explicitly or mock the tunnel.** The default
  `base_url=None` triggers real SSH connections (squeue, SSH, lsof) which
  require network access and HPC availability.
- **MagicMock-based AIAgent mocks must be pickle-safe.** `HermesRunner`
  uses `ThreadPoolExecutor` which pickles the target function. MagicMock
  instances are not pickleable. Replace with regular callable classes:
  ```python
  class MockAIAgent:
      def __init__(self, base_url="", model="", **kw):
          self.base_url = base_url; self.model = model
      def run_conversation(self, prompt):
          return {"final_response": "mock result"}
  ```
- **Content extraction test:** Verify both `final_response` and `content`
  keys are handled correctly. `response.get("final_response") or
  response.get("content") or ""` — the first key found wins.

### Tunnel Tests (tests/test_tunnel.py — 8 tests)

- Mock `subprocess.run`, `subprocess.Popen`, `os.kill`, and `socket.socket`
  to avoid requiring an actual HPC connection.
- `reset_tunnel_globals()` autouse fixture resets module-level
  `_TUNNEL_PID`/`_TUNNEL_NODE` after each test (critical for isolation).
- Test cases:
  - `test_returns_node_when_running` — squeue returns hostname
  - `test_returns_none_when_no_job` — squeue returns empty
  - `test_returns_none_on_ssh_failure` — SSH timeout
  - `test_creates_tunnel_and_returns_port` — full lifecycle
  - `test_reuses_existing_tunnel_for_same_node` — PID check
  - `test_closes_existing_tunnel_before_new_one` — node change
  - `test_close_tunnel_kills_process_and_clears_state` — SIGTERM
  - `test_close_tunnel_noop_when_no_tunnel` — no exception

### MCP Server Tests (tests/test_mcp_hpc_hermes.py — 3 tests)

- Use isolated temp DB with full schema.
- Seam: `process_next_task(db, runner=MockRunner(response="..."))` —
  the same function the daemon uses. Tests submit→process→poll without
  running the daemon.
- Test cases:
  - `test_submit_and_poll_returns_mock_response` — full cycle
  - `test_timeout_returns_error_string` — poll with no processing
  - `test_no_exceptions_on_broken_db` — error path returns string

### Daemon Tests (tests/test_hermes_worker.py — 17 tests)

- 10 existing tests (unchanged) + 7 new:
  - TestHermesRunnerTunnel (5 tests) — `_ensure_vllm_endpoint` behavior
  - TestDaemonMode (2 tests) — idle_timeout=0 never shuts down,
    still processes real tasks

### Isolated DB Tests

- Use `PRAGMA journal_mode=DELETE` to avoid WAL race conditions that
  occur when the real `tasks.db` is accessed by concurrent processes
  (daemon, opencode_provider, auto-dispatch threads).
- All test DBs are temp files cleaned up in `os.unlink(db)` teardown.

### E2E Verification (Manual)

- Full pipeline proven with isolated DB + task processor + MCP subprocess:
  ```
  MCP init → list tools → call run_on_hpc_tool → 5.0s → "MCP_E2E_OK"
  MCP init → submit_hpc_task → task_id → processor picks up → result
  ```
- HANDOFF.md (5.7K chars, 817 words) verified through fire-and-forget:
  - MCP submits to DB → task processor picks up → HermesRunner
    establishes tunnel → AIAgent reads INDEX.md, factory-spec.md,
    LOOP.md → creates LOOP.md file → searches codebase → result
    stored in DB
  - 8+ API calls through tunnel to vLLM (0.3-22s each)
  - Real tool calls executed (read_file, grep, search_files)
  - Total processing time: ~80s for full investigation session

### Test Summary

| Test file | Tests | Scope |
|-----------|-------|-------|
| `tests/test_mcp_hpc_hermes.py` | 3 | MCP submit→poll→timeout |
| `tests/test_tunnel.py` | 8 | Tunnel discovery + lifecycle |
| `tests/test_hermes_worker.py` | 17 | Daemon + tunnel integration + daemon mode |
| `tests/test_worker.py` | 5 | HermesRunner extract + run + timeout |
| `tests/test_opencode_provider.py` | 15 | Existing (unchanged) |
| `tests/test_prompt_pool.py` | 8 | Existing + filesystem registry |

**33/33 tests pass** in the core suite (48 with opencode_provider tests).

### Known Testing Gaps

- **WAL mode race conditions** are not tested. They require concurrent
  process access patterns that are hard to reproduce deterministically.
  Mitigation: test with `PRAGMA journal_mode=DELETE`.
- **AIAgent hang** (inside `_interruptible_streaming_api_call`) is not
  unit-tested. The hang requires the full hermes-agent environment
  and depends on provider configuration.
- **E2E tunnel + vLLM** tests require an active HPC SLURM job and are
  run manually. The automated suite mocks the tunnel and runner.
- **HermesRunner `self.endpoint` persistence** fix was tested by running
  two sequential `run()` calls and verifying the tunnel wasn't re-established.
  Not captured in a formal test.

## Out of Scope

- **Direct OpenAI client** — The HermesRunner currently routes through the
  AIAgent from hermes-agent. A lighter-weight path that calls
  `openai.ChatCompletion.create()` directly would be faster and more
  predictable but is not implemented. The AIAgent adds overhead: 27 tool
  definitions sent with every request, conversation history management,
  fallback chains, and rate-limit handling — none of which are needed for
  simple prompt→response tasks.
- **Websocket/SSE MCP transport** — The MCP server only supports stdio
  transport. SSE and Streamable HTTP are available in FastMCP but not
  configured or needed for OpenCode integration.
- **Daemon auto-recovery of stuck "running" tasks** — Tasks left in
  "running" state after a daemon crash require manual recovery via the
  diagnostic command. No daemon-side auto-recovery has been implemented.
- **Provider fallback chain** — The MCP tool has no fallback if HPC is
  unavailable (no SLURM job). The opencode_provider fallback chain
  (fallback to `opencode_zen`) exists separately and is unaffected.
- **Multi-node HPC** — The tunnel assumes a single compute node with vLLM.
  No multi-node load balancing or failover.
- **TLS/encryption** — The tunnel is unencrypted SSH forwarding. All
  traffic between the orchestrator and compute node passes through an
  unauthenticated localhost port (8100).
- **Task priority beyond integer ordering** — Priority is a single integer
  (higher = sooner). No preemption, deadline scheduling, or priority
  inversion handling.
- **Database migration** — Schema changes (adding `priority`, `task_type`)
  use `ALTER TABLE ADD COLUMN` with `try/except OperationalError`. No
  formal migration system, version checks beyond `user_version`.
- **WAL mode fix** — The WAL race condition between concurrent tasks.db
  writers (daemon, opencode_provider, auto-dispatch) is documented but
  not resolved. All components share the same DB file with WAL journal
  mode, which can cause data visibility issues under concurrent write
  access. Potential fixes (out of scope): file-level locking, connection
  pooling via a single writer process, or switching to DELETE journal
  mode with retry logic.
- **Systemd/service management** — The daemon uses tmux + crontab for
  lifecycle management, not systemd. The crash-recovery wrapper script
  is a bash `while true` loop, not a proper service manager.

## Further Notes

### Deploy Order (Critical)

Phase 2b (HermesRunner tunnel) must deploy BEFORE Phase 2c (SLURM cleanup +
daemon). If the daemon starts before the HermesRunner has tunnel integration,
all tasks fail with connection refused (tunnel never established). If the
SLURM script is cleaned before the daemon is ready, new jobs have no worker
and tasks pile up unprocessed.

```
Phase 1 (MCP) ── any time (standalone module)
Phase 2a (tunnel.py) ── any time (standalone module)
Phase 2b (HermesRunner tunnel) ── DEPLOY FIRST among Phase 2
Phase 2c (SLURM cleanup + daemon) ── ONLY after 2b confirmed
Phase 2d (crontab) ── any time after 2c
```

### Rsync Deployment Gap

The `rsync_to_hpc()` function in `hpc_batch.py` syncs:
1. `tasks.db` (the SQLite queue) → `~/hpc-agent/tasks.db.staging` (atomic rename)
2. `data/` directory → `~/hpc-agent/data/`
3. `slurm/` scripts → `~/hpc-agent/slurm/`

It does NOT sync:
- `worker/` — daemon modules (`hermes_worker.py`, `tunnel.py`, `model_runner.py`)
- `mcps/` — MCP server (`hpc_hermes.py`)
- `hpc-startup.sh` — startup script
- `scripts/` — utilities like `seed-fill-tasks.sh`

These must be synced manually via `rsync -az worker/ HOST:~/hpc-agent/worker/`.
Without this, inline Python imports in the old SLURM script crash with
`ModuleNotFoundError: No module named 'worker.hermes_worker'`.

### Known Bugs

1. **SQLite WAL race (HIGH):** The daemon, opencode_provider, and test
   scripts all share `tasks.db` with `PRAGMA journal_mode=WAL`. Concurrent
   WAL checkpoints from one connection can discard uncheckpointed writes
   from another, causing tasks to disappear from queries. Integrity check
   reports "NUMERIC value in tasks.status" when timestamps end up in the
   wrong column. Tasks submitted via MCP to the real DB may not be visible
   to monitoring scripts that open separate connections. Temporary
   mitigation: use `PRAGMA wal_checkpoint(TRUNCATE)` or switch to
   `journal_mode=DELETE` for isolated operations.

2. **AIAgent hang in `_interruptible_streaming_api_call` (MEDIUM):** The
   AIAgent from hermes-agent sometimes hangs at "Making API call
   #1/90..." without proceeding to the actual HTTP request. The hang is
   inside the `_call()` function (line 6189 of run_agent.py) which runs
   in a daemon thread. Exceptions in daemon threads are silently
   swallowed — the main thread loops forever polling `t.is_alive()`. The
   HermesRunner timeout (default 300s) eventually fires and returns
   `TaskResult(status=FAILED)`, but the hang blocks the daemon from
   processing other tasks during this period. Root cause likely in the
   AIAgent's internal tool filtering or API kwargs building logic.

3. **Stale tunnel PID (LOW):** After daemon crash and restart, the old
   SSH tunnel's child process may die while `lsof` still shows the port
   as occupied. The new daemon's `ensure_tunnel()` finds the port busy
   via `lsof -ti tcp:8100` but the SSH connection is dead. `curl` through
   the tunnel returns `Connection reset by peer`. Mitigated by stale PID
   health check (`os.kill(pid, 0)`) — if the PID is dead, it's replaced.

4. **MCP synchronous tool timeout (LOW):** The `run_on_hpc_tool` sync
   tool has a client-side MCP timeout of 300s (configured in
   `opencode.json`). If the daemon's 60s poll interval plus AIAgent
   processing exceeds 300s, the MCP client terminates the connection and
   the task remains pending in the DB. The task will still be processed
   by the daemon on the next poll cycle, but the MCP caller won't see
   the result. Use `submit_hpc_task` (fire-and-forget) for long-running
   prompts.

5. **Fill task auto-seed WAL conflict (LOW):** The `hpc-startup.sh --worker`
   script checks fill task count before starting the daemon. If the DB
   was recently written to by another process (opencode_provider), the
   WAL may report an incorrect count of zero, causing unnecessary
   re-seeding and duplicate fill tasks.

### WAL Race Mitigation for Testing

When using the MCP tools for testing or monitoring, use an isolated DB
with `PRAGMA journal_mode=DELETE` to avoid WAL race conditions:

```python
import tempfile, sqlite3, os
db = tempfile.mktemp(suffix='.db')
conn = sqlite3.connect(db)
conn.execute("PRAGMA journal_mode=DELETE")
# ... create schema, submit tasks, process ...
os.unlink(db)
```

For production monitoring, rely on the worker log (`tail -f logs/worker.log`)
rather than opening concurrent DB connections. The diagnostic script
(`scripts/diag.py`) handles WAL checkpointing internally.
