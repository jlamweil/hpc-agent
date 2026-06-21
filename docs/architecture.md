# Architecture Overview

## System Purpose

Route all LLM text generation through a local Hermes queue to an HPC vLLM (Qwen2.5-72B). Zero external API subscriptions. The orchestrator (OpenCode agent) calls MCP tools instead of spawning subagents.

---

## High-Level Data Flow

```
Orchestrator (OpenCode)
    │
    │  MCP tools (stdio JSON-RPC)
    ▼
mcps/hpc_hermes.py ──► tasks.db (SQLite)
    │                      │
    │                      │  poll (HermesWorker daemon)
    │                      ▼
    │              HermesRunner / AIAgent
    │                      │
    │         ┌────────────┼────────────┐
    │         ▼            ▼            ▼
    │    27 tools     vLLM (via    retry/fail
    │    (file,       SSH tunnel   if DOWN
    │     bash,       port 8100)
    │     browser,
    │     web search)
    │
    └──► submit ──► auto-dispatch ──► SLURM job (dsv4_fast.slurm)
                                        │
                                        ▼
                                    vLLM (HPC compute node)
```

## Modules

### `hpc_batch.py` — Task Queue + CLI
**Role:** Central queue. Insert, query, dispatch, collect.

| Method | Trigger | Effect |
|--------|---------|--------|
| `submit_task()` | CLI, MCP tools | Inserts row in `tasks.db`, fires auto-dispatch |
| `ensure_hpc_worker()` | auto-dispatch | Rsyncs DB → HPC, sbatch if cold |
| `rsync_to_hpc()` | dispatch | Syncs DB + data + slurm + worker + mcps to HPC login node |
| `collect_results()` | CLI | Pulls completed DB from HPC, merges into local |

### `mcps/hpc_hermes.py` — MCP Server (4 tools)
**Role:** stdio JSON-RPC server connecting orchestrator to task queue.

| Tool | Signature | Use case |
|------|-----------|----------|
| `run_on_hpc_tool(prompt, timeout)` | Submit + poll | Simple prompts |
| `run_hpc_agent_task(messages, system_prompt, timeout)` | Submit + poll | Role-based agent tasks |
| `submit_hpc_task(prompt)` | Fire-and-forget | Returns task ID |
| `check_hpc_task_status(task_id)` | Poll by ID | Fire-and-forget follow-up |

**Gap:** MCP tools submit tasks but the MCP server has no singleton guard. Two OpenCode sessions = two MCP servers connecting to the same DB concurrently. No authentication — any process on the orchestrator machine can call these tools.

### `worker/hermes_worker.py` — HermesWorker Daemon
**Role:** Single-threaded loop that polls `tasks.db` and processes tasks via HermesRunner/AIAgent.

| Method | Purpose |
|--------|---------|
| `_idle_cycle()` | Main dispatch: real tasks → process all, fill tasks → process one, truly idle → sleep |
| `_process_next()` | Claim + run one real task via AIAgent |
| `_process_random_fill()` | Claim + run one fill task via AIAgent (thread, up to 300s) |
| `_recover_stuck_tasks()` | Reset stale `running` → `pending` on startup and idle |
| `_acquire_singleton_lock()` | fcntl flock at `/tmp/hermes-worker.pid` |
| `_kill_hpc_job()` | SSH `scancel` on idle HPC job |
| `_vllm_available()` | HTTP check of `/v1/models` on port 8100 |
| `_write_heartbeat()` | JSON state to `/tmp/hermes-worker.heartbeat` |

**Lifecycle:**
```
startup → acquire lock → PDEATHSIG → pre-import AIAgent → recover stuck tasks → LOOP
    LOOP: _idle_cycle() → if work_done: immediate retry, else: sleep 5s
    _idle_cycle: real > 0 → process all; fill > 0 → process one; idle → sleep
```

**Gap — Single-threaded:** One task at a time. A fill task blocks the loop for up to 300s. Real tasks arrive in the DB during that window and sit untouched until the fill completes. The only mitigation is the sentinel file (`/tmp/.hpc-task-ready`) and the 5s poll interval — but these can't interrupt an in-flight AIAgent call.

**Gap — No origin tracking:** No field in the task record distinguishes "submit by orchestrator" from "submitted by fill agent's `execute_code` call." Both set `agent_id="hpc-batch"` (default). The idle timer (`_last_real_time`) resets on ANY real task processing, including fill-derived ones. A fill agent can keep HPC alive indefinitely by submitting tasks through its own tool calls.

**Gap — Fill tasks never complete:** Fill tasks have `priority=-1` and stay `pending` forever. They're never marked `completed` or `failed`. This means `_count_fill_pending()` always returns > 0, the "truly idle" branch is unreachable, and the idle timer never fires during fill cycles.

**Gap — Dead task recovery:** `_recover_stuck_tasks()` runs on startup and during idle cycles only. If the daemon stays busy for days and then crashes, tasks left in `running` state (from a `kill -9`) are not recovered until the daemon restarts. There's no proactive background sweep.

**Gap — Graceful shutdown:** No SIGTERM handler. `kill` causes immediate exit, leaving in-flight tasks in `running` state. No drain. fcntl lock file not cleaned up.

**Gap — No circuit breaker:** When vLLM is down, every task tries 3 API calls (each with ~30s timeout + retry). For 50 pending tasks, that's 50×3×30s = 75 minutes of retry before each task fails. Fill tasks too.

### `scripts/watchdog.sh` — Liveness Monitor
**Role:** Crontab entry (every 1 min). Checks `/tmp/hermes-worker.heartbeat`. If stale > 300s, kills and restarts daemon via `hpc-startup.sh`.

**Gap:** Only detects complete daemon death (stale heartbeat). Does not detect "daemon alive but stuck processing a task" (heartbeat may show "working=True" for 45 minutes while a task is in-flight).

### `hpc-startup.sh` — Daemon Launcher
**Role:** Single source of truth for starting the daemon. Creates tmux session, sets Python path to miniconda, configures logging.

**Gap:** `scripts/start_worker.sh` still exists (deprecated but on disk). Can be accidentally executed, starting an old-code daemon with different settings.

### `mcps/hpc_hermes.py` — MCP Server
**Role:** stdio JSON-RPC bridge. No singleton guard. Two instances can run.

---

## Infrastructure

### SLURM Job (`slurm/dsv4_fast.slurm`)
| Parameter | Value |
|-----------|-------|
| Model | Qwen2.5-72B-Instruct (vLLM 0.23.0) |
| Time limit | 1h (was 12h) |
| GPUs | 4×H100 80GB, tensor-parallel-size 4 |
| Context | 128K tokens |
| SHM issue | `ipcrm -a` + `rm -rf /dev/shm/vllm*` on startup |

**Gap — vLLM startup race:** Auto-dispatch submits a SLURM job. Job starts. vLLM takes ~4 min to load the model. During those 4 minutes, every task that the daemon tries to process fails with connection errors (AIAgent retries 3×, each task fails). A 5-minute grace period exists in `_kill_hpc_job()` but only prevents killing — it doesn't prevent futile task attempts. No startup monitoring state.

**Gap — SHM deadlock:** `shm_broadcast.py` hangs when stale shared memory segments exist from a previous job. The `ipcrm -a` fix is a best-effort workaround. If vLLM hangs for > 5 min, the daemon has no mechanism to detect and restart the SLURM job.

### SSH Tunnel
- **Port:** 8100 → localhost:8000 on compute node
- **Lifetime:** Standalone. Created by `HermesRunner._ensure_vllm_endpoint()` on first task. Killed by daemon shutdown or tunnel timeout.
- **Gap:** Port 8100 stays listening (SSH master process) even when the remote vLLM is dead. Socket-based liveness check gives false positives. HTTP-based check (`GET /v1/models`) is the correct probe — implemented in `_vllm_available()`.

### Auto-Dispatch Chain
```
submit_task(priority>=0)
    → _auto_dispatch_if_needed()    [rate-limited 60s]
    → ensure_hpc_worker()
    → rsync_to_hpc()                [DB, data, slurm, worker, mcps, scripts]
    → check_hpc_jobs()              [squeue for dsv4-*]
    → if cold: sbatch_on_hpc()
        if warm: (just rsync)
```

**Gap:** Best-effort. No retry on SSH/sbatch failure. No verification that the submitted job actually appears in `squeue`. Failures are silently swallowed (runs in a daemon thread with bare `except Exception: pass`).

### Task Queue (`tasks.db`)
| Column | Type | Purpose |
|--------|------|---------|
| `id` | UUID | Primary key |
| `status` | string | pending/running/completed/failed |
| `priority` | int | >=0 real, <0 fill |
| `task_type` | string | llm/subagent/fill |
| `messages` | JSON | OpenAI message array |
| `metadata` | JSON | Source, type, cycle info |
| `started_at` | float | When task was claimed |
| `source` (in metadata) | string | orchestrator/mcp/cron/fill-agent/fill |

**Gap — No SLA tracking:** No columns for `retry_count`, `last_error`, `max_retries`. No way to know how many times a task failed before succeeding (or giving up).

**Gap — Dead tasks invisible:** A task stuck in `running` with `started_at IS NULL` was invisible to recovery until the `_recover_stuck_tasks()` fix for NULL started_at. Still no proactive sweep for tasks left running by a killed daemon.

### Fill Tasks
| Property | Value |
|----------|-------|
| Priority | -1 |
| Source files | `prompts/fill/*.txt` |
| Registration | Filesystem IS registry |
| Lifecycle | Run once (up to 300s), stay pending forever |

**Gap — Never complete:** Fill tasks stay `pending` permanently. The daemon processes them in an infinite cycle (task-1 → task-2 → task-3 → task-1…). This prevents the queue from ever being empty, which prevents the "truly idle" state that triggers HPC shutdown.

**Gap — AIAgent concurrency:** When a fill task's AIAgent runs, it has access to `execute_code` and `terminal`. These can call `submit_task()` to insert real tasks. Those real tasks cannot be processed until the fill task finishes, because the daemon is single-threaded. A fill task doing MCP calls creates work that it blocks from being executed.

---

## Known Gaps Summary

| Priority | Gap | Impact | Fix in PRD |
|----------|-----|--------|------------|
| CRITICAL | Fill task blocks loop for 300s | Real tasks delayed 300s | D1 — thread-based fill dispatch, 5s check cycle |
| CRITICAL | Fill-derived MCP tasks indistinguishable | Idle timer never fires, HPC stays alive forever | D2 — source field in metadata |
| CRITICAL | Fill tasks never complete | "Truly idle" unreachable | D3 — mark fill done after one run |
| HIGH | vLLM startup race | Tasks fail during 4-min model load | D9 — startup monitoring, no tasks during load |
| HIGH | No circuit breaker | 75min retry storm on dead vLLM | D6 — 30s backoff, skip fills |
| HIGH | Auto-dispatch best-effort | Transient SSH failure = lost dispatch | D10 — retry + verify |
| HIGH | No stuck task sweep | Running tasks ghost forever after crash | D11 — background sweep every 300s |
| HIGH | Multiple launcher paths | Stale-code daemons, conflicting settings | D12 — single launcher |
| HIGH | No priority reordering | MCP tasks wait behind cron batch | D13 — priority reorder at dispatch |
| MEDIUM | No graceful shutdown | Abandoned tasks on kill | D5 + D8 — SIGTERM + PDEATHSIG |
| MEDIUM | MCP server no singleton | DB contention from duplicate instances | D7 — fcntl guard |
| MEDIUM | 10-min task SLA no warning | Stuck task blocks queue silently | D13 — warning at 600s |
| LOW | HPC compute node black box | No visibility into vLLM startup state | D9 — vLLM log tail during startup |
| LOW | Port 8100 false positive on dead vLLM | Tasks retry against dead endpoint | D6 — HTTP liveness |
