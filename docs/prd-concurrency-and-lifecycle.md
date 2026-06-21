# PRD: Concurrency, Task Lifecycle, and HPC Idle Management

## Problem Statement

The HermesWorker daemon is a single-threaded loop that processes one task at a time. This creates a cascade of interrelated failures:

A fill task (priority=-1, "keep GPU warm") holds the daemon hostage for up to 300 seconds. During that window, no real task can start — even though a fresh SLURM job with a working vLLM is right there. When the fill task's AIAgent calls MCP tools via `execute_code` or `terminal`, it inserts real tasks into the queue that cannot be processed until the fill task finishes, because the same daemon that must process them is busy running the fill task.

The idle-detection mechanism that should kill the HPC job when no real work is happening is defeated by the same fill tasks: they stay pending forever, prevent the daemon from ever being "truly idle," and reset the idle timer when the daemon eventually processes fill-derived MCP tasks.

The only thing that reliably kills the HPC job is the SLURM walltime (1h). The system was running for 7h50m doing no real work — paying for GPUs that were processing nothing useful — because no mechanism could distinguish "alive but useless" from "alive and productive."

## Solution

Two independent subsystems:

1. **Concurrent processing lanes** — a dispatcher loop that can interleave fill work and real work, and eventually process tasks in parallel when the infrastructure supports it. Fill tasks run in a background thread. Real tasks get a dedicated processing slot that is checked every cycle. The fill task's MCP calls can be picked up as soon as they land in the queue, not 300s later.

2. **Task origin tracking** — every task carries a `source` tag that records whether it was submitted by the orchestrator, an MCP tool, a fill agent, or a cron job. The idle timer only resets on orchestrator and direct MCP tasks. Fill-derived tasks are recognized and ignored for idle-detection purposes.

The fill task lifecycle changes from "run once, stay pending forever" to "run once, get marked done, next fill in the pool gets picked up." Fill tasks cycle through the pool naturally. Orphaned fill tasks (source file deleted) are still cleaned up.

## User Stories

1. As a user, I want a 30s orchestrator MCP task to not wait 300s behind a fill task, so that interactive tools return promptly.

2. As a user, I want fill tasks running in the background to not prevent real tasks from starting, so that the GPU is used for useful work first.

3. As a user, I want fill tasks to be marked complete after one run, so that the daemon can eventually become idle and trigger HPC shutdown.

4. As a user, I want the daemon to kill the HPC job after 5 minutes of no real work (even if fill tasks are available), so that GPU budget is not wasted on idle capacity.

5. As a user, I want vLLM to get a grace period after a fresh SLURM job starts before the idle timer considers killing it, so that model loading (~4 min) doesn't cause a kill/restart loop.

6. As a user, I want tasks submitted by a fill agent's `execute_code` or `terminal` calls to be tracked as fill-derived, so that they don't reset the HPC idle timer and create an infinite keep-alive.

7. As a user, I want the dispatcher to check for new real tasks every 5 seconds during fill processing, so that the maximum real-task latency from fill contention is bounded.

8. As a user, I want the heartbeat to include the idle-timer state (`hpc_idle_cycles`, `_last_real_time`), so that I can monitor whether HPC is about to be killed.

9. As a user, I want the diagnostics (`diag.py`) to report whether the HPC job is in startup grace period vs. idle-countdown vs. about-to-be-killed, so that I can understand system state without reading logs.

10. As a user, I want stuck `running` tasks (from a killed daemon) to be recovered within 5 minutes, so that they don't block new processing indefinitely.

11. As a user, I want the SSH tunnel to be probed with an actual HTTP request (not just a socket connect) to determine if vLLM is alive, so that a dead vLLM behind a live SSH master is correctly detected as DOWN.

12. As a user, I want fill tasks to be skipped when vLLM is DOWN, so that the daemon doesn't waste time in retry loops and can proceed to HPC idle kill faster.

13. As a user, I want the MCP server (`mcps/hpc_hermes.py`) to have a singleton guard, so that duplicate instances don't contend on the same SQLite DB.

14. As a user, I want graceful shutdown: SIGTERM drains in-flight tasks before exit, so that no tasks are abandoned in `running` state.

15. As a user, I want the daemon to survive a tmux restart (PDEATHSIG from parent), so that it doesn't orphan when the tmux session dies.

16. As a user, I want the daemon to detect when a freshly-dispatched SLURM job's vLLM is still loading (model load, torch compile) and wait before processing tasks, so that tasks don't fail during the 4-minute startup window.

17. As a user, I want auto-dispatch to retry on SSH or `sbatch` failure and verify the job actually appears in the queue, so that a transient HPC login node issue doesn't leave tasks unprocessed for 60+ seconds.

18. As a user, I want a background sweep to catch tasks stuck in `running` state from a killed daemon even if the daemon was never restarted, so that I don't have to manually reset them.

19. As a user, I want exactly ONE launcher script for the daemon (`hpc-startup.sh --worker`), so that I don't accidentally start old code with different settings from a deprecated path.

20. As a user, I want high-priority MCP tasks (priority >= 5) to jump ahead of low-priority cron work at dispatch time, so that interactive responses are not queued behind batch jobs.

21. As a user, I want the daemon to log a warning if any task runs longer than 10 minutes, so that I can detect stuck tasks and investigate before they block the queue.

22. As a user, I want the system's single-vLLM limitation documented explicitly, so that future architecture decisions don't assume task-level parallelism exists.

## Implementation Decisions

### D1 — `_dispatcher_cycle()` replaces the single-threaded loop

The current `_idle_cycle()` method unifies three branches (real, fill, idle). Replace it with a `_dispatcher_cycle()` that:

- Runs every `poll_interval` seconds (default 5).
- First: check for pending real tasks. If any exist, process one via HermesRunner.
  - During real-task processing, the main loop blocks (HermesRunner.run() is synchronous).
  - After this, if time remains in the cycle, check for fill tasks.
- Second: if no real tasks, launch ONE fill task in a daemon thread.
  - The fill thread runs `AIAgent.run()` with fill_timeout.
  - The main loop does NOT wait for fill completion — it returns immediately.
  - On the next cycle: if the fill thread is still alive, skip fill processing.
  - If the fill thread completed AND no new real tasks have arrived, pick the next fill.
- Third: if no tasks at all (real or fill), count an idle cycle.

This is not fully concurrent (HermesRunner calls vLLM, which is a single endpoint) — but it interleaves fill and real work at cycle granularity instead of task granularity.

### D2 — `source` field in task metadata

`submit_task()` gains a required `source` parameter with these values:

| Source | Who sets it | Meaning |
|--------|-------------|---------|
| `orchestrator` | `run_hpc_agent_task()`, `run_on_hpc_tool()` | User-initiated via MCP |
| `mcp` | `submit_hpc_task()` | Fire-and-forget MCP call |
| `fill-agent` | Code executed by fill AIAgent's `execute_code`/`terminal` | Fill-derived (does NOT reset idle timer) |
| `cron` | `cmd_submit()` and `cmd_submit_fill()` in CLI | Cron-scheduled batch work |
| `fill` | `cmd_submit_fill()` | Fill task itself (priority=-1) |

The `source` is stored in the `metadata` JSON column of the `tasks` table.

**Idle timer rule:** Only tasks with `source IN ('orchestrator', 'mcp', 'cron')` reset `_last_real_time`. Fill-derived and fill tasks do not.

### D3 — Fill tasks cycle: run once, mark done

Currently fill tasks stay `pending` forever. Change to: after a fill task completes, mark it `completed`. The next fill task in FIFO order is picked on the next idle cycle.

This means: when 3 fill tasks exist, the daemon processes fill-1 → marks done → fill-2 → marks done → fill-3 → marks done → all 3 exhausted → truly idle.

If new fill tasks are seeded (by crontab or startup script), they re-enter the pool.

### D4 — `_has_hpc_job()` caches job ID

The daemon queries `squeue` on every idle cycle. Cache the result with a 60s TTL to avoid SSH spam. Track `_hpc_job_seen_at` for startup grace.

### D5 — Graceful shutdown via signal handlers

Register SIGTERM handler in `HermesWorker.run()`:
- Set `_shutting_down = True`.
- If a task is in-flight (HermesRunner in progress), wait up to `shutdown_wait_max` for it to finish.
- Close fcntl lock file.
- Do NOT kill the SSH tunnel (let the SLURM job's vLLM shut down naturally).
- Exit with code 0.

### D6 — vLLM liveness via HTTP, not socket

`_vllm_available()` sends `GET /v1/models` with a 3s timeout and checks for HTTP 200. A socket connect is insufficient because the SSH master process keeps the port open even when the remote vLLM is dead.

Also: on vLLM DOWN, skip fill tasks AND defer real tasks with a 30s backoff (store `_next_retry_time` in memory) instead of letting each task hit the broken endpoint individually.

### D7 — MCP server singleton guard

`mcps/hpc_hermes.py` `main()` acquires `fcntl.flock` on `/tmp/hpc-hermes-mcp.lock` before starting `mcp.run()`. If the lock is held, exit silently.

### D8 — PDEATHSIG on daemon startup

`HermesWorker.run()` calls `prctl(PR_SET_PDEATHSIG, SIGTERM)` to ensure the daemon dies when its parent (tmux, shell, or supervisor) dies. This prevents orphan processes.

### D9 — vLLM startup progress polling

When the daemon detects that an HPC job is running but vLLM is not yet responding (HTTP timeout/error on `/v1/models`), enter a **startup monitoring state**:

- Poll `/v1/models` every 10s up to 5 minutes.
- Log the last line of the vLLM log file on the compute node (via `ssh tail -1 ~/hpc-agent/logs/vllm-fast-*.log`) every 60s for visibility.
- Do NOT process any tasks (real or fill) during this window — they will fail anyway.
- If vLLM starts responding within 5 min, exit monitoring state and resume normal dispatch.
- If 5 min expires without vLLM response, fail all pending tasks with error `"vLLM failed to start within 5 min of SLURM job start"`, kill the HPC job, and wait for the next auto-dispatch cycle.

This gives visibility into the `shm_broadcast` deadlock and prevents the task-retry storm during vLLM loading.

### D10 — Auto-dispatch with retry and verification

`_auto_dispatch_if_needed()` gets:

- **3 retries** with 10s backoff between SSH attempts (instead of silently swallowing failures).
- **Post-submission verification**: after `sbatch`, poll `squeue` for the submitted job name up to 3 times (5s apart). If the job doesn't appear, treat the dispatch as failed and retry.
- **Exponential backoff on repeated failures**: if dispatch fails 3 times in a row, wait 300s before the next attempt (instead of 60s).
- **Dispatch state file**: `/tmp/hpc-dispatch-state.json` records the last dispatch attempt, its result, and the next retry time. Survives daemon restart.
- **Manual fallback**: if auto-dispatch fails after max retries, log a clear actionable message: `[auto-dispatch] FAILED after 3 retries — run 'python3 hpc_batch.py dispatch' manually`.

### D11 — Proactive stuck task sweep

Add a separate background thread (runs every 300s) that:

1. Queries `SELECT id, started_at, metadata FROM tasks WHERE status = 'running'`.
2. For each running task: check if `started_at < now - 600` (10 min stale).
3. If stale: check if the owning daemon's PID is still alive by reading the task's `agent_id` or the daemon PID file.
4. If the daemon is dead: reset task to `pending` with error `"Recovered from stale running state (owner daemon not found)"`.
5. Log the recovery action.

This catches tasks left `running` by a `kill -9` or a daemon crash that the startup recovery missed (because the daemon wasn't restarted on the same machine, or the PID was reused).

### D12 — Single launcher path consolidation

- `scripts/start_worker.sh`: delete the file. It's been deprecated in code and is a source of stale-daemon spawning.
- `hpc-startup.sh --worker`: the ONLY supported launcher. All environment setup (conda path, PYTHON, working directory, log file) is contained in this one file.
- Watchdog (`scripts/watchdog.sh`): the ONLY automated restart mechanism. Crontab entry `* * * * * bash scripts/watchdog.sh` replaces all previous `*/5` tmux-based keepalives.
- Manual `python3 -c "from worker.hermes_worker import HermesWorker..."` commands: documented as "only for debugging — do not use in production" in the startup script header.

### D13 — Priority preemption timeout

Real tasks with `priority >= 5` get a **dispatch timeout**:

- Before starting a low-priority task (priority < 5), check if any higher-priority task has been pending for > 30s.
- If so, skip the low-priority task and process the high-priority one first.
- This is NOT preemption (doesn't kill an in-flight task). It's a reordering check at dispatch time.
- The default priority for orchestrator MCP calls is 0 (from `submit_task()` default). Change to 5 for all MCP-originated tasks so they jump ahead of cron-submitted batch work.

If a single task runs for > 600s (10 min) without completing, log a warning with the task ID, priority, and message preview. This is the SLA observability — not a kill switch (killing an in-flight AIAgent mid-conversation would lose state).

### D14 — Single-vLLM concurrency note (architectural limitation documented)

The entire system has one vLLM endpoint. This means:

- Only ONE text generation call can be in-flight at any time.
- Fill tasks and real tasks both need text generation. They cannot run simultaneously.
- D1's thread-based fill dispatch does NOT give true parallelism — it gives faster interleaving.
- True parallelism requires either: (a) a second vLLM instance on a different port, (b) a separate SLURM job for fill tasks, or (c) routing fill tasks through a cheaper/smaller model.

Document this limitation in `CONTEXT.md` and `docs/architecture.md` so future work does not assume D1 solved the concurrency problem.

## Testing Decisions

### Seam philosophy

All tests should exercise external behavior through public interfaces, not implementation internals. The three key seams are:

1. **`HermesWorker` public API** — `__init__()`, `run()`, `run_once()`. Test full cycle behavior via `run_once()` with a real SQLite DB (in-memory or temp file).

2. **`submit_task()` / task queue** — Insert tasks, check state transitions. Already tested in `test_schema_contract.py` and `test_priority_queue.py`. Extend to test `source` field behavior.

3. **MCP protocol** — `mcps/hpc_hermes.py` tools via `_poll()` and JSON-RPC calls. Already tested in `test_mcp_hpc_hermes.py`. Extend to test singleton guard.

### Prior art

| Test file | What it tests | Existing seams |
|-----------|---------------|----------------|
| `test_hermes_worker.py` | `process_next_task()`, `_idle_cycle()`, sentinel, stuck recovery, fill FIFO | Real temp DB, MockRunner |
| `test_mcp_hpc_hermes.py` | `_poll()`, `run_on_hpc()`, `run_hpc_agent_task()`, `check_hpc_task_status()` | Patch `_resolve_db()`, real DB |
| `test_idle_poll.py` | Poll intervals, sentinel wakeup | Real DB, `HermesWorker` with short timeouts |
| `test_tunnel.py` | `_ensure_vllm_endpoint()`, tunnel establish/close | Mock subprocess |
| `test_schema_contract.py` | DB schema, journal mode, merge behavior | Real temp DB |

### New tests needed

| Test | Seam | What it validates |
|------|------|-------------------|
| `_dispatcher_cycle` skips fills when vLLM down | `HermesWorker.run_once()` with MockRunner producing APIConnectionError | Fill branch returns `(None, False)` without processing |
| Fill task marks self completed after processing | `process_next_task()` with `min_priority=-1` | Fill task transitions to `completed` |
| Real task resets idle timer, fill task does not | Submit 1 real + 1 fill, check `_last_real_time` before/after | Only real task advances the timer |
| HPC idle kill fires after threshold | `HermesWorker` with `_hpc_idle_timeout=10`, `poll_interval=1`, mock HPC job | `_kill_hpc_job()` called after 10 cycles of idle |
| Startup grace prevents kill | Same as above but job_age < 300s | `_kill_hpc_job()` returns False |
| `source` field tracked in metadata | `submit_task(source="orchestrator")` then read DB | Metadata contains `{"source": "orchestrator"}` |
| MCP server singleton guard | Start two MCP server processes | Second exits immediately with code 0 |
| PDEATHSIG kills daemon on parent exit | Fork, start daemon in child, kill parent | Child receives SIGTERM within 1s |
| vLLM startup monitoring enters wait state | `HermesWorker` with mock `_vllm_available()` returning False, mock `_has_hpc_job()` returning True | No tasks processed for 5 min, `/v1/models` polled every 10s |
| Auto-dispatch retries on SSH failure | `ensure_hpc_worker()` with mock `subprocess.run` returning non-zero | Retry count incremented, next retry after backoff |
| Stuck task sweep recovers stale running tasks | Insert task with `status='running'`, `started_at < now - 600`, no matching PID | Task reset to `pending` |
| `hpc-startup.sh` is the only launcher | Check `scripts/start_worker.sh` deleted, `hpc-startup.sh` is sole entry point | Only one launcher path exists on disk and in crontab |
| High-priority task jumps queue at dispatch | Submit task with priority=5 and task with priority=0, then call `process_next_task()` | Priority=5 task claimed first |
| Long-running task warning logged | `MockRunner` with simulated 11-min execution | Warning log line with task ID and preview emitted |

### What makes a good test

- Only verify output behavior, not internal state (exception: `_last_real_time` is the core observable of the idle timer).
- Use `MockRunner` for AIAgent calls — do not call real vLLM.
- Use real SQLite (temp file), not mocks — the DB is the system's nervous system.
- Avoid timing-dependent assertions: use `timeout` parameters and bounded poll loops in tests.

## Out of Scope

- Parallel task execution across multiple vLLM instances (requires multi-GPU-aware scheduling). The single-vLLM limitation is documented (D14) but not solved.
- Preemption of an in-flight task (killing a running AIAgent mid-conversation). D13's priority reordering only applies at dispatch time.
- Web dashboard for queue monitoring.
- Secondary HPC cluster failover (multi-cluster dispatch).
- Rewrite of the AIAgent/HermesRunner integration (too coupled, too high risk for this cycle).
- GPU utilization metrics or cost tracking (A7 is limited to vLLM startup progress — not full observability).
- Authentication or authorization for the MCP tools (all tools are unauthenticated; the `source` field is a convention, not a security boundary).

## Further Notes

### Status: all 16 items from `issues/architecture.md` covered

| Issue | Coverage | Implementation |
|-------|----------|----------------|
| A1 — Single-threaded daemon | **covered** | D1 (threaded fill dispatch bounds hold time), D14 (limitation documented) |
| A2 — Fill MCP dead work | **covered** | D1 (background fill thread, 5s real-task check cycle) |
| A3 — No task origin tracking | **covered** | D2 (source field in metadata) |
| A4 — Fill resets idle timer | **covered** | D2 + D3 (source-gated idle reset + fill tasks mark complete) |
| A5 — vLLM startup race | **covered** | D9 (startup monitoring state, 5 min grace, no tasks during load) |
| A6 — SSH single point of failure | **covered** | D6 (HTTP liveness — detection only; secondary cluster is Out of Scope) |
| A7 — HPC black box | **covered** | D9 (vLLM log tailing every 60s during startup) |
| A8 — No circuit breaker | **covered** | D6 (30s backoff on vLLM down, fill skip) |
| A9 — Auto-dispatch best-effort | **covered** | D10 (3 retries, post-submit verification, exponential backoff) |
| A10 — No dead task cleanup | **covered** | D11 (background stuck sweep every 300s) |
| A11 — Fill tasks never complete | **covered** | D3 (run once, mark done, cycle pool) |
| A12 — Multiple launcher paths | **covered** | D12 (delete scripts/start_worker.sh, single source of truth) |
| A13 — DB contention | **covered** | D7 (MCP server singleton guard) |
| A14 — No SLA on completion | **covered** | D13 (priority reorder at dispatch, 10-min warning log) |
| A15 — Graceful shutdown | **covered** | D5 (SIGTERM handler) + D8 (PDEATHSIG) |
| A16 — Fill MCP starvation | **covered** | D1 (5s check cycle) + D14 (single-vLLM limitation documented) |

### Design constraints

The single-threaded limitation (A1) is architectural. D1's thread-based fill dispatch gives **faster interleaving**, not **true parallelism**. A fill task and a real task both need vLLM text generation. There is one vLLM endpoint. One call must finish before the next starts. The behavioral improvement is: a real task now waits at most 5s (poll cycle) instead of 300s (fill timeout). This is a 60x latency improvement on the worst case, but it is not concurrent execution. The D14 documentation makes this explicit so future work doesn't assume parallelism exists.

The `source` field (D2) is a convention, not a security boundary. Any caller of `submit_task()` can set any source. The assumption is that fill AIAgents are cooperative — they have no incentive to lie. If this becomes a concern, the AIAgent's tool sandbox can enforce source tagging at the `execute_code`/`terminal` execution boundary.

Fill tasks cycling through the pool (D3) changes the seeding model: `seed-fill-tasks.sh` must be run periodically (cron or daemon startup) to maintain N concurrent fill tasks. The startup script already does this on daemon start.
