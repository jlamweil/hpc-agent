# Design Hypotheses & Validation

For each architecture gap, the hypothesis we're operating on, the design approaches available, and how we test whether it actually works.

---

## H1 — Fill-to-Real Task Starvation

### The Gap
The daemon processes one task at a time. A fill task (up to 300s) blocks the entire queue. Real tasks submitted during that window sit untouched. The user expects fill tasks to generate real work via MCP calls — but those calls land in a queue that the fill task itself is blocking.

### Hypothesis
If we move fill task execution to a background thread and check for new real tasks every 5s, the maximum real-task latency drops from 300s to 5s. Fill tasks can still generate work via MCP — that work gets picked up on the next cycle instead of 300s later.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Thread per fill | `threading.Thread` for fill AIAgent, main loop checks `is_alive()` every 5s | AIAgent thread safety (HermesRunner is not designed for concurrency) | Medium |
| **B** — Process per fill | `multiprocessing.Process` for fill, shared DB | IPC overhead, fill can't access daemon machine's filesystem | High |
| **C** — Event-driven fill | asyncio event loop, fill AIAgent as a coroutine | Requires rewriting the entire daemon loop | Very high |
| **D** — Queue priority with fill preemption | Fill task runs synchronously but checks `pending_real_count` internally every N API calls | Fill AIAgent must be instrumented to yield; doesn't solve MCP dead work | Low |

**Recommended:** A (thread per fill). Lowest risk, highest compatibility with existing HermesRunner. The AIAgent does file I/O and HTTP calls — these release the GIL. The main loop's 5s check is simpler than instrumenting the AIAgent to yield.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Thread-isolation smoke | Start fill thread, assert `_fill_thread.is_alive()`, submit real task, assert real completes within 10s | Real task latency < 10s |
| Thread safety — DB writes | Fill task writes to DB via `submit_task()`, real task reads DB concurrently | No `OperationalError: database is locked` |
| Thread safety — HermesRunner | Fill thread calls `AIAgent.run()` while main loop checks `_vllm_available()` | No crash, no hang |
| Fill thread lifecycle | Fill completes naturally, daemon picks next fill on next cycle | `_fill_thread` is `None` after join, new fill starts |
| Fill thread abort on real task | Fill thread running, real task arrives, fill skipped | `_fill_thread.join(timeout=5)` exits early |
| No orphan threads on daemon shutdown | Daemon receives SIGTERM while fill thread running | Daemon exits within `shutdown_wait_max`, thread is daemon=True so dies with process |
| 5s bound verification | Submit fill, wait 4s, submit real task. Measure time from submit to real task starting. | Real task starts within 10s (one 5s cycle + processing overhead) |

---

## H2 — Fill-Origin Task Indistinguishability

### The Gap
A fill AIAgent can call `submit_task()` via `execute_code` or `terminal`. These tasks look identical to orchestrator-submitted tasks: both have `agent_id="hpc-batch"`, `priority=0`, `task_type="subagent"`. The idle timer (`_last_real_time`) resets on ANY real task processing. A fill agent can keep HPC alive indefinitely by submitting tasks.

### Hypothesis
If every task carries a `source` tag in its metadata (`orchestrator`, `mcp`, `cron`, `fill-agent`, `fill`), and the idle timer only resets for `source IN ('orchestrator', 'mcp', 'cron')`, then fill-derived work cannot artificially extend HPC lifetime.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — `source` field in `submit_task()` | New required parameter, stored in `metadata.source` | Existing callers must be updated; convention, not enforcement | Low |
| **B** — `source` column in tasks table | Schema migration, indexed column | Schema change, existing tasks have no source | Low |
| **C** — Call-stack-based detection | Inspect `sys._getframe()` at `submit_task()` time to detect caller origin | Fragile, doesn't work across threads/processes | Medium |
| **D** — Agent identity token | Each AIAgent gets a signed token at creation time; MCP tools verify token before accepting | Complex, requires crypto, still convention-based | Very high |

**Recommended:** A (metadata field) + **enforcement via AIAgent tool sandbox**. The AIAgent's `execute_code` and `terminal` tool handlers are in `hermes-agent/tools/`. Modify them to set an environment variable or thread-local before executing user code: `_fill_agent_context = True`. In `hpc_batch.py`, check for this thread-local at `submit_task()` time and override `source` to `"fill-agent"` if set.

**Note:** The `async-hermes-agent` fork was retired (Jun 2026). This design was never implemented. The upstream `hermes-agent` package now serves as the AIAgent source.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Source stored correctly | `submit_task(source="orchestrator")`, read `metadata.source` from DB | `{"source": "orchestrator"}` in JSON |
| Default source for existing callers | Call `submit_task()` without `source` parameter | Defaults to `"orchestrator"` (backward compat) |
| Idle timer not reset by fill source | Process task with `source="fill-agent"`, check `_last_real_time` before/after | Timer unchanged |
| Idle timer reset by orchestrator source | Process task with `source="orchestrator"`, check `_last_real_time` | Timer advances to ~now |
| AIAgent sandbox enforcement | Run `execute_code` that calls `submit_task()`, check resulting task source | Source is `"fill-agent"` |
| AIAgent sandbox — terminal | Run `terminal` that calls `submit_task()`, check resulting task source | Source is `"fill-agent"` |
| Backward compat — existing DB rows | Task with no `source` in metadata is treated as orchestrator | `metadata.get("source", "orchestrator")` returns `"orchestrator"` |

---

## H3 — Fill Tasks Never Complete / Truly Idle Unreachable

### The Gap
Fill tasks have `priority=-1` and stay `pending` forever. The daemon never marks them done. `_count_fill_pending()` always returns > 0. The "truly idle" branch in `_idle_cycle()` is unreachable as long as one fill task exists. The idle timer must fire inside the fill branch instead, but it's never incremented there because fills reset it to 0.

### Hypothesis
If fill tasks are marked `completed` after one run, and the daemon picks the next fill task from FIFO order on the next cycle, then all fill tasks eventually exhaust. The pool becomes empty. The "truly idle" branch becomes reachable. The idle timer can fire naturally.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Mark fill completed after run | In `_process_random_fill()`, after AIAgent completes, update status to `completed` | Need to re-seed fill tasks on daemon restart (already done in hpc-startup.sh) | Low |
| **B** — Soft-delete fill after run | Delete the fill DB row after processing. Next cycle hits "no tasks" path. | Fill seeding must run every N minutes (cron) instead of once at boot | Low |
| **C** — Fill task has `max_runs` counter | Each fill task runs 3× then marks done. Counter in metadata. | More complex state tracking; adds column | Medium |
| **D** — Fill pool with replenishment | N fill tasks in pool. When all exhausted, daemon auto-seeds via `_push_fill_task()`. | Daemon gets write access to fill pool; could infinite-loop | Medium |

**Recommended:** A (mark completed) + **enforce replenishment at daemon startup**. The `hpc-startup.sh` already runs `seed-fill-tasks.sh` on boot. The `_recover_stuck_tasks()` runs on startup and during idle cycles — add fill-task replenishment there too. After recovering stuck tasks and during idle, if fill count < 3, call `_push_fill_task()`.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Fill task marked completed | Process one fill, check DB status | `status = 'completed'` |
| Next fill picked on next cycle | After fill-1 completes, next cycle picks fill-2 | Different `id` than previous fill |
| All fills exhausted → truly idle | Process all 3 fills, check `_idle_cycle()` returns truly idle | `_count_fill_pending() == 0`, fill branch skipped |
| Auto-replenish on idle | All fills exhausted, daemon idle, check fill count after 60s | `_count_fill_pending() >= 3` |
| Replenish on startup | Fresh daemon, no fills in DB, after startup | `_count_fill_pending() >= 3` |
| Fill cycle timing | Fill-1 → done, fill-2 → done, fill-3 → done, measure total | Total time ~ 3 × fill_timeout (no blocking overlap) |
| Orphan fill still cleaned up | Fill with missing source `.txt` file removed | Row deleted from DB |

---

## H4 — vLLM Startup Race (4-Min Blind Window)

### The Gap
Auto-dispatch submits a SLURM job. vLLM takes ~4 minutes to load the model (3 min weights + 1 min torch compile). During those 4 minutes, every task the daemon tries to process fails with connection errors. The AIAgent retries 3 times per task (~30s). With N pending tasks, the retry storm burns O(N × minutes) before settling.

### Hypothesis
If the daemon enters a **startup monitoring state** when it detects a running HPC job but no responding vLLM, it stops processing tasks entirely, polls vLLM every 10s, and tails the vLLM log for visibility. After 5 minutes, if vLLM is still not responding, it kills the job and fails all pending tasks with a clear error.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Startup monitoring state | New `_monitor_vllm_startup()` run before main dispatch loop. Polls `/v1/models` + `tail vllm log`. No tasks during monitor. | Delays task processing by up to 5 min (acceptable — they'd all fail anyway) | Medium |
| **B** — Passive backoff with log | Keep processing tasks but with exponential backoff per task (1s, 2s, 4s, 8s…) on connection errors | Tasks still fail, just slower. Doesn't solve the retry storm. | Low |
| **C** — SLURM job ready signal | Modify `dsv4_fast.slurm` to write a sentinel file when vLLM responds. Daemon waits for sentinel. | Requires SSH filesystem access from compute node back to orchestrator (complex, fragile) | High |
| **D** — Webhook from vLLM | vLLM emits a callback when ready. Daemon receives it. | Requires HTTP server in daemon, vLLM plugin. Very complex. | Very high |

**Recommended:** A (startup monitoring). Simplest, most robust. The monitoring state also serves as observability — the vLLM log tail reveals `shm_broadcast` deadlocks during startup, which is the most common vLLM failure mode.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Enter monitor state | Daemon sees HPC job running, vLLM not responding | `_monitoring_vllm` flag set to True |
| No tasks processed during monitor | Submit task while monitoring, check status | Task stays `pending` |
| Exit monitor on vLLM ready | vLLM starts responding during monitor window | Daemon resumes normal dispatch, task starts within 10s |
| Exit monitor on timeout | vLLM not responding after 5 min | All pending tasks set to `failed` with clear error message |
| vLLM log tail visible | During monitor, log line printed every 60s from vLLM log | Daemon log contains `[vLLM startup] tail: ...` |
| Kill HPC job on monitor timeout | After 5 min, HPC job still running but vLLM dead | `_kill_hpc_job()` called, job cancelled |
| Monitor does not start if vLLM already up | Daemon starts, vLLM responding | `_monitoring_vllm` is False, normal dispatch immediately |
| Grace period does not delay dispatch | vLLM happens to be up immediately | Task starts within normal poll interval (5s) |

---

## H5 — No Circuit Breaker (Retry Storm on Dead vLLM)

### The Gap
When vLLM is down, every task retries 3 times with ~30s delays each. For 50 pending tasks: 50 × 3 × 30s = 75 minutes of futile retries. Fill tasks too. The daemon is "busy" (processing tasks that fail) but not productive. The idle timer can't fire because the daemon is never idle.

### Hypothesis
If the daemon detects vLLM is DOWN via `_vllm_available()` (HTTP check), it should: (a) skip all fill tasks immediately, (b) defer real tasks with a 30s backoff window, (c) check vLLM liveness once per backoff window instead of per-task. This collapses the retry storm from O(N) to O(1).

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Global backoff timer | Store `_next_vllm_retry_at` in daemon. Every cycle, if `now < _next_vllm_retry_at`, skip all task processing. | Tasks are delayed by up to 30s even if vLLM recovers quickly | Low |
| **B** — Per-task backoff with fail-fast | Each task tries once. If "connection error", fail immediately with `"vLLM down"`. Don't retry. Next task same. | Every task fails once even during short blips | Low |
| **C** — Adaptive circuit breaker | Track consecutive failures. After 3, open circuit for 60s. After 6, 300s. Reset on success. | More state, more code | Medium |

**Recommended:** A + C combined. Use C (adaptive circuit breaker) as the primary mechanism with A (global timer) as the stopgap when the breaker opens.

Breaker states:
- **CLOSED** — normal operation. Tasks processed, failures counted.
- **OPEN** — after `max_failures` consecutive failures. No tasks processed for `backoff_duration`.
- **HALF_OPEN** — after backoff expires. One probe task allowed. If it succeeds, back to CLOSED. If it fails, back to OPEN with doubled backoff.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Circuit opens after N failures | VLLM down, process 3 tasks (each fails), check breaker state | State = OPEN |
| No tasks processed while OPEN | Submit task while OPEN, check status | Stays `pending`, never claimed |
| Circuit half-opens after backoff | Wait for backoff_duration, check breaker state | State = HALF_OPEN |
| Circuit closes on success | Half-open, submit task, vLLM happens to be up, task completes | State = CLOSED |
| Circuit re-opens on probe failure | Half-open, submit task, vLLM still down, task fails | State = OPEN, backoff doubled |
| Backoff resets after sustained success | CLOSED state, 5 tasks complete consecutively | Counter reset to 0 |
| Fill tasks skipped while OPEN | Submit fill task while OPEN | Fill thread not started |

---

## H6 — Auto-Dispatch Best-Effort

### The Gap
`_auto_dispatch_if_needed()` runs in a daemon thread with bare `except Exception: pass`. If SSH fails, `rsync` fails, or `sbatch` fails, the error is silently swallowed. The next attempt is in 60s (rate limit). Meanwhile, tasks pile up in the queue. No verification that the submitted job actually appears in `squeue`.

### Hypothesis
If auto-dispatch has retry logic (3 attempts with 10s backoff), post-submission verification (poll `squeue` for job name), and exponential backoff on repeated failures (300s after 3 consecutive fails), then transient HPC login node issues don't cause dispatch gaps. A dispatch state file survives daemon restart.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Retry with verification | Wrap dispatch in `for attempt in range(3):` + `try/except` + `sleep(10)`. After sbatch, `squeue | grep dsv4-` up to 3 times. | Adds ~30s worst-case to dispatch latency (acceptable) | Low |
| **B** — Job manifest file | Write intended dispatch to `/tmp/hpc-dispatch.json`. Check it on daemon startup and retry if pending. | File I/O, stale state handling | Low |
| **C** — Dedicated dispatch thread | Separate thread for dispatch only. Never blocks `submit_task()`. Queue-based: dispatch requests go into a `queue.Queue`, worker processes them in order. | More complex, but isolates dispatch from task submission | Medium |

**Recommended:** A + B. Retry + verification catches transient failures. State file prevents silent loss across daemon restart. Dedicated dispatch thread (C) is overkill for the current failure rate.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Retry on SSH failure | Mock SSH to fail twice, succeed on 3rd | Dispatch succeeds after 2 retries |
| Post-submit verification | Mock `sbatch` to succeed, `squeue` to return empty for 2 polls, then show job | Dispatch succeeds after 3rd squeue poll |
| Max retries exhausted | Mock SSH to fail all 3 times | Dispatch gives up, error logged |
| Exponential backoff | 3 consecutive dispatch failures, check next retry time | `next_retry_at > now + 300` |
| State file written | Dispatch succeeds, check `/tmp/hpc-dispatch-state.json` | File exists with `{"status": "submitted", "job_id": "..."}` |
| State file read on daemon restart | Daemon restarts, finds state file with pending dispatch | Retries dispatch immediately |
| Successful dispatch resets backoff | After failed dispatch, next succeed | Counter reset to 0 |

---

## H7 — Dead Task Cleanup (Stuck Running)

### The Gap
`_recover_stuck_tasks()` runs on startup and during idle cycles. If the daemon stays busy for days and then crashes (or is killed without SIGTERM), tasks left in `running` state are not recovered until the daemon happens to be restarted. No proactive sweep.

### Hypothesis
If a background thread (runs every 300s) queries for stale `running` tasks, checks if their owning daemon PID is still alive, and resets them to `pending` if not, then stuck tasks are recovered within 5 minutes regardless of daemon restart cadence.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Background sweep thread | `threading.Timer(300, _sweep_stuck_running)`. Checks `started_at`, PID file, resets if stale. | Minimal — read-only except when resetting | Low |
| **B** — Startup recovery only | Keep current behavior but extend `_recover_stuck_tasks()` to also check PID file. | Doesn't help if daemon runs for weeks without restart | Very low |
| **C** — Cron-based cleanup | Separate script (`scripts/cleanup-stuck.py`), run from crontab every 5 min. | External dependency, separate deployment | Low |

**Recommended:** A (background sweep thread). Same process, no external dependency. The sweep also logs the recovery action, making stuck-task events visible without log spelunking.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| Sweep resets stale running task | Insert task with `status='running'`, `started_at < now - 600`, PID file points to dead PID | After sweep, `status='pending'`, `error='Recovered from stale...'` |
| Sweep skips fresh running task | Insert task with `status='running'`, `started_at = now`, PID alive | Status unchanged |
| Sweep skips task with alive PID | Insert task, PID file points to live unrelated process | Status unchanged |
| Sweep logs recovery action | Sweep resets a task | Log contains `[sweep] Recovered task X`
| Sweep respects daemon shutdown | Daemon exits, sweep thread should not prevent exit | Thread is daemon=True, dies with process |

---

## H8 — No Priority Reordering at Dispatch

### The Gap
Tasks are processed FIFO by priority. A cron job submits 50 tasks at priority 0. An orchestrator MCP call submits 1 task at priority 0. The MCP task waits behind all 50 cron tasks (could be hours). The MCP caller (OpenCode orchestrator) has a 300s timeout.

### Hypothesis
If the daemon checks for high-priority pending tasks (`priority >= 5`) before processing a low-priority task, and MCP-originated tasks use `priority=5` instead of `priority=0`, then interactive MCP tasks jump ahead of batch work. This is reordering at dispatch time, not preemption — an in-flight task is not interrupted.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — Priority reorder at dispatch | Before claiming a task, check for any `pending AND priority >= 5` created > 30s ago. If found, claim that instead of the priority=0 default. | Low-priority tasks could starve if high-priority tasks keep arriving | Low |
| **B** — Two-tier queue | Separate processing lanes: high-priority queue processed before low-priority. | Deeper architectural change | Medium |
| **C** — MCP tools use higher default priority | Change `submit_task(priority=0)` to `submit_task(priority=5)` in MCP tools. | Changes existing behavior; fill-derived MCP calls also get priority 5 | Very low |

**Recommended:** A + C. Change the MCP tool default priority to 5 (C). At dispatch time, scan for pending tasks with `priority >= 5` and process those first (A). Add a 30s age check to prevent starvation of low-priority tasks: if a priority-0 task has been waiting > 600s, process it next regardless.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| High-priority task claimed first | Submit priority=5 task, then priority=0 task, call `_process_next()` | Priority=5 task claimed |
| Low-priority task not starved | Submit priority=5 tasks continuously, priority=0 task waiting > 600s | After 600s, priority=0 task is claimed |
| MCP tools use priority 5 | Call `run_on_hpc_tool()`, check submitted task priority | `priority = 5` |
| Existing cron tasks use priority 0 | `cmd_submit()` with default priority | `priority = 0` |
| Reorder within same priority | Two tasks at priority 5, check FIFO order | Older one claimed first |

---

## H9 — No Graceful Shutdown

### The Gap
SIGTERM → immediate exit. In-flight tasks stay `running`. fcntl lock not released. SSH tunnel orphaned. No drain.

### Hypothesis
If the daemon registers a SIGTERM handler that sets `_shutting_down = True`, waits up to `shutdown_wait_max` for in-flight tasks, releases the fcntl lock, and lets the SSH tunnel die naturally with the SLURM job, then no tasks are abandoned and no resources leak.

### Design Possibilities

| Approach | Mechanism | Risk | Complexity |
|----------|-----------|------|------------|
| **A** — `signal.signal(SIGTERM, handler)` | Handler sets flag. Main loop checks flag after each task. | In-flight task still runs to completion (could be 45 min) — `shutdown_wait_max` bounds this | Low |
| **B** — `atexit.register()` | Register cleanup on normal exit. SIGTERM still needs handler. | Cleanup not guaranteed on SIGKILL | Very low |
| **C** — Context manager for lock | `with self._lock_fd:` ensures release on any exit path | Requires refactoring `run()` to use context manager | Low |

**Recommended:** A + C. Signal handler for SIGTERM. Context manager for fcntl lock so it's released on any exit (normal, exception, signal). Maximum wait for in-flight task: `shutdown_wait_max` (default 300s). After that, abandon.

### How to Test

| Test | Method | Pass Condition |
|------|--------|----------------|
| SIGTERM sets flag | Send SIGTERM to daemon | `_shutting_down = True` |
| In-flight task not interrupted | Task running, send SIGTERM, check task status | Task completes normally (`status = 'completed'`) |
| Drain timeout respected | Task that would take > shutdown_wait_max, send SIGTERM | Task abandoned after `shutdown_wait_max` (`status = 'running'`, recovered by sweep) |
| fcntl lock released on exit | SIGTERM, daemon exits, check lock | `fcntl.flock(LOCK_EX | LOCK_NB)` succeeds immediately after exit |
| PDEATHSIG works | Fork daemon, kill parent (tmux), wait | Daemon receives SIGTERM within 1s |

---

## Summary

| Hypothesis | Key Risk | Key Test |
|-----------|----------|----------|
| H1 — Thread-based fill | AIAgent thread safety | Thread isolation smoke + DB write concurrency |
| H2 — Source field | Convention not enforced | AIAgent sandbox overrides source for fill originators |
| H3 — Fill completes | Pool exhaustion without replenishment | Auto-replenish on startup + idle |
| H4 — Startup monitoring | 5 min delay for every fresh job | Grace period does not fire when vLLM is already up |
| H5 — Circuit breaker | Breaker opens on transient blip | Half-open probe succeeds on recovery |
| H6 — Auto-dispatch retry | State file stale on duplicate daemon | New daemon reads state file before dispatching |
| H7 — Stuck task sweep | Sweep recovers task that is actually running | PID alive check prevents false recovery |
| H8 — Priority reorder | Low-priority starvation | 600s age guard ensures eventual processing |
| H9 — Graceful shutdown | In-flight task interrupted mid-conversation | Drain timeout + fcntl release verifies clean exit |
