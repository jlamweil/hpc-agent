# Confirmed Design

Root cause chain identified and validated through live-system experiments. All 16 architecture items have a confirmed design.

## Root Cause Chain

```
vLLM loading (4 min) ──→ AIAgent APIConnectionError ──→ 3 retries + Transient handler
                                                               │
                                                               ▼
                                                    Infinite retry loop
                                                               │
                                                               ▼
                              HermesRunner 300s timeout fires ──→ returns TaskResult
                                                               │
                                                               ▼
                  ┌──── ThreadPoolExecutor.__exit__ BLOCKS waiting for AIAgent thread ────┐
                  │                                                                       │
                  ▼                                                                       │
         process_next_task() never returns                                                 │
                  │                                                                       │
                  ▼                                                                       │
         task stays "running" forever ◄───────────────────────────────────────────────────┘
                  │
                  ▼
         daemon appears busy → idle kill never fires → HPC stays alive wasting GPU
```

## Design Decisions

### D1 — Replace ThreadPoolExecutor with daemon thread + explicit timeout

**File:** `worker/model_runner.py`, `HermesRunner.run()` method.

**Current code (broken):**
```python
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
    fut = pool.submit(agent.run_conversation, prompt)
    try:
        response = fut.result(timeout=self.timeout)
    except concurrent.futures.TimeoutError:
        return TaskResult(...)  # <-- THIS NEVER PROPAGATES because with block blocks
```

**Fix:** Replace ThreadPoolExecutor with a `threading.Thread` + explicit `thread.join(timeout)`. If the thread doesn't finish within timeout, abandon it (daemon thread dies with process) and return the timeout TaskResult immediately.

```python
t = threading.Thread(target=_run_agent, args=(agent, prompt), daemon=True)
t.start()
t.join(timeout=self.timeout)
if t.is_alive():
    # Thread still running — abandon (daemon=True, dies with process)
    return TaskResult(status=TaskStatus.FAILED, error=f"HermesRunner timeout after {self.timeout}s")
# Thread completed — return its result
return _result_container[0]
```

**Validated by:** Live experiment showing task stuck "running" for 10+ minutes after HermesRunner timeout fired. Log line `[HermesRunner] TIMEOUT after 300s` confirms timeout fires but TaskResult is never returned to caller.

### D2 — Circuit breaker on vLLM DOWN

**File:** `worker/hermes_worker.py`, `HermesWorker`.

When `_vllm_available()` returns False for the first time, start a backoff timer. While the timer is active, no tasks are claimed from the queue (they stay pending). After the backoff expires, try one probe task. If it succeeds, close the breaker. If it fails, double the backoff.

Breaker states and transitions:
```
CLOSED ──(vLLM down)──→ OPEN ──(backoff expires)──→ HALF_OPEN
OPEN ──(no task processing)──→ (stays OPEN until backoff)
HALF_OPEN ──(probe succeeds)──→ CLOSED
HALF_OPEN ──(probe fails)──→ OPEN (backoff doubled)
```

Backoff sequence: 30s, 60s, 120s, 300s (max). Resets to 30s after 5 consecutive CLOSED probes.

**Validated by:** Live experiment showing AIAgent retrying indefinitely against dead vLLM. Breaker would collapse the infinite retry to a single 30s backoff probe.

### D3 — Startup grace for vLLM + tunnel

**File:** `worker/hermes_worker.py`, `_check_hpc_idle_kill()` + new `_monitor_vllm_startup()`.

When daemon detects an HPC job running but vLLM not responding:
- Enter `MONITORING` state
- Poll `/v1/models` every 10s for up to 5 min
- Do NOT claim any tasks during monitoring
- If `/v1/models` responds: exit monitoring, normal dispatch
- If 5 min expires: fail all pending tasks, kill HPC job, clear breaker

**Validated by:** Live experiment showing vLLM taking ~4 min to load. Tasks claimed during this window enter infinite retry loop. Monitoring prevents task claiming until vLLM is confirmed ready.

### D4 — Fill task marks completed after one run

**File:** `worker/hermes_worker.py`, `_process_random_fill()`.

After `_process_random_fill()` returns (AIAgent completed or timed out), update the fill task's status to `completed`. Next idle cycle picks the next fill task from FIFO. When all fill tasks exhausted, daemon reaches truly idle state.

Replenishment: `_recover_stuck_tasks()` on startup checks fill count. If < 3, calls `_push_fill_task()`.

**Validated by:** SQL experiment: manually setting 3 fills to `completed` drops pending count to 0, daemon reaches idle state. Currently, fills stay `pending` forever.

### D5 — Source field in task metadata

**File:** `hpc_batch.py`, `submit_task()` + `worker/hermes_worker.py`, idle timer logic.

`submit_task()` accepts `source` parameter (default `"orchestrator"`). Stored in `metadata.source`. Idle timer (`_last_real_time`) only resets for `source IN ('orchestrator', 'mcp', 'cron')`.

AIAgent sandbox enforcement was considered: `execute_code`/`terminal` handlers in `hermes-agent/tools/` would set `threading.local().is_fill_agent = True` before executing user code. `submit_task()` checks this flag and overrides source to `"fill-agent"`.

**Status:** Design recorded but never implemented. The `async-hermes-agent` fork was replaced with upstream `hermes-agent` + vendored `hermes_runtime/` package. Sandbox enforcement would need to be implemented in the upstream package or in a wrapper layer.

### D6 — Priority reorder at dispatch

**File:** `worker/hermes_worker.py`, `_process_next()`.

Before claiming a task, check for `pending AND priority >= 5 AND created_at > 30s ago`. If found, claim that instead of the default priority-0 task. MCP tools default to `priority=5`. Low-priority tasks get a 600s starvation guard: if a priority-0 task has been waiting >600s, process it next regardless.

**Validated by:** Unit test: submit priority=5 then priority=0, verify priority=5 claimed first. No live test needed.

### D7 — Graceful shutdown

**File:** `worker/hermes_worker.py`, `HermesWorker.run()`.

Register SIGTERM handler:
```python
signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_shutting_down', True))
```

Main loop checks `_shutting_down` after each task. If set, exit without claiming new tasks. Current in-flight HermesRunner call gets up to `shutdown_wait_max` (300s) to finish — the new daemon thread design (D1) prevents the ThreadPoolExecutor deadlock, so the timeout actually works.

### D8 — Single launcher path

Delete `scripts/start_worker.sh`. Sole entry point is `hpc-startup.sh --worker`. Watchdog (`scripts/watchdog.sh`) is sole automated restart mechanism.

### D9 — Auto-dispatch retry + verification

**File:** `hpc_batch.py`, `_auto_dispatch_if_needed()`.

Wrap dispatch in 3 attempts with 10s backoff. After `sbatch`, poll `squeue` for job name 3 times (5s apart). On repeated failures, exponential backoff to 300s max. Dispatch state file at `/tmp/hpc-dispatch-state.json` survives daemon restart.

### D10 — Proactive stuck task sweep

**File:** `worker/hermes_worker.py`, background thread.

Runs every 300s. Queries stale `running` tasks where `started_at < now - 600`. Checks if owning daemon PID is alive. If not, resets to `pending` with error `"Recovered from stale running state (owner daemon not found)"`. Dies as daemon thread.

---

## Test Hierarchy

Unit tests (fast, isolated, no vLLM needed):

| Test | Design | Validates |
|------|--------|-----------|
| Daemon thread replaces pool | D1 | Thread.join(timeout) returns before HermesRunner timeout |
| Circuit breaker blocks tasks | D2 | OPEN state prevents task claiming |
| Breaker half-opens on backoff | D2 | HALF_OPEN allows one probe task |
| Start grace skips processing | D3 | MONITORING state prevents task claiming |
| Fill marked completed | D4 | Fill status transitions to `completed` |
| Source stored in metadata | D5 | `metadata.source` contains correct value |
| Priority reorder at dispatch | D6 | Priority-5 claimed before priority-0 |
| SIGTERM sets flag | D7 | `_shutting_down` is True after signal |

Integration tests (require SQLite temp DB, MockRunner):

| Test | Design | Validates |
|------|--------|-----------|
| HermesRunner timeout does not block | D1 | TaskResult returned within timeout+1s after AIAgent hangs |
| Breaker re-closes on vLLM recovery | D2 | After HALF_OPEN probe succeeds, breaker back to CLOSED |
| Fill pool exhausts → truly idle → HPC kill | D3+D4 | After 3 fills complete, daemon enters idle branch, `_kill_hpc_job()` fires |
| Auto-dispatch retries on SSH fail | D9 | After 2 mock SSH failures, 3rd succeeds |

Live system tests (verify on actual HPC):

| Test | Validates |
|------|-----------|
| Submit task, vLLM down → task stays pending (not running) | D2 |
| vLLM comes up → task claimed within 5s | D3 |
| Fill task completes → new fill picked up on next cycle | D4 |
| Kill daemon → recover stuck task within 5 min | D10 |

---

## Coverage Map

| Gap | Design | Status |
|-----|--------|--------|
| A1 — Single-threaded daemon | D4 (fill completes) mitigates — deferred to future multi-instance work |
| A2 — Fill MCP dead work | D1 (daemon thread) + D4 (fill completes) |
| A3 — No task origin tracking | D5 (source field) |
| A4 — Fill resets idle timer | D5 (source-gated reset) |
| A5 — vLLM startup race | D3 (startup monitoring) |
| A6 — SSH single point of failure | D3 (HTTP liveness check) |
| A7 — HPC black box | D3 (vLLM log tail during monitoring) |
| A8 — No circuit breaker | D2 (circuit breaker) |
| A9 — Auto-dispatch best-effort | D9 (retry + verify + state file) |
| A10 — No dead task cleanup | D10 (stuck task sweep) |
| A11 — Fill tasks never complete | D4 (mark done after each run) |
| A12 — Multiple launcher paths | D8 (single launcher) |
| A13 — DB contention | D7 (MCP singleton, already implemented) |
| A14 — No SLA on completion | D6 (priority reorder) |
| A15 — Graceful shutdown | D7 (SIGTERM handler) |
| A16 — Fill MCP starvation | D1 (daemon thread breaks hold) + D4 (fill completes) |

All 16 gaps covered across 10 design decisions. The root cause (D1 — ThreadPoolExecutor deadlock) cascades across 9 other gaps — fixing it alone eliminates the "alive but useless" failure mode entirely.
