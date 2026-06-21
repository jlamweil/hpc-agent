# Hypothesis Validation Results

Experimental results from testing each hypothesis on the live system.

---

## H1 — Fill-to-Real Task Starvation
**Status: ✅ CONFIRMED**

| Aspect | Evidence |
|--------|----------|
| Fill blocks for 300s | Worker log: `Fill task ef7b43f8... timed out after 300.0s` |
| Real tasks wait | During that 300s window, 0 tasks completed, queue remained blocked |
| AIAgent does real work during fill | Log shows 20+ API calls, file searches, cargo runs inside the fill AIAgent |
| Fill thread would bound to 5s | Not yet tested (no thread implementation) — design D1 |

**Key insight:** The fill task is NOT idle — it does real investigation work (searching files, running cargo, reading docs). Killing it early (after 5s of real task arrival) would lose that work. The design needs to decide: preempt fill immediately (lose work) or let it finish (block real tasks).

---

## H2 — Task Source Field
**Status: ✅ CONFIRMED**

| Aspect | Evidence |
|--------|----------|
| `source` stored in metadata | `submit_task(source="orchestrator")` → `{"source": "orchestrator"}` in DB |
| Default value | `submit_task()` without source → `{"source": "orchestrator"}` (backward compat) |
| Fill-agent value | `submit_task(source="fill-agent")` → `{"source": "fill-agent"}` |
| AIAgent sandbox enforcement | Not tested (async-hermes-agent fork retired Jun 2026; would require upstream hermes-agent change) |
| Idle timer respects source | Logic coded but untested in live system (no real tasks to process during test) |

**Key insight:** The DB-level source tracking works. Enforcement via AIAgent tool sandbox is untested and was rendered moot when the async-hermes-agent fork was retired (Jun 2026).

---

## H3 — Fill Tasks Never Complete
**Status: ✅ CONFIRMED**

| Aspect | Evidence |
|--------|----------|
| Fills stay pending after processing | 3 fill tasks in DB, all `status='pending'` after daemon processed them |
| Marking fills done exhausts pool | Manual UPDATE: 3 fills → completed, pending count dropped to 0 |
| Truly idle reachable after pool empty | After all fills cleared: 0 pending, 0 running → daemon reaches idle state |
| Current vLLM skip achieves same | `_vllm_available()` returns False → fill branch returns `(None, False)` → daemon sleeps |
| Auto-replenish mechanism | Not tested — requires `_push_fill_task()` on pool exhaustion |

**Key insight:** The current system achieves true idle only by accident (vLLM is down, fills skipped). When vLLM is UP, fills would cycle infinitely because they never complete. The fix (D3: mark completed after each run) is simple and validated.

---

## H4 — vLLM Startup Race
**Status: ✅ CONFIRMED — with new sub-findings**

| Aspect | Evidence |
|--------|----------|
| vLLM takes ~4 min to load | Log shows model loading at 2.5+ min for 70% shards (37 shards total ~3.5 min) |
| Task claimed during startup | Test task `test-redispatch-001` went `pending→running` within 5s of submit |
| Task retries during load window | Log shows 3 retries + "Transient APIConnectionError" → rebuilt client → retry |
| Task stayed "running" for 10+ min | Started at 08:39, still running at 08:50+ |
| SSH tunnel fails after job kill | New job (17219991) started but old SSH tunnel (from killed job 17218847) is stale |
| **NEW:** Tunnel establishment fails | `_ensure_vllm_endpoint()` repeatedly logs "No running SLURM job found" — can't find the NEW job via squeue |
| **NEW:** Daemon stuck in retry loop | AIAgent keeps rebuilding HTTP client and retrying; never fails the task |

**Key insight:** H4 has TWO sub-problems:
1. **Model loading** (~4 min) — tasks fail during this window
2. **Tunnel establishment** (can take longer) — `_ensure_vllm_endpoint()` can't find the new job via SSH/squeue, causing infinite retry

The startup monitoring state needs to check BOTH vLLM HTTP readiness AND tunnel establishment. A simple "wait until `/v1/models` returns 200" check would cover both.

---

## H5 — Circuit Breaker (Retry Storm)
**Status: ✅ CONFIRMED — worse than expected**

| Aspect | Evidence |
|--------|----------|
| Task retries 3× per API call | Log: `Retrying in 2.9s (attempt 1/3)`, `Retrying in 5.0s (attempt 2/3)` |
| Then final attempt | `Transient APIConnectionError... rebuilt client, waiting 6s before one last primary attempt` |
| Then ANOTHER cycle | After final attempt fails, task stays "running" — AIAgent doesn't give up |
| No max-task-time kill | Task was "running" for 10+ minutes with no mechanism to stop it |
| Daemon busy-failing | During retry, daemon is stuck on this task — can't process any others |
| **NEW:** Infinite retry | The "Transient APIConnectionError" handler seems to loop indefinitely — it rebuilds the HTTP client and retries without decrementing the retry counter |

**Key insight:** The retry loop is effectively infinite. The "3 retries" counter applies to individual API calls, but the "Transient" handler bypasses the counter and retries indefinitely. A circuit breaker would collapse this entire loop to a single check every 30s.

---

## H6 — Auto-Dispatch Reliability
**Status: ✅ CONFIRMED (partial — retry logic untested)**

| Aspect | Evidence |
|--------|----------|
| Auto-dispatch fires on submit | Log: `[auto-dispatch] Job 17219991 submitted` within seconds of `submit_task()` |
| Rsync works | DB + data + slurm + worker + mcps + scripts all synced |
| sbatch succeeds | `[dispatch] Job submitted: 17219991` |
| Job appears in squeue | Verified manually: `17219991 RUNNING` |
| Post-submit verification | Not implemented (D10) — job appears but daemon's `_ensure_vllm_endpoint()` can't find it via SSH |
| Retry on failure | Not implemented (D10) — no retry in current code |
| State file | Not implemented (D10) — no `/tmp/hpc-dispatch-state.json` |
| **NEW:** Auth/credential mismatch | `_ensure_vllm_endpoint()` can't find the job, but manual `ssh` works. Different SSH user/key? |

**Key insight:** Auto-dispatch fires reliably but the feedback loop is broken. The daemon's `_ensure_vllm_endpoint()` uses a different SSH session than the dispatch and can't find the newly submitted job. This is a tunnel-establishment issue, not a dispatch issue.

---

## H7 — Stuck Task Sweep
**Status: ⏳ NOT TESTED** (need a task to get stuck in `running` state without the daemon crashing)

---

## H8 — Priority Reorder
**Status: ⏳ PARTIALLY TESTED** (code tested: submit_task accepts priority. Live test not done — no concurrent tasks to observe reordering)

---

## H9 — Graceful Shutdown
**Status: ⏳ NOT TESTED** (need to send SIGTERM to a live daemon — would disrupt current experiments)

---

## Summary of Validated Findings

| # | Hypothesis | Status | New Sub-Findings |
|---|-----------|--------|------------------|
| H1 | Fill blocks 300s | ✅ | Fill AIAgent does real work — preemption would lose it |
| H2 | Source field works | ✅ | AIAgent sandbox enforcement untested (different repo) |
| H3 | Fills never complete | ✅ | Current idle state is accidental (vLLM down) |
| H4 | vLLM startup race | ✅ | TWO problems: model loading + tunnel establishment |
| H5 | Retry storm | ✅ | Effectively infinite retry (Transient handler bypasses counter) |
| H6 | Auto-dispatch fires | ✅ | But tunnel establishment has credential/auth issue |
| H7 | Stuck sweep | ⏳ | Not tested |
| H8 | Priority reorder | ⏳ | Code-level test only |
| H9 | Graceful shutdown | ⏳ | Not tested (would disrupt live system) |
