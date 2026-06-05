# PRD: Live vLLM Interaction via tasks.db + Priority Scheduling

Status: `ready-for-agent`

## Problem Statement

Every SLURM batch job on the HPC pays a 30-60 minute cold-start penalty to load DeepSeek V4 Flash into vLLM. Once loaded, the model processes tasks rapidly (10-120s each), then the job exits and the GPU allocation is released. For jobs with few tasks, the GPU idles for most of the walltime. For jobs that finish early, the remaining walltime (often 6+ hours) is wasted.

There is no way to:
- Submit new work to a running vLLM session without waiting for a new SLURM queue slot
- Keep the GPU fed when the primary task queue runs dry
- Prioritize urgent tasks over background work
- Access the loaded model for interactive use during its walltime

The cluster has BeeGFS shared filesystem, and the task queue is already SQLite. The filesystem can serve as a live message bus between the local machine and the compute node — no network tunnel needed.

## Solution

Modify the batch job to stay alive in an idle-poll loop after the initial task queue empties, continuously checking for new tasks via the shared `tasks.db`. Add priority-based task ordering so high-urgency work always preempts fill work. Add auto-generation of fill tasks when a job starts, providing a reservoir of low-priority work that absorbs idle GPU capacity.

The architectural principle: **tasks.db is the message bus.** The local machine writes tasks; the live vLLM session polls and processes them. No SSH tunnel, no Redis, no custom daemon — just BeeGFS file I/O.

## User Stories

1. As a researcher, I want a running vLLM session to automatically poll for new tasks every 60 seconds, so that I can submit analysis work and get results back within minutes instead of waiting days for a new SLURM slot
2. As a researcher, I want the batch job to stay alive for a configurable idle timeout so that it catches late-arriving tasks, maximizing GPU utilization
3. As a system operator, I want the idle timeout to reset on any new task activity, so that the job naturally stays alive as long as work keeps arriving
4. As a system operator, I want the idle timeout to be configurable per-job, so that long-walltime jobs can wait longer for new tasks than short ones
5. As a user, I want tasks to be processed in strict priority order (higher = sooner), so that urgent analysis is never blocked by background fill work
6. As a user, I want to submit a task while a SLURM job is running and have it picked up within the next poll cycle, so that I get near-interactive response times from a live vLLM instance
7. As a pipeline operator, I want fill tasks at priority -1 to be auto-generated when a SLURM job starts, so that idle GPU cycles are productively consumed
8. As a pipeline operator, I want fill tasks to be configurable (source, query, model, max count), so that I control what background work is done
9. As a user, I want my high-priority tasks to immediately supersede any running fill work, so that fill tasks never delay real work
10. As a system operator, I want fill tasks tagged with a distinct type (`fill`), so that I can easily identify, count, or bulk-delete them
11. As a user, I want the existing `hpc_batch.py submit` to work unchanged with a live vLLM session — submit a task, poll, see the result appear — so that there is no new CLI to learn
12. As a system operator, I want the idle-poll loop to produce clear logs on the compute node, so that I can observe when the job is busy, idle, or timing out
13. As a researcher, I want the fetch proxy (from the hybrid infrastructure PRD) to be alive when fill tasks trigger internet calls, so that background paper analysis can fetch external context

## Implementation Decisions

### 1. Idle-Poll Loop in the Batch Job

The core change is in the DS V4 Flash batch job (analogous to the current `dsv4_batch.slurm`). After the initial batch of tasks is exhausted, the job enters a poll loop:

```
while idle_seconds < IDLE_TIMEOUT:
    claim batch of pending tasks (ORDER BY priority DESC)
    if tasks claimed:
        process them via vLLM
        idle_seconds = 0
    else:
        sleep POLL_INTERVAL (60s)
        idle_seconds += POLL_INTERVAL
    rsync tasks.db to results directory periodically
```

Configuration (from environment variables or task metadata):
- `IDLE_TIMEOUT`: max seconds to stay alive with no work (default 600 = 10 min)
- `POLL_INTERVAL`: seconds between queue checks (default 60)
- `BATCH_SIZE`: tasks to claim per cycle (existing, default 5)

**Session reuse insight**: The vLLM server is already running on `localhost:8000`. It's stateless (no session, just prompt → response). The poll loop just submits new requests to the same server. No vLLM restart needed.

### 2. Priority-Ordered Task Claiming

The existing task claiming SQL uses `ORDER BY priority DESC, created_at ASC` — but the inline claim logic in the batch job doesn't always respect this (it claims *all* pending tasks at once, ignoring priority order).

Change: the idle-poll batch job claims tasks using a priority-ordered LIMIT query:

```
SELECT id, messages, model, metadata
FROM tasks
WHERE status = 'pending'
  AND NOT EXISTS (SELECT 1 FROM task_deps WHERE ...)  -- DAG deps
ORDER BY priority DESC, created_at ASC
LIMIT BATCH_SIZE
```

The `dsv4_batch.slurm` already has `BEGIN IMMEDIATE` + atomic claim logic. This just adds `ORDER BY priority DESC`.

Priority scale:
- 10: User urgent
- 5: Normal pipeline analysis
- 0: Default (existing behavior, unchanged)
- -1: Fill tasks (background, no urgency)
- -3: Maintenance/re-runs

### 3. Fill Task Auto-Generation

When `async_queue_monitor.py` detects that a SLURM job has started (or emits a "job_started" metric), it generates fill tasks:

```
config.yaml:
  fill_tasks:
    enabled: true
    max_per_job: 50  # cap fill task generation
    generators:
      - collector: arxiv
        query: "cat:cs.AI"
        max: 10
        type: paper_analysis
        tags: ["fill", "background"]
        priority: -1
```

Each generator runs its collector, converts results to tasks at priority -1, and tags them with `type: fill`. The `type: fill` tag allows easy bulk cleanup: `hpc_batch.py archive --type fill --older-than 7`.

Fill tasks are only generated when a job is starting (not when queue is idle), preventing queue bloat.

### 4. No Schema Changes

The existing `tasks` table already has a `priority` INTEGER column (default 0). No migration needed. The `metadata` JSON column already supports `type` and `tags` for identifying fill tasks.

### 5. Fill Task Preemption Guard

SLURM preempts jobs on the `preemptqos` QoS. When a job running fill tasks is preempted:

- The fill tasks that were `running` have consumed 0 tokens (killed before completion)
- The existing preemption guard (`cascade_failure` in `hpc_batch.py`) checks `total_tokens > 0` before cascading failure to dependents
- Fill tasks have no dependents (they are independent analysis), so cascade is irrelevant
- But fill tasks must be **retried** after preemption, not left in `failed` state

Solution: the batch job's cleanup handler detects preemption (via `SLURM_JOB_QOS` preemption signal or exit code) and resets any `running` fill tasks back to `pending` with `retry_count` incremented. Fill tasks have `max_attempts` set higher (default 5 vs 3 for user tasks) to survive multiple preemptions.

Implementation in the batch job's `cleanup` trap:

```python
# On preemption signal, reset running fill tasks to pending
if os.environ.get("SLURM_JOB_QOS") == "preemptqos":
    conn.execute("""
        UPDATE tasks SET status = 'pending', retry_at = NULL,
        attempt_count = COALESCE(attempt_count, 0) + 1
        WHERE status = 'running'
        AND json_extract(metadata, '$.type') = 'fill'
        AND COALESCE(attempt_count, 0) < 5
    """)
    conn.commit()
```

Fill tasks that exceed `max_attempts` (5) are left as `failed` — they are genuinely unrunnable.

### 6. Fetch Proxy Integration (Pairing with fill tasks)

Fill tasks that need internet calls (arXiv paper analysis, web search) should work seamlessly with the existing fetch proxy. The proxy is a local process; as long as it's running on the local machine, fill tasks on the compute node can write `fetch-requests/` to BeeGFS and poll for responses.

This is *not* a dependency — fill tasks that don't need internet (e.g., re-running failed tasks, embedding batch) work without any proxy.

## Testing Decisions

### What makes a good test

- Test external behavior: tasks with known priorities are claimed in the right order; idle timeout fires correctly; fill tasks appear after job start
- Do not test the vLLM call itself — that path is already tested by the existing batch job
- Do not test SSH/rsync — those are OS-level operations, not our logic

### Test seams (highest to lowest)

1. **Priority claim SQL unit** — Submit tasks with priorities [-1, 0, 5, 10]; run the claim query; verify high-priority tasks are claimed first. This is a pure SQL test, no batch job needed.
2. **Idle-poll loop integration** — Start the batch processing loop against a temp SQLite DB with no tasks; verify it sleeps and re-checks; inject a task mid-loop; verify it's claimed within POLL_INTERVAL.
3. **Fill task generation unit** — Mock a "job started" event; call the fill generator; verify fill tasks appear in the DB with priority -1 and type "fill".
4. **Fill vs real priority integration** — Queue 5 fill tasks (-1) and 1 urgent task (10); run the claim loop; verify the urgent task is claimed before any fill task.
5. **Idle timeout integration** — Start the loop with empty queue, set IDLE_TIMEOUT=0; verify the loop exits immediately. Set IDLE_TIMEOUT=120; inject one task at t=60; verify the loop resets idle timer.

### Prior art

The existing `local_process.py` integration test (which does claim → Ollama → write result against a real SQLite DB) is the template. The idle-poll tests follow the same pattern: connect to SQLite, simulate work, verify state transitions.

Test utilities in the `tests/` directory use temp DB files with `sqlite3.connect(":memory:")` or tempfile paths. The poll-loop test should use a real file path so the loop can connect from a different cursor.

## Out of Scope

- **Real-time streaming (<1s latency)** — The 60s poll interval is acceptable for research work but not interactive chat. Poll interval can be tuned down to 10s if needed, but filesystem sync latency (~2s) is the floor.
- **Preempting a running task** — Once a task is being processed by vLLM, it finishes. Only new claims respect priority ordering.
- **GPU preemption** — This is about keeping the *loaded model* fed, not about time-sharing GPUs between jobs. SLURM still allocates the full GPU.
- **Multi-job coordination** — No load balancing across multiple concurrent vLLM sessions. Each job polls independently.
- **Redis or real-time message queue** — The filesystem-based approach is deliberately primitive. Redis was proven blocked by network isolation.
- **Job health alerting** — No push notification (email, Slack, etc.) when a batch job crashes silently. User discovers failures by running `hpc_batch.py collect` and finding nothing. A polling-based alert can be built externally (cron job monitoring `squeue` + `tasks.db` staleness), but is not part of this PRD.
- **vLLM model load optimization** — The 30-60 minute model load is the largest single bottleneck. Potential mitigations (persistent vLLM pool, checkpoint caching, DeepSeek API fallback) require cluster-level changes or external services and are not addressed here.

## Further Notes

- The idle-poll loop makes every SLURM job serve double duty: its primary task batch plus a live service period. For a 12h walltime job that finishes its batch in 2h, this captures **10 extra GPU-hours** of productive work.
- The fill task reservoir should be large enough that a job never runs out of work, but not so large that it floods the queue. The recommended cap is 50-100 fill tasks per job generation.
- Priority -1 vs 0 vs 10 means fill tasks are invisible to users — they only see their own high-priority tasks getting fast responses. The fill work silently absorbs idle cycles.
- The fetch proxy should be started *before* dispatching the SLURM job if fill tasks need internet. If no proxy is running, fill tasks that call `fetch_url` will time out gracefully (per the proxy's graceful degradation design).
- This feature pairs naturally with the `hpc_batch.py watch` command — submit a task, watch it get picked up and completed by the live vLLM session in real-time.
- vLLM model load (30-60 min) is the dominant bottleneck. The idle-poll loop works *around* it by extending session lifetime, but doesn't reduce the load time itself. If load time exceeds 60 min, consider: (a) checkpoint caching on BeeGFS to avoid re-download, (b) keeping a persistent low-priority keepalive job that holds the model loaded, (c) using the DeepSeek API as a fallback for latency-sensitive tasks.

### Known Gaps

| Gap | Status | Notes |
|---|---|---|
| Fill task preemption handling | Covered (Section 5) | Running fill tasks are reset to pending on preemption |
| Multi-job coordination | Out of scope | Each SLURM job polls independently; no load balancing |
| Job health alerting | Out of scope | Can be built externally via cron + `squeue` + `tasks.db` staleness |
| vLLM model load optimization | Out of scope | Requires cluster-level changes or external API fallback |
| Real-time <1s latency | Out of scope | Filesystem sync floor is ~2s
