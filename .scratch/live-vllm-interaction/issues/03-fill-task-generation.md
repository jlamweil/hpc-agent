Status: `ready-for-agent`

# Fill task auto-generation on job start

## What to build

When a SLURM job starts (or the monitor detects one running), auto-generate a reservoir of low-priority fill tasks using the existing collectors system. These tasks sit in the queue at priority -1 and are only processed when no higher-priority work exists.

### Fill task configuration (`config.yaml`)

```yaml
fill_tasks:
  enabled: true
  max_per_job: 50
  generators:
    - collector: arxiv
      query: "cat:cs.AI AND submittedDate:[20250101 TO 20250601]"
      max: 10
      tasks:
        - type: paper_analysis
          priority: -1
          tags: ["fill", "background"]
    - collector: arxiv
      query: "cat:cs.CL AND submittedDate:[20250101 TO 20250601]"
      max: 10
      tasks:
        - type: paper_analysis
          priority: -1
          tags: ["fill", "background"]
    - echo: true
      description: "Re-run failed tasks at low priority"
      priority: -2
```

### Integration with async_queue_monitor

When `async_queue_monitor.py` detects a job start (via a metric event or squeue state change), it:
1. Checks `fill_tasks.enabled` in config
2. For each generator: runs the collector with params
3. Submits each result as a task at the configured priority (default -1)
4. Tags all fill tasks with `type: fill` in metadata
5. Caps total fill tasks at `max_per_job`

### Fill task lifecycle

- Fill tasks are generated once per job start, not continuously
- They sit in the queue at priority -1 until idle cycles are available
- High-priority tasks (0+) are always claimed first
- Fill tasks can be cleaned up via `hpc_batch.py archive --type fill --older-than 7`
- Fill tasks are skipped if the fetch proxy is not running and they need internet (graceful degradation)

## Acceptance criteria

- [ ] Fill tasks are generated when a SLURM job starts
- [ ] Each fill task has `priority: -1` and `type: fill` in metadata
- [ ] Fill task count is capped by `max_per_job`
- [ ] Fill tasks never block higher-priority work (verified via test seam 4)
- [ ] Echo generator re-runs failed tasks at low priority
- [ ] Fill generation is skipped if `fill_tasks.enabled: false`

## Blocked by

- Issue #1 (Idle-poll loop) — fill tasks are only useful if the job stays alive to process them
- Issue #2 (Priority claiming) — fill tasks at priority -1 must not block tasks at priority 0+
- Depends on the collectors framework (Issue 01 from the hybrid infrastructure PRD)
