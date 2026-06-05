Status: `ready-for-agent`

# Priority-ordered task claiming

## What to build

Ensure all task claiming in the batch job uses `ORDER BY priority DESC, created_at ASC` so that high-priority tasks are always claimed before low-priority ones.

The existing batch job claims "all pending tasks" without priority ordering. Change to claim in priority order:

```sql
SELECT id, messages, model, metadata
FROM tasks
WHERE status = 'pending'
  AND NOT EXISTS (SELECT 1 FROM task_deps WHERE task_id = tasks.id AND depends_on NOT IN (SELECT id FROM tasks WHERE status = 'completed'))
ORDER BY priority DESC, created_at ASC
LIMIT BATCH_SIZE
```

This is a small SQL change — the existing `BEGIN IMMEDIATE` + atomic claim pattern remains unchanged.

Also update `local_process.py` dequeue SQL to use the same priority ordering (currently `ORDER BY created_at ASC`).

## Acceptance criteria

- [ ] Batch job claims tasks in priority order (higher priority first)
- [ ] `local_process.py` also respects priority ordering
- [ ] Tasks with equal priority are ordered by created_at (oldest first)
- [ ] DAG dependencies are still respected (blocked tasks not claimed)
- [ ] Existing task claiming behavior unchanged for tasks with default priority (0)

## Blocked by

- Issue #1 (Idle-poll loop) — the poll loop is where priority ordering matters most
