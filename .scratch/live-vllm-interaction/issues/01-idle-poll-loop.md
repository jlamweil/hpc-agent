Status: `ready-for-agent`

# Idle-poll loop in the batch job

## What to build

Modify the DS V4 Flash batch job so that after the initial task batch is exhausted, the job enters a poll loop instead of exiting.

The loop:
1. Claims up to `BATCH_SIZE` pending tasks (ordered by priority)
2. If tasks found: processes them via vLLM, resets idle timer, loop
3. If no tasks: sleeps `POLL_INTERVAL` seconds, increments idle timer
4. If idle timer >= `IDLE_TIMEOUT`: exits

Environment variables for configuration:
- `IDLE_TIMEOUT` — max seconds idle before exit (default 600 = 10 min)
- `POLL_INTERVAL` — seconds between queue checks (default 60)
- `BATCH_SIZE` — tasks per claim cycle (existing, default 5)

The vLLM server (port 8000) is already running — the poll loop just makes new API calls to it. No vLLM restart.

Results must be written back to `tasks.db` and periodically synced to the results directory (existing pattern).

## Acceptance criteria

- [ ] Batch job polls for new tasks every POLL_INTERVAL when queue is empty
- [ ] New tasks submitted while polling are picked up within POLL_INTERVAL + rsync latency
- [ ] Idle timer resets to 0 when any task is claimed
- [ ] Job exits when idle timer reaches IDLE_TIMEOUT
- [ ] All existing batch job behavior preserved (task claiming, vLLM call, result writing, archive on exit)
- [ ] Clear log messages: "Idle Ns, checking for new tasks...", "Idle timeout reached, exiting"
- [ ] Configuration via env vars with sensible defaults

## Blocked by

None — can start immediately
