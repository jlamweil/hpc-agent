Status: `ready-for-agent`

# Fill task preemption guard

## What to build

When SLURM preempts a job running fill tasks, those tasks are killed mid-flight with 0 tokens consumed. They must be retried, not left in `failed` state.

Add to the batch job's cleanup handler: on preemption signal (detected via `SLURM_JOB_QOS` or exit code), reset any `running` fill tasks back to `pending` with incremented attempt count.

Fill tasks have `max_attempts = 5` (vs 3 for user tasks) to survive multiple preemptions.

## Acceptance criteria

- [ ] On job exit, running fill tasks (status='running', type='fill') are reset to 'pending'
- [ ] attempt_count is incremented on each reset
- [ ] Fill tasks exceeding 5 attempts are left as 'failed'
- [ ] User tasks (type != 'fill') are not affected by this logic

## Blocked by

- Issues #01-03 (idle-poll, priority, fill generation) — needs the fill task infrastructure
