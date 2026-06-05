Status: `ready-for-agent`

# HPC opencode provider — core implementation

## What to build

Create a Python module that implements the opencode LLM provider interface, routing messages through the HPC task queue. HPC is text generation only — all tool execution stays local.

### Provider class

`HpcProvider` with:
- `__init__(config)` — reads db_path, model, poll_interval, timeout, fallback, warmth_policy from `opencode.json` provider config; reads HPC SSH config from `config.yaml`
- `chat_complete(messages, **kwargs) → dict` — submits messages to HPC, polls for completion, returns `{"content": "..."}`
- No `tools` parameter, no `tool_calls` in response — HPC is text only

### Task submission

Uses `hpc_batch.py submit_task()` to write to SQLite. Messages array stored in the `messages` TEXT column. `tools` column stays empty. Sets `metadata.type = "agent_call"`. No tool definitions are sent to HPC.

### Response polling

Polls `tasks.db` at `poll_interval`. Uses the same submit → poll → return pattern as the existing `hpc_provider.py`. Parses the `result` JSON and returns `{"content": ..., "usage": ...}`.

### Warmth detection

`_check_warmth()` checks the BeeGFS proxy heartbeat file (`proxy-heartbeat.json`):
- If heartbeat file exists and is < 60s old → `"hot"` (vLLM is loaded and idle-polling, response in ~60s)
- If a SLURM job exists (via `squeue`) but no recent heartbeat → `"warm"` (model may be loading, 45+ min)
- Otherwise → `"cold"` (must submit new batch job)

When `cold` and a fallback provider is configured → delegate to fallback. When `cold` and `warmth_policy=prefer_hpc` → submit a new batch job.

### Error handling

All errors return dict with `error` key — never throw. Timeout, DB unavailable, model not found, malformed response all handled gracefully.

### Configuration

Reads HPC connection settings from `config.yaml` (host, user, dir). Reads provider settings from `opencode.json` (model, poll_interval, timeout, fallback, warmth_policy). API keys for fallback provider read from environment variables.

## Acceptance criteria

- [ ] `chat_complete([{"role": "user", "content": "hi"}])` returns dict with `content` key
- [ ] No tools in input or output — HPC is text generation only
- [ ] Warmth detection correctly identifies hot/warm/cold via heartbeat file
- [ ] Cold fallback: delegates to API provider when no HPC job is running
- [ ] Timeout returns `{"error": "timeout", ...}` within timeout period
- [ ] All errors return dict, never raise exceptions
- [ ] HPC config loaded from `config.yaml`, provider config from `opencode.json`

## Blocked by

None — the existing `hpc_batch.py submit_task()` and `submit-and-wait` already exist; the idle-poll loop in `dsv4_batch.slurm` is already implemented
