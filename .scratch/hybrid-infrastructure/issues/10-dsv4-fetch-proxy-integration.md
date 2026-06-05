Status: `ready-for-agent`

# dsv4_batch.slurm — fetch proxy integration

## What to build

Wire the existing fetch proxy (Issue 06) into the DS V4 Flash batch job script. The proxy package exists at `proxy/` but the batch job never calls it.

Changes in `slurm/dsv4_batch.slurm`:

1. **Tool definition** — Add `fetch_url` to the vLLM API call's tool list so the model can request internet fetches
2. **Tool call handler** — After vLLM returns, check for `tool_calls`. If `fetch_url`, write the request to `fetch-requests/{task_id}.json` on BeeGFS, poll `fetch-responses/{task_id}.json` with backoff (up to 2 min), then re-call vLLM with the fetched context injected
3. **Graceful degradation** — If no proxy heartbeat file exists, skip all fetch attempts and complete without internet

## Acceptance criteria

- [ ] `fetch_url` tool is sent in the vLLM API payload
- [ ] When vLLM returns a `fetch_url` tool call, the batch job writes a request file to BeeGFS fetch-requests/
- [ ] Batch job polls for the response file with exponential backoff
- [ ] Fetched content is injected back into the vLLM conversation and re-submitted
- [ ] If proxy heartbeat is stale (>60s), tasks complete without fetch attempts (graceful degradation)
- [ ] No changes to the existing task processing pipeline for tasks that don't use fetch_url

## Blocked by

- Issue #06 (Fetch proxy) — the proxy server and handlers must exist (they do)
