Status: `ready-for-agent`

# Test suite for HPC opencode provider

## What to build

Automated tests for the provider, reusing existing patterns from `tests/`.

### Test 1: Provider contract

- Create provider with mock config
- Call `chat_complete([{"role": "user", "content": "test"}])`
- Verify returned dict has `content` key
- Verify no `tool_calls` in response (HPC is text-only)
- Verify no exceptions raised

### Test 2: Task lifecycle (local_process backend)

- Submit task via provider
- Process via `local_process.py` (Ollama backend or mock)
- Poll and verify result returned correctly with `content` field

### Test 3: Warmth detection

- Mock the BeeGFS heartbeat file (`proxy-heartbeat.json`) with different timestamps
- Recent heartbeat (< 60s old) → `"hot"`
- Stale heartbeat (> 60s old) → `"warm"`
- No heartbeat file → `"cold"`
- Verify `_check_warmth()` returns correct state for each case

### Test 4: Cold fallback

- Configure a fallback API provider
- Mock warmth to return `"cold"`
- Call `chat_complete()`
- Verify it delegates to the fallback provider (by checking that a non-HPC path was taken)

### Test 5: Timeout

- Set timeout to 1s
- Submit a task but don't process it
- Verify timeout error returned gracefully with `error` key

### Prior art

Tests follow the same pattern as `tests/test_idle_poll.py` and `tests/test_fill_tasks.py` — temp SQLite DB, connection management, state verification. The existing `hpc_provider.py` (95 lines) demonstrates the submit → poll → return pattern that the new provider extends.

## Acceptance criteria

- [ ] All 5 test scenarios pass
- [ ] Tests don't require actual HPC access (mocked where needed)
- [ ] Tests complete in <10 seconds
- [ ] Provider contract test validates exact API surface expected by opencode

## Blocked by

- Issue #01 (provider core) — tests need the provider class to exist
