# PRD: HPC LLM Provider for Opencode Subagents

Status: `ready-for-agent`

## Problem Statement

Opencode uses subagents (explorer, fixer, oracle, librarian, designer) that make LLM calls via subscription APIs (Anthropic, OpenAI). These calls are fast (1-10s) but incur per-token costs and rate limits. The project has access to free HPC GPU capacity with DeepSeek V4 Flash on H100s via SLURM batch jobs.

Currently, the HPC can only be used via `hpc_batch.py submit` which requires manual `submit → dispatch → collect` workflow. There is no way for opencode subagents to use HPC vLLM as their LLM backend without changing their code or workflow.

The HPC compute nodes are network-isolated, but the BeeGFS filesystem + idle-poll loop + fetch proxy provide a bidirectional communication bridge. The subagent stays local (has full filesystem/tool access) but routes its LLM calls to HPC via the file-based task queue.

The latency per round trip (60-120s warm, days cold) means this is suitable for batch research tasks, not interactive subagent conversations. But the idle-poll loop makes it viable when a vLLM session is already loaded.

## Solution

Create an opencode LLM provider that routes LLM calls through the existing `tasks.db` + BeeGFS file exchange. The subagent runs locally with full tool access. Only the `messages → response` call goes to HPC.

The provider follows the opencode provider interface:
- Input: `messages`, `config`
- Output: `{"content": "..."}`

The HPC handles **text generation only**. All tool execution stays local — the subagent's own orchestration loop (bash, grep, read_file, fetch_url, etc.) runs on the local machine at full speed. The HPC never sees tool definitions, never calls tools, and never enters an agent loop. This avoids tool schema mismatch, eliminates extra BeeGFS round trips for tool results, and keeps interactive tool use responsive.

It writes the messages as a task to `tasks.db`, rsyncs to HPC (if not already on shared filesystem), polls for completion via `hpc_batch.py submit-and-wait`, and returns the response.

A companion `warmth` check detects whether a live vLLM session is available (via BeeGFS heartbeat or `squeue`) and either uses the fast path (idle-poll, ~60s) or falls back to batch dispatch (1-4 day queue wait).

## User Stories

1. As a user, I want opencode subagents to use HPC vLLM as their LLM backend, so that I can avoid per-token API costs for heavy analysis work
2. As a user, I want the provider to work with any opencode subagent without code changes, so that explorer, fixer, oracle, and other subagents all benefit
3. As a user, I want the subagent to stay local with full filesystem/tool access, so that file operations, grep, glob, and internet research continue to work
4. As a user, I want the provider to automatically detect whether a live vLLM session is running, so that it uses the fast idle-poll path when available and falls back to batch dispatch when cold
5. As a user, I want to configure which subagents use HPC vs API via opencode.json, so that I can route interactive work to fast APIs and batch work to HPC
6. As a user, I want the provider to handle tool calls from the model (fetch_url, python_repl, bash), so that agentic research loops work on HPC
7. As a user, I want the provider to return errors gracefully (timeout, DB unavailable, model not responding), so that the subagent doesn't hang or crash
8. As a user, I want the provider to stream partial results when available (via periodic `hpc_batch.py watch`-style polling), so that I can see progress on long-running tasks
9. As a user, I want to submit batch agent tasks ("analyze all 20 files in this directory") that get processed by HPC in one job, so that the per-task overhead is amortized across the batch
10. As a user, I want the provider to support the same `model` parameter as other providers, so that I can switch between HPC models and API models without changing my workflow

## Implementation Decisions

### 1. Provider Architecture

A Python module that implements the opencode provider interface. The existing `hpc_provider.py` (used for promptfoo) is the template — it already does `submit → poll → return`. This provider extends it with:

- Support for `messages` array (not just a single prompt string)
- Response parsing for standard `content` output (no tool calls — handled locally)
- Warmth detection (check for live vLLM session via BeeGFS heartbeat file)
- Configurable routing (per-subagent or per-task-type)

The provider is installed as a custom opencode provider and configured in `opencode.json`.

### 2. Provider Interface

```python
class HpcProvider:
    def __init__(self, config: dict):
        self.db_path = config.get("db_path", "tasks.db")
        self.model = config.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        self.poll_interval = config.get("poll_interval", 30)
        self.timeout = config.get("timeout", 86400)
        self.fallback_provider = config.get("fallback")  # optional API provider

    def chat_complete(self, messages: list, **kwargs) -> dict:
        """Send messages to HPC vLLM and return response.
        
        Returns: {"content": str, ...}
        Tool calls are NOT returned — the subagent handles all
        tool execution locally. HPC is text generation only.
        """
        ...
```

### 3. Warmth Detection

Before submitting, check if a live HPC session can serve this request quickly. Use the BeeGFS proxy heartbeat file (same mechanism the idle-poll loop uses) rather than `squeue`, because a SLURM job in model-load phase (45 min) registers in `squeue` but won't respond for nearly an hour:

```python
def _check_warmth(self) -> str:
    """Return 'hot', 'warm', or 'cold' based on HPC state.
    
    hot:   Live vLLM session with idle-poll heartbeat present.
           Response in ~60s.
    warm:  SLURM job is running (squeue) but no heartbeat yet.
           Model may be loading (45+ min wait).
    cold:  No active job. Must submit + wait in queue (1-4 days).
    """
    # Check BeeGFS heartbeat file first
    # If heartbeat < 60s old → hot (vLLM is loaded and polling)
    # Then check squeue as fallback → warm
    # Otherwise → cold
```

The heartbeat was already implemented as part of the fetch proxy (`proxy-heartbeat.json`). The idle-poll loop in `dsv4_batch.slurm` writes it every 10 seconds. The provider reads the same file. If the heartbeat is stale or missing, the provider knows no live session is available.

When `cold`, the provider can submit a new batch job (slow path) or delegate to a fallback API provider (fast path). This is configurable via `warmth_policy`.

### 4. Task Submission

Uses the existing `submit_task()` function from `hpc_batch.py` — no new infrastructure. The messages array is stored in the `messages` TEXT column (same schema). The `tools` column remains empty — the HPC does not handle tool calls.

The `metadata.type` is set to `agent_call` to distinguish provider calls from user-submitted tasks.

### 5. Response Polling

Polls `tasks.db` for completion at `poll_interval`. Uses the same `submit-and-wait` pattern as `hpc_provider.py`. On completion, parses the `result` JSON and returns `{"content", "tool_calls", "usage"}`.

### 6. No Tool Call Handling (by Design)

The HPC is text generation only. The provider never sends tool definitions, never parses tool calls, and never returns tools in the response. All tool execution is handled by the subagent on the local machine:

1. Subagent sends `messages` to HPC via provider
2. HPC vLLM generates text response (no tools involved)
3. Provider returns `{"content": "..."}` to subagent
4. Subagent decides what to do next — read files, run commands, make API calls
5. If the subagent needs another LLM call, it sends new messages to HPC again

This avoids: tool schema mismatch, extra BeeGFS round trips for tool results, agent loop complexity on the HPC, and mid-conversation stalls.

### 7. Configuration

```json
// opencode.json (user's project)
{
  "providers": {
    "hpc": {
      "type": "hpc_agent",
      "config": {
        "db_path": "~/hpc-agent/tasks.db",
        "model": "deepseek-ai/DeepSeek-V4-Flash",
        "poll_interval": 30,
        "timeout": 86400,
        "fallback": {
          "type": "anthropic",
          "model": "claude-sonnet-4"
        },
        "warmth_policy": "prefer_hpc"  // "prefer_hpc" | "fallback_on_cold" | "hpc_only"
      }
    }
  },
  "subagents": {
    "oracle": { "provider": "hpc" },
    "explorer": { "provider": "anthropic" },
    "fixer": { "provider": "anthropic" },
    "librarian": { "provider": "hpc", "fallback": "anthropic" }
  }
}
```

### 8. Integration Points

The provider reuses these existing components:

| Component | Use | Status |
|---|---|---|
| `hpc_batch.py submit_task()` | Write task to SQLite | ✅ Existing |
| `hpc_batch.py submit-and-wait` | Poll for completion | ✅ Existing |
| `infra/sqlite_queue.py` | Task queue operations | ✅ Existing |
| `slurm/dsv4_batch.slurm` | vLLM processing (idle-poll mode) | ✅ Existing |
| `config.yaml` | HPC connection settings + SSH config | ✅ Existing |
| `proxy-heartbeat.json` | Warmth detection (BeeGFS heartbeat file) | ✅ Existing |

### 9. Credential & Config Loading

The provider needs:
- HPC SSH config (host, user, dir) — read from `config.yaml` via `_get_hpc_config()`
- Optional fallback API key — read from environment variable (same pattern as collector `token_env`)
- DB path — from `opencode.json` provider config, defaulting to project `tasks.db`

The provider does NOT use `opencode.json` for HPC connection settings. Those stay in `config.yaml` (the single source of truth for HPC config). The `opencode.json` only specifies which provider to use and model name.

## Testing Decisions

### What makes a good test

- Test the provider contract: given `messages`, return `{"content": "..."}`
- Test with a real SQLite DB and `local_process.py` as the backend (avoids needing SLURM)
- Do NOT test actual HPC submission in automated tests — that's a manual integration test
- Do test error paths: timeout, DB locked, malformed response, model not found

### Test seams (highest to lowest)

1. **Provider contract unit** — Call `chat_complete()` with a simple messages array. Mock the DB operations. Verify output shape matches `{"content": str}`.
2. **Task lifecycle integration** — Write a task via `submit_task()`, process it via `local_process.py`, poll for result via the provider. Tests the full submit → process → poll → return cycle without HPC.
3. **Warmth detection unit** — Mock the BeeGFS heartbeat file (present vs stale vs missing). Verify the provider correctly categorizes state as hot/warm/cold.
4. **Fallback routing** — Configure a fallback API provider. Call `chat_complete()` when HPC is cold. Verify it delegates to the fallback.
5. **Timeout integration** — Submit a task, set a short timeout, verify the provider returns a timeout error gracefully.

### Prior art

The existing `hpc_provider.py` (95 lines) tests the submit → poll → return pattern for promptfoo. The new provider extends this same pattern for messages arrays. The existing test suite (40 tests) uses temp SQLite DBs for task lifecycle tests — the new tests follow the same pattern.

## Out of Scope

- **Latency improvement** — The 60-120s per round trip on warm HPC is inherent to the BeeGFS file exchange. Reducing it would require real-time connectivity to compute nodes, which is blocked by network isolation. Latency will be evaluated after real-world usage.
- **Streaming responses** — vLLM supports streaming, but the BeeGFS file exchange doesn't. Streaming via file-based polling would add unacceptable complexity for marginal benefit.
- **Multi-model routing** — The provider uses one model per instance. Dynamic model selection based on task type is a future enhancement.
- **Subagent code modification** — Subagents run unchanged. Only the LLM provider changes.
- **Subagent execution on HPC** — The subagent stays local with full tool access. HPC only handles the LLM call.
- **Tool calls on HPC** — All tool execution stays local. HPC is text generation only.

## Known Gaps

These gaps are acknowledged but deferred or addressed by design decisions above:

| Gap | Status | Notes |
|---|---|---|
| **Mid-conversation warmth loss** — Idle-poll timeout expires mid-conversation (10 min with no tasks), next LLM call fails silently | Deferred | The fallback provider handles this: if HPC is cold, delegate to API. The subagent never hangs — it either gets an HPC response or a fallback response. No mid-conversation switch is implemented (once cold, stays cold for that conversation). |
| **Warmth detection is approximate** — Heartbeat tells you vLLM is running but not whether it's responsive (could be stuck on a long task) | Deferred | False "hot" positives cause a timeout (~60s for the poll loop to exhaust) then fallback. This is acceptable — the penalty is one extra minute of latency, not a hang. |
| **Parallel subagent calls** — Orchestrator spawns 3 subagents in parallel, each submits a separate HPC task. No batching — 3 separate BeeGFS round trips | Deferred | Batching concurrent submissions into one DAG is a future optimization. At current scale, multiple concurrent round trips are acceptable. The idle-poll loop processes them sequentially as they arrive. |
| **Error recovery in agentic loops** — If one LLM call times out mid-conversation, the provider returns `{"error": "..."}`. The subagent may or may not handle this gracefully | Deferred | The provider does not implement retry at the call level. If a call times out, it returns an error to the subagent. Subagent resilience is an opencode concern, not an HPC provider concern. |
| **Provider interface discovery** — The PRD assumes opencode has a documented provider interface. If opencode's interface differs from `chat_complete(messages) → dict`, the design changes | Needs verification | Must verify the actual opencode provider contract before implementation. The PRD assumes a reasonable interface based on common LLM provider patterns. |

## Further Notes

- The provider is opt-in per subagent. Interactive subagents (explorer, fixer) should keep using fast APIs. Heavy analysis subagents (oracle, librarian) benefit most from HPC.
- The largest productivity gain is **batch submission**: "oracle, analyze all 20 files in src/" submits 20 HPC tasks that all get processed in one SLURM job with one model load. The per-task overhead (30-60 min model load) is paid once for the entire batch.
- When a vLLM session is warm (idle-poll mode), the provider can serve requests in ~60s — comparable to "ask a question, get coffee, read the answer." This is acceptable for non-interactive research workflows.
- The fallback provider ensures the system degrades gracefully. When the HPC queue is cold (1-4 day wait), the provider can automatically route to a paid API provider, and switch back to HPC when a job comes online.
- The existing `hpc_batch.py submit-and-wait` already implements the submit → poll → return pattern. The new provider is a thin wrapper that adds message parsing and warmth detection around this existing function.
