Status: `ready-for-agent`

# opencode integration — wiring the provider

## What to build

Wire the HPC provider into opencode's provider system so it can be used in `opencode.json`.

### Provider registration

Register the provider so opencode can discover it. The simplest approach is a custom provider config in `opencode.json` — no plugin system required.

### opencode.json schema

```json
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
          "api_key_env": "ANTHROPIC_API_KEY",
          "model": "claude-sonnet-4"
        },
        "warmth_policy": "prefer_hpc"
      }
    }
  }
}
```

HPC connection settings (host, user, dir) are read from `config.yaml`, not `opencode.json`.

### Per-subagent routing

Document how users configure which subagents use HPC vs API:

```json
{
  "subagents": {
    "oracle": { "provider": "hpc" },
    "explorer": { "provider": "anthropic" },
    "fixer": { "provider": "anthropic" },
    "librarian": { "provider": "hpc", "fallback": "anthropic" }
  }
}
```

### Example workflows

- `opencode "summarize the architecture of this project"` → explorer on API (fast, interactive)
- `opencode "oracle, analyze our dependency tree and suggest optimizations"` → oracle on HPC (free, non-interactive)
- `opencode "librarian, research vector databases and compare to our current approach"` → librarian on HPC with API fallback when cold

### Credential loading

- Fallback API keys: read from environment variable named in `fallback.api_key_env`
- HPC SSH config: read from `config.yaml` (reuses existing `_get_hpc_config()`)
- No secrets stored in `opencode.json`

## Acceptance criteria

- [ ] Provider is usable from `opencode.json`
- [ ] Per-subagent routing works (oracle→HPC, explorer→API)
- [ ] Fallback provider works when HPC is cold
- [ ] HPC config loaded from `config.yaml`, not `opencode.json`
- [ ] Example configs and workflows are documented

## Blocked by

- Issue #01 (provider core) — the provider class must exist to be referenced from opencode.json
