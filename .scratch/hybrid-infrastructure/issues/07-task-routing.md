Status: `ready-for-agent`

# Task routing — mixed local/HPC dispatch

## What to build

A routing layer that decides whether each task runs locally (lightweight, internet-capable) or on HPC (heavy, air-gapped), enabling mixed workloads without manual intervention.

### TaskRouter (`infra/routing.py`)

A class that deterministically routes tasks:

```python
class TaskRouter:
    LOCAL_MODELS = {"qwen2.5:0.5b", "llama3.2:1b", "tinyllama:1.1b"}
    HPC_MODELS = {"deepseek-ai/DeepSeek-V4-Flash"}
    
    @staticmethod
    def route(task: dict) -> str:
        """Return 'local' or 'hpc' based on rules."""
        ...
```

Routing rules (in priority order):
1. **Explicit override** — if `metadata.route` is set (`"local"` or `"hpc"`), use it
2. **Model name** — known local models → local; known HPC models → HPC
3. **Task type** — `summarize`, `extract_keywords`, `classify` → local; `paper_analysis`, `code_review`, `comparison` → HPC
4. **Context size heuristic** — if prompt < 2000 tokens → local; otherwise → HPC
5. **Default** → HPC (safe fallback for heavy models)

### Integration with async_queue_monitor

`async_queue_monitor.py` gets a `--router` flag:
- `--router hpc` — only dispatches HPC-routed tasks to SLURM
- `--router local` — only dispatches local-routed tasks (for testing; normally local is handled by local_process.py)
- Without flag — submits all tasks (current behavior, backward compatible)

Claim logic: when claiming tasks for dispatch, filter by routing rule:

```sql
WHERE status = 'pending'
  AND (
    CASE WHEN ? = 'hpc' THEN json_extract(metadata, '$.route') IN (NULL, 'hpc', '')
    ELSE json_extract(metadata, '$.route') = 'hpc'
  )
```

### Integration with local_process.py

`local_process.py` gets a `--router local` flag:
- Only claims and processes tasks routed to `local`
- Routes follow the same TaskRouter logic
- Can run alongside `async_queue_monitor.py --router hpc` in parallel

### Config

```yaml
routing:
  default: "hpc"
  local_models: ["qwen2.5:0.5b", "llama3.2:1b", "tinyllama:1.1b"]
  local_types: ["summarize", "extract_keywords", "classify"]
  hpc_types: ["paper_analysis", "code_review", "comparison", "repo_analysis"]
  context_threshold: 2000  # tokens, below this = candidate for local
```

### CLI

- `hpc_batch.py route <task-id>` — show routing decision for a specific task
- `hpc_batch.py route --pending` — show routing breakdown of all pending tasks (X local, Y HPC)

## Acceptance criteria

- [ ] `TaskRouter.route()` returns correct destination for all rule combinations
- [ ] Unit tests cover: model-based, type-based, context-size, and explicit-override routing
- [ ] `metadata.route` override is respected when set
- [ ] `async_queue_monitor.py --router hpc` only dispatches HPC tasks
- [ ] `local_process.py --router local` only processes local tasks
- [ ] Without `--router` flag, both monitors behave as before (backward compatible)
- [ ] `hpc_batch.py route <id>` shows routing decision with reason

## Blocked by

- Issue #1 (Collector framework + arXiv) — needs tasks to exist in the queue for routing decisions to matter
