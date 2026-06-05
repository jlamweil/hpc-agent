# PRD: Hybrid Local + HPC Research Infrastructure

Status: `ready-for-agent`

## Problem Statement

The `hpc-agent` project has a working SLURM batch inference pipeline: submit tasks to a SQLite queue, rsync to the HPC login node, sbatch to GPU compute nodes, process with DeepSeek V4 Flash on H100s. But compute nodes are **network-isolated** — no internet access. This means the HPC can only do pure LLM inference on pre-packaged prompts. There is no way to:

- Collect research data (arXiv papers, GitHub repos, Kaggle competitions) as part of a workflow
- Have the LLM fetch additional context mid-analysis (a paper mentions a reference — fetch it)
- Mix lightweight local inference (quick summarization, classification) with heavy HPC analysis (full paper review, multi-perspective synthesis)
- Tag, track, and reproduce research workflows across sessions

The system needs a **hybrid architecture** where the local machine (internet-connected) handles data collection and preprocessing, while the HPC (air-gapped, H100 GPUs) handles heavy batch analysis. Both must work together seamlessly through the existing task queue.

## Solution

Build three new layers on top of the existing SQLite+SLURM pipeline:

1. **Collectors** — a `collectors/` package where each module knows how to fetch data from a specific source (arXiv, GitHub, Kaggle, web search, HuggingFace), structure it, and submit it as well-formed HPC analysis tasks
2. **Preprocessing pipeline** — chunking for large content, local summarization via Ollama, DAG wiring for multi-step analysis workflows
3. **File-based internet proxy** — a BeeGFS-backed request/response pattern that lets HPC compute jobs fetch URLs, search the web, or call APIs by writing to a shared filesystem, with a local process fulfilling the requests

The key architectural principle: **the HPC knows nothing about papers, code, or web research.** To the SLURM compute nodes, every task is just `messages` + `model`. All domain intelligence lives on the local machine.

## User Stories

1. As a researcher, I want to search arXiv and submit analysis tasks for the top results, so that I can batch-analyze multiple papers in one HPC job
2. As a researcher, I want to fetch a paper by arXiv ID and have it extracted from PDF to text, chunked by section, and submitted as a DAG of analysis tasks, so that long papers are analyzed section-by-section with a final synthesis
3. As a researcher, I want collectors to produce structured metadata (source, type, tags, provenance), so that I can query, filter, and reproduce research workflows
4. As a developer, I want to clone a GitHub repo locally, extract file tree and key files, and submit architecture review tasks to the HPC, so that the large model can analyze code structure
5. As a developer, I want code review tasks to include the repo's language, file count, and dependency structure in metadata, so that analysis prompts can be tailored to each project
6. As a Kaggle user, I want to fetch competition descriptions, dataset metadata, and leaderboard data, and submit analysis tasks to generate baseline scaffolding code, so that I can accelerate my competition entry
7. As a researcher, I want a local preprocessing step that chunks large content (50+ page papers, large repos) into manageable pieces and wires them into a DAG, so that analysis stays within token limits without manual splitting
8. As a user, I want to optionally run local summarization of each chunk via Ollama before submitting to HPC, so that HPC inference runs on focused prompts and uses less context
9. As a user, I want an HPC compute job to be able to fetch a URL by writing a request file to BeeGFS and polling for the response, so that the LLM can gather additional context mid-analysis despite network isolation
10. As a user, I want the internet proxy to handle multiple request types (HTTP GET, web search, GitHub API, arXiv fetch), so that HPC tasks have broad internet access through a single mechanism
11. As a user, I want API keys to never leave my local machine — the proxy handles auth on my behalf, so that credentials are not exposed on the HPC cluster
12. As a pipeline operator, I want the fetch proxy to be a long-running process that polls for requests, fulfills them within seconds, and writes responses back, so that HPC tasks experience minimal latency for internet calls
13. As a system integrator, I want the fetch proxy protocol to be filesystem-based (JSON request → JSON response via BeeGFS), so that it needs zero changes to HPC cluster configuration
14. As a user, I want to programmatically submit collection + analysis workflows via `hpc_batch.py collect <source>`, so that I don't need to write Python code for common operations
15. As a user, I want to run `hpc_batch.py preprocess paper --pdf path/to/paper.pdf --chunk 4000`, so that I can prepare ad-hoc research content without writing scripts
16. As a user, I want to run `hpc_batch.py preprocess repo --url https://github.com/user/repo`, so that I can quickly analyze any public repository
17. As a user, I want `hpc_batch.py pipeline run workflow.yaml` to execute a YAML-defined multi-step research workflow, so that common research patterns are reproducible and scriptable
18. As a pipeline author, I want YAML pipelines to compose collectors + queries + model selection + metadata tags, so that I can define standard analysis templates for my research area
19. As a user, I want the system to decide whether each task runs locally (lightweight, internet-capable) or on HPC (heavy, air-gapped), so that I don't have to manually route tasks
20. As a system operator, I want routing rules based on task type, model, and context size, so that routing decisions are predictable and configurable
21. As a user, I want to watch for completed results in near-real-time via `hpc_batch.py watch`, so that I can review partial results while a long HPC job is still running
22. As a user, I want metadata validation that warns me when required keys are missing at submission time, so that research workflows remain searchable and reproducible
23. As a system operator, I want the infrastructure to use the existing SQLite schema without modifications, so that the proven claim/retry/DAG infrastructure continues to work unchanged
24. As a user, I want to be able to collect research from all sources with a consistent CLI interface (`hpc_batch.py collect <source> --query "..." --max N`), so that I don't need to learn different tools for different data sources
25. As a researcher, I want the metadata taxonomy to be extensible — new analysis types should need nothing more than a new `type` string and convention, so that the system adapts to new research domains without code changes
26. As a user, I want each collector to have a config file (cache dir, rate limits, API keys, default models), so that I don't have to pass these as CLI args every time
27. As a user, I want collectors to respect per-source rate limits with automatic backoff, so that I don't get IP-banned by arXiv/GitHub/Kaggle for hammering their APIs
28. As a user, I want collector results to be cached locally by source URL/content hash, so that re-running the same query returns instant results without redundant network calls
29. As a user, I want cache entries to have a configurable TTL (fresh vs stale), so that I can force-refresh when I know content has changed
30. As a pipeline operator, I want the fetch proxy to have a health check endpoint and auto-restart on crash, so that long-running HPC jobs don't stall because the proxy died mid-batch
31. As a system operator, I want the proxy to expose a readiness indicator (e.g., a BeeGFS heartbeat file) that HPC tasks can check before attempting a fetch, so that tasks fail fast instead of waiting 2 minutes for a timeout
32. As a system operator, I want HPC tasks that don't need internet to work normally when the local machine is offline, so that compute jobs gracefully degrade rather than stalling
33. As a user, I want each collector to fail gracefully when the local machine has no internet — it should return empty results with a warning, not crash or hang
34. As a researcher, I want each research project to have its own workspace (separate SQLite DB or namespace), so that concurrent projects don't mix their tasks/results
35. As a user, I want to export task results to markdown, JSON, or CSV, so that I can share findings or ingest them into downstream tools (notebooks, LaTeX, dashboards)
36. As a reviewer, I want each task's metadata to include the collector version, query params, and model parameters that produced it, so that I can independently reproduce a past analysis
37. As a project owner, I want each implementation phase to have clear acceptance criteria (exit gates), so that I know when it's safe to move to the next phase

## Implementation Decisions

### 1. Collectors Package Structure

A new `collectors/` Python package at the repo root. Pattern: each collector is a class extending `BaseCollector` with two main methods:

- `collect(params) -> list[CollectorResult]` — internet-facing, fetches raw data
- `to_tasks(results) -> list[dict]` — pure transform, produces task dicts ready for `submit_task()`

Initial collectors: `arxiv`, `github`, `web`, `kaggle`, `huggingface`. Each wrapped has its external dependency optional (e.g., `arxiv` requires the `arxiv` PyPI package, `web` requires `requests` + optional `playwright`).

No changes to the existing `hpc_batch.py submit` path — collectors call the same `submit_task()` function.

### 2. Metadata Taxonomy (extensible conventions in `metadata` JSON column)

Standardized top-level keys to be set by collectors and validated by a `validate_metadata()` function:

| Key | Type | Required | Example |
|---|---|---|---|
| `type` | string | Yes | `paper_analysis`, `code_review`, `web_research` |
| `source` | string | Yes | `arxiv`, `github`, `kaggle`, `web` |
| `tags` | list[string] | No | `["deep_learning", "attention"]` |
| `domain` | string | No | `nlp`, `cv`, `systems` |
| `source_url` | string | No | `https://arxiv.org/abs/...` |
| `source_id` | string | No | arXiv ID, GitHub slug |
| `collection.pipeline` | string | No | Pipeline name for traceability |
| `preprocessing.chunked` | bool | No | Whether the task is a chunk of larger content |
| `preprocessing.total_chars` | int | No | Original content size |

No schema changes to `tasks` table — `metadata` is already a freeform TEXT/JSON column.

### 3. ChunkedTaskBuilder (for large content)

A utility in `collectors/base.py` that:
- Splits text by token count (configurable window + overlap)
- Submits each chunk as a separate analysis task with metadata indicating chunk index
- Submits a synthesis task that depends on all chunks (via `task_deps` table)
- Returns the root synthesis task ID for downstream chaining

The existing DAG infrastructure (`task_deps`, `cascade_failure`, preemption guard) handles this without modification.

### 4. File-Based Internet Proxy (BeeGFS-backed)

Protocol:
- Compute node writes `~/hpc-agent/fetch-requests/{task_id}.json` with request payload
- Local polling process picks it up (via rsync or direct BeeGFS access), fulfills it, writes response to `~/hpc-agent/fetch-responses/{task_id}.json`
- Compute node polls for response with backoff (up to ~2 min timeout)
- Request payload: `{"id": str, "type": str, "url": str, "timeout": int, ...}`
- Response payload: `{"status": int, "body": str, "headers": dict, "error": str}`

Proxy handles auth entirely on the local machine — API keys never reach the HPC.

Implemented as a new `proxy/` package with:
- `proxy/server.py` — polling loop (rsync-based)
- `proxy/handlers/http.py` — `requests.get/post`
- `proxy/handlers/search.py` — web search via configured provider
- `proxy/schema.py` — request/response format

Integration in `dsv4_batch.slurm`: add the `fetch_url` tool to vLLM payloads, and after a tool call, write the request file, poll for response, then re-call vLLM with the fetched context.

This is the single highest-value addition — it retrofits internet access onto air-gapped compute nodes using only shared filesystem I/O.

### 5. CLI Extensions

New subcommands on `hpc_batch.py`:

```
hpc_batch.py collect <source> [--query Q] [--max N] [--model M] [--tags TAGS]
hpc_batch.py preprocess <type> [--pdf PATH | --url URL] [--chunk N] [--summarize]
hpc_batch.py pipeline run <yaml_path>
hpc_batch.py watch [--poll SEC] [--type TYPE]
```

The `preprocess` subcommand invokes collectors + chunking, submits the DAG, and returns the root task ID. The `watch` subcommand polls the local DB (after periodic collect) and prints new results.

### 6. Task Routing (mixed local/HPC dispatch)

A `TaskRouter` class that decides based on:
- Explicit model name (`ollama/qwen2.5` → local, `deepseek-ai/DeepSeek-V4-Flash` → HPC)
- Task type (`summarize`, `extract_keywords` → local; `paper_analysis`, `code_review` → HPC)
- Context size heuristic (short → local, long → HPC)

The router can be consumed by:
- `async_queue_monitor.py` — only dispatches HPC-routed tasks to SLURM
- `local_process.py --router local` — only processes local-routed tasks

Routing is non-binding and overridable — a task can force a destination via metadata.

### 7. No Schema Changes

The existing `tasks` table (id, agent_id, messages, tools, model, api_config, metadata, priority, status, result, ...) and `task_deps` table remain unchanged. All extensions use conventions within the existing `metadata` JSON column.

This is a deliberate decision to keep the proven queue infrastructure stable.

### 8. Existing `dsv4_batch.slurm` Modified Minimally

The SLURM batch script gets two additions:
- Inject `fetch_url` tool definition into the vLLM payload
- After a tool call is returned, write fetch request to BeeGFS path, poll for response, call vLLM again with the fetched context

All other batch logic (task claiming, processing loop, result writing, archiving) remains identical.

### 9. Content Caching (per collector)

Each collector gets a local cache directory (`~/.cache/hpc-agent/collectors/<name>/`) keyed by source URL or content hash:

| Field | Purpose |
|---|---|
| Cache key | Source URL or SHA256 of query params |
| Stored | Raw response + parsed `CollectorResult` + fetch timestamp |
| TTL | Configurable per collector (default: arXiv 1h, GitHub 5min, web search 0 = never cache) |
| Invalidation | `--refresh` flag forces re-fetch; expired entries are skipped silently |

Cache is a simple JSON-line file per collector, not a separate database. Cache directory is gitignored.

### 10. Collector Configuration

Each collector reads from `config.yaml` under a `collectors.<name>` key:

```yaml
collectors:
  arxiv:
    cache_ttl: 3600
    max_results: 50
    rate_limit: 3  # requests per second
  github:
    cache_ttl: 300
    rate_limit: 30
    token_env: GITHUB_TOKEN
  web:
    cache_ttl: 0  # never cache searches
    rate_limit: 10
    user_agent: "hpc-agent-research/1.0"
```

API keys are read from environment variables, not `config.yaml` (to avoid committing secrets). Each collector documents which env vars it expects.

### 11. Proxy Health & Reliability

The proxy polling loop is wrapped in a simple supervisor:

- **Heartbeat** — proxy writes a timestamp to `~/hpc-agent/proxy-heartbeat.json` every 10 seconds. HPC tasks check this file before attempting a fetch; if heartbeat is stale (>60s), the task skips all proxy calls and proceeds without internet.
- **Auto-restart** — proxy is launched via a supervisor script that restarts it on crash (simple `while true; do python3 -m proxy.server; sleep 2; done`). For production use, wrap in systemd or tmux.
- **Request timeouts** — each request has a hard timeout (configurable, default 30s for HTTP, 10s for search). If the handler doesn't respond in time, the proxy writes an error response and moves on.
- **Stale request cleanup** — request files older than 10 minutes are cleaned up periodically to prevent directory bloat from orphaned requests.

### 12. Graceful Degradation & Offline Mode

- **No proxy, no problem** — tasks that don't call `fetch_url` work identically whether the proxy is running or not
- **Proxy readiness** — tasks check `proxy-heartbeat.json` before first fetch. If proxy is down, the task skips all internet calls and completes with what it has
- **Collector offline** — each collector wraps its API calls in try/except with a 10s timeout. On network failure, it returns an empty result set with a warning message (logged, not crashed)
- **Partial results** — if a collector fetches 7/10 results before a network blip, it returns the 7 it got with a warning. The user can re-run with `--refresh` to fill gaps

### 13. Provenance & Reproducibility

Extended metadata fields for provenance:

| Key | Required | Set by | Example |
|---|---|---|---|
| `provenance.collector` | Yes | Collector base | `arxiv/1.0` |
| `provenance.collected_at` | Yes | Collector base | ISO 8601 timestamp |
| `provenance.query` | Yes | Collector | `{"query": "attention mechanisms", "max": 5}` |
| `provenance.model` | Yes | Task submitter | `deepseek-ai/DeepSeek-V4-Flash` |
| `provenance.model_params` | Yes | Task submitter | `{"temperature": 0.7, "max_tokens": 4096}` |
| `provenance.cache_hit` | No | Collector base | `true` or `false` |

This is enough to reproduce any past analysis: re-run the collector with the same query, apply the same model params. The chain is self-contained in `metadata`.

### 14. Data Lifecycle

- **Export** — `hpc_batch.py export <task-id> [--format markdown|json|csv]` extracts the task's messages, result, and metadata into a standalone file
- **Batch export** — `hpc_batch.py export --type paper_analysis --source arxiv --format markdown` exports all matching tasks
- **Archival** — a `hpc_batch.py archive` command moves old tasks (>90 days) to an archive SQLite DB (`tasks-archive.db`) and deletes from the active DB. Configurable threshold.
- **Cleanup** — existing `cleanup.py` is enhanced to also purge proxy request/response files older than 7 days and collector cache entries older than 30 days

### 15. Workspace Isolation

- **Single-user default** — single `tasks.db` is fine for one researcher
- **Project namespaces** — use `metadata.source` / `metadata.type` / `metadata.tags` as query filters. `hpc_batch.py status --source arxiv --type paper_analysis` scopes output to one project
- **Separate DBs (future)** — if multi-project isolation becomes painful, the CLI supports `--db <path>` to point at a separate SQLite file. Workspace = separate file

No dedicated multi-user infrastructure — deemed premature for a solo research tool.

### 16. Security (Proxy Request Validation)

The proxy validates every incoming request to prevent abuse from compromised compute jobs:

- **Allowlist** — only URLs matching configured patterns are fetched (default: any `https://` URL). Configurable to restrict to specific domains.
- **SSRF protection** — private IP ranges (10.x, 172.16-31.x, 192.168.x, 127.x) are blocked. No internal network discovery.
- **Request size limit** — request files larger than 10KB are rejected (prevents log injection through oversized payloads).
- **Response size limit** — responses larger than 5MB are truncated with a warning header.
- **No auth on requests** (by design) — any compute process can submit fetch requests. The proxy is a shared service, not a per-user gate. Auth can be added later with a shared secret in the request payload if needed.

### 17. Acceptance Criteria per Phase

Each phase is considered complete when:

**Phase 1 (Collectors)** — All collectors pass the mock-API unit test seam. At least one collector (arXiv) passes the full integration test (real API call, verified). Cache layer implemented and passing.

**Phase 2 (Preprocessing + Chunking)** — `ChunkedTaskBuilder` unit tests pass (chunk count, overlap, DAG edges, synthesis wiring). `hpc_batch.py preprocess paper --pdf` works end-to-end on a test PDF.

**Phase 3 (CLI Extensions)** — All new subcommands parse correctly. `hpc_batch.py collect arxiv --query test --max 1` works. `hpc_batch.py watch` shows new results within one poll cycle.

**Phase 4 (Fetch Proxy)** — Proxy round-trip test passes (temp directory, request→response). `dsv4_batch.slurm` with `fetch_url` tool works in mock mode. Heartbeat + auto-restart tested.

**Phase 5 (Routing)** — `TaskRouter` unit tests pass for all routing dimensions. `async_queue_monitor.py --router hpc` dispatches only HPC tasks. `local_process.py --router local` processes only local tasks.

**Phase 6 (YAML Pipelines)** — A `research_pipeline.yaml` with arxiv collection + analysis + synthesis runs end-to-end locally (Ollama). Output matches expected structure.

## Testing Decisions

### What makes a good test

- Test external behavior, not implementation details
- Collectors: mock the external API at the HTTP level, verify CollectorResult and task dict shape
- ChunkedTaskBuilder: verify chunk count, DAG edges, metadata correctness — pure logic, no I/O
- Proxy: verify request/response file format and round-trip using a temp directory, not an actual BeeGFS mount
- Routing: verify model → destination mapping, type → destination mapping, context-size heuristics

### Test seams (highest to lowest)

1. **Collector unit** — feed mock API response → verify structured output shape. All collectors test the same contract.
2. **ChunkedTaskBuilder unit** — feed text + params → verify chunk count, overlap, DAG edges, synthesis task wiring.
3. **Metadata validation unit** — feed metadata dicts → verify correct warnings for missing keys.
4. **TaskRouter unit** — feed task dict → verify `routing` output string.
5. **Fetch proxy integration** — start proxy against temp directory → write request file → verify response appears with correct content. Tests the polling loop + handler dispatch.
6. **Full pipeline (local)** — run collector → submit → local_process with Ollama → verify results in DB. Integration test requiring Ollama running locally.

### What is not tested

- Actual HPC SLURM submission (tested manually by running `hpc_batch.py dispatch`)
- Actual internet fetching from arXiv/GitHub/Kaggle APIs (network-dependent — tested by the upstream libraries)
- vLLM model loading (exists and tested in `dsv4_batch.slurm` — no changes to that path)

## Out of Scope

- **Real-time streaming of partial results** — the periodic rsync collect pattern is sufficient for research batch processing. Redis-based streaming was considered and rejected due to network isolation.
- **Web UI / dashboard** — all interaction is CLI-based. A dashboard is a separate future project.
- **Multi-node SLURM parallelism** — each batch is a single-node job. Job arrays and MPI parallelism are out of scope for this PRD.
- **Model fine-tuning on HPC** — this PRD focuses on inference workflows. Fine-tuning is a separate capability.
- **PDF figure/chart extraction** — text-only extraction (PDF→text via PyMuPDF). Structured extraction of figures, tables, and charts requires additional work.
- **Non-English research content** — collectors and analysis assume English-language content as the primary use case.
- **Binary file downloads through proxy** — the proxy returns `body: str` (text). Binary payloads (images, datasets, PDFs) are not supported. The proxy can fetch URLs that return text, but not download files.
- **Scheduled / periodic collection** — no cron-like polling of research sources with alerting. Collection is manual or pipeline-driven. Scheduled re-runs can be handled by the user's external scheduler (cron, CI).
- **Multi-user / collaborative features** — no user accounts, permissions, or shared workspaces. Single-researcher tool. Collaborators can share `tasks.db` files or exported results.
- **Re-analysis workflow** — no built-in "re-run this task with different params" command. Users can re-submit similar tasks via a modified pipeline YAML.
- **GPU budget / cost tracking** — no per-project costing or quota enforcement. The user monitors their own SLURM allocation manually. A cost-tracking layer is a separate future concern.

## Further Notes

- **Development order**: Phase 1 (collectors + base) → Phase 2 (preprocessing + chunking) → Phase 3 (CLI extensions) → Phase 4 (fetch proxy) → Phase 5 (routing) → Phase 6 (YAML pipelines). Each phase is independently shippable.
- **The BeeGFS file proxy is the most novel and highest-leverage component** — it retrofits internet access onto air-gapped compute nodes using only filesystem I/O. It should be built early in the cycle because it unlocks agentic research loops.
- **Ollama local summarization is optional and can be added later** — the core collectors + chunking pattern works without it. Pre-summarization reduces HPC token costs but adds latency on the local side.
- **The entire design assumes the HPC remains network-isolated.** If cluster policy changes to allow compute-node internet access, the fetch proxy becomes unnecessary but the collectors and preprocessing pipeline remain valuable.
- **Phase 1 consolidation** (extract shared schema, deduplicate monitors) is not strictly required but recommended before or alongside Phase 1 to reduce maintenance burden.

### Known Gaps Register

These gaps are acknowledged but deferred — each has a trigger condition for re-evaluation:

| Gap | Deferred because | Re-evaluate when |
|---|---|---|
| GPU budget tracking + quota enforcement | Premature for single-user tool; SLURM `sacct` suffices | Second user joins the project, or allocation consistently runs out mid-month |
| Multi-user / collaborative workspaces | Premature for solo research tool; SQLite doesn't scale | Need to share a research queue with a collaborator |
| Real-time result streaming | rsync-poll latency (30s-60s) is acceptable for batch research; Redis path is blocked by network isolation | Tasks consistently take <30s and user needs sub-second feedback |
| Re-analysis workflow ("rerun with new params") | Can be approximated by re-running a pipeline YAML with different model params | User finds themselves manually re-submitting the same tasks with minor param tweaks |
| Binary file proxy support | No immediate use case; text-only covers papers, code, and search results | HPC task needs to download a dataset, image, or model file |
| Scheduled/periodic collection | Manually triggered pipelines are sufficient for exploratory research | User needs weekly arXiv alerts or "monitor this GitHub repo for changes" |
| Non-English content | All current research targets English-language sources | User needs to analyze papers in Chinese, French, German, etc. |
| Dedicated multi-user SQLite (Postgres migration) | SQLite works fine for single-user task queues; added complexity not justified | Queue contention becomes a problem (>1000 tasks/min) or multi-user needed |
| vLLM model load optimization (30-60 min cold start) | Requires cluster-level changes (persistent pool, checkpoint cache) or exotic quantization — not a local-code fix | vLLM load time becomes dominant bottleneck (>60 min) |
| Job health alerting (crash notification) | No push notification infrastructure; user discovers failures via `hpc_batch.py collect` | User misses a deadline due to undetected silent crash |
| Content dedup across collectors | Low collision likelihood; cost of duplicate HPC inference is small at current scale | >10% of submitted tasks are duplicate content |
| Multi-collection orchestration | Manual flow works: fetch → wait → fetch → dispatch. Auto-batching adds complexity | User consistently runs 3+ fetches before every dispatch |
| Per-domain proxy rate limiting | Global rate limit suffices for single-user research | Proxy gets consistent 429 from one domain |

Each deferred gap is a clean future PRD, not scope creep on this one.
