Status: `ready-for-agent`

# Content caching + provenance

## What to build

Robust caching layer for all collectors and full provenance tracking in task metadata.

### Cache layer enhancements

Builds on the basic cache from issue #1:

- Cache eviction: remove entries older than the collector's `cache_ttl` (can be 0 = never cache)
- Cache stats: `hpc_batch.py collect --cache-stats` shows hit/miss counts per collector
- Cache directory configurable via `config.yaml`: `collectors.<name>.cache_dir`
- Stale cache warnings: if returning cached results, log "Returning cached result from {timestamp}"
- Shared cache across collectors: option to use a global cache keyed by URL, avoiding re-downloading the same arXiv PDF from two different queries

### Provenance metadata

Extended metadata validation and tracking:

Provenance fields enforced at submission time:
- `provenance.collector`: semver string like `"arxiv/1.0"`
- `provenance.collected_at`: RFC 3339 timestamp
- `provenance.query`: the exact params dict used
- `provenance.cache_hit`: boolean
- `provenance.model`: model name used for analysis
- `provenance.model_params`: dict of temperature, max_tokens, etc.

### Metadata validation CLI

- `hpc_batch.py validate --task <id>` — validates a single task's metadata
- `hpc_batch.py validate --all` — validates all pending tasks and reports warnings count
- Validation checks: required keys present, source is a known collector, type is in known types list, timestamps are parseable

### Extensible type registry

- `collectors/types.py` — mapping of known `type` strings to their required/optional metadata keys
- Initial types: `paper_analysis`, `code_review`, `repo_analysis`, `web_research`, `comparison`, `generic`
- New types can be registered by adding a dict entry (no class inheritance needed)

## Acceptance criteria

- [ ] Cache respects TTL per collector; expired entries are skipped
- [ ] `--refresh` flag on `hpc_batch.py collect` bypasses cache
- [ ] `hpc_batch.py collect --cache-stats` shows hit/miss/eviction counts
- [ ] Provenance metadata is populated on all tasks submitted via collectors
- [ ] `validate_metadata()` flags missing provenance fields as errors (not just warnings)
- [ ] `hpc_batch.py validate --all` scans pending tasks and reports summary
- [ ] Known types are registered in `collectors/types.py` with their metadata schemas
- [ ] Adding a new type requires only a new entry in the types registry — no code changes elsewhere

## Blocked by

- Issue #1 (Collector framework + arXiv) — caching and provenance depend on the collector infrastructure
