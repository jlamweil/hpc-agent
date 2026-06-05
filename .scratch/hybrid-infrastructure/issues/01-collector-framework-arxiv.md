Status: `ready-for-agent`

# Collector framework + arXiv

## What to build

Build the foundational collector framework and the first working collector (arXiv).

### Collector framework (`collectors/` package)

- `collectors/base.py` — `BaseCollector` abstract class with two primary methods:
  - `collect(params) -> list[CollectorResult]` — fetches raw data from the source
  - `to_tasks(results) -> list[dict]` — transforms results into task dicts ready for `submit_task()`
- `collectors/__init__.py` — exports all collectors, auto-discovery by name
- `CollectorResult` dataclass with fields: `source_id`, `title`, `text`, `url`, `metadata` (dict)
- Collector config loaded from `config.yaml` under `collectors.<name>.*`
- Per-source rate limiting (configurable requests/sec, automatic backoff on 429)
- Graceful offline handling — on network failure, return empty results with logged warning, don't crash

### ArXiv collector (`collectors/arxiv.py`)

- Search arXiv API by query, return paper metadata + abstracts
- Fetch full PDF by arXiv ID, extract text via PyMuPDF (`fitz`)
- Produce `CollectorResult` per paper with: title, authors, abstract/full text, arXiv ID, categories, published date, PDF URL
- `to_tasks()` generates a `paper_analysis` task per paper with metadata:
  - `type: "paper_analysis"`, `source: "arxiv"`
  - `source_id`, `source_url`, `title`
  - `tags`: categories
  - `provenance.collector`: `"arxiv/1.0"`
  - `provenance.query`: the search params used

### CLI integration

- `hpc_batch.py collect arxiv --query "..." --max 5 --model deepseek-ai/DeepSeek-V4-Flash` submits tasks to the queue
- `hpc_batch.py collect arxiv --id 2301.12345 --analyze` fetches a single paper by arXiv ID

### Config

Config section in `config.yaml`:

```yaml
collectors:
  arxiv:
    cache_ttl: 3600
    max_results: 50
    rate_limit: 3    # requests per second
```

### Caching (basic)

Simple per-collector JSON-line cache in `~/.cache/hpc-agent/collectors/<name>/`. Keyed by query hash. TTL from config. `--refresh` flag bypasses cache.

### Metadata validation

`validate_metadata()` function that warns on missing required keys (`type`, `source`, `provenance.collector`, `provenance.collected_at`).

## Acceptance criteria

- [ ] `BaseCollector` abstract class works: a minimal subclass can collect and submit
- [ ] arXiv collector fetches papers by query, returns `CollectorResult` list
- [ ] arXiv collector fetches paper by ID, extracts PDF text via PyMuPDF
- [ ] `collectors/config.py` reads per-collector config from `config.yaml` with env var fallback for API keys
- [ ] Rate limiter sleeps between requests to stay under configured RPS; backs off on 429
- [ ] `hpc_batch.py collect arxiv --query "attention mechanisms" --max 3` submits 3 analysis tasks to the SQLite queue
- [ ] Each submitted task has correct `metadata` (type, source, provenance, tags)
- [ ] Cache returns instant results on second identical query; `--refresh` re-fetches
- [ ] Offline mode: collector returns empty results with warning, doesn't crash
- [ ] `validate_metadata()` warns on missing required fields
- [ ] Collector follows same package for future collectors (`collectors/github.py` etc.)

## Blocked by

None — can start immediately
