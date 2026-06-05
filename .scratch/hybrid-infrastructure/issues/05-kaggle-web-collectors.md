Status: `ready-for-agent`

# Kaggle + web collectors

## What to build

Two additional collectors following the pattern established in Issue #1.

### Kaggle collector (`collectors/kaggle.py`)

- `collect({"competition": "titanic"})` — fetch competition description, dataset files, leaderboard data
- `collect({"dataset": "user/dataset-name"})` — fetch dataset metadata, column descriptions, file listing
- `collect({"query": "nlp", "type": "competition", "max": 10})` — search competitions by keyword
- Produces analysis tasks for:
  - Competition scaffolding: generate baseline model code
  - Dataset exploration: understand columns, data types, target variable
  - Leaderboard analysis: understand what top solutions look like

Metadata: `type: "scaffolding" | "data_analysis"`, `source: "kaggle"`, `competition`, `dataset`

Config:
```yaml
collectors:
  kaggle:
    cache_ttl: 86400      # competitions change rarely
    rate_limit: 5
    token_env: KAGGLE_TOKEN
```

### Web search/scrape collector (`collectors/web.py`)

- `collect({"query": "latest AI research papers 2025", "max": 5})` — web search via configured provider (DuckDuckGo, or configurable)
- `collect({"url": "https://example.com/article", "extract": "main"})` — scrape a specific URL, extract main content
- `collect({"query": "...", "scrape": true})` — search then scrape each result's full content
- Uses `requests` + `BeautifulSoup` for basic extraction; optional `playwright` for JS-rendered pages

Metadata: `type: "web_research"`, `source: "web"`, `urls`, `query`

Config:
```yaml
collectors:
  web:
    cache_ttl: 0          # web content changes; never cache by default
    rate_limit: 10
    user_agent: "hpc-agent-research/1.0"
    search_provider: duckduckgo
    playwright: false     # set true if pages need JS rendering
```

## Acceptance criteria

- [ ] Kaggle collector fetches competition metadata and produces a `scaffolding` task
- [ ] Kaggle collector fetches dataset metadata and produces a `data_analysis` task
- [ ] Kaggle collector gracefully handles missing API key (skip with warning)
- [ ] Web collector searches via configured search provider, returns clean result titles + snippets
- [ ] Web collector fetches and extracts main content from a single URL
- [ ] `hpc_batch.py collect kaggle --competition titanic` works end-to-end
- [ ] `hpc_batch.py collect web --query "transformers survey 2025" --max 5` works end-to-end
- [ ] Both collectors implement rate limiting, caching, offline graceful degradation
- [ ] Both collectors set correct metadata (type, source, provenance)

## Blocked by

- Issue #1 (Collector framework + arXiv) — both collectors extend `BaseCollector` and use the `collect` CLI pattern
