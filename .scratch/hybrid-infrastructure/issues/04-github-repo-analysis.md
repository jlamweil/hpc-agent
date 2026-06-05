Status: `ready-for-agent`

# GitHub repo analysis

## What to build

A GitHub collector that clones public repos locally, extracts code structure, and submits architecture review tasks to the HPC.

### GitHub collector (`collectors/github.py`)

- `collect({"url": "https://github.com/user/repo"})`:
  - Shallow clones the repo to a local cache dir (`~/.cache/hpc-agent/collectors/github/repos/<owner>/<repo>/`)
  - Extracts file tree (all paths, directory structure)
  - Reads README, LICENSE, and key config files (`package.json`, `Cargo.toml`, `requirements.txt`, etc.)
  - Collects language-level stats via `linguist` or simple extension counting
  - Returns a single `CollectorResult` with the full repo context
- `collect({"url": ..., "files": ["src/main.py", "README.md"]})` — targets specific files
- `collect({"query": "transformers attention", "language": "python", "max": 5})` — searches GitHub repos by topic

### to_tasks() output

Single repo → one or more analysis tasks:
1. **`repo_analysis`** — whole-repo architecture: file tree, language breakdown, dependency analysis, tech stack identification
2. **`code_review`** (per specified file) — deep code review of individual files
3. **`comparison`** (when analyzing multiple repos) — cross-repo architecture comparison

Metadata includes:
- `type`: `repo_analysis` | `code_review` | `comparison`
- `source: "github"`
- `language`, `file_count`, `stars` (if available from API), `topics`

### CLI

```
hpc_batch.py collect github --url https://github.com/user/repo
hpc_batch.py collect github --url https://github.com/user/repo --files "src/main.py,src/lib.py"
hpc_batch.py collect github --query "attention mechanism" --language python --max 5
hpc_batch.py preprocess repo --url https://github.com/user/repo  # (alias for collect + analyze)
```

### Config

```yaml
collectors:
  github:
    cache_ttl: 300    # repos change, don't cache long
    rate_limit: 30     # GitHub allows 60/hr unauthenticated, 5000/hr with token
    token_env: GITHUB_TOKEN   # optional, increases rate limit
    max_file_size: 512000     # skip files larger than this for analysis
    clone_depth: 1            # shallow clone depth
```

## Acceptance criteria

- [ ] GitHub collector shallow-clones a public repo to cache dir
- [ ] File tree extraction works (all paths, directory structure)
- [ ] README + key config files are extracted and included in collector result
- [ ] Language detection works (extension-based)
- [ ] `hpc_batch.py collect github --url https://github.com/user/repo` submits a `repo_analysis` task
- [ ] `--files` flag targets specific files for `code_review` tasks
- [ ] `--query` searches GitHub repos by topic
- [ ] Rate limiting respects GitHub API limits
- [ ] Repo cache respects TTL; `--refresh` re-clones
- [ ] Graceful handling of: private repos (skip with warning), very large repos (>100MB), network failures

## Blocked by

- Issue #1 (Collector framework + arXiv) — uses `BaseCollector` and the `collect` CLI pattern
