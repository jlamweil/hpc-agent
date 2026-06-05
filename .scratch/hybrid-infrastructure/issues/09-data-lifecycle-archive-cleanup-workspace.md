Status: `ready-for-agent`

# Data lifecycle — archive, cleanup, workspace

## What to build

Data management for long-term use: archival policies, enhanced cleanup, and workspace isolation.

### 1. Archive command

`hpc_batch.py archive` — moves old completed/failed tasks to an archive database:

```
hpc_batch.py archive --older-than 90    # archive tasks older than 90 days
hpc_batch.py archive --all              # archive all completed/failed tasks
hpc_batch.py archive --status failed    # archive only failed tasks
```

Behavior:
- Creates `tasks-archive.db` with the same schema
- Moves archived tasks from `tasks.db` → `tasks-archive.db` (INSERT INTO archive, DELETE FROM active, in a transaction)
- Preserves all metadata, results, and `task_deps` edges
- Reports: "Archived 142 tasks (120 completed, 22 failed)"

### 2. Archive query

```
hpc_batch.py archive list               # list archive databases with stats
hpc_batch.py archive query --type paper_analysis --source arxiv  # search archived tasks
hpc_batch.py archive restore <task-id>  # restore a specific task back to active DB
```

### 3. Enhanced cleanup

Building on existing `cleanup.py`:

- Cleans proxy request/response files older than 7 days from BeeGFS
- Cleans collector cache entries older than 30 days
- Cleans orphaned HPC results directories (no corresponding active or archived tasks)
- Reports: "Cleaned 3 stale proxy directories, 142 stale cache entries, 2 orphaned result dirs"
- Dry-run mode: `--dry-run` shows what would be deleted
- Configurable thresholds in `config.yaml`:

```yaml
cleanup:
  completed_days: 30      # delete completed tasks after 30 days
  failed_days: 7          # delete failed tasks after 7 days
  hpc_results_days: 7     # delete HPC result dirs after 7 days
  proxy_files_days: 7     # clean proxy request/response files
  cache_entries_days: 30  # clean collector cache
```

### 4. Workspace isolation

Multiple approaches (implement all three):

**a. Metadata scoping** — all `hpc_batch.py` commands accept `--tag` or `--source` filters:

```bash
hpc_batch.py status --tag "attention-survey"  # only tasks with that tag
hpc_batch.py export --type paper_analysis --source arxiv --tag "attention-survey" --dir ./survey-report/
```

**b. Separate DB files** — `hpc_batch.py` accepts `--db <path>`:

```bash
hpc_batch.py --db ~/research/attention-survey/tasks.db collect arxiv --query "attention" --max 5
hpc_batch.py --db ~/research/attention-survey/tasks.db dispatch
```

When `--db` is provided, all commands operate on that database instead of the default `tasks.db`.

**c. Pipeline-run scoping** — `hpc_batch.py pipeline run survey.yaml` automatically creates a scoped workspace:

- Creates `.scratch/pipeline-{uuid}/` directory
- Copies/cross-references this directory with the submitted tasks
- `hpc_batch.py pipeline status` shows all tasks scoped to that run

## Acceptance criteria

- [ ] `hpc_batch.py archive --older-than 90` moves old tasks to archive DB
- [ ] Archived tasks are fully preserved (metadata, results, task_deps)
- [ ] `hpc_batch.py archive query --type paper_analysis` searches archived tasks
- [ ] `hpc_batch.py archive restore <id>` restores from archive to active DB
- [ ] Enhanced cleanup removes stale proxy files, cache entries, and orphaned result dirs
- [ ] `--dry-run` shows what cleanup would delete without actually deleting
- [ ] `hpc_batch.py status --tag "attention-survey"` filters by tag
- [ ] `hpc_batch.py --db ~/custom/path/tasks.db` uses a separate database file
- [ ] Pipeline runs produce scoped task sets

## Blocked by

- Issue #1 (Collector framework + arXiv) — needs tasks in the queue
- Issue #3 (Paper chunking + DAG) — pipeline/workspace features benefit from DAG support
