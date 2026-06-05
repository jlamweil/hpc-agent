Status: `ready-for-agent`

# CLI tools — watch, export, pipeline

## What to build

Three new user-facing CLI capabilities for the research workflow.

### 1. `hpc_batch.py watch` — live result monitoring

Polls the local tasks.db and prints new results as they arrive (like `tail -f` for HPC tasks):

```
hpc_batch.py watch                          # watch all tasks
hpc_batch.py watch --type paper_analysis    # filter by type
hpc_batch.py watch --source arxiv           # filter by source
hpc_batch.py watch --poll 30               # poll every 30 seconds
hpc_batch.py watch --json                  # output as JSON lines (for piping)
```

Output:
```
[15:23:01] task_abc123  paper_analysis  completed  "Found 3 key contributions..."
[15:23:45] task_def456  paper_analysis  failed      "Token limit exceeded"
[15:24:12] task_ghi789  paper_analysis  completed  "Architecture uses... "
```

Implementation: polls `SELECT id, type, status, result, error FROM tasks WHERE status IN ('completed', 'failed') ORDER BY completed_at DESC LIMIT 50` periodically, diffs against known set, prints new ones.

### 2. `hpc_batch.py export` — result export

Exports task results in human-readable formats:

```
hpc_batch.py export <task-id>                     # print to stdout (markdown)
hpc_batch.py export <task-id> --format json       # raw JSON
hpc_batch.py export <task-id> --format markdown   # formatted markdown
hpc_batch.py export --type paper_analysis --source arxiv --format markdown --output papers.md
```

Markdown output structure:
```markdown
# Analysis: {title}
**Source:** arxiv | **Type:** paper_analysis | **Task:** {id}
**Model:** deepseek-ai/DeepSeek-V4-Flash | **Completed:** {timestamp}

## Prompt
{original prompt text}

## Result
{analysis result content}

## Metadata
```json
{full metadata JSON}
```
```

Batch export with filter:

```
hpc_batch.py export --type paper_analysis --source arxiv --format markdown --dir ./reports/
```

Creates one `.md` file per task in `./reports/` with the above template.

### 3. `hpc_batch.py pipeline` — YAML workflow runner

Executes YAML-defined research pipelines:

```
hpc_batch.py pipeline run survey.yaml
```

YAML format:

```yaml
# survey.yaml
name: "Attention mechanisms survey"
steps:
  - collector: arxiv
    params:
      query: "attention mechanisms survey"
      max_results: 3
    tasks:
      - type: paper_analysis
        model: deepseek-ai/DeepSeek-V4-Flash
        queries:
          - "Summarize the key contributions"
          - "What datasets were used for evaluation?"
          - "What are the limitations?"
        tags: ["attention", "survey"]

  - collector: web
    params:
      query: "attention mechanism benchmarks 2025"
      max_results: 3
    tasks:
      - type: web_research
        model: deepseek-ai/DeepSeek-V4-Flash
        tags: ["attention", "benchmarks"]
```

Pipeline runner (`collectors/pipeline.py`):
- Parses YAML
- For each step, runs the collector with params
- Submits the specified analysis tasks
- Wires DAG: step 2 waits for step 1 (if explicit `depends_on`) or runs in parallel (default)
- Returns summary of submitted task IDs
- `hpc_batch.py pipeline status <pipeline-run-id>` shows all tasks in that run

Pipeline run tracking: a `pipeline_runs` table or metadata tag (`pipeline_run: <uuid>`) on all submitted tasks, so you can query by pipeline run.

## Acceptance criteria

- [ ] `hpc_batch.py watch` shows new completed/failed tasks in near-real-time
- [ ] `hpc_batch.py watch --type` and `--source` filters work
- [ ] `hpc_batch.py watch --json` outputs valid JSON lines
- [ ] `hpc_batch.py export <id>` prints formatted markdown with prompt + result + metadata
- [ ] `hpc_batch.py export --type --source --format markdown --dir ./reports/` batch-exports matching tasks
- [ ] `hpc_batch.py pipeline run survey.yaml` executes both collector steps and submits tasks
- [ ] Pipeline-run tag is applied to all submitted tasks
- [ ] `hpc_batch.py pipeline status <pipeline-uuid>` shows task states

## Blocked by

- Issue #1 (Collector framework + arXiv) — watch and export need tasks in the queue; pipeline needs collectors
- Issue #3 (Paper chunking + DAG) — pipeline can express DAG workflows
