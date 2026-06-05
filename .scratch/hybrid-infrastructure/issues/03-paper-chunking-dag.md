Status: `ready-for-agent`

# Paper chunking + DAG workflow

## What to build

Handle large documents (50+ page papers, long reports) by splitting them into chunks and wiring them into a DAG of analysis + synthesis tasks.

### ChunkedTaskBuilder (`collectors/base.py`)

A utility class that:

- Accepts text content + chunk parameters (window size, overlap)
- Splits by token count using a simple tokenizer (tiktoken or character-based approximation)
- Returns a list of chunk dicts with: `index`, `total_chunks`, `text`, `token_count`, `section_heading` (if detected)
- Generates task definitions for each chunk with DAG edges to a final synthesis task
- Each chunk task includes metadata: `type`, `source`, `chunk_index`, `total_chunks`, `is_chunk: true`
- The synthesis task includes metadata: `type`, `source`, `is_synthesis: true`, `depends_on` referencing all chunk task IDs
- The entire DAG is submitted atomically (all tasks in one batch, or none)

### PDF text extraction

- `collectors/pdf.py` — wraps PyMuPDF with section heading detection
- Extracts text preserving heading structure (font size heuristics for `#`, `##`, `###`)
- Handles multi-column layouts (basic heuristic)
- Reports extraction quality: total chars, pages, sections detected, warnings (e.g., "Page 5 has embedded images, text may be incomplete")

### CLI integration

- `hpc_batch.py preprocess paper --pdf path/to/paper.pdf --chunk 4000 --overlap 200`
  - Extracts text from PDF
  - Chunks by token count
  - Submits chunk tasks + synthesis task as DAG
  - Returns the synthesis task ID
- `hpc_batch.py preprocess paper --pdf path/to/paper.pdf --chunk 4000 --summarize`
  - Same but also runs local Ollama summarization on each chunk before submission
  - Summarized text replaces raw chunk text in the task prompt

### Local Ollama summarization (optional, `--summarize` flag)

- Calls local Ollama (`qwen2.5:0.5b`) to summarize each chunk
- Prompts: "Summarize this section concisely, preserving key findings, methodology, and results"
- Returns the summary as the task prompt text (much shorter, saves HPC token budget)
- Falls back to raw text if Ollama is not running

### DAG workflow integration

- `hpc_batch.py pipeline status <synthesis-task-id>` — shows status of all tasks in the DAG (how many chunks completed, how many pending, synthesis blocked or ready)
- `hpc_batch.py pipeline retry <synthesis-task-id>` — re-submits only the failed tasks in the DAG

## Acceptance criteria

- [ ] `ChunkedTaskBuilder` splits text correctly given window + overlap
- [ ] Chunk tasks have correct `chunk_index`, `total_chunks`, `is_chunk: true` metadata
- [ ] Synthesis task depends on all chunk tasks (verified via `task_deps` table)
- [ ] PDF text extraction produces usable text with section headings
- [ ] `hpc_batch.py preprocess paper --pdf test.pdf --chunk 2000` works end-to-end on a real PDF
- [ ] `--summarize` flag runs Ollama locally, substitutes summary text for raw chunk text
- [ ] Synthesis task is blocked until all chunk tasks complete
- [ ] `hpc_batch.py pipeline status <id>` shows correct completion state of the DAG

## Blocked by

- Issue #1 (Collector framework + arXiv) — chunked tasks use the same `submit_task()` path
