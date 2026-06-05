"""Base classes and protocols for all collectors."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class CollectorResult:
    """Structured result from a collector fetch."""

    source_id: str
    title: str
    text: str
    url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseCollector:
    """Extend for each data source.
    
    Subclasses override collect() and optionally to_tasks().
    The default to_tasks() wraps each result as a single analysis task.
    """

    name: str = "base"
    source: str = "base"

    def collect(self, params: dict) -> list[CollectorResult]:
        """Fetch raw data from the source. Must be overridden."""
        raise NotImplementedError

    def to_tasks(self, results: list[CollectorResult], **kwargs) -> list[dict]:
        """Convert CollectorResults to task dicts for submit_task().
        
        Each task dict supports the same keys as hpc_batch.py submit_task():
          - prompt (str)
          - model (str)
          - metadata (dict)
          - depends_on (list[str])
        
        Override for custom per-source task generation.
        """
        model = kwargs.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        tasks = []
        for r in results:
            provenance = {
                "collector": f"{self.name}/1.0",
                "collected_at": time.time(),
                "source": self.source,
            }
            meta = {
                "type": "paper_analysis" if self.source == "arxiv" else "generic",
                "source": self.source,
                "source_id": r.source_id,
                "source_url": r.url,
                "title": r.title,
                "tags": r.metadata.get("tags", []),
                "provenance": provenance,
                **r.metadata,
            }
            tasks.append({
                "prompt": self._build_prompt(r),
                "model": model,
                "metadata": meta,
                "depends_on": kwargs.get("depends_on"),
            })
        return tasks

    def _build_prompt(self, result: CollectorResult) -> str:
        """Build a prompt from a collector result. Override for custom formatting."""
        return f"Title: {result.title}\n\n{result.text}"


def validate_metadata(metadata: dict) -> list[str]:
    """Validate metadata against the standard taxonomy. Returns warnings list."""
    warnings = []
    required_keys = ["type", "source"]
    for k in required_keys:
        if k not in metadata or not metadata.get(k):
            warnings.append(f"Missing required metadata key: '{k}'")

    provenance = metadata.get("provenance", {})
    if not isinstance(provenance, dict):
        warnings.append("'provenance' must be a dict")
    else:
        for pk in ("collector", "collected_at", "source"):
            if pk not in provenance:
                warnings.append(f"Missing provenance key: '{pk}'")

    known_types = {
        "paper_analysis", "code_review", "repo_analysis",
        "web_research", "comparison", "scaffolding", "data_analysis", "generic",
    }
    t = metadata.get("type", "")
    if t and t not in known_types:
        warnings.append(f"Unknown type '{t}' — not in known types: {known_types}")

    return warnings


# Token estimation (rough: 4 chars ≈ 1 token)
def estimate_tokens(text: str) -> int:
    return len(text) // 4


class ChunkedTaskBuilder:
    """Split large content into chunk tasks + synthesis DAG.
    
    Usage:
        builder = ChunkedTaskBuilder(window=4000, overlap=200)
        tasks = builder.build(full_text, task_type="paper_analysis", base_metadata={...})
        # tasks[0:-1] are chunk tasks, tasks[-1] is the synthesis task
    """

    def __init__(self, window: int = 4000, overlap: int = 200):
        self.window = window
        self.overlap = overlap

    def build(self, text: str, task_type: str = "paper_analysis",
              base_metadata: dict | None = None,
              model: str = "deepseek-ai/DeepSeek-V4-Flash",
              analysis_query: str | None = None) -> list[dict]:
        """Split text into chunks and create a DAG of analysis + synthesis tasks.
        
        Returns a list of task dicts. The synthesis task is last and depends on
        all chunk tasks. Caller should submit them and wire depends_on via task IDs.
        
        After submission, use get_chunk_ids() to get the actual task IDs,
        then resubmit the synthesis task with the real IDs.
        """
        base_metadata = base_metadata or {}
        chunks = self._chunk_text(text)
        tasks = []
        chunk_ids: list[str] = []

        if not chunks:
            return tasks

        for i, chunk_text in enumerate(chunks):
            cid = str(uuid.uuid4())
            chunk_ids.append(cid)
            meta = {
                **base_metadata,
                "type": task_type,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "is_chunk": True,
                "preprocessing": {
                    "chunked": True,
                    "window": self.window,
                    "overlap": self.overlap,
                    "total_chars": len(text),
                },
                "provenance": {
                    **base_metadata.get("provenance", {}),
                    "chunked": True,
                },
            }

            query = analysis_query or f"Analyze this section ({i+1}/{len(chunks)})"
            prompt = f"{query}\n\n{chunk_text}"

            tasks.append({
                "task_id": cid,
                "prompt": prompt,
                "model": model,
                "metadata": meta,
            })

        # Synthesis task depends on all chunks
        synthesis_id = str(uuid.uuid4())
        synthesis_meta = {
            **base_metadata,
            "type": task_type,
            "is_synthesis": True,
            "total_chunks": len(chunks),
            "preprocessing": {
                "chunked": True,
                "total_chars": len(text),
            },
        }
        tasks.append({
            "task_id": synthesis_id,
            "prompt": f"Synthesize the analysis of all {len(chunks)} sections into a coherent analysis.",
            "model": model,
            "metadata": synthesis_meta,
            "depends_on": chunk_ids,
        })

        return tasks

    def _chunk_text(self, text: str) -> list[str]:
        """Split text into chunks by approximate token count."""
        if not text:
            return []

        # Try to split by section headings first (## or ###)
        lines = text.split("\n")
        chunks: list[str] = []
        current_chunk: list[str] = []
        current_tokens = 0

        for line in lines:
            line_tokens = estimate_tokens(line)

            # If this is a heading and current chunk is substantial, start new chunk
            is_heading = line.strip().startswith("#") and current_tokens > self.window // 2

            if is_heading and current_chunk:
                chunks.append("\n".join(current_chunk))
                # Add overlap: last few lines of previous chunk
                overlap_text = ""
                if self.overlap > 0 and current_chunk:
                    overlap_lines = []
                    overlap_tok = 0
                    for l in reversed(current_chunk):
                        lt = estimate_tokens(l)
                        if overlap_tok + lt > self.overlap:
                            break
                        overlap_lines.insert(0, l)
                        overlap_tok += lt
                    overlap_text = "\n".join(overlap_lines) + "\n"
                current_chunk = [overlap_text + line] if overlap_text else [line]
                current_tokens = estimate_tokens("".join(current_chunk))
                continue

            # If adding this line would exceed window, finalize chunk
            if current_tokens + line_tokens > self.window and current_chunk:
                chunks.append("\n".join(current_chunk))
                # Add overlap
                overlap_text = ""
                if self.overlap > 0 and current_chunk:
                    overlap_lines = []
                    overlap_tok = 0
                    for l in reversed(current_chunk):
                        lt = estimate_tokens(l)
                        if overlap_tok + lt > self.overlap:
                            break
                        overlap_lines.insert(0, l)
                        overlap_tok += lt
                    overlap_text = "\n".join(overlap_lines) + "\n"
                current_chunk = [overlap_text] if overlap_text else []
                current_tokens = estimate_tokens(overlap_text)
                if overlap_text:
                    current_chunk.append(line)
                    current_tokens += line_tokens
                else:
                    current_chunk = [line]
                    current_tokens = line_tokens
            else:
                current_chunk.append(line)
                current_tokens += line_tokens

        if current_chunk:
            chunks.append("\n".join(current_chunk))

        return chunks
