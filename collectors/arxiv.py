"""arXiv collector — fetch papers, extract text, submit analysis tasks."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from collectors.base import BaseCollector, CollectorResult
from collectors.cache import CollectorCache, RateLimiter
from collectors.config import get_collector_config, get_cache_dir

log = logging.getLogger(__name__)

try:
    import arxiv as arxiv_api

    HAS_ARXIV = True
except ImportError:
    HAS_ARXIV = False
    arxiv_api = None  # type: ignore

try:
    import fitz  # PyMuPDF

    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    fitz = None  # type: ignore


class ArxivCollector(BaseCollector):
    """Fetch papers from arXiv by query or ID."""

    name = "arxiv"
    source = "arxiv"

    def __init__(self):
        cfg = get_collector_config("arxiv")
        self.max_results = cfg.get("max_results", 50)
        self.cache_ttl = cfg.get("cache_ttl", 3600)
        self.cache = CollectorCache(get_cache_dir("arxiv"), ttl=self.cache_ttl)
        self.rate_limiter = RateLimiter(cfg.get("rate_limit", 3.0))

    def collect(self, params: dict) -> list[CollectorResult]:
        """Fetch papers from arXiv.
        
        Params:
            query: str — search query (e.g. "attention mechanisms")
            id_list: list[str] — specific arXiv IDs
            max_results: int — max papers to return (default from config)
            fetch_full_text: bool — download and extract PDF text
        """
        if not HAS_ARXIV:
            raise ImportError(
                "arxiv package required. Install: pip install arxiv"
            )

        # Check cache first (unless --refresh)
        refresh = params.pop("refresh", False)
        cache_params = {k: v for k, v in sorted(params.items()) if k != "refresh"}
        if not refresh:
            cached = self.cache.get(cache_params)
            if cached is not None:
                log.info(f"arXiv cache hit for query: {params.get('query', params.get('id_list', ''))}")
                return [CollectorResult(**r) for r in cached]

        self.rate_limiter.wait()

        max_results = params.get("max_results", self.max_results)
        fetch_full_text = params.get("fetch_full_text", False)

        raw_results: list[Any] = []

        if "id_list" in params:
            # Fetch by specific IDs
            search = arxiv_api.Search(
                id_list=params["id_list"],
                max_results=len(params["id_list"]),
            )
        else:
            query = params.get("query", "")
            if not query:
                return []
            search = arxiv_api.Search(
                query=query,
                max_results=max_results,
                sort_by=arxiv_api.SortCriterion.Relevance,
            )

        try:
            raw_results = list(search.results())
        except Exception as e:
            log.warning(f"arXiv API error: {e}")
            return []

        results = []
        for paper in raw_results:
            text = paper.summary
            meta: dict[str, Any] = {
                "tags": list(paper.categories),
                "authors": [a.name for a in paper.authors],
                "published": str(paper.published) if paper.published else "",
                "updated": str(paper.updated) if paper.updated else "",
                "pdf_url": paper.pdf_url,
                "comment": getattr(paper, "comment", ""),
                "journal_ref": getattr(paper, "journal_ref", ""),
            }

            if fetch_full_text and paper.pdf_url:
                full_text = self._fetch_pdf_text(paper.pdf_url)
                if full_text:
                    text = full_text
                    meta["extraction"] = "pdf_full_text"

            results.append(CollectorResult(
                source_id=paper.entry_id,
                title=paper.title,
                text=text,
                url=paper.entry_id,
                metadata=meta,
            ))

        # Cache results (serializable format)
        if results:
            self.cache.set(cache_params, [asdict_safe(r) for r in results])

        return results

    def _fetch_pdf_text(self, pdf_url: str) -> str | None:
        """Download PDF and extract text using PyMuPDF."""
        if not HAS_PYMUPDF:
            log.warning("PyMuPDF not installed, can't extract PDF text. Install: pip install pymupdf")
            return None

        self.rate_limiter.wait()

        import urllib.request

        try:
            req = urllib.request.Request(
                pdf_url,
                headers={"User-Agent": "hpc-agent-research/1.0"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()

            doc = fitz.open(stream=data, filetype="pdf")
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text())
            doc.close()
            return "\n\n".join(text_parts)

        except Exception as e:
            log.warning(f"PDF fetch failed for {pdf_url}: {e}")
            return None

    def to_tasks(self, results: list[CollectorResult], **kwargs) -> list[dict]:
        """Generate paper analysis tasks from arXiv results."""
        model = kwargs.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        max_chars = kwargs.get("max_chars", 8000)
        tasks = []

        for r in results:
            # Truncate very long text to avoid prompt bloat
            text = r.text
            if len(text) > max_chars:
                text = text[:max_chars] + f"\n\n...[truncated from {len(r.text)} chars]"

            meta = {
                "type": "paper_analysis",
                "source": self.source,
                "source_id": r.source_id,
                "source_url": r.url,
                "title": r.title,
                "tags": r.metadata.get("tags", []),
                "provenance": {
                    "collector": "arxiv/1.0",
                    "collected_at": time.time(),
                    "source": self.source,
                },
            }

            tasks.append({
                "prompt": f"Analyze this research paper:\n\nTitle: {r.title}\n\n{text}",
                "model": model,
                "metadata": meta,
            })

        return tasks


def asdict_safe(r: CollectorResult) -> dict:
    """Serialize CollectorResult for caching."""
    return {
        "source_id": r.source_id,
        "title": r.title,
        "text": r.text,
        "url": r.url,
        "metadata": r.metadata,
    }
