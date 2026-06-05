"""Kaggle collector — fetch competition and dataset metadata."""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import quote

from collectors.base import BaseCollector, CollectorResult
from collectors.cache import CollectorCache, RateLimiter
from collectors.config import get_collector_config, get_cache_dir, get_env_key

log = logging.getLogger(__name__)

try:
    import requests as _requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    _requests = None  # type: ignore[assignment]

try:
    import kagglehub

    HAS_KAGGLEHUB = True
except ImportError:
    HAS_KAGGLEHUB = False


class KaggleCollector(BaseCollector):
    """Fetch competition and dataset info from Kaggle.

    Modes (via ``collect()`` params):

    * ``{"competition": "titanic"}`` — competition details + optional dataset info
    * ``{"dataset": "user/dataset-name"}`` — dataset metadata
    * ``{"query": "nlp", "type": "competition", "max_results": 10}`` — search
    """

    name = "kaggle"
    source = "kaggle"

    def __init__(self):
        cfg = get_collector_config("kaggle")
        self.cache_ttl = cfg.get("cache_ttl", 86400)
        self.cache = CollectorCache(get_cache_dir("kaggle"), ttl=self.cache_ttl)
        self.rate_limiter = RateLimiter(cfg.get("rate_limit", 5.0))
        self.api_key = get_env_key("kaggle", "token") or None
        self.username = self._resolve_username()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, params: dict) -> list[CollectorResult]:
        """Fetch data from Kaggle.

        Accepts ``refresh=True`` to bypass cache.
        """
        refresh = params.pop("refresh", False)
        cache_params = {k: v for k, v in sorted(params.items()) if k != "refresh"}

        if not refresh:
            cached = self.cache.get(cache_params)
            if cached is not None:
                log.info("Kaggle cache hit for %s", cache_params)
                return [CollectorResult(**r) for r in cached]

        self.rate_limiter.wait()

        if "competition" in params:
            results = self._fetch_competition(str(params["competition"]))
        elif "dataset" in params:
            results = self._fetch_dataset(str(params["dataset"]))
        elif "query" in params:
            stype = params.get("type", "competition")
            max_results = params.get("max_results", 10)
            results = self._search(str(params["query"]), stype, int(max_results))
        else:
            return []

        if results:
            self.cache.set(cache_params, [_asdict_safe(r) for r in results])

        return results

    def to_tasks(self, results: list[CollectorResult], **kwargs) -> list[dict]:
        """Convert results to tasks.

        * Competitions → ``type: "scaffolding"``
        * Datasets     → ``type: "data_analysis"``
        """
        model = kwargs.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        tasks: list[dict] = []

        for r in results:
            is_competition = "competition" in r.source_id
            task_type = "scaffolding" if is_competition else "data_analysis"

            meta: dict[str, Any] = {
                "type": task_type,
                "source": self.source,
                "source_id": r.source_id,
                "source_url": r.url,
                "title": r.title,
                "tags": r.metadata.get("tags", []),
                "provenance": {
                    "collector": f"{self.name}/1.0",
                    "collected_at": time.time(),
                    "source": self.source,
                },
            }
            # Forward useful metadata fields
            for key in ("competition", "dataset"):
                if key in r.metadata:
                    meta[key] = r.metadata[key]

            tasks.append({
                "prompt": f"Title: {r.title}\n\n{r.text}",
                "model": model,
                "metadata": meta,
                "depends_on": kwargs.get("depends_on"),
            })

        return tasks

    # ------------------------------------------------------------------
    # Internals — Kaggle API
    # ------------------------------------------------------------------

    def _resolve_username(self) -> str:
        """Read KAGGLE_USERNAME from env."""
        import os

        return os.environ.get("KAGGLE_USERNAME", "")

    def _api_get(self, path: str) -> dict | list | None:
        """Authenticated GET to the Kaggle REST API v1.

        Returns parsed JSON or ``None`` on any failure.
        """
        if not HAS_REQUESTS or _requests is None:
            log.warning("Cannot call Kaggle API — requests not installed")
            return None
        if not self.api_key or not self.username:
            log.warning(
                "Kaggle API credentials not configured. "
                "Set KAGGLE_USERNAME + KAGGLE_KEY (or the env var in config.yaml)."
            )
            return None

        url = f"https://www.kaggle.com/api/v1/{path.lstrip('/')}"
        try:
            resp = _requests.get(
                url,
                auth=(self.username, self.api_key),  # type: ignore[arg-type]
                timeout=30,
                headers={"User-Agent": "hpc-agent-research/1.0"},
            )
            resp.raise_for_status()
            return resp.json()
        except _requests.RequestException:
            log.warning("Kaggle API request failed: %s", url, exc_info=True)
        except Exception as exc:
            log.warning("Unexpected error calling Kaggle API (%s): %s", url, exc)
        return None

    # ------------------------------------------------------------------
    # Competition mode
    # ------------------------------------------------------------------

    def _fetch_competition(self, competition: str) -> list[CollectorResult]:
        data = self._api_get(f"competitions/{competition}")
        if data is None:
            return self._competition_fallback(competition)

        if not isinstance(data, dict):
            return []

        title = data.get("title", competition)
        parts: list[str] = [f"Competition: {title}"]

        desc = data.get("description", "") or ""
        if desc:
            parts.append(f"\nDescription: {desc}")

        meta: dict[str, Any] = {
            "tags": ["kaggle", "competition"],
            "competition": competition,
            "category": data.get("category", ""),
            "reward": data.get("reward", ""),
            "deadline": data.get("deadline", ""),
            "team_count": data.get("teamCount", 0),
            "evaluation_metric": data.get("evaluationMetric", ""),
        }

        return [
            CollectorResult(
                source_id=f"competition/{competition}",
                title=title,
                text="\n".join(parts),
                url=f"https://www.kaggle.com/competitions/{competition}",
                metadata=meta,
            )
        ]

    def _competition_fallback(self, competition: str) -> list[CollectorResult]:
        """Fallback when API is unavailable — use kagglehub if possible."""
        if not HAS_KAGGLEHUB:
            log.warning(
                "No API credentials and kagglehub not installed. "
                "Install: pip install kagglehub"
            )
            return []

        try:
            path = kagglehub.competition_download(competition)  # type: ignore
            text = (
                f"Competition: {competition}\n"
                f"Downloaded to: {path}\n\n"
                "(Metadata unavailable — configure KAGGLE_USERNAME/KAGGLE_KEY "
                "for full details.)"
            )
            return [
                CollectorResult(
                    source_id=f"competition/{competition}",
                    title=f"Kaggle Competition: {competition}",
                    text=text,
                    url=f"https://www.kaggle.com/competitions/{competition}",
                    metadata={
                        "tags": ["kaggle", "competition"],
                        "competition": competition,
                    },
                )
            ]
        except Exception as exc:
            log.warning("kagglehub competition_download failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Dataset mode
    # ------------------------------------------------------------------

    def _fetch_dataset(self, dataset: str) -> list[CollectorResult]:
        data = self._api_get(f"datasets/{dataset}")
        if data is not None and isinstance(data, dict):
            title = data.get("title", dataset)
            desc = data.get("description", "") or ""
            text = f"Dataset: {title}\n\n{desc}" if desc else f"Dataset: {title}"

            meta: dict[str, Any] = {
                "tags": ["kaggle", "dataset"],
                "dataset": dataset,
                "size": data.get("datasetSize", ""),
                "download_count": data.get("downloadCount", 0),
                "kernel_count": data.get("kernelCount", 0),
                "usability_rating": data.get("usabilityRating", 0),
            }

            return [
                CollectorResult(
                    source_id=f"dataset/{dataset}",
                    title=title,
                    text=text,
                    url=f"https://www.kaggle.com/datasets/{dataset}",
                    metadata=meta,
                )
            ]

        # Fallback — kagglehub
        if HAS_KAGGLEHUB:
            try:
                path = kagglehub.dataset_download(dataset)  # type: ignore
                text = (
                    f"Dataset: {dataset}\n"
                    f"Downloaded to: {path}\n\n"
                    "(Metadata unavailable — configure credentials for full details.)"
                )
                return [
                    CollectorResult(
                        source_id=f"dataset/{dataset}",
                        title=f"Kaggle Dataset: {dataset}",
                        text=text,
                        url=f"https://www.kaggle.com/datasets/{dataset}",
                        metadata={
                            "tags": ["kaggle", "dataset"],
                            "dataset": dataset,
                        },
                    )
                ]
            except Exception as exc:
                log.warning("kagglehub dataset_download failed: %s", exc)

        log.warning("Could not fetch dataset '%s' — no API key and kagglehub unavailable", dataset)
        return []

    # ------------------------------------------------------------------
    # Search mode
    # ------------------------------------------------------------------

    def _search(self, query: str, stype: str, max_results: int) -> list[CollectorResult]:
        if stype == "competition":
            return self._search_competitions(query, max_results)
        elif stype == "dataset":
            return self._search_datasets(query, max_results)
        else:
            log.warning("Unknown Kaggle search type '%s' — must be 'competition' or 'dataset'", stype)
            return []

    def _search_competitions(self, query: str, max_results: int) -> list[CollectorResult]:
        data = self._api_get(
            f"competitions/list?search={quote(query)}&maxResults={max_results}"
        )
        if not data or not isinstance(data, list):
            return []

        results: list[CollectorResult] = []
        for i, comp in enumerate(data):
            if i >= max_results:
                break
            ref = comp.get("ref", "") or comp.get("id", "")
            results.append(
                CollectorResult(
                    source_id=f"competition/{ref}",
                    title=comp.get("title", ref) or ref,
                    text=comp.get("description", comp.get("subtitle", "")),
                    url=f"https://www.kaggle.com/competitions/{ref}",
                    metadata={
                        "tags": ["kaggle", "competition", "search"],
                        "category": comp.get("category", ""),
                        "reward": comp.get("reward", ""),
                        "team_count": comp.get("teamCount", 0),
                        "search_rank": i + 1,
                    },
                )
            )
        return results

    def _search_datasets(self, query: str, max_results: int) -> list[CollectorResult]:
        data = self._api_get(
            f"datasets/list?search={quote(query)}&maxResults={max_results}"
        )
        if not data or not isinstance(data, list):
            return []

        results: list[CollectorResult] = []
        for i, ds in enumerate(data):
            if i >= max_results:
                break
            ref = ds.get("ref", "")
            results.append(
                CollectorResult(
                    source_id=f"dataset/{ref}",
                    title=ds.get("title", ref) or ref,
                    text=ds.get("description", ""),
                    url=f"https://www.kaggle.com/datasets/{ref}",
                    metadata={
                        "tags": ["kaggle", "dataset", "search"],
                        "dataset": ref,
                        "download_count": ds.get("downloadCount", 0),
                        "usability_rating": ds.get("usabilityRating", 0),
                        "search_rank": i + 1,
                    },
                )
            )
        return results


def _asdict_safe(r: CollectorResult) -> dict:
    """Serialize a CollectorResult for caching (mirrors pattern in arxiv.py)."""
    return {
        "source_id": r.source_id,
        "title": r.title,
        "text": r.text,
        "url": r.url,
        "metadata": r.metadata,
    }
