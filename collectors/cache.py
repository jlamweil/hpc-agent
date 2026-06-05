"""Simple per-collector local cache."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


class CollectorCache:
    """Filesystem cache for collector results.
    
    One JSON-lines file per collector. Each line is a cached entry:
      {"key": "...", "results": [...], "cached_at": timestamp, "ttl": seconds}
    """

    def __init__(self, cache_dir: Path, ttl: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "cache.jsonl"
        self.ttl = ttl
        self._entries: dict[str, dict] | None = None

    def _load(self) -> dict[str, dict]:
        if self._entries is not None:
            return self._entries
        self._entries = {}
        if self.cache_file.exists():
            for line in self.cache_file.read_text().splitlines():
                if line.strip():
                    try:
                        entry = json.loads(line)
                        self._entries[entry["key"]] = entry
                    except (json.JSONDecodeError, KeyError):
                        continue
        return self._entries

    def _save(self):
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(e, default=str) for e in self._entries.values()]
        self.cache_file.write_text("\n".join(lines) + "\n")

    @staticmethod
    def _make_key(params: dict) -> str:
        raw = json.dumps(params, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def get(self, params: dict) -> list[dict] | None:
        """Return cached results if fresh, or None."""
        entries = self._load()
        key = self._make_key(params)
        entry = entries.get(key)
        if entry is None:
            return None
        age = time.time() - entry["cached_at"]
        if age > entry.get("ttl", self.ttl):
            return None
        return entry["results"]

    def set(self, params: dict, results: list[dict]):
        """Cache results for params."""
        entries = self._load()
        key = self._make_key(params)
        entries[key] = {
            "key": key,
            "params": params,
            "results": results,
            "cached_at": time.time(),
            "ttl": self.ttl,
        }
        self._save()

    def invalidate(self, params: dict):
        """Remove a cached entry."""
        entries = self._load()
        key = self._make_key(params)
        entries.pop(key, None)
        self._save()

    def clear_expired(self):
        """Remove expired entries."""
        entries = self._load()
        now = time.time()
        expired = [
            k for k, v in entries.items()
            if now - v["cached_at"] > v.get("ttl", self.ttl)
        ]
        for k in expired:
            entries.pop(k, None)
        if expired:
            self._save()

    def stats(self) -> dict:
        """Return cache hit/miss/entry stats."""
        entries = self._load()
        now = time.time()
        fresh = sum(1 for v in entries.values() if now - v["cached_at"] <= v.get("ttl", self.ttl))
        expired = len(entries) - fresh
        return {
            "total_entries": len(entries),
            "fresh": fresh,
            "expired": expired,
        }


class RateLimiter:
    """Simple per-source rate limiter."""

    def __init__(self, requests_per_second: float = 3.0):
        self.min_interval = 1.0 / max(requests_per_second, 0.1)
        self._last_call: float = 0.0

    def wait(self):
        """Sleep if needed to respect rate limit."""
        now = time.time()
        elapsed = now - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.time()
