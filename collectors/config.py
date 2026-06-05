"""Per-collector configuration loader."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def _load_config() -> dict:
    """Load config.yaml from the project root."""
    candidates = [
        Path(__file__).resolve().parent.parent / "config.yaml",
        Path.cwd() / "config.yaml",
    ]
    for path in candidates:
        if path.exists():
            try:
                with open(path) as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                return {}
    return {}


def get_collector_config(name: str) -> dict[str, Any]:
    """Get configuration for a specific collector.
    
    Reads from config.yaml collectors.<name> section.
    Falls back to sensible defaults.
    """
    config = _load_config()
    collectors = config.get("collectors", {})
    return collectors.get(name, {})


def get_cache_dir(name: str) -> Path:
    """Get the cache directory for a collector."""
    base = Path.home() / ".cache" / "hpc-agent" / "collectors" / name
    base.mkdir(parents=True, exist_ok=True)
    return base


def get_env_key(name: str, key: str) -> str | None:
    """Read API key from environment variable.
    
    Checks collector config for token_env, then falls back to
    a conventional env var name like <NAME>_TOKEN.
    """
    cfg = get_collector_config(name)
    env_var = cfg.get("token_env", f"{name.upper()}_TOKEN")
    return os.environ.get(env_var) or None
