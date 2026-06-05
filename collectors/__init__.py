"""Collectors — fetch research data from external sources and submit as HPC tasks."""

from collectors.base import BaseCollector, CollectorResult, validate_metadata
from collectors.config import get_collector_config
from collectors.arxiv import ArxivCollector

__all__ = [
    "BaseCollector",
    "CollectorResult",
    "ArxivCollector",
    "validate_metadata",
    "get_collector_config",
]

_COLLECTOR_REGISTRY: dict[str, type[BaseCollector]] = {}


def register(name: str, cls: type[BaseCollector]):
    _COLLECTOR_REGISTRY[name] = cls


# Auto-register built-in collectors
register("arxiv", ArxivCollector)

try:
    from collectors.github import GitHubCollector  # type: ignore[import-untyped]
    register("github", GitHubCollector)
    __all__.append("GitHubCollector")
except ImportError:
    pass

try:
    from collectors.kaggle import KaggleCollector  # type: ignore[import-untyped]
    register("kaggle", KaggleCollector)
    __all__.append("KaggleCollector")
except ImportError:
    pass

try:
    from collectors.web import WebCollector  # type: ignore[import-untyped]
    register("web", WebCollector)
    __all__.append("WebCollector")
except ImportError:
    pass


def get_collector(name: str) -> type[BaseCollector]:
    if name in _COLLECTOR_REGISTRY:
        return _COLLECTOR_REGISTRY[name]
    raise KeyError(f"Unknown collector '{name}'. Available: {list(_COLLECTOR_REGISTRY.keys())}")


def list_collectors() -> list[str]:
    return list(_COLLECTOR_REGISTRY.keys())
