"""Type registry — known task types and their metadata schemas.

Adding a new type requires one entry in this dict.
No code changes elsewhere.
"""

from __future__ import annotations

from typing import Any

TYPE_REGISTRY: dict[str, dict[str, Any]] = {
    "paper_analysis": {
        "description": "Single paper analysis (arXiv, PDF, etc.)",
        "required_metadata": ["source", "source_id", "title"],
        "optional_metadata": ["tags", "domain", "provenance"],
    },
    "code_review": {
        "description": "Code review of a specific file or module",
        "required_metadata": ["source", "source_id"],
        "optional_metadata": ["language", "file_path", "repo_url"],
    },
    "repo_analysis": {
        "description": "Full repository architecture analysis",
        "required_metadata": ["source", "source_id", "language"],
        "optional_metadata": ["file_count", "stars", "topics"],
    },
    "web_research": {
        "description": "Web search result analysis",
        "required_metadata": ["source", "query"],
        "optional_metadata": ["urls", "tags"],
    },
    "comparison": {
        "description": "Multi-item comparison",
        "required_metadata": ["source", "items"],
        "optional_metadata": ["criteria"],
    },
    "scaffolding": {
        "description": "Code scaffolding / baseline generation",
        "required_metadata": ["source", "source_id"],
        "optional_metadata": ["competition", "dataset"],
    },
    "data_analysis": {
        "description": "Dataset or metric analysis",
        "required_metadata": ["source", "source_id"],
        "optional_metadata": ["columns", "row_count", "dataset"],
    },
    "generic": {
        "description": "Generic prompt — no special schema",
        "required_metadata": [],
        "optional_metadata": [],
    },
}


def known_types() -> list[str]:
    return list(TYPE_REGISTRY.keys())


def get_type_info(type_name: str) -> dict | None:
    return TYPE_REGISTRY.get(type_name)


def register_type(name: str, description: str, required: list[str] | None = None,
                  optional: list[str] | None = None):
    """Register a new task type at runtime."""
    TYPE_REGISTRY[name] = {
        "description": description,
        "required_metadata": required or [],
        "optional_metadata": optional or [],
    }
