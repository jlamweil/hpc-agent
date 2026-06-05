"""GitHub collector — clone repos, extract files, search code."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from collectors.base import BaseCollector, CollectorResult
from collectors.cache import CollectorCache, RateLimiter
from collectors.config import get_collector_config, get_cache_dir, get_env_key

log = logging.getLogger(__name__)

try:
    import requests

    HAS_REQUESTS: bool = True
except ImportError:  # pragma: no cover
    HAS_REQUESTS = False
    requests = None  # type: ignore[assignment]


# ── Constants ────────────────────────────────────────────────────────────────

# Key config files to read when present (checked by basename)
KEY_CONFIG_FILES: set[str] = {
    "package.json",
    "Cargo.toml",
    "requirements.txt",
    "pyproject.toml",
    "go.mod",
    "go.sum",
    "Gemfile",
    "Gemfile.lock",
    "Pipfile",
    "Pipfile.lock",
    "setup.py",
    "setup.cfg",
    "CMakeLists.txt",
    "Makefile",
    "Dockerfile",
    "docker-compose.yml",
    ".gitignore",
    "tsconfig.json",
    "composer.json",
    "pom.xml",
    "build.gradle",
    "README.md",
    "README.rst",
}

# Extension → language name mapping (for primary language detection)
EXTENSION_LANGUAGE_MAP: dict[str, str] = {
    ".py": "Python",
    ".js": "JavaScript",
    ".ts": "TypeScript",
    ".tsx": "TypeScript/React",
    ".jsx": "JavaScript/React",
    ".go": "Go",
    ".rs": "Rust",
    ".java": "Java",
    ".kt": "Kotlin",
    ".rb": "Ruby",
    ".php": "PHP",
    ".c": "C",
    ".h": "C",
    ".cpp": "C++",
    ".hpp": "C++",
    ".cs": "C#",
    ".swift": "Swift",
    ".scala": "Scala",
    ".r": "R",
    ".m": "MATLAB",
    ".sql": "SQL",
    ".sh": "Shell",
    ".bash": "Shell",
    ".zsh": "Shell",
    ".yaml": "YAML",
    ".yml": "YAML",
    ".json": "JSON",
    ".toml": "TOML",
    ".md": "Markdown",
    ".ipynb": "Jupyter Notebook",
    ".vue": "Vue",
    ".svelte": "Svelte",
    ".lua": "Lua",
    ".ex": "Elixir",
    ".exs": "Elixir",
    ".clj": "Clojure",
    ".cljs": "ClojureScript",
    ".dart": "Dart",
    ".lisp": "Lisp",
    ".hs": "Haskell",
    ".zig": "Zig",
    ".nim": "Nim",
    ".cr": "Crystal",
    ".fs": "F#",
    ".fsx": "F#",
}

# Files at repo root whose presence hints at framework/ecosystem
ECOSYSTEM_HINTS: dict[str, str] = {
    "next.config.js": "Next.js",
    "next.config.ts": "Next.js",
    "vite.config.ts": "Vite",
    "vite.config.js": "Vite",
    "astro.config.mjs": "Astro",
    "svelte.config.js": "SvelteKit",
    "remix.config.js": "Remix",
    "tailwind.config.js": "Tailwind CSS",
    "tailwind.config.ts": "Tailwind CSS",
    "Dockerfile": "Docker",
    "docker-compose.yml": "Docker Compose",
    "Tiltfile": "Tilt",
}


# ── URL parsing ──────────────────────────────────────────────────────────────

_GITHUB_URL_PATTERNS = [
    re.compile(r"github\.com[:/]([^/]+)/([^/#?]+?)(?:\.git)?(?:/.*)?$"),
    re.compile(r"github\.com[:/]([^/]+)/([^/#?]+)(?:/.*)?$"),
]


def _parse_github_url(url: str) -> tuple[str, str] | None:
    """Extract (owner, repo) from a GitHub URL, or None."""
    for pat in _GITHUB_URL_PATTERNS:
        m = pat.search(url)
        if m:
            owner = m.group(1)
            repo = m.group(2).rstrip("/").replace(".git", "")
            return owner, repo
    return None


# ── File-tree helpers ────────────────────────────────────────────────────────


def _build_file_tree(repo_path: Path, max_depth: int = 4) -> dict[str, Any]:
    """Walk a cloned repo and return a file tree dict.

    Returns {rel_path: {"type": "file"|"dir", "size": int, "ext": str}}.
    Skips .git, hidden files at root (except .github), and paths beyond max_depth.
    """
    tree: dict[str, Any] = {}
    str_repo = str(repo_path)

    if not repo_path.exists():
        return tree

    for root, dirs, files in os.walk(str_repo, topdown=True):
        root_path = Path(root)
        root_rel = root_path.relative_to(repo_path)
        parts = root_rel.parts

        # Skip .git
        if ".git" in dirs:
            dirs.remove(".git")

        # Skip hidden dirs at root (except .github)
        if parts and parts[0].startswith(".") and parts[0] not in (".github",):
            dirs.clear()
            continue

        # Depth limit
        if len(parts) > max_depth:
            dirs.clear()
            continue

        # Record directory
        if parts:
            tree[str(root_rel)] = {"type": "dir"}

        # Record files
        for fname in files:
            fpath = root_path / fname
            try:
                size = fpath.stat().st_size
                rel_path = str(root_rel / fname) if parts else fname
                tree[rel_path] = {
                    "type": "file",
                    "size": size,
                    "ext": Path(fname).suffix.lower(),
                }
            except OSError:
                continue

    return tree


def _detect_language(file_tree: dict[str, Any]) -> str:
    """Detect primary language by counting file extensions."""
    ext_counts: dict[str, int] = {}
    for info in file_tree.values():
        if info.get("type") == "file" and info.get("ext"):
            ext = info["ext"]
            ext_counts[ext] = ext_counts.get(ext, 0) + 1

    if not ext_counts:
        return "Unknown"

    top_ext = max(ext_counts, key=ext_counts.get)  # type: ignore
    return EXTENSION_LANGUAGE_MAP.get(top_ext, f"(.{top_ext.lstrip('.')})")


def _detect_ecosystems(repo_path: Path) -> list[str]:
    """Detect framework/ecosystem hints from root-level files."""
    found: list[str] = []
    for fname, ecosys in ECOSYSTEM_HINTS.items():
        if (repo_path / fname).exists():
            found.append(ecosys)
    return found


def _extract_readme(repo_path: Path) -> str | None:
    """Find and read README file (case-insensitive). Returns None if missing."""
    if not repo_path.exists():
        return None
    for p in repo_path.iterdir():
        if p.is_file() and p.name.lower().startswith("readme"):
            try:
                return p.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
    return None


def _read_file_safe(file_path: Path, max_size: int) -> str | None:
    """Read a text file if within size limit. Returns None on any failure."""
    try:
        if not file_path.exists() or not file_path.is_file():
            return None
        if file_path.stat().st_size > max_size:
            return None
        return file_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


# ── Git subprocess ───────────────────────────────────────────────────────────


def _clone_repo(url: str, clone_path: Path, clone_depth: int = 1, timeout: int = 120) -> bool:
    """Shallow-clone a git repo. Returns True on success."""
    try:
        clone_path.parent.mkdir(parents=True, exist_ok=True)

        # Wipe existing clone directory if present
        if clone_path.exists():
            shutil.rmtree(clone_path)

        cmd = ["git", "clone", "--depth", str(clone_depth), url, str(clone_path)]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            log.warning("Git clone failed for %s: %s", url, result.stderr.strip())
            return False
        return True
    except subprocess.TimeoutExpired:
        log.warning("Git clone timed out for %s (>%ss)", url, timeout)
        return False
    except Exception as e:
        log.warning("Git clone exception for %s: %s", url, e)
        return False


# ── GitHub API helpers ───────────────────────────────────────────────────────


def _github_api_headers() -> dict[str, str]:
    """Build headers with optional auth token."""
    headers: dict[str, str] = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "hpc-agent-research/1.0",
    }
    token = get_env_key("github", "token_env")
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _github_api_get(url: str, params: dict | None = None, timeout: int = 15) -> dict | None:
    """GET GitHub API JSON, or None on failure."""
    if not HAS_REQUESTS:
        log.warning("requests not installed; GitHub API unavailable")
        return None
    try:
        resp = requests.get(url, headers=_github_api_headers(), params=params, timeout=timeout)  # type: ignore[union-attr]
        if resp.status_code == 404:
            return None
        if resp.status_code == 403:
            log.warning("GitHub API rate limit hit (or forbidden): %s", url)
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:  # type: ignore[union-attr]
        log.warning("GitHub API request failed: %s", e)
        return None


def _check_repo_public(owner: str, repo: str) -> bool:
    """Return True if the repo exists and is public/accessible."""
    url = f"https://api.github.com/repos/{owner}/{repo}"
    data = _github_api_get(url)
    if data is None:
        return False
    if data.get("private", False):
        log.warning("Repository %s/%s is private — skipping", owner, repo)
        return False
    return True


def _search_github_repos(query: str, language: str | None = None, max_results: int = 5) -> list[dict[str, Any]]:
    """Search GitHub repos via the REST API. Returns list of repo summaries."""
    url = "https://api.github.com/search/repositories"
    q = query
    if language:
        q += f"+language:{language}"

    params = {
        "q": q,
        "sort": "stars",
        "order": "desc",
        "per_page": min(max_results, 100),
    }

    data = _github_api_get(url, params=params)
    if data is None:
        return []

    repos = []
    for item in data.get("items", [])[:max_results]:
        repos.append({
            "id": item["id"],
            "full_name": item["full_name"],
            "html_url": item["html_url"],
            "description": item.get("description") or "",
            "stars": item.get("stargazers_count", 0),
            "language": item.get("language") or "",
            "topics": item.get("topics", []),
            "updated_at": item.get("updated_at", ""),
        })
    return repos


# ── Collector ────────────────────────────────────────────────────────────────


class GitHubCollector(BaseCollector):
    """Fetch repository metadata, file trees, and code from GitHub.

    Supports three modes:

    1. **Repo clone** — ``{"url": "https://github.com/user/repo"}``
       Shallow-clone the repo, build a file tree, read README and key config
       files, and detect the primary programming language.

    2. **Repo clone + specific files** — ``{"url": "...", "files": ["src/main.py"]}``
       Same as above, but also extract the content of the listed files.

    3. **Search** — ``{"query": "transformers", "language": "python", "max_results": 5}``
       Search GitHub repositories via the REST API and return summaries.
    """

    name = "github"
    source = "github"

    def __init__(self):
        cfg = get_collector_config("github")
        self.cache_ttl = cfg.get("cache_ttl", 300)
        self.max_file_size = cfg.get("max_file_size", 512000)
        self.clone_depth = cfg.get("clone_depth", 1)
        self.rate_limiter = RateLimiter(cfg.get("rate_limit", 30))
        self.cache_dir = get_cache_dir("github")
        self.cache = CollectorCache(self.cache_dir, ttl=self.cache_ttl)

    # ── public API ───────────────────────────────────────────────────────

    def collect(self, params: dict) -> list[CollectorResult]:
        """Fetch data from GitHub.

        Parameters
        ----------
        params : dict
            See class docstring for admissible keys.
        refresh : bool, optional
            Bypass cache when True.

        Returns
        -------
        list[CollectorResult]
        """
        params = dict(params)  # defensive copy
        refresh = params.pop("refresh", False)

        # -- cache lookup --
        cache_params = {k: v for k, v in sorted(params.items())}
        if not refresh:
            cached = self.cache.get(cache_params)
            if cached is not None:
                log.info("GitHub cache hit for %s", cache_params)
                return [CollectorResult(**r) for r in cached]

        # -- routing --
        if "url" in params:
            results = self._collect_repo(params)
        elif "query" in params:
            results = self._collect_search(params)
        else:
            log.warning("GitHub collector requires 'url' or 'query' parameter")
            return []

        # -- cache --
        if results:
            self.cache.set(cache_params, [asdict_safe(r) for r in results])

        return results

    def to_tasks(self, results: list[CollectorResult], **kwargs) -> list[dict]:
        """Convert collector results to task dicts for HPC dispatch.

        Task types produced:
          * ``repo_analysis`` — single repo with file-tree info, no file payload
          * ``code_review`` — single repo where specific file contents were requested
          * ``comparison`` — search results (multiple repos bundled into one task)
        """
        model = kwargs.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        max_chars = kwargs.get("max_chars", 8000)
        tasks: list[dict] = []

        if len(results) > 1:
            # ── Comparison task (search results) ──
            query = results[0].metadata.get("query", "GitHub repositories")
            title = f"Comparison: {query}"
            text_parts: list[str] = [
                f"Compare the following GitHub repositories found for query \"{query}\":\n",
            ]
            for i, r in enumerate(results, 1):
                text = (r.text or "")[: max_chars // max(len(results), 1)]
                text_parts.append(f"--- Repository {i}: {r.title} ---\n{text}\n")

            meta = self._base_meta(
                task_type="comparison",
                source_id="|".join(r.source_id for r in results),
                source_url=results[0].url,
                title=title,
                tags=["github", "comparison", query],
            )
            tasks.append(self._make_task("\n".join(text_parts), model, meta))

        else:
            for r in results:
                has_files = bool(r.metadata.get("requested_files_extracted"))
                task_type = "code_review" if has_files else "repo_analysis"
                text = r.text or ""
                if len(text) > max_chars:
                    text = text[:max_chars] + f"\n\n...[truncated from {len(r.text)} chars]"

                meta = self._base_meta(
                    task_type=task_type,
                    source_id=r.source_id,
                    source_url=r.url,
                    title=r.title,
                    tags=r.metadata.get("tags", []),
                )
                prefix = "Review the following code from" if task_type == "code_review" else "Analyze this GitHub repository"
                tasks.append(self._make_task(f"{prefix} {r.title}:\n\n{text}", model, meta))

        return tasks

    # ── internal: repo clone mode ────────────────────────────────────────

    def _collect_repo(self, params: dict) -> list[CollectorResult]:
        url = params["url"]
        parsed = _parse_github_url(url)
        if not parsed:
            log.warning("Could not parse GitHub URL: %s", url)
            return []
        owner, repo_name = parsed
        repo_full = f"{owner}/{repo_name}"

        # Verify repo exists and is public
        self.rate_limiter.wait()
        if not _check_repo_public(owner, repo_name):
            return []

        # Shallow clone
        clone_path = self.cache_dir / "repos" / owner / repo_name
        self.rate_limiter.wait()
        if not _clone_repo(url, clone_path, self.clone_depth):
            log.warning("Failed to clone %s", repo_full)
            return []

        # Build file tree
        file_tree = _build_file_tree(clone_path)
        language = _detect_language(file_tree)
        ecosystems = _detect_ecosystems(clone_path)

        # README
        readme_text = _extract_readme(clone_path) or ""

        # Key config files
        config_files: dict[str, str] = {}
        for cf in KEY_CONFIG_FILES:
            content = _read_file_safe(clone_path / cf, self.max_file_size)
            if content is not None:
                config_files[cf] = content

        # Specific requested files
        requested_files = params.get("files", [])
        file_contents: dict[str, str] = {}
        for fname in requested_files:
            content = _read_file_safe(clone_path / fname, self.max_file_size)
            if content is not None:
                file_contents[fname] = content

        # ── Build text payload ──
        text = self._format_repo_text(
            repo_full=repo_full,
            url=url,
            language=language,
            ecosystems=ecosystems,
            file_tree=file_tree,
            readme=readme_text,
            config_files=config_files,
            file_contents=file_contents,
        )

        # ── Metadata ──
        languages_found: set[str] = set()
        for info in file_tree.values():
            if info.get("type") == "file" and info.get("ext"):
                lang = EXTENSION_LANGUAGE_MAP.get(info["ext"])
                if lang:
                    languages_found.add(lang)

        metadata: dict[str, Any] = {
            "repo": repo_full,
            "owner": owner,
            "repo_name": repo_name,
            "url": url,
            "language": language,
            "languages_found": sorted(languages_found) if languages_found else [language],
            "ecosystems": ecosystems,
            "file_count": sum(1 for v in file_tree.values() if v["type"] == "file"),
            "dir_count": sum(1 for v in file_tree.values() if v["type"] == "dir"),
            "has_readme": bool(readme_text),
            "config_files_found": list(config_files.keys()),
            "requested_files_extracted": list(file_contents.keys()),
            "tags": ["github", "repo", language.lower()] if language != "Unknown" else ["github", "repo"],
        }

        return [
            CollectorResult(
                source_id=repo_full,
                title=repo_full,
                text=text,
                url=url,
                metadata=metadata,
            )
        ]

    # ── internal: search mode ────────────────────────────────────────────

    def _collect_search(self, params: dict) -> list[CollectorResult]:
        query = params.get("query", "")
        if not query:
            return []

        language = params.get("language")
        max_results = params.get("max_results", 5)

        if not HAS_REQUESTS:
            raise ImportError("requests package required for GitHub search. Install: pip install requests")

        self.rate_limiter.wait()
        repos = _search_github_repos(query, language, max_results)

        results: list[CollectorResult] = []
        for repo in repos:
            text = (
                f"Repository: {repo['full_name']}\n"
                f"URL: {repo['html_url']}\n"
                f"Stars: {repo['stars']}\n"
                f"Language: {repo['language']}\n"
                f"Description: {repo['description']}\n"
            )
            if repo.get("topics"):
                text += f"Topics: {', '.join(repo['topics'])}\n"
            text += f"Last updated: {repo.get('updated_at', 'N/A')}"

            metadata: dict[str, Any] = {
                "repo": repo["full_name"],
                "url": repo["html_url"],
                "stars": repo["stars"],
                "language": repo["language"],
                "topics": repo.get("topics", []),
                "description": repo.get("description", ""),
                "query": query,
                "tags": ["github", "search", query],
            }

            results.append(
                CollectorResult(
                    source_id=f"github:search:{repo['full_name']}",
                    title=f"GitHub: {repo['full_name']}",
                    text=text,
                    url=repo["html_url"],
                    metadata=metadata,
                )
            )

        return results

    # ── helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _format_repo_text(
        repo_full: str,
        url: str,
        language: str,
        ecosystems: list[str],
        file_tree: dict[str, Any],
        readme: str,
        config_files: dict[str, str],
        file_contents: dict[str, str],
    ) -> str:
        """Assemble the text payload for a repo clone result."""
        lines: list[str] = []
        file_count = sum(1 for v in file_tree.values() if v["type"] == "file")
        dir_count = sum(1 for v in file_tree.values() if v["type"] == "dir")

        lines.append(f"Repository: {repo_full}")
        lines.append(f"URL: {url}")
        lines.append(f"Detected Language: {language}")
        if ecosystems:
            lines.append(f"Ecosystems: {', '.join(ecosystems)}")
        lines.append(f"Total files: {file_count}")
        lines.append(f"Total directories: {dir_count}")
        lines.append("")

        # File tree (capped at 200 entries)
        lines.append("── File Tree ──")
        entries = list(file_tree.items())
        # Show dirs first, then files
        dir_entries = [(p, v) for p, v in entries if v["type"] == "dir"]
        file_entries = [(p, v) for p, v in entries if v["type"] == "file"]
        shown = 0
        for path_str, info in dir_entries[:50]:
            lines.append(f"  {path_str}/")
            shown += 1
        for path_str, info in file_entries[:150]:
            size_str = f" ({info['size']} bytes)" if info.get("size") else ""
            lines.append(f"  {path_str}{size_str}")
            shown += 1
        remaining = len(entries) - shown
        if remaining > 0:
            lines.append(f"  ... and {remaining} more entries")
        lines.append("")

        # README
        if readme:
            lines.append("── README ──")
            if len(readme) > 5000:
                lines.append(readme[:5000] + "\n...[truncated]")
            else:
                lines.append(readme)
            lines.append("")

        # Key config files
        if config_files:
            lines.append("── Key Config Files ──")
            for cf_name, cf_content in config_files.items():
                lines.append(f"\n### {cf_name} ###")
                if len(cf_content) > 3000:
                    lines.append(cf_content[:3000] + "\n...[truncated]")
                else:
                    lines.append(cf_content)
            lines.append("")

        # Requested files
        if file_contents:
            lines.append("── Requested Files ──")
            for fname, fcontent in file_contents.items():
                lines.append(f"\n### {fname} ###")
                if len(fcontent) > 5000:
                    lines.append(fcontent[:5000] + f"\n...[truncated from {len(fcontent)} chars]")
                else:
                    lines.append(fcontent)
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _base_meta(
        task_type: str,
        source_id: str,
        source_url: str,
        title: str,
        tags: list[str],
    ) -> dict[str, Any]:
        return {
            "type": task_type,
            "source": "github",
            "source_id": source_id,
            "source_url": source_url,
            "title": title,
            "tags": tags,
            "provenance": {
                "collector": "github/1.0",
                "collected_at": time.time(),
                "source": "github",
            },
        }

    @staticmethod
    def _make_task(prompt: str, model: str, meta: dict[str, Any]) -> dict:
        return {
            "prompt": prompt,
            "model": model,
            "metadata": meta,
        }


# ── Serialisation helper ────────────────────────────────────────────────────


def asdict_safe(r: CollectorResult) -> dict:
    """Serialize a CollectorResult for caching (JSON-safe)."""
    return {
        "source_id": r.source_id,
        "title": r.title,
        "text": r.text,
        "url": r.url,
        "metadata": r.metadata,
    }
