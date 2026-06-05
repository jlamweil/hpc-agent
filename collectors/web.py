"""Web collector — search and scrape web content for research tasks."""

from __future__ import annotations

import logging
import re
import time
from typing import Any
from urllib.parse import urlparse

from collectors.base import BaseCollector, CollectorResult
from collectors.cache import CollectorCache, RateLimiter
from collectors.config import get_collector_config, get_cache_dir

log = logging.getLogger(__name__)

# ── Optional dependencies ──────────────────────────────────────────────

try:
    import requests as _requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    _requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup as _BeautifulSoup

    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    _BeautifulSoup = None  # type: ignore[assignment]

try:
    from playwright.sync_api import sync_playwright as _sync_playwright

    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    _sync_playwright = None  # type: ignore[assignment]

# ── Collector ──────────────────────────────────────────────────────────


class WebCollector(BaseCollector):
    """Search the web and fetch URL content.

    Modes (via ``collect()`` params):

    * ``{"query": "...", "max_results": 5}`` — web search via DuckDuckGo Lite
    * ``{"url": "https://..."}`` — fetch and extract main content via BeautifulSoup
    * ``{"query": "...", "scrape": true}`` — search, then scrape full text of each result
    """

    name = "web"
    source = "web"

    def __init__(self):
        cfg = get_collector_config("web")
        self.cache_ttl: int = cfg.get("cache_ttl", 0)  # 0 = no cache for searches
        self.cache = CollectorCache(get_cache_dir("web"), ttl=self.cache_ttl)
        self.rate_limiter = RateLimiter(cfg.get("rate_limit", 10.0))
        self.user_agent: str = cfg.get(
            "user_agent", "hpc-agent-research/1.0"
        )
        self._playwright_enabled: bool = cfg.get("playwright", False) and HAS_PLAYWRIGHT

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, params: dict) -> list[CollectorResult]:
        """Fetch data from the web.

        Accepts ``refresh=True`` to bypass cache.
        """
        refresh = params.pop("refresh", False)
        cache_params = {k: v for k, v in sorted(params.items()) if k != "refresh"}

        # Respect cache_ttl = 0 (no caching, especially for searches)
        if not refresh and self.cache_ttl > 0:
            cached = self.cache.get(cache_params)
            if cached is not None:
                log.info("Web cache hit for %s", cache_params)
                return [CollectorResult(**r) for r in cached]

        self.rate_limiter.wait()

        if "url" in params:
            results = self._fetch_url(str(params["url"]))
        elif "query" in params:
            max_results = int(params.get("max_results", 5))
            results = self._search(str(params["query"]), max_results)
            if params.get("scrape") and results:
                results = self._scrape_results(results)
        else:
            return []

        # Only cache non-search results (or when cache_ttl > 0)
        if results and self.cache_ttl > 0 and "url" in params:
            self.cache.set(cache_params, [_asdict_safe(r) for r in results])

        return results

    def to_tasks(self, results: list[CollectorResult], **kwargs) -> list[dict]:
        """Convert web results to ``web_research`` tasks."""
        model = kwargs.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        tasks: list[dict] = []

        for r in results:
            meta: dict[str, Any] = {
                "type": "web_research",
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
            tasks.append({
                "prompt": f"Source: {r.title}\nURL: {r.url}\n\n{r.text}",
                "model": model,
                "metadata": meta,
                "depends_on": kwargs.get("depends_on"),
            })

        return tasks

    # ------------------------------------------------------------------
    # Search (DuckDuckGo Lite)
    # ------------------------------------------------------------------

    def _search(self, query: str, max_results: int) -> list[CollectorResult]:
        """Search via DuckDuckGo Lite (no API key required)."""
        if not HAS_REQUESTS or _requests is None:
            raise ImportError(
                "requests required for web search. Install: pip install requests"
            )

        try:
            resp = _requests.post(
                "https://lite.duckduckgo.com/lite/",
                data={"q": query},
                headers={"User-Agent": self.user_agent},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            log.warning("DuckDuckGo search failed: %s", exc)
            return []

        if not HAS_BS4 or _BeautifulSoup is None:
            # Fallback: return raw text if BeautifulSoup is not available
            raw = resp.text[:5000]
            return [
                CollectorResult(
                    source_id="search/raw",
                    title=f"Search results for: {query}",
                    text=raw,
                    url="",
                    metadata={
                        "tags": ["web", "search"],
                        "query": query,
                        "raw": True,
                    },
                )
            ]

        soup = _BeautifulSoup(resp.text, "html.parser")
        results: list[CollectorResult] = []

        # DuckDuckGo Lite: results appear as <a rel="nofollow" href="..."> in the page.
        # Each link is typically in a table cell with a snippet nearby.
        for i, link in enumerate(soup.select('a[rel="nofollow"]')):
            if i >= max_results:
                break
            href = link.get("href", "")
            title = link.get_text(strip=True) or href

            # Extract snippet from surrounding text nodes
            snippet = ""
            parent_td = link.find_parent("td")
            if parent_td:
                # Get text after the <a> — e.g. the snippet text in the same cell
                raw_text = parent_td.get_text(separator=" ", strip=True)
                # Remove the title text from the beginning
                snippet = raw_text[len(title):].strip()

            results.append(
                CollectorResult(
                    source_id=f"search/{i + 1}",
                    title=title,
                    text=snippet,
                    url=href,
                    metadata={
                        "tags": ["web", "search"],
                        "query": query,
                        "rank": i + 1,
                    },
                )
            )

        return results

    # ------------------------------------------------------------------
    # URL fetch
    # ------------------------------------------------------------------

    def _fetch_url(self, url: str) -> list[CollectorResult]:
        """Fetch a URL and extract its main text content.

        Uses Playwright if enabled and available; falls back to requests + BS4.
        """
        if self._playwright_enabled:
            return self._fetch_url_playwright(url)
        return self._fetch_url_requests(url)

    def _fetch_url_requests(self, url: str) -> list[CollectorResult]:
        """Fetch a URL using requests and extract text with BeautifulSoup."""
        if not HAS_REQUESTS or _requests is None:
            raise ImportError(
                "requests required for URL fetching. Install: pip install requests"
            )

        try:
            resp = _requests.get(
                url,
                headers={"User-Agent": self.user_agent},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as exc:
            log.warning("URL fetch failed for %s: %s", url, exc)
            return []

        content_type = resp.headers.get("content-type", "")
        html = resp.text

        # Extract title
        title = url
        text = html

        if HAS_BS4 and _BeautifulSoup is not None:
            soup = _BeautifulSoup(html, "html.parser")

            # Title
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True) or url

            # Main content
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            main = soup.find("main") or soup.find("article") or soup.find("body") or soup
            text = main.get_text(separator="\n", strip=True)
            # Collapse repeated blank lines
            text = re.sub(r"\n{3,}", "\n\n", text)
            if len(text) > 10000:
                text = text[:10000] + "\n\n...[truncated]"

        return [
            CollectorResult(
                source_id=f"url/{urlparse(url).netloc}",
                title=title,
                text=text,
                url=url,
                metadata={
                    "tags": ["web", "url"],
                    "url": url,
                    "content_type": content_type,
                    "status_code": resp.status_code,
                },
            )
        ]

    def _fetch_url_playwright(self, url: str) -> list[CollectorResult]:
        """Fetch a JS-rendered page via Playwright.

        Falls back to requests-based fetch on any failure.
        """
        if not HAS_PLAYWRIGHT or _sync_playwright is None:
            log.warning("Playwright not available, falling back to requests")
            return self._fetch_url_requests(url)

        try:
            with _sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                title = page.title()
                content = page.content()
                browser.close()
        except Exception as exc:
            log.warning("Playwright fetch failed for %s: %s", url, exc)
            return self._fetch_url_requests(url)

        text = content
        if HAS_BS4 and _BeautifulSoup is not None:
            soup = _BeautifulSoup(content, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            main = soup.find("main") or soup.find("article") or soup.find("body") or soup
            text = main.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            if len(text) > 10000:
                text = text[:10000] + "\n\n...[truncated]"

        return [
            CollectorResult(
                source_id=f"url/{urlparse(url).netloc}",
                title=title or url,
                text=text,
                url=url,
                metadata={
                    "tags": ["web", "url", "playwright"],
                    "url": url,
                    "renderer": "playwright",
                },
            )
        ]

    # ------------------------------------------------------------------
    # Scrape mode: search then fetch each result
    # ------------------------------------------------------------------

    def _scrape_results(
        self, results: list[CollectorResult]
    ) -> list[CollectorResult]:
        """Scrape full text for each search result."""
        scraped: list[CollectorResult] = []
        for r in results:
            self.rate_limiter.wait()
            fetched = self._fetch_url(r.url)
            if fetched:
                cr = fetched[0]
                cr.metadata["tags"] = ["web", "search", "scraped"]
                cr.metadata["search_query"] = r.metadata.get("query", "")
                cr.metadata["search_rank"] = r.metadata.get("rank", 0)
                scraped.append(cr)
            else:
                # Keep the search snippet as fallback
                scraped.append(r)
        return scraped


def _asdict_safe(r: CollectorResult) -> dict:
    """Serialize a CollectorResult for caching."""
    return {
        "source_id": r.source_id,
        "title": r.title,
        "text": r.text,
        "url": r.url,
        "metadata": r.metadata,
    }
