"""Web search handler using DuckDuckGo lite API."""

import logging
import re
import time
import urllib.parse

from ..schema import FetchRequest, FetchResponse

logger = logging.getLogger(__name__)

DDG_LITE_URL = "https://lite.duckduckgo.com/lite/"


def _parse_ddg_lite(html: str, max_results: int = 10) -> list[dict]:
    """Parse DuckDuckGo lite HTML results into structured dicts."""
    results = []
    # DDG lite returns results in a simple table format.
    # Each result row has a link in <a> and text in subsequent <td>.
    rows = re.findall(
        r'<tr[^>]*>.*?<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>.*?<td[^>]*class="result-snippet"[^>]*>(.*?)</td>.*?</tr>',
        html,
        re.DOTALL,
    )
    for url, title, snippet in rows:
        if len(results) >= max_results:
            break
        results.append(
            {
                "title": _clean_html(title),
                "url": url,
                "snippet": _clean_html(snippet),
            }
        )

    # Fallback: simpler pattern if the above misses results
    if not results:
        simple_rows = re.findall(
            r'class="result-link"[^>]*>.*?<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>',
            html,
            re.DOTALL,
        )
        for url, title in simple_rows[:max_results]:
            results.append(
                {
                    "title": _clean_html(title),
                    "url": url,
                    "snippet": "",
                }
            )

    return results


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode common entities."""
    text = re.sub(r"<[^>]*>", "", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#x27;", "'")
    return text.strip()


def handle(request: FetchRequest, config: dict) -> FetchResponse:
    """Perform a web search via DuckDuckGo lite.

    The request *url* field is treated as the search query if present,
    otherwise *body* is used.
    """
    try:
        import requests as req_lib
    except ImportError:
        return FetchResponse(
            id=request.id,
            status=500,
            error="Search handler requires 'requests' library: pip install requests",
        )

    timeout = request.timeout or config.get("request_timeout", 30)

    query = request.url.strip() if request.url else (request.body or "").strip()
    if not query:
        return FetchResponse(
            id=request.id,
            status=400,
            error="No search query provided (use url or body field)",
        )

    params = {"q": query}
    user_agent = config.get("user_agent", "hpc-agent-research/1.0")

    try:
        resp = req_lib.get(
            DDG_LITE_URL,
            params=params,
            headers={"User-Agent": user_agent},
            timeout=timeout,
        )
        resp.raise_for_status()

        results = _parse_ddg_lite(resp.text, max_results=10)

        return FetchResponse(
            id=request.id,
            status=200,
            body=__import__("json").dumps(
                {"query": query, "results": results, "count": len(results)},
                indent=2,
            ),
            headers={"content-type": "application/json"},
            fetched_at=time.time(),
        )

    except req_lib.exceptions.Timeout:
        return FetchResponse(
            id=request.id,
            status=504,
            error=f"Search timed out after {timeout}s",
        )
    except req_lib.exceptions.ConnectionError as exc:
        return FetchResponse(
            id=request.id,
            status=502,
            error=f"Search connection error: {exc}",
        )
    except Exception as exc:
        logger.exception("Search handler error for %s", request.id)
        return FetchResponse(
            id=request.id,
            status=500,
            error=f"Search handler error: {exc}",
        )
