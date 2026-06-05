"""arXiv API proxy handler — search or fetch paper metadata."""

import logging
import time
import urllib.parse
import xml.etree.ElementTree as ET

from ..schema import FetchRequest, FetchResponse

logger = logging.getLogger(__name__)

ARXIV_API_BASE = "http://export.arxiv.org/api/query"


def _parse_atom(xml_text: str, max_results: int = 10) -> list[dict]:
    """Parse arXiv Atom XML response into structured dicts."""
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    papers = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("arXiv XML parse error: %s", exc)
        return []

    entries = root.findall("atom:entry", ns)
    for entry in entries[:max_results]:
        paper = {
            "id": _text_or(entry.find("atom:id", ns)),
            "title": _text_or(entry.find("atom:title", ns)).replace("\n", " ").strip(),
            "summary": _text_or(entry.find("atom:summary", ns)).replace("\n", " ").strip(),
            "published": _text_or(entry.find("atom:published", ns)),
            "updated": _text_or(entry.find("atom:updated", ns)),
            "authors": [],
            "links": {},
            "categories": [],
        }

        for author in entry.findall("atom:author", ns):
            name = _text_or(author.find("atom:name", ns))
            if name:
                paper["authors"].append(name)

        for link in entry.findall("atom:link", ns):
            rel = link.get("rel", "")
            href = link.get("href", "")
            if rel == "alternate":
                paper["links"]["abstract"] = href
            elif rel == "related":
                paper["links"]["doi"] = href

        for cat in entry.findall("arxiv:primary_category", ns):
            term = cat.get("term", "")
            if term:
                paper["categories"].append(term)
        for cat in entry.findall("atom:category", ns):
            term = cat.get("term", "")
            if term and term not in paper["categories"]:
                paper["categories"].append(term)

        papers.append(paper)

    return papers


def _text_or(elem) -> str:
    """Return element text or empty string."""
    return elem.text or "" if elem is not None else ""


def handle(request: FetchRequest, config: dict) -> FetchResponse:
    """Search arXiv or fetch paper metadata.

    Two modes:
      - **Search**: Provide a query string in *url* or *body*.
        Uses ``search_query=all:<query>``.
      - **Fetch by ID**: Provide an arXiv ID (e.g. ``2101.12345``
        or ``astro-ph/0501001``) in *url*. Uses ``id_list=...``.

    Returns JSON with a list of paper entries.
    """
    try:
        import requests as req_lib
    except ImportError:
        return FetchResponse(
            id=request.id,
            status=500,
            error="arXiv handler requires 'requests' library: pip install requests",
        )

    timeout = request.timeout or config.get("request_timeout", 30)
    config_max = config.get("max_results", 50)
    max_results = min(config_max, 50)  # arXiv max is 50 per query

    query = (request.url or request.body or "").strip()
    if not query:
        return FetchResponse(
            id=request.id,
            status=400,
            error="No arXiv query or ID provided (use url field)",
        )

    # Determine if this is an ID lookup or a search
    # arXiv IDs look like: 1234.56789, cond-mat/1234567, etc.
    is_id_lookup = bool(
        re_search := __import__("re").match(
            r"^[\w.-]+/\d{6,7}$|^\d{4}\.\d{4,5}(v\d+)?$", query
        )
    )

    params = {
        "max_results": max_results,
    }
    if is_id_lookup:
        params["id_list"] = query
    else:
        params["search_query"] = f"all:{query}"

    try:
        resp = req_lib.get(
            ARXIV_API_BASE,
            params=params,
            headers={"User-Agent": "hpc-agent-arxiv-proxy/1.0"},
            timeout=timeout,
        )
        resp.raise_for_status()

        papers = _parse_atom(resp.text, max_results=max_results)

        return FetchResponse(
            id=request.id,
            status=200,
            body=__import__("json").dumps(
                {"query": query, "papers": papers, "count": len(papers)},
                indent=2,
            ),
            headers={"content-type": "application/json"},
            fetched_at=time.time(),
        )

    except req_lib.exceptions.Timeout:
        return FetchResponse(
            id=request.id,
            status=504,
            error=f"arXiv API timed out after {timeout}s",
        )
    except req_lib.exceptions.ConnectionError as exc:
        return FetchResponse(
            id=request.id,
            status=502,
            error=f"arXiv API connection error: {exc}",
        )
    except Exception as exc:
        logger.exception("arXiv handler error for %s", request.id)
        return FetchResponse(
            id=request.id,
            status=500,
            error=f"arXiv handler error: {exc}",
        )
