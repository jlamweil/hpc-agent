"""GitHub API proxy handler."""

import logging
import os
import time

from ..schema import FetchRequest, FetchResponse

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"


def _get_token() -> str | None:
    """Read GitHub token from environment."""
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")


def handle(request: FetchRequest, config: dict) -> FetchResponse:
    """Proxy a request to the GitHub REST API.

    The request *url* should be the path after ``https://api.github.com``,
    e.g. ``/repos/user/repo`` or a full URL is also accepted and rewritten.

    Adds Authorization header if GITHUB_TOKEN is set.
    """
    try:
        import requests as req_lib
    except ImportError:
        return FetchResponse(
            id=request.id,
            status=500,
            error="GitHub handler requires 'requests' library: pip install requests",
        )

    timeout = request.timeout or config.get("request_timeout", 30)

    # Build the target URL
    url = request.url.strip()
    if url.startswith("https://"):
        target = url
    elif url.startswith("/"):
        target = GITHUB_API_BASE + url
    else:
        target = GITHUB_API_BASE + "/" + url.lstrip("/")

    # Headers
    headers = dict(request.headers or {})
    token = _get_token()
    if token:
        headers.setdefault("Authorization", f"Bearer {token}")
    headers.setdefault("Accept", "application/vnd.github.v3+json")
    headers.setdefault("User-Agent", "hpc-agent-github-proxy/1.0")

    try:
        method = request.method.upper()
        req_kwargs = dict(url=target, headers=headers, timeout=timeout)

        if method == "GET":
            resp = req_lib.get(**req_kwargs)
        elif method == "POST":
            resp = req_lib.post(**req_kwargs, json=__import__("json").loads(request.body or "{}"))
        elif method == "PUT":
            resp = req_lib.put(**req_kwargs, json=__import__("json").loads(request.body or "{}"))
        elif method == "DELETE":
            resp = req_lib.delete(**req_kwargs)
        elif method == "PATCH":
            resp = req_lib.patch(**req_kwargs, json=__import__("json").loads(request.body or "{}"))
        else:
            resp = req_lib.request(method, **req_kwargs)

        # Respect rate-limit headers
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining is not None and int(remaining) == 0:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
            wait = max(0, reset_ts - int(time.time()))
            logger.warning("GitHub API rate limit exhausted, reset in %ds", wait)

        return FetchResponse(
            id=request.id,
            status=resp.status_code,
            body=resp.text,
            headers=dict(resp.headers),
            fetched_at=time.time(),
        )

    except req_lib.exceptions.Timeout:
        return FetchResponse(
            id=request.id,
            status=504,
            error=f"GitHub API timed out after {timeout}s",
        )
    except req_lib.exceptions.ConnectionError as exc:
        return FetchResponse(
            id=request.id,
            status=502,
            error=f"GitHub API connection error: {exc}",
        )
    except Exception as exc:
        logger.exception("GitHub handler error for %s", request.id)
        return FetchResponse(
            id=request.id,
            status=500,
            error=f"GitHub handler error: {exc}",
        )
