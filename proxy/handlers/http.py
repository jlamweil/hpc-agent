"""HTTP/HTTPS request handler with SSRF protection."""

import logging
import socket
from urllib.parse import urlparse

from ..schema import FetchRequest, FetchResponse

logger = logging.getLogger(__name__)

# Private / reserved IP ranges (IPv4) to block for SSRF prevention.
SSRF_BLOCKED_PREFIXES = (
    "10.",
    "127.",
    "0.",
)
SSRF_BLOCKED_NETWORKS = (
    ("172.16", "172.31"),
    ("192.168", "192.168"),
)
SSRF_BLOCKED_PORTS = (22, 23, 25, 135, 139, 445, 3389, 5900, 6379, 27017)


def _is_ssrf_target(url: str) -> bool:
    """Check whether *url* resolves to a private/reserved IP or blocked port."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except Exception:
        return True  # fail closed

    # Blocked ports
    if port in SSRF_BLOCKED_PORTS:
        logger.warning("SSRF block: port %d in %s", port, url)
        return True

    # Check for private IP patterns
    if host.startswith(SSRF_BLOCKED_PREFIXES):
        logger.warning("SSRF block (prefix match): %s", url)
        return True

    for lo, hi in SSRF_BLOCKED_NETWORKS:
        if host.startswith(lo) or host.startswith(hi):
            logger.warning("SSRF block (network match): %s", url)
            return True

    # Resolve hostname and check resolved IP
    try:
        addrs = socket.getaddrinfo(host, port, socket.AF_INET)
        for family, _type, _proto, _canon, sa in addrs:
            ip = sa[0]
            if ip.startswith(SSRF_BLOCKED_PREFIXES):
                logger.warning("SSRF block (resolved IP %s): %s", ip, url)
                return True
            for lo, hi in SSRF_BLOCKED_NETWORKS:
                if ip.startswith(lo) or ip.startswith(hi):
                    logger.warning("SSRF block (resolved IP %s): %s", ip, url)
                    return True
    except OSError:
        logger.warning("SSRF block (DNS resolution failed): %s", url)
        return True

    return False


def handle(request: FetchRequest, config: dict) -> FetchResponse:
    """Execute an HTTP request with SSRF protection and size limits.

    Expects the optional *requests* library.
    """
    try:
        import requests as req_lib
    except ImportError:
        return FetchResponse(
            id=request.id,
            status=500,
            error="HTTP handler requires 'requests' library: pip install requests",
        )

    max_size = config.get("max_response_size", 5_242_880)
    timeout = request.timeout or config.get("request_timeout", 30)

    # SSRF check
    if _is_ssrf_target(request.url):
        return FetchResponse(
            id=request.id,
            status=403,
            error="SSRF blocked: target resolves to a private/reserved IP",
        )

    try:
        method = request.method.upper()
        req_kwargs = dict(
            url=request.url,
            headers=request.headers or {},
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )
        if method == "GET":
            resp = req_lib.get(**req_kwargs)
        elif method == "POST":
            req_kwargs["data"] = request.body
            resp = req_lib.post(**req_kwargs)
        elif method == "HEAD":
            resp = req_lib.head(**req_kwargs)
            resp.raise_for_status()
            return FetchResponse(
                id=request.id,
                status=resp.status_code,
                headers=dict(resp.headers),
                fetched_at=__import__("time").time(),
            )
        else:
            req_kwargs["data"] = request.body
            resp = req_lib.request(method, **req_kwargs)

        resp.raise_for_status()

        # Read with size limit
        chunks = []
        total = 0
        truncated = False
        for chunk in resp.iter_content(chunk_size=65536, decode_unicode=False):
            chunks.append(chunk)
            total += len(chunk)
            if total > max_size:
                truncated = True
                break

        body_bytes = b"".join(chunks)
        # Try to decode; fall back to latin-1 if utf-8 fails
        try:
            body = body_bytes.decode("utf-8")
        except UnicodeDecodeError:
            body = body_bytes.decode("latin-1")

        return FetchResponse(
            id=request.id,
            status=resp.status_code,
            body=body,
            headers=dict(resp.headers),
            truncated=truncated,
            fetched_at=__import__("time").time(),
        )

    except req_lib.exceptions.Timeout:
        return FetchResponse(
            id=request.id,
            status=504,
            error=f"Request timed out after {timeout}s",
        )
    except req_lib.exceptions.ConnectionError as exc:
        return FetchResponse(
            id=request.id,
            status=502,
            error=f"Connection error: {exc}",
        )
    except req_lib.exceptions.HTTPError as exc:
        return FetchResponse(
            id=request.id,
            status=resp.status_code if "resp" in dir() else 502,
            body=resp.text[:2000] if "resp" in dir() else "",
            error=str(exc),
        )
    except Exception as exc:
        logger.exception("HTTP handler error for %s", request.id)
        return FetchResponse(
            id=request.id,
            status=500,
            error=f"HTTP handler error: {exc}",
        )
