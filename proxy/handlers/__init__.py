"""Handler registry and dispatch for fetch proxy requests."""

import importlib
import logging

from ..schema import FetchRequest, FetchResponse

logger = logging.getLogger(__name__)

HANDLERS = {
    "http": "proxy.handlers.http",
    "search": "proxy.handlers.search",
    "github": "proxy.handlers.github",
    "arxiv": "proxy.handlers.arxiv",
}


def dispatch(request: FetchRequest, config: dict) -> FetchResponse:
    """Route a FetchRequest to the appropriate handler module.

    Args:
        request: The parsed fetch request.
        config: Proxy configuration dict from the main config.

    Returns:
        A FetchResponse populated by the handler, or an error response
        if the request type is unknown or the handler fails.
    """
    mod_path = HANDLERS.get(request.type)
    if mod_path is None:
        return FetchResponse(
            id=request.id,
            status=400,
            error=f"Unknown request type: {request.type}",
        )

    try:
        mod = importlib.import_module(mod_path)
        handler = getattr(mod, "handle", None)
        if handler is None:
            return FetchResponse(
                id=request.id,
                status=500,
                error=f"Handler module '{mod_path}' has no handle() function",
            )
        return handler(request, config)
    except Exception:
        logger.exception("Handler dispatch failed for %s type=%s", request.id, request.type)
        return FetchResponse(
            id=request.id,
            status=500,
            error="Internal handler dispatch error",
        )
