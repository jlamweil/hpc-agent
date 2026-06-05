"""Fetch Proxy Server — internet access bridge for air-gapped HPC compute nodes.

Watches a BeeGFS-mounted request directory, dispatches fetch requests
to the appropriate handler, and writes responses back to the response
directory.  Designed to run on a node that *does* have internet access.
"""

import argparse
import json
import logging
import os
import pathlib
import sys
import time

from .schema import FetchRequest, FetchResponse
from .handlers import dispatch

logger = logging.getLogger(__name__)


class FetchProxyServer:
    """Poll-based fetch proxy that bridges HPC → internet via filesystem exchange."""

    def __init__(
        self,
        request_dir: str,
        response_dir: str,
        heartbeat_path: str | None = None,
        config: dict | None = None,
    ):
        self.request_dir = pathlib.Path(request_dir)
        self.response_dir = pathlib.Path(response_dir)
        self.heartbeat_path = pathlib.Path(heartbeat_path) if heartbeat_path else None
        self.config = config or {}
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Main poll loop: scan request_dir, dispatch, write responses."""
        self._running = True
        self._ensure_dirs()
        self._write_heartbeat()

        last_cleanup = 0.0
        cleanup_interval = self.config.get("stale_request_cleanup", 600)

        logger.info(
            "FetchProxyServer watching %s → %s",
            self.request_dir,
            self.response_dir,
        )

        while self._running:
            try:
                self._process_pending_requests()
                self._write_heartbeat()

                # Periodic cleanup of stale requests
                now = time.time()
                if now - last_cleanup > cleanup_interval:
                    self._cleanup_stale_requests()
                    last_cleanup = now

                time.sleep(self.config.get("poll_interval", 5))
            except KeyboardInterrupt:
                logger.info("Shutdown requested.")
                self._running = False
                break
            except Exception:
                logger.exception("Error in main loop")
                time.sleep(5)

    def stop(self) -> None:
        """Signal the main loop to exit."""
        self._running = False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_dirs(self) -> None:
        self.request_dir.mkdir(parents=True, exist_ok=True)
        self.response_dir.mkdir(parents=True, exist_ok=True)

    def _process_pending_requests(self) -> None:
        for req_path in sorted(self.request_dir.iterdir()):
            if not req_path.is_file() or req_path.name.startswith("."):
                continue
            if not self._running:
                break

            # Read and parse the request file
            request = self._read_request_file(req_path)
            if request is None:
                continue

            # Dispatch to appropriate handler
            logger.info("Dispatching request %s (type=%s)", request.id, request.type)
            response = dispatch(request, self.config)

            # Write response
            self._write_response(response)

            # Remove the request file
            try:
                req_path.unlink()
            except OSError:
                logger.warning("Could not remove request file %s", req_path)

    def _read_request_file(self, path: pathlib.Path) -> FetchRequest | None:
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            return FetchRequest(**data)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            logger.warning("Invalid request file %s: %s", path, exc)
            # Write an error response and remove the bad file
            error_resp = FetchResponse(
                id=path.stem,
                status=400,
                error=f"Invalid request format: {exc}",
            )
            self._write_response(error_resp)
            try:
                path.unlink()
            except OSError:
                pass
            return None

    def _write_response(self, response: FetchResponse) -> None:
        resp_path = self.response_dir / f"{response.id}.json"
        try:
            resp_path.write_text(
                json.dumps(response.__dict__, indent=2, default=str),
                encoding="utf-8",
            )
            logger.info("Wrote response %s", resp_path)
        except OSError as exc:
            logger.error("Failed to write response %s: %s", resp_path, exc)

    def _write_heartbeat(self) -> None:
        if self.heartbeat_path is None:
            return
        interval = self.config.get("heartbeat_interval", 10)
        try:
            heartbeat = {
                "status": "alive",
                "timestamp": time.time(),
                "pid": os.getpid(),
                "request_dir": str(self.request_dir),
                "response_dir": str(self.response_dir),
            }
            self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            self.heartbeat_path.write_text(
                json.dumps(heartbeat, indent=2), encoding="utf-8"
            )
        except OSError as exc:
            logger.warning("Failed to write heartbeat: %s", exc)

    def _cleanup_stale_requests(self) -> None:
        """Remove request files older than *stale_request_cleanup* seconds."""
        max_age = self.config.get("stale_request_cleanup", 600)
        now = time.time()
        cleaned = 0
        for req_path in self.request_dir.iterdir():
            if not req_path.is_file() or req_path.name.startswith("."):
                continue
            try:
                mtime = req_path.stat().st_mtime
                if now - mtime > max_age:
                    req_path.unlink()
                    cleaned += 1
            except OSError:
                pass
        if cleaned > 0:
            logger.info("Cleaned up %d stale request files", cleaned)


# ------------------------------------------------------------------
# Supervisor: simple while True restart
# ------------------------------------------------------------------

def supervisor_main(config: dict) -> None:
    """Run the proxy server in a restart loop."""
    request_dir = config.get("proxy", {}).get(
        "hpc_dir", os.path.expanduser("~/hpc-agent")
    )
    response_dir = request_dir  # same parent, different subdir

    req = os.path.join(os.path.expanduser(request_dir), "fetch-requests")
    resp = os.path.join(os.path.expanduser(response_dir), "fetch-responses")
    heartbeat = os.path.join(os.path.expanduser(request_dir), "proxy_heartbeat.json")

    proxy_config = config.get("proxy", {})

    while True:
        logger.info("Starting FetchProxyServer ...")
        server = FetchProxyServer(
            request_dir=req,
            response_dir=resp,
            heartbeat_path=heartbeat,
            config=proxy_config,
        )
        try:
            server.run()
        except Exception as exc:
            logger.exception("FetchProxyServer exited: %s", exc)
        logger.warning("FetchProxyServer crashed at %s, restarting...", time.ctime())
        time.sleep(2)


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main() -> None:
    """Console entry point for ``python3 -m proxy.server``."""
    parser = argparse.ArgumentParser(
        description="Fetch Proxy Server — internet bridge for HPC compute nodes"
    )
    parser.add_argument(
        "--request-dir",
        default=os.path.expanduser("~/hpc-agent/fetch-requests"),
        help="Directory to watch for fetch request JSON files",
    )
    parser.add_argument(
        "--response-dir",
        default=os.path.expanduser("~/hpc-agent/fetch-responses"),
        help="Directory to write fetch response JSON files",
    )
    parser.add_argument(
        "--heartbeat",
        default=None,
        help="Path to heartbeat JSON file (default: request-dir/../proxy_heartbeat.json)",
    )
    parser.add_argument(
        "--poll",
        type=int,
        default=5,
        help="Poll interval in seconds (default: 5)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Default request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=5_242_880,
        help="Max response body size in bytes (default: 5 MB)",
    )
    parser.add_argument(
        "--cleanup",
        type=int,
        default=600,
        help="Stale request cleanup interval in seconds (default: 600)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--supervise",
        action="store_true",
        help="Wrap in a while-true restart loop",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    config = {
        "poll_interval": args.poll,
        "request_timeout": args.timeout,
        "max_response_size": args.max_size,
        "stale_request_cleanup": args.cleanup,
        "heartbeat_interval": 10,
    }

    request_dir = args.request_dir
    response_dir = args.response_dir
    heartbeat = args.heartbeat

    if heartbeat is None:
        # Default: parent of response_dir
        parent = os.path.dirname(os.path.normpath(response_dir))
        heartbeat = os.path.join(parent, "proxy_heartbeat.json")

    if args.supervise:
        # Wrap in restart loop
        while True:
            logger.info("Starting FetchProxyServer ...")
            server = FetchProxyServer(
                request_dir=request_dir,
                response_dir=response_dir,
                heartbeat_path=heartbeat,
                config=config,
            )
            try:
                server.run()
            except Exception as exc:
                logger.exception("FetchProxyServer exited: %s", exc)
            logger.warning("FetchProxyServer crashed at %s, restarting...", time.ctime())
            time.sleep(2)
    else:
        server = FetchProxyServer(
            request_dir=request_dir,
            response_dir=response_dir,
            heartbeat_path=heartbeat,
            config=config,
        )
        server.run()


if __name__ == "__main__":
    main()
