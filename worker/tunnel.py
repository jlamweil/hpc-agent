"""SSH tunnel to HPC compute node vLLM.

Standalone module extracted from hpc_gateway.py:HpcProvider pattern.
Used by HermesRunner to tunnel to the HPC compute node's vLLM (port 8000).

Debugging: set TUNNEL_DEBUG=1 env var for verbose stderr logging.
"""

import os
import signal
import socket
import subprocess
import sys
import time

_TUNNEL_PID: int | None = None
_TUNNEL_NODE: str = ""
_TUNNEL_PORT: int = 8100

_DEBUG = int(os.environ.get("TUNNEL_DEBUG", "0"))


def _log(msg: str):
    if _DEBUG:
        print(f"[tunnel] {msg}", file=sys.stderr, flush=True)


def find_vllm_node(ssh_host: str) -> str | None:
    """Query squeue for a running dsv4-fast job, return compute node hostname."""
    _log(f"find_vllm_node(ssh_host={ssh_host})")
    try:
        result = subprocess.run(
            [
                "ssh", ssh_host,
                "squeue -u $(whoami) -o '%i %T %N %j' --noheader 2>/dev/null | "
                "awk '/dsv4-.*/ && /RUNNING/ {print $3; exit}'",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            node = result.stdout.strip()
            _log(f"find_vllm_node → {node}")
            return node
        _log(f"find_vllm_node → None (rc={result.returncode}, stderr={result.stderr.strip()[:100]})")
    except subprocess.TimeoutExpired:
        _log("find_vllm_node → None (SSH timeout)")
    except OSError as e:
        _log(f"find_vllm_node → None (OSError: {e})")
    return None


def ensure_tunnel(
    ssh_host: str, node: str, tunnel_port: int = 8100
) -> int | None:
    """Create SSH tunnel from localhost:tunnel_port to node:8000.

    Reuses existing tunnel if still alive and targeting the same node.
    Returns the local port number, or None on failure.
    """
    global _TUNNEL_PID, _TUNNEL_NODE, _TUNNEL_PORT

    _log(f"ensure_tunnel(ssh={ssh_host}, node={node}, port={tunnel_port})")

    # Reuse existing tunnel if still alive and for same node
    if _TUNNEL_PID is not None and _TUNNEL_NODE == node:
        try:
            os.kill(_TUNNEL_PID, 0)
            _log(f"Reusing existing tunnel PID {_TUNNEL_PID} → {node}:{tunnel_port}")
            return tunnel_port
        except (OSError, ProcessLookupError):
            _log(f"Stale tunnel PID {_TUNNEL_PID} is dead, closing")
            pass

    # Close any stale tunnel (wrong node or dead)
    close_tunnel()

    # Kill ANY process listening on our tunnel port, not just the one we
    # remember. This handles stale tunnels from prior daemon instances
    # where _TUNNEL_PID was never set.
    _kill_port_listeners(tunnel_port)

    # Create new SSH tunnel with keepalive so the tunnel self-destructs
    # when the remote side dies (instead of leaving a stale port listener).
    # ServerAliveInterval=15 + ServerAliveCountMax=3 = death within ~45s.
    _log(f"Creating SSH tunnel localhost:{tunnel_port} → {node}:8000")
    try:
        proc = subprocess.Popen(
            [
                "ssh",
                "-o", "ServerAliveInterval=15",
                "-o", "ServerAliveCountMax=3",
                "-L", f"{tunnel_port}:{node}:8000",
                "-N", "-f", ssh_host,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _log(f"SSH Popen PID={proc.pid}, waiting for port to listen...")

        # Wait for tunnel to actually listen (up to 10s)
        for attempt in range(20):
            time.sleep(0.5)
            try:
                s = socket.socket()
                s.settimeout(1)
                s.connect(("127.0.0.1", tunnel_port))
                s.close()

                # Found the tunnel — find PID via lsof
                lsof = subprocess.run(
                    ["lsof", "-ti", f"tcp:{tunnel_port}"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if lsof.returncode == 0 and lsof.stdout.strip():
                    _TUNNEL_PID = int(lsof.stdout.strip().split("\n")[0])
                    _TUNNEL_NODE = node
                    _TUNNEL_PORT = tunnel_port
                    _log(f"Tunnel UP PID={_TUNNEL_PID} port={tunnel_port}→{node}:8000")
                    return tunnel_port
                _log(f"  lsof attempt {attempt}: rc={lsof.returncode} out={lsof.stdout.strip()[:60]}")
            except Exception as e:
                if attempt == 0:
                    _log(f"  waiting for port {tunnel_port}... ({e})")
                continue

        _log(f"Tunnel FAILED — port {tunnel_port} never became available after 10s")
        return None
    except Exception as e:
        _log(f"Tunnel creation error: {e}")
        return None


def close_tunnel():
    """Close the SSH tunnel if open and reset state."""
    global _TUNNEL_PID, _TUNNEL_NODE
    if _TUNNEL_PID is not None:
        _log(f"Closing tunnel PID {_TUNNEL_PID}")
        try:
            os.kill(_TUNNEL_PID, signal.SIGTERM)
            _log("SIGTERM sent")
        except (OSError, ProcessLookupError) as e:
            _log(f"kill failed: {e}")
        _TUNNEL_PID = None
        _TUNNEL_NODE = ""
    else:
        _log("close_tunnel — no active tunnel to close")


def _kill_port_listeners(port: int):
    """Kill all processes listening on a given TCP port.

    Uses lsof to find listeners and sends SIGTERM. Handles the case
    where the port is held by an orphaned SSH process from a previous
    daemon instance.
    """
    try:
        lsof = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"],
            capture_output=True, text=True, timeout=5,
        )
        if lsof.returncode == 0 and lsof.stdout.strip():
            for pid in lsof.stdout.strip().split("\n"):
                pid = pid.strip()
                if not pid:
                    continue
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    _log(f"Killed port listener PID {pid} on port {port}")
                except (OSError, ValueError) as e:
                    _log(f"Failed to kill PID {pid}: {e}")
            # Give processes time to release the port
            time.sleep(1)
    except Exception as e:
        _log(f"_kill_port_listeners error: {e}")
