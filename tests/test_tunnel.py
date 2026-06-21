"""Tests for worker/tunnel.py — SSH tunnel discovery and lifecycle."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import subprocess
import signal
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def reset_tunnel_globals():
    """Reset tunnel module globals after each test for isolation."""
    yield
    from worker import tunnel
    tunnel.close_tunnel()


class TestFindVllmNode:
    """find_vllm_node() queries squeue for a running dsv4-fast job."""

    def test_returns_node_when_running(self):
        """When squeue returns a running dsv4-fast job, return the compute node."""
        from worker.tunnel import find_vllm_node

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "node-001\n"
        mock_result.stderr = ""

        with patch.object(subprocess, "run", return_value=mock_result) as mock_run:
            node = find_vllm_node("user@host.example.com")

        assert node == "node-001"
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "squeue" in str(args)

    def test_returns_none_when_no_job(self):
        """When squeue returns nothing, return None."""
        from worker.tunnel import find_vllm_node

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch.object(subprocess, "run", return_value=mock_result):
            node = find_vllm_node("user@host.example.com")

        assert node is None

    def test_returns_none_on_ssh_failure(self):
        """When SSH fails, return None."""
        from worker.tunnel import find_vllm_node

        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(
            cmd=["ssh", "..."], timeout=15,
        )):
            node = find_vllm_node("user@host.example.com")

        assert node is None


class TestTunnelLifecycle:
    """ensure_tunnel() and close_tunnel() lifecycle."""

    def test_creates_tunnel_and_returns_port(self):
        """Ensure tunnel creates SSH process and returns local port."""
        from worker.tunnel import ensure_tunnel, close_tunnel, _TUNNEL_PID

        mock_proc = MagicMock()
        mock_proc.pid = 12345
        mock_proc.returncode = None  # Still running

        with patch.object(subprocess, "Popen", return_value=mock_proc):
            with patch("os.kill", return_value=None) as mock_kill:
                with patch("socket.socket") as mock_socket:
                    mock_sock = MagicMock()
                    mock_socket.return_value.__enter__.return_value = mock_sock
                    # First connect fails, second succeeds
                    mock_sock.connect.side_effect = [OSError("not yet"), None]

                    with patch("subprocess.run") as mock_lsof:
                        mock_lsof_result = MagicMock()
                        mock_lsof_result.returncode = 0
                        mock_lsof_result.stdout = "12345\n"
                        mock_lsof.return_value = mock_lsof_result

                        port = ensure_tunnel(
                            "user@host.example.com",
                            "node-001",
                            tunnel_port=8100,
                        )

        assert port == 8100

    def test_reuses_existing_tunnel_for_same_node(self):
        """If tunnel exists and is alive for same node, reuse it."""
        from worker.tunnel import ensure_tunnel, _TUNNEL_PID, _TUNNEL_NODE

        # Set up existing tunnel
        import worker.tunnel as t
        t._TUNNEL_PID = 12345
        t._TUNNEL_NODE = "node-001"

        with patch("os.kill", return_value=None) as mock_kill:
            port = ensure_tunnel("user@host.example.com", "node-001")

        assert port == 8100
        mock_kill.assert_called_once_with(12345, 0)  # Just a health check

    def test_closes_existing_tunnel_before_new_one(self):
        """If tunnel exists for different node, close it first."""
        from worker.tunnel import ensure_tunnel

        import worker.tunnel as t
        t._TUNNEL_PID = 99999
        t._TUNNEL_NODE = "node-999"

        mock_proc = MagicMock()
        mock_proc.pid = 54321

        with patch("os.kill", return_value=None) as mock_kill:
            with patch.object(subprocess, "Popen", return_value=mock_proc):
                with patch("socket.socket") as mock_socket:
                    mock_sock = MagicMock()
                    mock_socket.return_value.__enter__.return_value = mock_sock
                    mock_sock.connect.side_effect = [None]  # Immediate success

                    with patch("subprocess.run") as mock_lsof:
                        mock_lsof_result = MagicMock()
                        mock_lsof_result.returncode = 0
                        mock_lsof_result.stdout = "54321\n"
                        mock_lsof.return_value = mock_lsof_result

                        port = ensure_tunnel(
                            "user@host.example.com",
                            "node-001",
                            tunnel_port=8100,
                        )

        assert port == 8100
        # Should have killed old tunnel (SIGTERM) and created new one
        assert mock_kill.call_count >= 1

    def test_close_tunnel_kills_process_and_clears_state(self):
        """close_tunnel() sends SIGTERM and resets globals."""
        from worker.tunnel import close_tunnel

        import worker.tunnel as t
        t._TUNNEL_PID = 12345
        t._TUNNEL_NODE = "node-001"

        with patch("os.kill", return_value=None) as mock_kill:
            close_tunnel()

        mock_kill.assert_called_once_with(12345, signal.SIGTERM)
        assert t._TUNNEL_PID is None
        assert t._TUNNEL_NODE == ""

    def test_close_tunnel_noop_when_no_tunnel(self):
        """close_tunnel() with no active tunnel is a no-op."""
        from worker.tunnel import close_tunnel
        # No tunnel set up
        close_tunnel()  # Should not raise
