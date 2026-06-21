"""Tests for mcps/hpc_hermes.py — MCP server for HPC Hermes."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
pytest.importorskip("mcp", reason="mcp (FastMCP) package not installed")

import json
import sqlite3
import tempfile
import time
from unittest.mock import patch


def _make_db() -> str:
    """Create temp DB with full schema."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY, agent_id TEXT NOT NULL,
            messages TEXT NOT NULL, tools TEXT NOT NULL,
            model TEXT NOT NULL, api_config TEXT NOT NULL,
            metadata TEXT NOT NULL, priority INTEGER DEFAULT 0,
            created_at REAL NOT NULL, status TEXT DEFAULT 'pending',
            result TEXT, started_at REAL, completed_at REAL,
            error TEXT, slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0, task_type TEXT DEFAULT 'llm'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
    conn.commit()
    conn.close()
    return path


class TestPoll:
    """Test _poll — the DB polling function."""

    def test_returns_content_on_completed(self):
        """_poll returns content when task is completed."""
        from mcps.hpc_hermes import _poll

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, created_at, status, result) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t1", "test", "[]", "[]", "m", "{}", "{}", time.time(),
             "completed", '{"content":"hello world"}'),
        )
        conn.commit()
        conn.close()

        result = _poll("t1", db, stall_timeout=10)
        assert result == "hello world"
        os.unlink(db)

    def test_returns_error_on_failed(self):
        """_poll returns ERROR: prefixed string on failed status."""
        from mcps.hpc_hermes import _poll

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, created_at, status, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t2", "test", "[]", "[]", "m", "{}", "{}", time.time(),
             "failed", "something broke"),
        )
        conn.commit()
        conn.close()

        result = _poll("t2", db, stall_timeout=10)
        assert result.startswith("ERROR:")
        assert "broke" in result
        os.unlink(db)

    def test_pending_task_stalls(self):
        """_poll returns stall error when pending task makes no progress."""
        from mcps.hpc_hermes import _poll

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t3", "test", "[]", "[]", "m", "{}", "{}", time.time(), "pending"),
        )
        conn.commit()
        conn.close()

        result = _poll("t3", db, stall_timeout=1)
        assert "stall" in result.lower()
        assert "pending" in result
        os.unlink(db)

    def test_running_task_uses_running_timeout(self):
        """_poll uses _RUNNING_TIMEOUT for running tasks, not stall_timeout."""
        from mcps.hpc_hermes import _poll, _RUNNING_TIMEOUT

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t4", "test", "[]", "[]", "m", "{}", "{}", time.time(), "running"),
        )
        conn.commit()
        conn.close()

        # Patch _RUNNING_TIMEOUT low to prove the running-phase threshold is used,
        # while stall_timeout=999 ensures we'd never stall via pending threshold
        with patch("mcps.hpc_hermes._RUNNING_TIMEOUT", 1):
            result = _poll("t4", db, stall_timeout=999)

        assert "stall" in result.lower()
        assert "running" in result
        os.unlink(db)

    def test_completed_task_returns_immediately(self):
        """A task that completes mid-poll returns content immediately."""
        from mcps.hpc_hermes import _poll
        import threading

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("t5", "test", "[]", "[]", "m", "{}", "{}", time.time(), "pending"),
        )
        conn.commit()
        conn.close()

        # In a background thread, update the task to completed after 0.5s
        def _complete():
            time.sleep(0.5)
            c = sqlite3.connect(db)
            c.execute("UPDATE tasks SET status='completed', result=? WHERE id='t5'",
                      (json.dumps({"content": "async result"}),))
            c.commit()
            c.close()

        t = threading.Thread(target=_complete, daemon=True)
        t.start()
        result = _poll("t5", db, stall_timeout=10)
        assert result == "async result"
        os.unlink(db)


class TestRunOnHpc:
    """Test run_on_hpc — the core submission function."""

    def test_submits_task_and_polls(self):
        """run_on_hpc submits a task and returns the stall error."""
        from mcps.hpc_hermes import run_on_hpc

        db = _make_db()

        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            result = run_on_hpc("hi", stall_timeout=1)

        assert isinstance(result, str)
        assert "stall" in result.lower()  # stalled, not crash
        os.unlink(db)


class TestRunHpcAgentTask:
    """Tests for run_hpc_agent_task MCP tool."""

    def test_accepts_message_array(self):
        """run_hpc_agent_task accepts full message arrays (stalls without daemon)."""
        from mcps.hpc_hermes import run_hpc_agent_task

        db = _make_db()

        # Patch _PENDING_STALL to 1s so the test finishes quickly
        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            with patch("mcps.hpc_hermes._PENDING_STALL", 1):
                result = run_hpc_agent_task(
                    messages=[{"role": "user", "content": "hi"}],
                )

        assert isinstance(result, str)
        assert "stall" in result.lower()
        os.unlink(db)

    def test_system_prompt_is_prepended(self):
        """System prompt is prepended to messages before submission."""
        from mcps.hpc_hermes import run_hpc_agent_task
        from hpc_batch import submit_task

        db = _make_db()

        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            with patch("mcps.hpc_hermes._PENDING_STALL", 1):
                with patch("hpc_batch.submit_task", wraps=submit_task) as mock_submit:
                    run_hpc_agent_task(
                        messages=[{"role": "user", "content": "hello"}],
                        system_prompt="Be concise.",
                    )

                    call_kwargs = mock_submit.call_args[1]
                    stored = call_kwargs.get("messages", [])
                    assert len(stored) == 2
                    assert stored[0]["role"] == "system"
                    assert stored[0]["content"] == "Be concise."
        os.unlink(db)


class TestCheckHpcTaskStatus:
    """Tests for check_hpc_task_status MCP tool."""

    def test_returns_pending_for_unprocessed(self):
        """Unprocessed task returns status='pending'."""
        from mcps.hpc_hermes import check_hpc_task_status
        from hpc_batch import submit_task

        db = _make_db()
        tid = submit_task(prompt="hello", db_path=db)

        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            result = check_hpc_task_status(tid)

        assert result["status"] == "pending"
        assert "result" not in result
        os.unlink(db)

    def test_returns_completed_for_processed(self):
        """Processed task returns status='completed' with result."""
        from mcps.hpc_hermes import check_hpc_task_status
        from worker.hermes_worker import process_next_task
        from worker.model_runner import MockRunner

        db = _make_db()
        from hpc_batch import submit_task
        tid = submit_task(prompt="hello", db_path=db)
        process_next_task(db, runner=MockRunner(response="Hi"))

        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            result = check_hpc_task_status(tid)

        assert result["status"] == "completed"
        assert "result" in result
        os.unlink(db)

    def test_returns_unknown_for_nonexistent(self):
        """Bogus task ID returns status='unknown'."""
        from mcps.hpc_hermes import check_hpc_task_status

        db = _make_db()  # valid DB, but no task with our ID
        with patch("mcps.hpc_hermes._resolve_db", return_value=db):
            result = check_hpc_task_status("bogus-id")
        assert result["status"] == "unknown"
        os.unlink(db)
