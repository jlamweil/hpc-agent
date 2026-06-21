"""Tests for priority queue + submit-fill CLI."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import argparse
import sqlite3
import tempfile
import time
import pytest
from unittest.mock import patch

import hpc_batch


def _fresh_db() -> str:
    """Create temp DB with NO priority/task_type columns (old schema)."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            messages TEXT NOT NULL,
            tools TEXT NOT NULL,
            model TEXT NOT NULL,
            api_config TEXT NOT NULL,
            metadata TEXT NOT NULL,
            created_at REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            result TEXT,
            started_at REAL,
            completed_at REAL,
            error TEXT,
            slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_created ON tasks(created_at)")
    conn.commit()
    conn.close()
    return path


class TestDBMigration:
    """DB migration for priority and task_type columns."""

    def test_migration_adds_columns(self):
        """init_db adds priority and task_type columns to existing table."""
        path = _fresh_db()

        # Verify they don't exist initially
        conn = sqlite3.connect(path)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        conn.close()
        assert "priority" not in cols
        assert "task_type" not in cols

        # Run init_db (should apply migration)
        conn2 = hpc_batch.init_db(path)
        conn2.close()

        # Verify columns now exist
        conn3 = sqlite3.connect(path)
        cols2 = [r[1] for r in conn3.execute("PRAGMA table_info(tasks)").fetchall()]
        conn3.close()
        assert "priority" in cols2
        assert "task_type" in cols2

        os.unlink(path)

    def test_migration_idempotent(self):
        """Running init_db twice doesn't error."""
        path = _fresh_db()
        c1 = hpc_batch.init_db(path)
        c1.close()
        c2 = hpc_batch.init_db(path)  # should not raise
        c2.close()
        os.unlink(path)


class TestPrioritySubmit:
    """submit_task with priority and task_type."""

    def test_submit_with_priority_and_type(self):
        """submit_task stores priority and task_type."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        tid = hpc_batch.submit_task(
            prompt="test priority",
            db_path=path,
            priority=-1,
            task_type="fill",
            metadata={"type": "fill", "fill_type": "stress-test"},
        )

        conn = sqlite3.connect(path)
        row = conn.execute(
            "SELECT priority, task_type, metadata FROM tasks WHERE id = ?", (tid,)
        ).fetchone()
        conn.close()
        assert row[0] == -1  # priority
        assert row[1] == "fill"  # task_type
        meta = json.loads(row[2])
        assert meta["type"] == "fill"
        assert meta["fill_type"] == "stress-test"
        os.unlink(path)

    def test_default_priority_zero(self):
        """Default priority is 0 and task_type is 'llm'."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        tid = hpc_batch.submit_task(prompt="default priority", db_path=path)

        conn = sqlite3.connect(path)
        row = conn.execute(
            "SELECT priority, task_type FROM tasks WHERE id = ?", (tid,)
        ).fetchone()
        conn.close()
        assert row[0] == 0
        assert row[1] == "llm"
        os.unlink(path)


class TestSubmitFillCLI:
    """submit-fill CLI command."""

    @patch("hpc_batch._next_fill_prompt", return_value="stress test prompt")
    def test_submit_fill_creates_task(self, mock_next):
        """CLI submit-fill creates a fill task with priority=-1."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        # Simulate CLI: hpc_batch.py submit-fill --type stress-test
        args = _make_args("submit-fill", db=path, fill_type="stress-test")
        hpc_batch.cmd_submit_fill(args)

        pending = hpc_batch.get_pending_tasks(path)
        assert len(pending) == 1

        conn = sqlite3.connect(path)
        row = conn.execute(
            "SELECT priority, task_type, metadata FROM tasks WHERE id = ?",
            (pending[0]["id"],),
        ).fetchone()
        conn.close()
        assert row[0] == -1  # priority
        assert row[1] == "fill"  # task_type
        meta = json.loads(row[2])
        assert meta["type"] == "fill"
        assert meta["fill_type"] == "stress-test"
        os.unlink(path)

    @patch("hpc_batch._next_fill_prompt", return_value="warmup prompt")
    def test_submit_fill_with_explicit_prompt(self, mock_next):
        """submit-fill --prompt uses explicit text."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        # Simulate CLI with explicit prompt (should NOT call _next_fill_prompt)
        args = _make_args("submit-fill", db=path, prompt="explicit prompt", fill_type="warmup")
        hpc_batch.cmd_submit_fill(args)

        pending = hpc_batch.get_pending_tasks(path)
        assert len(pending) == 1
        assert "explicit" in pending[0]["prompt_preview"]
        os.unlink(path)


def _make_args(cmd: str, **kwargs) -> argparse.Namespace:
    """Build a fake argparse Namespace for CLI testing."""
    ns = argparse.Namespace()
    ns.db = kwargs.get("db", "")
    ns.command = cmd
    if cmd == "submit-fill":
        ns.prompt = kwargs.get("prompt", None)
        ns.fill_type = kwargs.get("fill_type", "stress-test")
        ns.model = kwargs.get("model", "test-model")
    return ns


class TestPriorityOrdering:
    """Dequeue queries use priority DESC, created_at ASC."""

    def test_fill_task_dequeued_last(self):
        """Real task (priority=0) dequeued before fill task (priority=-1)."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        hpc_batch.submit_task(prompt="fill task", db_path=path, priority=-1, task_type="fill",
                              metadata={"type": "fill"})
        hpc_batch.submit_task(prompt="real task", db_path=path, priority=0)

        pending = hpc_batch.get_pending_tasks(path)
        assert len(pending) == 2
        assert "real" in pending[0]["prompt_preview"]
        assert "fill" in pending[1]["prompt_preview"]
        os.unlink(path)

    def test_highest_priority_first(self):
        """Higher priority tasks are dequeued first."""
        path = _fresh_db()
        conn = hpc_batch.init_db(path)
        conn.close()

        # Submit tasks with different priorities
        hpc_batch.submit_task(prompt="low", db_path=path, priority=-1)
        hpc_batch.submit_task(prompt="high", db_path=path, priority=10)
        hpc_batch.submit_task(prompt="medium", db_path=path, priority=0)

        pending = hpc_batch.get_pending_tasks(path)
        assert len(pending) == 3
        # First should be "high" (priority 10), sorted by prompt content
        assert "high" in pending[0]["prompt_preview"]
        assert "medium" in pending[1]["prompt_preview"]
        assert "low" in pending[2]["prompt_preview"]
        os.unlink(path)
