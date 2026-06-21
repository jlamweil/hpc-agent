"""Tests for Slice 4: task expiry and cycle metadata."""
import json
import sqlite3
import time
import pytest
from argparse import Namespace

from hpc_batch import submit_task
from conftest import complete_task


class TestExpire:
    """Task expiry: old pending tasks get marked as failed."""

    def _expire(self, db_path, older_than):
        from hpc_batch import expire_old_tasks
        return expire_old_tasks(db_path, older_than=older_than)

    def test_expire_no_tasks(self, db_path):
        """No tasks → expire does nothing."""
        count = self._expire(db_path, 10)
        assert count == 0

    def test_expire_only_old_pending(self, db_path):
        """Only old pending tasks get expired."""
        # Create a task with old timestamp
        import sqlite3
        conn = sqlite3.connect(db_path)
        from hpc_batch import init_db
        init_db(db_path)
        old_time = time.time() - 100
        conn.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model,
                api_config, metadata, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("old-task", "test", '[]', '[]', 'm', '{}', '{}', old_time, 'pending'))
        conn.commit()
        conn.close()

        # New task
        new_id = submit_task("new", db_path=db_path, task_id="new-task")

        # Expire tasks older than 50 seconds
        count = self._expire(db_path, older_than=50)
        assert count == 1, "Only the old task should expire"

        # Verify old task is failed, new task still pending
        conn = sqlite3.connect(db_path)
        old_status = conn.execute(
            "SELECT status, error FROM tasks WHERE id = ?", ("old-task",)
        ).fetchone()
        assert old_status[0] == 'failed'
        assert 'expired' in (old_status[1] or '').lower()

        new_status = conn.execute(
            "SELECT status FROM tasks WHERE id = ?", (new_id,)
        ).fetchone()
        assert new_status[0] == 'pending'
        conn.close()

    def test_expire_not_completed_or_running(self, db_path):
        """completed/running tasks should not be expired."""
        import sqlite3
        conn = sqlite3.connect(db_path)
        from hpc_batch import init_db
        init_db(db_path)
        old_time = time.time() - 100
        for status in ('completed', 'running', 'failed'):
            conn.execute("""
                INSERT INTO tasks (id, agent_id, messages, tools, model,
                    api_config, metadata, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (f"task-{status}", "test", '[]', '[]', 'm', '{}', '{}', old_time, status))
        conn.commit()
        conn.close()

        count = self._expire(db_path, older_than=50)
        assert count == 0, "No pending tasks to expire"

    def test_expire_skip_recent(self, db_path):
        """Recent pending tasks should not be expired."""
        recent_id = submit_task("recent", db_path=db_path)
        time.sleep(0.1)  # Ensure time separation

        count = self._expire(db_path, older_than=1)
        assert count == 0, "Recent task should not expire"

        conn = sqlite3.connect(db_path)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id = ?", (recent_id,)
        ).fetchone()[0]
        assert status == 'pending', "Recent task should still be pending"
        conn.close()


class TestCycleMetadata:
    """--cycle N stores cycle number in task metadata."""

    def test_cycle_in_metadata(self, db_path):
        """Cycle number should be stored in metadata."""
        tid = submit_task("test", db_path=db_path, metadata={"type": "test", "cycle": 7})
        import sqlite3
        conn = sqlite3.connect(db_path)
        meta = conn.execute(
            "SELECT metadata FROM tasks WHERE id = ?", (tid,)
        ).fetchone()[0]
        conn.close()
        data = json.loads(meta)
        assert data.get("cycle") == 7

    def test_cycle_default(self, db_path):
        """Without cycle, metadata should not contain cycle key."""
        tid = submit_task("test", db_path=db_path, metadata={"type": "test"})
        import sqlite3
        conn = sqlite3.connect(db_path)
        meta = conn.execute(
            "SELECT metadata FROM tasks WHERE id = ?", (tid,)
        ).fetchone()[0]
        conn.close()
        data = json.loads(meta)
        assert "cycle" not in data
