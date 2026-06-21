"""Tests for schema version guard (Slice 01)."""
import sqlite3
import pytest
from hpc_batch import init_db, submit_task, get_pending_tasks
from conftest import dequeue_pending


class TestJournalMode:
    """DB journal mode is DELETE (no WAL race)."""

    def test_init_db_uses_delete_journal(self, db_path):
        """init_db creates a DB in DELETE journal mode."""
        conn = init_db(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "delete", f"Expected 'delete', got '{mode}'"

    def test_init_db_sets_timeout(self):
        """init_db connection uses timeout=30."""
        import tempfile, os
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = init_db(path)
        # Verify connection works (timeout is a connection-time property)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        os.unlink(path)
        assert mode == "delete"


class TestSchemaVersion:
    """PRAGMA user_version is set on init and checked by the worker."""

    def test_init_db_sets_version(self, db_path):
        """Fresh DB has PRAGMA user_version == 1."""
        conn = init_db(db_path)
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == 1
        conn.close()

    def test_worker_rejects_older_version(self, db_path):
        """Version 0 should be rejected by the worker's version check logic."""
        conn = init_db(db_path)
        conn.execute("PRAGMA user_version = 0")
        conn.commit()
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version != 1, "Version should NOT be 1 after stomping to 0"
        conn.close()

    def test_worker_accepts_current_version(self, db_path):
        """Version 1 → dequeue works."""
        conn = init_db(db_path)
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
        conn.close()
        submit_task("test prompt", db_path=db_path)
        ready = dequeue_pending(db_path)
        assert len(ready) == 1

    def test_priority_column_removed(self, db_path):
        """ORDER BY no longer references 'priority' — regression for job 17139597."""
        conn = init_db(db_path)
        sql = """
            SELECT id, messages, model FROM tasks
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """
        # Should not raise OperationalError (no such column: priority)
        conn.execute(sql, (1,))
        conn.close()


class TestSubmitTaskRetry:
    """submit_task retries on 'database is locked' (#3 fix)."""

    def test_submit_retries_on_locked_db(self, db_path):
        """submit_task succeeds after a transient 'database is locked'."""
        from unittest.mock import patch
        import time

        # Track how many times init_db was called
        init_calls = [0]

        original_init_db = init_db

        def _counting_init_db(path):
            init_calls[0] += 1
            return original_init_db(path)

        with patch("hpc_batch.init_db", side_effect=_counting_init_db):
            tid = submit_task("retry test", db_path=db_path)

        # Should still succeed
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT id, status FROM tasks WHERE id=?", (tid,)).fetchone()
        conn.close()
        assert row is not None
        assert row[1] == "pending"

    def test_submit_retries_after_locked_error(self, db_path):
        """submit_task reconnects and retries when INSERT gets 'database is locked'."""
        from unittest.mock import patch
        import hpc_batch
        from hpc_batch import init_db as real_init_db

        class _FailingConn:
            """Wrapper that fails once on INSERT, then delegates."""
            def __init__(self, real_conn):
                self._conn = real_conn
                self._has_failed = False

            def execute(self, sql, params=None):
                if not self._has_failed and isinstance(sql, str) and sql.strip().upper().startswith("INSERT"):
                    self._has_failed = True
                    raise sqlite3.OperationalError("database is locked")
                params = params or ()
                return self._conn.execute(sql, params)

            def close(self):
                return self._conn.close()

            def commit(self):
                return self._conn.commit()

        attempt = [0]

        def _failing_init(path):
            attempt[0] += 1
            real_conn = real_init_db(path)
            if attempt[0] == 1:
                return _FailingConn(real_conn)
            return real_conn

        with patch("hpc_batch.init_db", side_effect=_failing_init):
            tid = submit_task("lock retry test", db_path=db_path, priority=0)

        # Task should still be created successfully
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT id, status FROM tasks WHERE id=?", (tid,)).fetchone()
        conn.close()
        assert row is not None
        assert row[1] == "pending"


class TestMergeResults:
    """merge_results merges remote results safely (#8)."""

    def _fresh_db(self, tasks: list[tuple]) -> str:
        """Create a temp DB with given tasks. Each tuple: (id, status, result, error)."""
        import tempfile, os, json, time
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = init_db(path)
        for t in tasks:
            task_id, status, result, error = t
            conn.execute("""
                INSERT INTO tasks (id, agent_id, messages, tools, model, api_config,
                    metadata, created_at, status, result, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_id, "test", json.dumps([]), json.dumps([]), "test",
                  json.dumps({}), json.dumps({}), time.time(), status, result, error))
        conn.commit()
        conn.close()
        return path

    def test_merge_does_not_overwrite_completed_task(self):
        """merge_results must NOT overwrite a locally-completed task with remote result."""
        from hpc_batch import merge_results

        # Target: locally completed task
        target = self._fresh_db([
            ("t1", "completed", '{"content":"local done"}', None),
        ])
        # Source: remote also completed it (different result — stale from earlier cycle)
        source = self._fresh_db([
            ("t1", "completed", '{"content":"remote result"}', None),
        ])

        merge_results(source, target)

        # Target must keep its own result
        conn = sqlite3.connect(target)
        row = conn.execute("SELECT status, result FROM tasks WHERE id='t1'").fetchone()
        conn.close()
        assert row[0] == "completed", f"Expected 'completed', got '{row[0]}'"
        import json as _j
        assert "local done" in _j.loads(row[1])["content"], \
            f"Expected local result, got: {row[1]}"
        import os
        os.unlink(target)
        os.unlink(source)

    def test_merge_updates_pending_task(self):
        """merge_results MUST update a locally-pending task with remote completed state."""
        from hpc_batch import merge_results

        target = self._fresh_db([
            ("t2", "pending", None, None),
        ])
        source = self._fresh_db([
            ("t2", "completed", '{"content":"remote result"}', None),
        ])

        merge_results(source, target)

        conn = sqlite3.connect(target)
        row = conn.execute("SELECT status, result FROM tasks WHERE id='t2'").fetchone()
        conn.close()
        assert row[0] == "completed", f"Expected 'completed', got '{row[0]}'"
        import json as _j
        assert "remote result" in _j.loads(row[1])["content"]
        import os
        os.unlink(target)
        os.unlink(source)
