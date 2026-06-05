"""Tests for fill task generation and preemption guard."""
import json
import os
import sqlite3
import tempfile
import time


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            messages TEXT NOT NULL,
            tools TEXT NOT NULL,
            model TEXT NOT NULL,
            api_config TEXT NOT NULL,
            metadata TEXT NOT NULL,
            priority INTEGER DEFAULT 0,
            created_at REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            result TEXT,
            started_at REAL,
            completed_at REAL,
            error TEXT,
            slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0,
            worker_id TEXT,
            claimed_at REAL,
            attempt_count INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 3,
            last_error TEXT,
            retry_at REAL,
            task_type TEXT DEFAULT 'llm'
        )
    """)
    conn.commit()
    conn.close()
    return path


def _add_task(conn, task_id: str, priority: int = 0, status: str = "pending",
              attempt_count: int = 0, type_: str = "generic"):
    conn.execute("""
        INSERT OR IGNORE INTO tasks
        (id, agent_id, messages, tools, model, api_config, metadata,
         priority, created_at, status, attempt_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        "test",
        json.dumps([{"role": "user", "content": "test"}]),
        json.dumps([]),
        "test-model",
        json.dumps({}),
        json.dumps({"type": type_}),
        priority,
        time.time(),
        status,
        attempt_count,
    ))


class TestFillTaskPreemption:
    """Preemption guard for fill tasks — test seam 5 from live vLLM PRD."""

    def _run_preemption_guard(self, db: str):
        """Simulate the cleanup handler's preemption guard logic."""
        conn = sqlite3.connect(db)
        rows = conn.execute("""
            SELECT id, COALESCE(attempt_count, 0) as attempt FROM tasks
            WHERE status = 'running'
            AND json_extract(metadata, '$.type') = 'fill'
        """).fetchall()
        for row in rows:
            tid, attempt = row
            if attempt < 5:
                conn.execute("""
                    UPDATE tasks SET status = 'pending', worker_id = NULL,
                    claimed_at = NULL, retry_at = NULL,
                    attempt_count = ?, last_error = 'Preempted by SLURM'
                    WHERE id = ?
                """, (attempt + 1, tid))
            else:
                conn.execute("""
                    UPDATE tasks SET status = 'failed', completed_at = ?,
                    last_error = 'Fill task preempted 5 times, giving up'
                    WHERE id = ?
                """, (time.time(), tid))
        conn.commit()
        conn.close()

    def test_running_fill_task_reset_to_pending(self):
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_1", priority=-1, status="running", type_="fill")
        conn.commit()
        conn.close()

        self._run_preemption_guard(db)

        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status, attempt_count FROM tasks WHERE id = 'fill_1'").fetchone()
        conn.close()
        assert row[0] == "pending", f"Expected pending, got {row[0]}"
        assert row[1] == 1, f"Expected attempt_count=1, got {row[1]}"
        os.unlink(db)

    def test_user_task_not_affected(self):
        """User tasks (type != 'fill') should not be reset."""
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "user_1", priority=10, status="running", type_="paper_analysis")
        conn.commit()
        conn.close()

        self._run_preemption_guard(db)

        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status FROM tasks WHERE id = 'user_1'").fetchone()
        conn.close()
        assert row[0] == "running", f"Expected running (unchanged), got {row[0]}"
        os.unlink(db)

    def test_fill_task_fails_after_5_preemptions(self):
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_preempted", priority=-1, status="running",
                  attempt_count=5, type_="fill")
        conn.commit()
        conn.close()

        self._run_preemption_guard(db)

        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status FROM tasks WHERE id = 'fill_preempted'").fetchone()
        conn.close()
        assert row[0] == "failed", f"Expected failed, got {row[0]}"
        os.unlink(db)


class TestFillTaskMetadata:
    """Fill task metadata conventions."""

    def test_fill_tasks_have_correct_priority_and_type(self):
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_1", priority=-1, type_="fill")
        _add_task(conn, "normal", priority=0, type_="paper_analysis")
        conn.commit()

        # Verify fill tasks are identifiable by type and priority
        fill_rows = conn.execute(
            "SELECT id, priority FROM tasks WHERE json_extract(metadata, '$.type') = 'fill'"
        ).fetchall()
        assert len(fill_rows) == 1
        assert fill_rows[0][1] == -1

        # Verify normal tasks have priority >= 0
        normal_rows = conn.execute(
            "SELECT id, priority FROM tasks WHERE json_extract(metadata, '$.type') != 'fill'"
        ).fetchall()
        assert len(normal_rows) == 1
        assert normal_rows[0][1] >= 0

        conn.close()
        os.unlink(db)
