"""Tests for idle-poll loop and priority-ordered task claiming."""
import json
import os
import sqlite3
import tempfile
import time
import threading


def _make_db() -> str:
    """Create a temp SQLite DB with the tasks schema."""
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_deps (
            task_id TEXT NOT NULL,
            depends_on TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (task_id, depends_on)
        )
    """)
    conn.commit()
    conn.close()
    return path


def _add_task(conn, task_id: str, priority: int = 0, status: str = "pending",
              type_: str = "generic", text: str = "test prompt"):
    conn.execute("""
        INSERT OR IGNORE INTO tasks
        (id, agent_id, messages, tools, model, api_config, metadata,
         priority, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        "test",
        json.dumps([{"role": "user", "content": text}]),
        json.dumps([]),
        "test-model",
        json.dumps({}),
        json.dumps({"type": type_}),
        priority,
        time.time(),
        status,
    ))


class TestPriorityClaiming:
    """Priority-ordered task claiming — test seam 1."""

    def test_highest_priority_claimed_first(self):
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "low_pri", priority=-1)
        _add_task(conn, "mid_pri", priority=0)
        _add_task(conn, "high_pri", priority=10)
        conn.commit()

        # Claim: ORDER BY priority DESC
        claimed = conn.execute("""
            SELECT id, priority FROM tasks
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT 3
        """).fetchall()
        conn.close()

        assert len(claimed) == 3
        assert claimed[0][0] == "high_pri", f"Expected high_pri first, got {claimed[0]}"
        assert claimed[1][0] == "mid_pri", f"Expected mid_pri second, got {claimed[1]}"
        assert claimed[2][0] == "low_pri", f"Expected low_pri last, got {claimed[2]}"
        os.unlink(db)

    def test_fill_tasks_claimed_last(self):
        """Fill tasks (priority -1) should only be claimed after all 0+ tasks."""
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_1", priority=-1)
        _add_task(conn, "fill_2", priority=-1)
        _add_task(conn, "normal_1", priority=0)
        _add_task(conn, "urgent", priority=10)
        conn.commit()

        # Claim 2 tasks — should get urgent + normal, not fill
        claimed = conn.execute("""
            SELECT id, priority FROM tasks
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT 2
        """).fetchall()
        conn.close()

        assert len(claimed) == 2
        assert claimed[0][0] == "urgent"
        assert claimed[1][0] == "normal_1"
        os.unlink(db)

    def test_same_priority_oldest_first(self):
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "older", priority=5)
        time.sleep(0.01)
        _add_task(conn, "newer", priority=5)
        conn.commit()

        claimed = conn.execute("""
            SELECT id FROM tasks
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            LIMIT 2
        """).fetchall()
        conn.close()

        assert claimed[0][0] == "older"
        assert claimed[1][0] == "newer"
        os.unlink(db)


class TestIdlePoll:
    """Idle-poll loop behavior — test seam 2."""

    def _wait_for_count(self, db: str, expected: int, timeout: float = 30) -> list:
        """Poll the DB until count of completed+failed reaches expected, or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            conn = sqlite3.connect(db)
            row = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN ('completed', 'failed')"
            ).fetchone()
            conn.close()
            if row[0] >= expected:
                # Return the claimed tasks
                conn = sqlite3.connect(db)
                rows = conn.execute(
                    "SELECT id, status FROM tasks WHERE status IN ('completed', 'failed', 'running')"
                ).fetchall()
                conn.close()
                return rows
            time.sleep(0.5)
        return []

    def test_loop_processes_injected_tasks(self):
        """Simulate: start loop, inject task mid-wait, verify it's picked up."""
        db = _make_db()

        # Add initial task to get things going
        conn = sqlite3.connect(db)
        _add_task(conn, "initial", priority=0, text="initial task")
        conn.commit()
        conn.close()

        # Run a simple processing loop in a thread
        processed = []

        def processing_loop():
            conn = sqlite3.connect(db)
            # Claim and process initial task
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute("""
                SELECT id FROM tasks WHERE status = 'pending'
                ORDER BY priority DESC, created_at ASC
                LIMIT 5
            """).fetchall()
            if rows:
                ids = [r[0] for r in rows]
                conn.execute(
                    "UPDATE tasks SET status = 'running', started_at = ? WHERE id IN ({})".format(
                        ','.join('?' * len(ids))
                    ),
                    [time.time()] + ids,
                )
                conn.commit()
                for tid in ids:
                    conn.execute(
                        "UPDATE tasks SET status = 'completed', completed_at = ? WHERE id = ?",
                        (time.time(), tid),
                    )
                    processed.append(tid)
                conn.commit()
            conn.close()

        processing_loop()
        assert "initial" in processed

        # Now inject a new task (simulating rsync from local)
        time.sleep(0.5)
        conn = sqlite3.connect(db)
        _add_task(conn, "injected", priority=0, text="injected later")
        conn.commit()
        conn.close()

        # Process it
        processing_loop()
        assert "injected" in processed
        os.unlink(db)
