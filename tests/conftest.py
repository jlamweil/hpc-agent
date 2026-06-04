"""Pytest fixtures for HPC Batch tests."""
import os
import sqlite3
import tempfile
import pytest

# Import the module under test
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hpc_batch import init_db, submit_task, get_pending_tasks


@pytest.fixture
def db_path():
    """Provide a temporary database path and clean up after the test."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    os.unlink(path)


@pytest.fixture
def conn(db_path):
    """Provide an initialized database connection."""
    conn = init_db(db_path)
    yield conn
    conn.close()


def dequeue_pending(db_path: str, limit: int = 10):
    """Simulate the dequeue query used by batch workers.

    Returns tasks that are pending AND whose unmet dependencies are all completed.
    This mirrors the NOT EXISTS pattern that will go into slurm scripts.
    """
    conn = init_db(db_path)
    conn.row_factory = sqlite3.Row

    # Check if task_deps table exists
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='task_deps'"
    ).fetchall()

    if tables:
        rows = conn.execute("""
            SELECT id, messages, model, metadata
            FROM tasks
            WHERE status = 'pending'
              AND NOT EXISTS (
                  SELECT 1 FROM task_deps
                  WHERE task_deps.task_id = tasks.id
                    AND task_deps.depends_on NOT IN (
                        SELECT id FROM tasks WHERE status = 'completed'
                    )
              )
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, messages, model, metadata
            FROM tasks WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def complete_task(db_path: str, task_id: str):
    """Mark a task as completed (helper for test setup)."""
    import time
    conn = init_db(db_path)
    conn.execute(
        "UPDATE tasks SET status = 'completed', completed_at = ? WHERE id = ?",
        (time.time(), task_id),
    )
    conn.commit()
    conn.close()
