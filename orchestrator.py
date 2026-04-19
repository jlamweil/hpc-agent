#!/usr/bin/env python3
"""Deterministic HPC Task Orchestration API.

Pure Python functions for task submission and retrieval.
No LLM, no Hermes, no tool wrappers - just deterministic SQLite operations.
"""
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional


DEFAULT_DB = "tasks.db"


def _get_db_path() -> str:
    """Find the task database path."""
    db_path = os.environ.get("HPC_TASK_DB")
    if db_path:
        return db_path
    
    candidates = [
        "tasks.db",
        "/home/lam/Documents/GAIA/hpc-agent/tasks.db",
        os.path.expanduser("~/Documents/GAIA/hpc-agent/tasks.db"),
    ]
    
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    
    return "tasks.db"


def _init_db(db_path: str) -> None:
    """Initialize tasks table if needed."""
    conn = sqlite3.connect(db_path)
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
            retry_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
    conn.commit()
    conn.close()


def submit_hpc_task(prompt: str, db_path: str = None, priority: int = 0,
                  metadata: dict = None) -> str:
    """
    Deterministically enqueue a task into SQLite.
    
    Args:
        prompt: Task message to execute
        db_path: Optional path to SQLite database
        priority: Task priority (higher = first)
        metadata: Optional metadata dict
    
    Returns:
        UUID of submitted task
    """
    path = db_path or _get_db_path()
    _init_db(path)
    
    task_id = str(uuid.uuid4())
    created_at = time.time()
    
    messages = [{"role": "user", "content": prompt}]
    task_metadata = (metadata or {})
    task_metadata["source"] = "orchestrator"
    
    conn = sqlite3.connect(path)
    conn.execute("""
        INSERT INTO tasks (
            id, agent_id, messages, tools, model, api_config, metadata,
            priority, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        "hpc-worker",
        json.dumps(messages),
        json.dumps([]),
        "hpc-model",
        json.dumps({}),
        json.dumps(task_metadata),
        priority,
        created_at,
        "pending"
    ))
    conn.commit()
    conn.close()
    
    return task_id


def get_task_result(task_id: str, db_path: str = None) -> dict:
    """
    Get task result (non-blocking).
    
    Args:
        task_id: UUID of task to check
        db_path: Optional path to SQLite database
    
    Returns:
        dict with status, result, error
    """
    path = db_path or _get_db_path()
    
    if not os.path.exists(path):
        return {"status": "error", "error": f"Database not found: {path}"}
    
    conn = sqlite3.connect(path)
    row = conn.execute("""
        SELECT status, result, error, created_at, started_at, completed_at 
        FROM tasks WHERE id = ?
    """, (task_id,)).fetchone()
    conn.close()
    
    if not row:
        return {"status": "error", "error": f"Task not found: {task_id}"}
    
    return {
        "status": row[0],
        "result": json.loads(row[1]) if row[1] else None,
        "error": row[2],
        "created_at": row[3],
        "started_at": row[4],
        "completed_at": row[5]
    }


def wait_for_task(task_id: str, db_path: str = None, timeout: int = 300) -> dict:
    """
    Wait for task completion.
    
    Args:
        task_id: UUID of task to wait for
        db_path: Optional path to SQLite database
        timeout: Max seconds to wait (default: 300)
    
    Returns:
        dict with status, result, error
    
    Raises:
        TimeoutError: If task not completed within timeout
    """
    path = db_path or _get_db_path()
    start = time.time()
    
    while time.time() - start < timeout:
        result = get_task_result(task_id, path)
        
        if result["status"] in ("done", "completed", "failed"):
            return result
        
        if result["status"] == "error":
            return result
        
        time.sleep(0.5)
    
    return {
        "status": "timeout",
        "error": f"Task not completed after {timeout}s"
    }


def get_queue_status(db_path: str = None) -> dict:
    """Get queue statistics."""
    path = db_path or _get_db_path()
    
    if not os.path.exists(path):
        return {"pending": 0, "running": 0, "completed": 0, "failed": 0, "total": 0}
    
    conn = sqlite3.connect(path)
    
    def count(status: str) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?", (status,)
        ).fetchone()[0]
    
    result = {
        "pending": count("pending"),
        "running": count("running"),
        "completed": count("completed"),
        "failed": count("failed"),
    }
    result["total"] = sum(result.values())
    
    conn.close()
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="HPC Task Orchestrator")
    parser.add_argument("command", choices=["submit", "status", "result"], help="Command to run")
    parser.add_argument("prompt", nargs="?", help="Prompt for submit command")
    parser.add_argument("--task-id", help="Task ID for result command")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database path")
    
    args = parser.parse_args()
    
    if args.command == "submit":
        if not args.prompt:
            print("Error: prompt required", file=sys.stderr)
            sys.exit(1)
        task_id = submit_hpc_task(args.prompt, args.db)
        print(task_id)
    elif args.command == "status":
        print(get_queue_status(args.db))
    elif args.command == "result":
        if not args.task_id:
            print("Error: --task-id required", file=sys.stderr)
            sys.exit(1)
        result = get_task_result(args.task_id, args.db)
        print(json.dumps(result, indent=2))