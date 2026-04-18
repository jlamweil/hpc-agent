#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys

DEFAULT_DB = "tasks.db"


def get_task(task_id: str, db_path: str = None) -> dict:
    actual_db = db_path or DEFAULT_DB
    conn = sqlite3.connect(actual_db)
    conn.row_factory = sqlite3.Row
    
    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
    
    if not row:
        conn.close()
        return {"error": f"Task {task_id} not found"}
    
    result = {
        "task_id": row["id"],
        "status": row["status"],
        "agent_id": row["agent_id"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "slurm_job_id": row["slurm_job_id"],
        "retry_count": row["retry_count"],
        "error": row["error"]
    }
    
    if row["messages"]:
        result["prompt"] = json.loads(row["messages"])[0]["content"]
    
    if row["metadata"]:
        result["metadata"] = json.loads(row["metadata"])
    
    if row["result"]:
        result["result"] = json.loads(row["result"])
    
    conn.close()
    return result


def get_all_tasks(status: str = None, limit: int = 100, db_path: str = None):
    actual_db = db_path or DEFAULT_DB
    conn = sqlite3.connect(actual_db)
    conn.row_factory = sqlite3.Row
    
    if status:
        rows = conn.execute(
            "SELECT id, status, created_at, completed_at, slurm_job_id FROM tasks WHERE status = ? ORDER BY created_at DESC LIMIT ?",
            (status, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, status, created_at, completed_at, slurm_job_id FROM tasks ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    
    tasks = []
    for row in rows:
        tasks.append({
            "task_id": row["id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "slurm_job_id": row["slurm_job_id"]
        })
    
    conn.close()
    return tasks


def get_metrics(task_id: str = None, db_path: str = None):
    actual_db = db_path or DEFAULT_DB
    conn = sqlite3.connect(actual_db)
    conn.row_factory = sqlite3.Row
    
    if task_id:
        rows = conn.execute(
            "SELECT * FROM metrics WHERE task_id = ? ORDER BY timestamp DESC",
            (task_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 100"
        ).fetchall()
    
    metrics = []
    for row in rows:
        m = {
            "id": row["id"],
            "event": row["event"],
            "task_id": row["task_id"],
            "job_id": row["job_id"],
            "timestamp": row["timestamp"]
        }
        if row["metadata"]:
            m["metadata"] = json.loads(row["metadata"])
        metrics.append(m)
    
    conn.close()
    return metrics


def print_task(t: dict):
    print(f"Task ID: {t.get('task_id', 'N/A')}")
    print(f"  Status: {t.get('status', 'N/A')}")
    print(f"  Agent: {t.get('agent_id', 'N/A')}")
    
    if "prompt" in t:
        print(f"  Prompt: {t['prompt'][:60]}...")
    
    if t.get("slurm_job_id"):
        print(f"  Slurm Job: {t['slurm_job_id']}")
    
    if t.get("retry_count", 0) > 0:
        print(f"  Retries: {t['retry_count']}")
    
    if t.get("error"):
        print(f"  Error: {t['error']}")
    
    if "result" in t:
        print(f"  Result: {json.dumps(t['result'], indent=2)}")
    
    print()


def main():
    parser = argparse.ArgumentParser(description="Get task results")
    parser.add_argument("--task-id", help="Specific task ID")
    parser.add_argument("--all", action="store_true", help="List all tasks")
    parser.add_argument("--status", choices=["pending", "running", "completed", "failed"], help="Filter by status")
    parser.add_argument("--limit", type=int, default=20, help="Max results")
    parser.add_argument("--metrics", action="store_true", help="Show metrics")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        return
    
    if args.task_id:
        t = get_task(args.task_id, args.db)
        if "error" in t:
            print(f"Error: {t['error']}")
        else:
            print_task(t)
            
            if args.metrics:
                m = get_metrics(args.task_id, args.db)
                if m:
                    print("Metrics:")
                    for met in m:
                        print(f"  {met['event']} at {met['timestamp']}")
    
    elif args.all or args.status:
        tasks = get_all_tasks(args.status, args.limit, args.db)
        print(f"Found {len(tasks)} tasks:")
        for t in tasks:
            status_marker = {
                "pending": "○",
                "running": "◐",
                "completed": "●",
                "failed": "✕"
            }.get(t["status"], "?")
            print(f"  {status_marker} {t['task_id'][:8]}... | {t['status']} | job:{t.get('slurm_job_id', 'none')}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()