#!/usr/bin/env python3
"""HPC Batch — accumulate LLM tasks locally, dispatch to HPC GPU, collect results.

Usage:
    # Submit a single prompt to the queue (use in cron instead of 'opencode run')
    hpc_batch.py submit --prompt "analyze this data: ..."

    # Submit a batch from a file (one prompt per line, or JSONL)
    hpc_batch.py submit --file prompts.jsonl

    # Check queue status
    hpc_batch.py status

    # Dispatch accumulated tasks to HPC (rsync DB + submit SLURM job)
    hpc_batch.py dispatch [--force]

    # Collect completed results from HPC
    hpc_batch.py collect [--all]

    # View task result
    hpc_batch.py view <task-id>

    # Show pending tasks and their IDs
    hpc_batch.py pending

    # Count tasks by status and type
    hpc_batch.py count [--status pending] [--type TYPE]

    # Submit and wait for result
    hpc_batch.py submit-and-wait --prompt "..." [--type TYPE] [--poll 60] [--timeout 86400]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Defaults from config.yaml or env ─────────────────────────────────────────
HPC_AGENT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = HPC_AGENT_DIR / "config.yaml"
DEFAULT_DB = HPC_AGENT_DIR / "tasks.db"
DEFAULT_PARTITION = "pri2021gpu"
DEFAULT_ACCOUNT = "qosmivgpu"
DEFAULT_QOS = "preemptqos"

def load_config() -> dict[str, Any]:
    """Load settings from config.yaml if it exists."""
    try:
        import yaml
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as f:
                return yaml.safe_load(f) or {}
    except Exception:
        pass
    return {}

def cfg(key: str, default: Any = None) -> Any:
    """Get config value with fallback."""
    config = load_config()
    parts = key.split(".")
    for p in parts:
        if isinstance(config, dict):
            config = config.get(p, {})
        else:
            return default
    return config if config != {} else default


# ── Task Queue ──────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize the task database."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
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
    return conn


def submit_task(
    prompt: str,
    db_path: str = "",
    model: str = "deepseek-ai/DeepSeek-V4-Flash",
    agent_id: str = "hpc-batch",
    metadata: dict | None = None,
    task_id: str = "",
) -> str:
    """Add a task to the local SQLite queue."""
    db = db_path or str(DEFAULT_DB)
    task_id = task_id or str(uuid.uuid4())
    created_at = time.time()

    conn = init_db(db)
    conn.execute("""
        INSERT INTO tasks (
            id, agent_id, messages, tools, model, api_config,
            metadata, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        agent_id,
        json.dumps([{"role": "user", "content": prompt}]),
        json.dumps([]),
        model,
        json.dumps({}),
        json.dumps(metadata or {}),
        created_at,
        "pending",
    ))
    conn.commit()
    conn.close()
    return task_id


def get_queue_status(db_path: str = "") -> dict[str, int]:
    """Get counts of tasks by status."""
    db = db_path or str(DEFAULT_DB)
    if not os.path.exists(db):
        return {"pending": 0, "running": 0, "completed": 0, "failed": 0}

    conn = init_db(db)
    counts = {}
    for status in ("pending", "running", "completed", "failed"):
        c = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?", (status,)
        ).fetchone()[0]
        counts[status] = c
    counts["total"] = sum(counts.values())
    conn.close()
    return counts


def get_pending_tasks(db_path: str = "", task_type: str | None = None) -> list[dict]:
    """List pending tasks."""
    db = db_path or str(DEFAULT_DB)
    if not os.path.exists(db):
        return []

    conn = init_db(db)
    conn.row_factory = sqlite3.Row
    if task_type:
        rows = conn.execute("""
            SELECT id, messages, model, created_at
            FROM tasks WHERE status = 'pending' AND json_extract(metadata, '$.type') = ?
            ORDER BY created_at ASC
        """, (task_type,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, messages, model, created_at
            FROM tasks WHERE status = 'pending'
            ORDER BY created_at ASC
        """).fetchall()
    conn.close()

    tasks = []
    for r in rows:
        tasks.append({
            "id": r["id"],
            "prompt_preview": json.loads(r["messages"])[0]["content"][:80],
            "model": r["model"],
            "created_at": datetime.fromtimestamp(r["created_at"], tz=timezone.utc)
                .isoformat(),
        })
    return tasks


def get_result(db_path: str = "", task_id: str = "") -> dict | None:
    """Get task result."""
    if not os.path.exists(db_path):
        return None
    conn = init_db(db_path)
    row = conn.execute("""
        SELECT status, result, error, started_at, completed_at
        FROM tasks WHERE id = ?
    """, (task_id,)).fetchone()
    conn.close()
    if not row:
        return None

    result = {
        "status": row[0],
        "error": row[2],
        "started_at": row[3],
        "completed_at": row[4],
    }
    if row[1]:
        try:
            parsed = json.loads(row[1])
            result["content"] = parsed.get("content", "")
            result["usage"] = parsed.get("usage")
        except json.JSONDecodeError:
            result["content"] = row[1][:200]
    return result


# ── HPC Operations ─────────────────────────────────────────────────────────

def _ssh_cmd(host: str, user: str = "") -> list[str]:
    """Build SSH command."""
    if user:
        return ["ssh", f"{user}@{host}", "-o", "ConnectTimeout=10"]
    return ["ssh", host, "-o", "ConnectTimeout=10"]


def _get_hpc_config() -> dict[str, str]:
    """Get HPC connection details."""
    return {
        "user": cfg("hpc.user", "jlam"),
        "host": cfg("hpc.host", "hpc-login.u-strasbg.fr"),
        "dir": cfg("hpc.dir", "~/hpc-agent"),
        "partition": cfg("hpc.partition", DEFAULT_PARTITION),
        "account": cfg("hpc.account", DEFAULT_ACCOUNT),
        "qos": cfg("hpc.qos", DEFAULT_QOS),
    }


def rsync_to_hpc(db_path: str, hpc_config: dict[str, str] | None = None) -> bool:
    """Rsync the local DB to HPC login node."""
    cfg_hpc = hpc_config or _get_hpc_config()
    src = db_path
    dst = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/tasks.db"

    print(f"[dispatch] rsync {src} -> {dst}")
    result = subprocess.run(
        ["rsync", "-az", "--progress", src, dst],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"[dispatch] rsync FAILED: {result.stderr.strip()}")
        return False
    print(f"[dispatch] rsync OK")
    return True


def sbatch_on_hpc(hpc_config: dict[str, str] | None = None) -> dict[str, Any]:
    """Submit the DS V4 Flash batch job to HPC via SSH."""
    cfg_hpc = hpc_config or _get_hpc_config()
    ssh = _ssh_cmd(cfg_hpc["host"], cfg_hpc["user"])

    # Build sbatch command
    sbatch_cmd = (
        f"cd {cfg_hpc['dir']} && "
        f"sbatch --partition={cfg_hpc['partition']} "
        f"--account={cfg_hpc['account']} "
        f"--qos={cfg_hpc['qos']} "
        f"slurm/dsv4_batch.slurm"
    )

    print(f"[dispatch] ssh {cfg_hpc['user']}@{cfg_hpc['host']}: {sbatch_cmd}")
    result = subprocess.run(
        ssh + [sbatch_cmd],
        capture_output=True, text=True, timeout=30,
    )

    if result.returncode != 0:
        return {
            "success": False,
            "error": result.stderr.strip() or result.stdout.strip(),
        }

    output = result.stdout.strip()
    # Parse "Submitted batch job 12345"
    if "Submitted batch job" in output:
        job_id = output.split()[-1]
        print(f"[dispatch] Job submitted: {job_id}")
        return {"success": True, "job_id": job_id, "output": output}
    else:
        return {"success": False, "error": output}


def check_hpc_jobs(hpc_config: dict[str, str] | None = None) -> list[dict]:
    """Check SLURM job status on HPC."""
    cfg_hpc = hpc_config or _get_hpc_config()
    ssh = _ssh_cmd(cfg_hpc["host"], cfg_hpc["user"])

    result = subprocess.run(
        ssh + [f"squeue -u {cfg_hpc['user']} -o '%i %T %N %M %S' --noheader"],
        capture_output=True, text=True, timeout=15,
    )

    jobs = []
    if result.returncode == 0 and result.stdout.strip():
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) >= 3:
                jobs.append({
                    "id": parts[0],
                    "state": parts[1],
                    "node": parts[2] if len(parts) > 2 else "?",
                    "time": parts[3] if len(parts) > 3 else "?",
                    "start": parts[4] if len(parts) > 4 else "?",
                })
    return jobs


def collect_results(hpc_config: dict[str, str] | None = None, db_path: str = "") -> int:
    """Collect results from completed HPC batch jobs."""
    cfg_hpc = hpc_config or _get_hpc_config()
    local_db = db_path or str(DEFAULT_DB)

    ssh = _ssh_cmd(cfg_hpc["host"], cfg_hpc["user"])
    ls_cmd = f"ls -d {cfg_hpc['dir']}/results-* 2>/dev/null || true"
    result = subprocess.run(
        ssh + [ls_cmd],
        capture_output=True, text=True, timeout=15,
    )

    remote_dirs = result.stdout.strip().split("\n") if result.stdout.strip() else []
    collected = 0

    for remote_dir in remote_dirs:
        remote_dir = remote_dir.strip()
        if not remote_dir:
            continue

        # Check for summary.json (job is done)
        check_cmd = f"cat {remote_dir}/summary.json 2>/dev/null || echo 'not_found'"
        r = subprocess.run(ssh + [check_cmd], capture_output=True, text=True, timeout=10)
        summary_text = r.stdout.strip()

        if summary_text == "not_found" or not summary_text:
            continue

        try:
            summary = json.loads(summary_text)
            job_id = summary.get("slurm_job_id") or summary.get("job")
        except json.JSONDecodeError:
            job_id = remote_dir.split("-")[-1]
        
        # Fallback: use directory name if job_id is still unknown
        if not job_id or str(job_id) == "None":
            job_id = remote_dir.rstrip("/").split("-")[-1]

        # rsync the completed DB back
        remote_db_path = f"{remote_dir}/tasks-completed.db"
        local_result_path = HPC_AGENT_DIR / f"results-{job_id}.db"

        rsync_result = subprocess.run(
            ["rsync", "-az",
             f"{cfg_hpc['user']}@{cfg_hpc['host']}:{remote_db_path}",
             str(local_result_path)],
            capture_output=True, text=True, timeout=30,
        )

        if rsync_result.returncode == 0 and local_result_path.exists():
            collected += 1
            print(f"[collect] Job {job_id}: {local_result_path}")

            # Optionally merge into local tasks.db
            merge_results(str(local_result_path), local_db, job_id)
        else:
            print(f"[collect] Job {job_id}: rsync FAILED")

    return collected


def merge_results(source_db: str, target_db: str, job_id: str = ""):
    """Merge completed results from HPC DB back into local DB."""
    if not os.path.exists(source_db):
        print(f"[merge] Source DB not found: {source_db}")
        return

    if os.path.getsize(source_db) == 0:
        print(f"[merge] Source DB is empty: {source_db}")
        return

    src = sqlite3.connect(source_db)
    tgt = sqlite3.connect(target_db)
    init_db(target_db)

    # Check if source has a tasks table
    tables = src.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'").fetchall()
    if not tables:
        print(f"[merge] No tasks table in source DB: {source_db}")
        src.close()
        tgt.close()
        return

    # Update tasks that exist in target with results from source
    rows = src.execute("""
        SELECT id, status, result, error, started_at, completed_at
        FROM tasks WHERE status IN ('completed', 'failed')
    """).fetchall()

    updated = 0
    for row in rows:
        task_id, status, result, error, started_at, completed_at = row
        tgt.execute("""
            UPDATE tasks SET
                status = ?, result = ?, error = ?,
                started_at = COALESCE(started_at, ?),
                completed_at = COALESCE(completed_at, ?)
            WHERE id = ?
        """, (status, result, error, started_at, completed_at, task_id))
        if tgt.total_changes > 0:
            updated += 1

    tgt.commit()
    src.close()
    tgt.close()
    print(f"[merge] Updated {updated} tasks in {target_db}")


# ── CLI ─────────────────────────────────────────────────────────────────────

def cmd_submit(args):
    """hpc_batch.py submit --prompt "..." [--prompt-file /path/to/prompt.txt] [--file tasks.jsonl]"""
    # Resolve prompt source: --prompt-file takes precedence over --prompt if both provided
    if args.prompt_file and not args.prompt:
        with open(args.prompt_file) as f:
            args.prompt = f.read()
    if args.file:
        with open(args.file) as f:
            if args.file.endswith(".jsonl"):
                lines = [json.loads(line) for line in f if line.strip()]
            else:
                lines = [{"prompt": line.strip()}
                        for line in f if line.strip()]

        ids = []
        for item in lines:
            meta = dict(item.get("metadata") or {})
            meta["type"] = args.type
            tid = submit_task(
                prompt=item.get("prompt", item.get("messages", "")),
                db_path=args.db,
                model=item.get("model", args.model),
                metadata=meta,
            )
            ids.append(tid)

        print(f"Submitted {len(ids)} tasks")
        return ids

    else:
        tid = submit_task(
            prompt=args.prompt,
            db_path=args.db,
            model=args.model,
            metadata={"type": args.type},
        )
        print(tid)
        return [tid]


def cmd_status(args):
    """hpc_batch.py status [--hpc]"""
    local = get_queue_status(args.db)
    print(f"Local queue ({args.db}):")
    print(f"  Pending:   {local['pending']}")
    print(f"  Running:   {local['running']}")
    print(f"  Completed: {local['completed']}")
    print(f"  Failed:    {local['failed']}")
    print(f"  Total:     {local['total']}")

    if args.hpc:
        print(f"\nHPC cluster ({cfg('hpc.host', 'hpc-login.u-strasbg.fr')}):")
        jobs = check_hpc_jobs()
        if jobs:
            for j in jobs:
                print(f"  [{j['state']}] {j['id']} on {j['node']} ({j['time']})")
                if j.get("start") and j["start"] != "?":
                    print(f"    start: {j['start']}")
        else:
            print("  No active jobs")


def cmd_pending(args):
    """hpc_batch.py pending [--json] [--type TYPE]"""
    tasks = get_pending_tasks(args.db, args.type)
    if args.json:
        print(json.dumps(tasks, indent=2))
        return

    if not tasks:
        print("No pending tasks")
        return

    print(f"{'ID':<40} {'PROMPT':<60} {'CREATED'}")
    print("-" * 110)
    for t in tasks:
        created = t["created_at"][:19] if t["created_at"] else "?"
        print(f"{t['id']:<40} {t['prompt_preview']:<60} {created}")


def cmd_dispatch(args):
    """hpc_batch.py dispatch [--force]"""
    local = get_queue_status(args.db)

    if local["pending"] == 0 and not args.force:
        print("[dispatch] No pending tasks. Use --force to submit anyway.")
        return

    if local["pending"] == 0 and args.force:
        print("[dispatch] Force: submitting even with empty queue (testing).")

    print(f"[dispatch] {local['pending']} pending tasks")

    # Rsync DB to HPC
    hpc_cfg = _get_hpc_config()
    if not rsync_to_hpc(args.db, hpc_cfg):
        print("[dispatch] FAILED: db sync to HPC")
        return

    # Submit SLURM job
    result = sbatch_on_hpc(hpc_cfg)
    if result["success"]:
        print(f"[dispatch] OK — job {result['job_id']} submitted")

        # Mark dispatched tasks with the job ID
        mark_dispatched(args.db, result["job_id"])
    else:
        print(f"[dispatch] FAILED: {result.get('error', 'unknown')}")


def mark_dispatched(db_path: str, job_id: str):
    """Mark pending tasks with the SLURM job ID."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        UPDATE tasks SET slurm_job_id = ?
        WHERE status = 'pending' AND slurm_job_id IS NULL
    """, (int(job_id),))
    affected = conn.total_changes
    conn.commit()
    conn.close()
    print(f"[dispatch] Marked {affected} tasks with job {job_id}")


def cmd_collect(args):
    """hpc_batch.py collect [--all]"""
    count = collect_results(
        db_path=args.db,
    )

    if count > 0:
        print(f"[collect] Collected results from {count} job(s)")
    else:
        print("[collect] No completed jobs found")


def cmd_view(args):
    """hpc_batch.py view <task-id> [--json]"""
    result = get_result(args.db, args.task_id)
    if not result:
        err = f"Task not found: {args.task_id}"
        if args.json:
            print(json.dumps({"error": err}))
        else:
            print(err)
        return

    if args.json:
        print(json.dumps(result, indent=2))
        return

    print(f"Status:      {result['status']}")
    print(f"Error:       {result.get('error', 'none')}")
    print(f"Started:     {result.get('started_at', '?')}")
    print(f"Completed:   {result.get('completed_at', '?')}")

    if result.get("usage"):
        print(f"Usage:       {json.dumps(result['usage'])}")

    content = result.get("content", "")
    if content:
        print(f"\nContent ({len(content)} chars):")
        print(content[:2000])
        if len(content) > 2000:
            print(f"... ({len(content) - 2000} more chars)")


def cmd_submit_and_wait(args):
    """hpc_batch.py submit-and-wait --prompt "..." [--prompt-file /path/to/prompt.txt] [--type TYPE] [--poll SEC] [--timeout SEC]"""
    if args.prompt_file and not args.prompt:
        with open(args.prompt_file) as f:
            args.prompt = f.read()
    task_id = submit_task(
        prompt=args.prompt,
        db_path=args.db,
        model=args.model,
        agent_id="hpc-batch",
        metadata={"type": args.type},
    )
    print(f"Task: {task_id}", file=sys.stderr)

    poll = args.poll
    deadline = time.time() + args.timeout
    db = args.db or str(DEFAULT_DB)

    while time.time() < deadline:
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status, result, error FROM tasks WHERE id = ?", (task_id,)).fetchone()
        conn.close()

        if row is None:
            print(f"ERROR: task {task_id} not found")
            sys.exit(1)

        status = row[0]
        if status == "completed":
            print(json.dumps({"task_id": task_id, "status": "completed", "result": json.loads(row[1]) if row[1] else None}))
            return
        elif status == "failed":
            print(json.dumps({"task_id": task_id, "status": "failed", "error": row[2]}))
            sys.exit(1)

        time.sleep(poll)

    print(f"TIMEOUT: task {task_id} not completed within {args.timeout}s")
    sys.exit(1)


def cmd_count(args):
    """hpc_batch.py count [--status pending] [--type TYPE]"""
    db = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db)
    if args.type:
        row = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = ? AND json_extract(metadata, '$.type') = ?",
                          (args.status, args.type)).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM tasks WHERE status = ?", (args.status,)).fetchone()
    conn.close()
    print(row[0])


def main():
    parser = argparse.ArgumentParser(
        description="HPC Batch — accumulate LLM tasks, dispatch to HPC GPU, collect results",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB),
                        help=f"SQLite database path (default: {DEFAULT_DB})")
    parser.add_argument("--model", default="deepseek-ai/DeepSeek-V4-Flash",
                        help="Model name")
    sub = parser.add_subparsers(dest="command", required=True)

    # submit
    p_submit = sub.add_parser("submit", help="Submit task(s) to the queue")
    p_submit.add_argument("--prompt", help="Single prompt text")
    p_submit.add_argument("--prompt-file", help="File containing prompt text (avoids argv limits)")
    p_submit.add_argument("--file", help="File with prompts (one per line or JSONL)")
    p_submit.add_argument("--type", default="generic", help="Task type tag for filtering")
    p_submit.set_defaults(func=cmd_submit)


    # status
    p_status = sub.add_parser("status", help="Show queue status")
    p_status.add_argument("--hpc", action="store_true", help="Also show HPC job status")
    p_status.set_defaults(func=cmd_status)

    # pending
    p_pending = sub.add_parser("pending", help="List pending tasks")
    p_pending.add_argument("--json", action="store_true", help="JSON output")
    p_pending.add_argument("--type", help="Filter by task type")
    p_pending.set_defaults(func=cmd_pending)

    # dispatch
    p_dispatch = sub.add_parser("dispatch", help="Dispatch tasks to HPC")
    p_dispatch.add_argument("--force", action="store_true",
                           help="Submit even if queue is empty (testing)")
    p_dispatch.set_defaults(func=cmd_dispatch)

    # collect
    p_collect = sub.add_parser("collect", help="Collect results from HPC")
    p_collect.add_argument("--all", action="store_true",
                          help="Collect from all completed jobs")
    p_collect.set_defaults(func=cmd_collect)

    # view
    p_view = sub.add_parser("view", help="View task result")
    p_view.add_argument("task_id", help="Task UUID")
    p_view.add_argument("--json", action="store_true", help="JSON output (machine-readable)")
    p_view.set_defaults(func=cmd_view)

    # submit-and-wait
    p_sw = sub.add_parser("submit-and-wait", help="Submit and block until result")
    p_sw.add_argument("--prompt", help="Prompt text")
    p_sw.add_argument("--prompt-file", help="File containing prompt text")
    p_sw.add_argument("--type", default="generic")
    p_sw.add_argument("--poll", type=int, default=60)
    p_sw.add_argument("--timeout", type=int, default=86400)
    p_sw.set_defaults(func=cmd_submit_and_wait)

    # count
    p_count = sub.add_parser("count", help="Count tasks by status and type")
    p_count.add_argument("--type", help="Filter by task type")
    p_count.add_argument("--status", default="pending", help="Filter by status (default: pending)")
    p_count.set_defaults(func=cmd_count)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
