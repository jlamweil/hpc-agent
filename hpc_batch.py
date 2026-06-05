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

    # Show tasks blocked by unmet dependencies
    hpc_batch.py blocked [--json]

    # Expire old pending tasks
    hpc_batch.py expire [--older-than SECONDS]

    # DAG dependency support:
    #   hpc_batch.py submit --prompt "A"                           # task with no deps
    #   hpc_batch.py submit --depends-on <A-id> --prompt "B"       # B runs after A
    #   hpc_batch.py blocked                                       # show what's stuck
    #
    # Data collectors:
    #   hpc_batch.py fetch arxiv --query "attention" --max 5      # fetch from arXiv, submit tasks
    #   hpc_batch.py fetch github --url https://github.com/...    # analyze a repo
    #   hpc_batch.py fetch kaggle --competition titanic           # competition scaffolding
    #   hpc_batch.py fetch web --query "topic"                    # web search + analyze
    #   hpc_batch.py fetch --list                                 # list available collectors
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

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

    # Task dependencies (DAG support)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_deps (
            task_id TEXT NOT NULL,
            depends_on TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (task_id, depends_on)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_deps_task ON task_deps(task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_deps_dep ON task_deps(depends_on)")

    conn.commit()
    return conn


def _detect_cycle(
    task_id: str,
    depends_on: list[str],
    conn: sqlite3.Connection,
) -> bool:
    """BFS up the dep chain to detect if adding these deps creates a cycle.

    For each dependency, follow its transitive dependency chain.
    If we ever encounter task_id, adding this edge would create a cycle.

    Returns True if a cycle is detected.
    """
    visited: set[str] = set()
    queue: list[str] = list(depends_on)

    while queue:
        current = queue.pop(0)
        if current == task_id:
            return True  # Cycle: task_id is reachable from its own deps
        if current in visited:
            continue
        visited.add(current)
        rows = conn.execute(
            "SELECT depends_on FROM task_deps WHERE task_id = ?",
            (current,),
        ).fetchall()
        for row in rows:
            queue.append(row[0])

    return False


def cascade_failure(task_id: str, db_path: str = "") -> int:
    """Cascade failure of a task to all its pending dependents.

    Preemption guard: only cascades if the failed task has usage > 0
    (real failure, not preemption where 0 tokens were consumed).

    Args:
        task_id: The ID of the failed task.
        db_path: Path to SQLite DB.

    Returns:
        Number of tasks cascaded.
    """
    db = db_path or str(DEFAULT_DB)
    conn = init_db(db)

    # ── Preemption guard ─────────────────────────────────────────────
    # Only cascade if the task had usage tokens > 0 (real work was done).
    # If 0 or no result (preempted/killed), skip cascade — the task can be retried.
    row = conn.execute(
        "SELECT result FROM tasks WHERE id = ? AND status = 'failed'",
        (task_id,),
    ).fetchone()

    should_cascade = False
    if row and row[0]:
        try:
            result = json.loads(row[0])
            usage = result.get("usage", {})
            total_tokens = usage.get("total_tokens", 0)
            if total_tokens > 0:
                should_cascade = True
        except (json.JSONDecodeError, AttributeError, TypeError):
            should_cascade = False

    if not should_cascade:
        conn.close()
        return 0

    # ── Find and fail dependents ─────────────────────────────────────
    dependents = conn.execute(
        "SELECT task_id FROM task_deps WHERE depends_on = ?",
        (task_id,),
    ).fetchall()

    now = time.time()
    count = 0
    for dep_row in dependents:
        dep_id = dep_row[0]

        # Only cascade pending or running tasks
        status_row = conn.execute(
            "SELECT status FROM tasks WHERE id = ?", (dep_id,)
        ).fetchone()
        if status_row and status_row[0] in ("pending", "running"):
            conn.execute(
                """
                UPDATE tasks SET status = 'failed', error = ?,
                    completed_at = ?
                WHERE id = ?
                """,
                (f"Cascade failure: dependency {task_id} failed", now, dep_id),
            )
            count += 1

    conn.commit()
    conn.close()
    return count


def submit_task(
    prompt: str = "",
    db_path: str = "",
    model: str = "deepseek-ai/DeepSeek-V4-Flash",
    agent_id: str = "hpc-batch",
    metadata: dict | None = None,
    task_id: str = "",
    depends_on: list[str] | None = None,
    messages: list[dict] | None = None,
) -> str:
    """Add a task to the local SQLite queue.

    Args:
        prompt: Task prompt text (wrapped as user message if messages not provided).
        messages: Full messages array (overrides prompt if provided).
        db_path: Path to SQLite DB (default: tasks.db in HPC_AGENT_DIR).
        model: Model name for inference.
        agent_id: Identifier for the submitting agent.
        metadata: Arbitrary key-value metadata.
        task_id: Explicit task ID (auto-generated if empty).
        depends_on: List of task IDs this task depends on.
                     All must exist in the DB at submission time.

    Returns:
        The task ID.
    """
    db = db_path or str(DEFAULT_DB)
    task_id = task_id or str(uuid.uuid4())
    created_at = time.time()
    depends_on = depends_on or []

    # Resolve messages: use provided array, or wrap prompt as user message
    if messages:
        messages_json = json.dumps(messages)
    else:
        messages_json = json.dumps([{"role": "user", "content": prompt or ""}])

    conn = init_db(db)

    # Validate dependencies exist
    for dep_id in depends_on:
        row = conn.execute(
            "SELECT 1 FROM tasks WHERE id = ?", (dep_id,)
        ).fetchone()
        if not row:
            conn.close()
            raise ValueError(
                f"Dependency task '{dep_id}' not found in database"
            )

    # Cycle detection: BFS from each dep, check if task_id is reachable
    if _detect_cycle(task_id, depends_on, conn):
        conn.close()
        raise ValueError(
            f"Circular dependency detected: task would depend on itself"
        )

    conn.execute("""
        INSERT INTO tasks (
            id, agent_id, messages, tools, model, api_config,
            metadata, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        agent_id,
        messages_json,
        json.dumps([]),
        model,
        json.dumps({}),
        json.dumps(metadata or {}),
        created_at,
        "pending",
    ))

    # Insert dependency edges
    for dep_id in depends_on:
        conn.execute("""
            INSERT OR IGNORE INTO task_deps (task_id, depends_on, created_at)
            VALUES (?, ?, ?)
        """, (task_id, dep_id, created_at))

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


# ── Preprocess ──────────────────────────────────────────────────────────

def cmd_preprocess(args):
    """hpc_batch.py preprocess paper --pdf path/to/paper.pdf [--chunk N] [--overlap M]
       hpc_batch.py preprocess repo --url https://github.com/user/repo [--chunk N]
    
    Preprocesses content for HPC analysis: extracts text, chunks by token count,
    and submits as a DAG of analysis + synthesis tasks.
    """
    from collectors.base import ChunkedTaskBuilder

    if args.pdf:
        # PDF preprocessing
        from collectors.pdf import extract_text
        print(f"[preprocess] Extracting text from {args.pdf}...")
        result = extract_text(args.pdf)
        if "error" in result:
            print(f"[preprocess] ERROR: {result['error']}")
            return

        text = result["text"]
        print(f"[preprocess] Extracted {result['total_chars']} chars from {result['pages']} pages")
        for w in result.get("warnings", []):
            print(f"  WARNING: {w}")

        if not text.strip():
            print("[preprocess] No text extracted — cannot create tasks")
            return

        window = args.chunk
        overlap = args.overlap
        builder = ChunkedTaskBuilder(window=window, overlap=overlap)

        base_meta = {
            "source": "pdf",
            "source_id": str(Path(args.pdf).resolve()),
            "title": Path(args.pdf).stem,
            "provenance": {
                "collector": "preprocess/1.0",
                "collected_at": time.time(),
                "source": "pdf",
            },
        }

        tasks = builder.build(
            text,
            task_type=args.type,
            base_metadata=base_meta,
            model=args.model,
            analysis_query=args.query,
        )

        if not tasks:
            print("[preprocess] No tasks created")
            return

        print(f"[preprocess] Created {len(tasks)} tasks ({len(tasks)-1} chunks + 1 synthesis)")

    elif args.url:
        # TODO: repo preprocessing (clone + chunk source files)
        print("[preprocess] Repo preprocessing not yet implemented. Use 'hpc_batch.py fetch github' instead.")
        return

    else:
        print("USAGE: hpc_batch.py preprocess paper --pdf <path> [--chunk N]")
        print("       hpc_batch.py preprocess repo --url <url> [--chunk N]")
        return

    # Submit tasks and wire dependencies
    task_ids = []
    chunk_ids = []

    for task in tasks:
        meta = task.get("metadata", {})
        is_chunk = meta.get("is_chunk", False)
        is_synthesis = meta.get("is_synthesis", False)
        deps = task.get("depends_on", [])

        # For the first submission, synthesis depends on chunk IDs that don't exist yet.
        # We submit chunks first, then re-submit synthesis with real IDs.
        if is_synthesis:
            continue  # handle after chunks

        tid = submit_task(
            prompt=task["prompt"],
            db_path=args.db,
            model=task.get("model", args.model),
            metadata=meta,
            depends_on=deps if deps else None,
            task_id=task.get("task_id", ""),
        )
        task_ids.append(tid)
        if is_chunk:
            chunk_ids.append(tid)

    # Now submit synthesis with real chunk IDs
    synthesis_task = tasks[-1] if tasks else None
    if synthesis_task and synthesis_task.get("metadata", {}).get("is_synthesis"):
        tid = submit_task(
            prompt=synthesis_task["prompt"],
            db_path=args.db,
            model=synthesis_task.get("model", args.model),
            metadata=synthesis_task.get("metadata", {}),
            depends_on=chunk_ids,
            task_id=synthesis_task.get("task_id", ""),
        )
        task_ids.append(tid)
        print(f"[preprocess] Synthesis task: {tid}")
    else:
        print(f"[preprocess] Warning: no synthesis task created")

    print(f"[preprocess] Submitted {len(task_ids)} tasks")
    for tid in task_ids:
        print(f"  {tid}")


# ── Validate ─────────────────────────────────────────────────────────────

def cmd_validate(args):
    """hpc_batch.py validate [--task <id>] [--all] [--cache-stats]"""
    from collectors.base import validate_metadata
    from collectors.types import known_types, get_type_info

    if args.cache_stats:
        # Show cache stats for all collectors
        from collectors.cache import CollectorCache
        from collectors.config import get_collector_config, get_cache_dir
        import glob
        cache_dir = Path.home() / ".cache" / "hpc-agent" / "collectors"
        for d in sorted(cache_dir.glob("*")):
            if d.is_dir():
                try:
                    cfg = get_collector_config(d.name)
                    ttl = cfg.get("cache_ttl", 3600)
                    cache = CollectorCache(d, ttl=ttl)
                    stats = cache.stats()
                    print(f"  {d.name}: {stats['total_entries']} entries ({stats['fresh']} fresh, {stats['expired']} expired)")
                except Exception:
                    pass
        return

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if args.task:
        rows = conn.execute(
            "SELECT id, metadata FROM tasks WHERE id = ?", (args.task,)
        ).fetchall()
    elif args.all:
        rows = conn.execute(
            "SELECT id, metadata FROM tasks WHERE status = 'pending'"
        ).fetchall()
    else:
        conn.close()
        print("USAGE: hpc_batch.py validate --task <id> | --all | --cache-stats")
        return

    total_warnings = 0
    type_counts: dict[str, int] = {}
    for row in rows:
        meta = json.loads(row["metadata"])
        warnings = validate_metadata(meta)

        # Check against type registry
        t = meta.get("type", "")
        type_counts[t] = type_counts.get(t, 0) + 1
        type_info = get_type_info(t)
        if type_info and type_info["required_metadata"]:
            for req in type_info["required_metadata"]:
                if req not in meta:
                    warnings.append(f"Type '{t}' requires metadata key: '{req}'")

        if warnings:
            print(f"Warnings for {row['id']}:")
            for w in warnings:
                print(f"  - {w}")
                total_warnings += 1

    if total_warnings == 0:
        print(f"No warnings ({len(rows)} tasks checked)")

    # Show type distribution
    if type_counts:
        print(f"\nTask type distribution:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            info = get_type_info(t)
            desc = f" — {info['description']}" if info else ""
            print(f"  {t}: {c}{desc}")

    conn.close()


# ── Collectors ──────────────────────────────────────────────────────────

def _import_collectors():
    """Import collectors package to trigger auto-registration."""
    import collectors  # noqa: F401 — auto-registers on import
    return collectors


# ── Archive ────────────────────────────────────────────────────────────

def cmd_archive(args):
    """hpc_batch.py archive [--older-than DAYS] [--status completed|failed] [--list] [--query TYPE]
       hpc_batch.py archive restore <task-id>
    """
    db = args.db or str(DEFAULT_DB)
    archive_db = str(Path(db).parent / "tasks-archive.db")

    if args.restore:
        # Restore a task from archive back to active DB
        arch = sqlite3.connect(archive_db)
        row = arch.execute("SELECT * FROM tasks WHERE id = ?", (args.restore,)).fetchone()
        if not row:
            print(f"Task {args.restore} not found in archive")
            arch.close()
            return
        # Get column names
        cols = [d[0] for d in arch.execute("PRAGMA table_info(tasks)").fetchall()]
        arch.close()

        active = init_db(db)
        placeholders = ",".join("?" * len(cols))
        cols_str = ",".join(cols)
        active.execute(f"INSERT OR REPLACE INTO tasks ({cols_str}) VALUES ({placeholders})", list(row))
        active.commit()

        # Delete from archive
        arch2 = sqlite3.connect(archive_db)
        arch2.execute("DELETE FROM tasks WHERE id = ?", (args.restore,))
        arch2.commit()
        arch2.close()
        active.close()
        print(f"Restored task {args.restore} to active DB")
        return

    if args.list:
        if not os.path.exists(archive_db):
            print("No archive database found")
            return
        arch = sqlite3.connect(archive_db)
        arch.row_factory = sqlite3.Row

        if args.query:
            rows = arch.execute(
                "SELECT id, status, created_at, json_extract(metadata, '$.type') as task_type, "
                "json_extract(metadata, '$.title') as title "
                "FROM tasks WHERE json_extract(metadata, '$.type') = ? ORDER BY created_at DESC LIMIT 50",
                (args.query,),
            ).fetchall()
        else:
            rows = arch.execute(
                "SELECT id, status, created_at, json_extract(metadata, '$.type') as task_type, "
                "json_extract(metadata, '$.title') as title "
                "FROM tasks ORDER BY created_at DESC LIMIT 50",
            ).fetchall()

        print(f"{'ID':<40} {'STATUS':<12} {'TYPE':<20} {'CREATED'}")
        print("-" * 90)
        for r in rows:
            created = datetime.fromtimestamp(r["created_at"], tz=timezone.utc).strftime("%Y-%m-%d") if r["created_at"] else "?"
            print(f"{r['id']:<40} {r['status']:<12} {str(r['task_type'] or ''):<20} {created}")
        arch.close()
        return

    # Archive old tasks
    older_than_days = args.older_than
    cutoff = time.time() - (older_than_days * 86400)

    active = init_db(db)

    # Get tasks to archive
    status_filter = args.status or ""
    if status_filter:
        rows = active.execute(
            "SELECT * FROM tasks WHERE completed_at < ? AND status = ?",
            (cutoff, status_filter),
        ).fetchall()
    else:
        rows = active.execute(
            "SELECT * FROM tasks WHERE completed_at < ? AND status IN ('completed', 'failed')",
            (cutoff,),
        ).fetchall()

    if not rows:
        print(f"No tasks older than {older_than_days} days to archive")
        active.close()
        return

    # Get column names
    cols = [d[0] for d in active.execute("PRAGMA table_info(tasks)").fetchall()]
    placeholders = ",".join("?" * len(cols))
    cols_str = ",".join(cols)

    # Open archive DB (ensure schema)
    arch_db = init_db(archive_db)

    # Copy task_deps too
    active.execute("""
        CREATE TABLE IF NOT EXISTS task_deps (
            task_id TEXT NOT NULL, depends_on TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY (task_id, depends_on)
        )
    """)
    arch_db.execute("""
        CREATE TABLE IF NOT EXISTS task_deps (
            task_id TEXT NOT NULL, depends_on TEXT NOT NULL, created_at REAL NOT NULL,
            PRIMARY KEY (task_id, depends_on)
        )
    """)

    moved = 0
    task_ids = []
    for row in rows:
        task_ids.append(row[0])
        try:
            arch_db.execute(f"INSERT INTO tasks ({cols_str}) VALUES ({placeholders})", list(row))
            # Copy deps
            deps = active.execute("SELECT depends_on, created_at FROM task_deps WHERE task_id = ?", (row[0],)).fetchall()
            for dep in deps:
                arch_db.execute("INSERT OR IGNORE INTO task_deps (task_id, depends_on, created_at) VALUES (?, ?, ?)",
                                (row[0], dep[0], dep[1]))
        except sqlite3.IntegrityError:
            # Already in archive
            pass
        moved += 1

    # Delete from active
    task_placeholders = ",".join("?" * len(task_ids))
    active.execute(f"DELETE FROM tasks WHERE id IN ({task_placeholders})", task_ids)
    active.execute(f"DELETE FROM task_deps WHERE task_id IN ({task_placeholders})", task_ids)

    arch_db.commit()
    active.commit()
    arch_db.close()
    active.close()

    print(f"Archived {moved} tasks to {archive_db}")


def _run_cleanup(args):
    """Run cleanup of old tasks, proxy files, cache entries."""
    from datetime import datetime, timedelta
    import glob

    db = args.db or str(DEFAULT_DB)
    dry_run = args.dry_run

    # 1. Cleanup old tasks
    print("Cleaning up old tasks...")
    if not dry_run:
        before_completed = (datetime.now() - timedelta(days=args.completed_days)).timestamp()
        before_failed = (datetime.now() - timedelta(days=args.failed_days)).timestamp()
        conn = sqlite3.connect(db)
        deleted_comp = conn.execute(
            "DELETE FROM tasks WHERE status = 'completed' AND created_at < ?",
            (before_completed,),
        ).rowcount
        deleted_fail = conn.execute(
            "DELETE FROM tasks WHERE status = 'failed' AND created_at < ?",
            (before_failed,),
        ).rowcount
        conn.commit()
        conn.close()
        print(f"  Deleted {deleted_comp} completed, {deleted_fail} failed tasks")
    else:
        print(f"  Would delete tasks older than {args.completed_days}d (completed) / {args.failed_days}d (failed)")

    # 2. Cleanup proxy files
    import subprocess
    hpc_cfg = _get_hpc_config()
    proxy_files_days = cfg("cleanup.proxy_files_days", 7)
    print(f"Cleaning up proxy files older than {proxy_files_days} days...")
    if not dry_run:
        result = subprocess.run(
            ["ssh", f"{hpc_cfg['user']}@{hpc_cfg['host']}",
             f"find {hpc_cfg['dir']}/fetch-requests {hpc_cfg['dir']}/fetch-responses "
             f"-type f -mtime +{proxy_files_days} -delete 2>/dev/null; "
             f"echo 'done'"],
            capture_output=True, text=True, timeout=30,
        )
        print(f"  HPC proxy cleanup: {result.stdout.strip()}")
    else:
        print(f"  Would clean proxy files on HPC older than {proxy_files_days}d")

    # 3. Cleanup collector cache
    cache_dir = Path.home() / ".cache" / "hpc-agent" / "collectors"
    cache_entries_days = cfg("cleanup.cache_entries_days", 30)
    if cache_dir.exists():
        print(f"Cleaning up cache entries older than {cache_entries_days} days...")
        count = 0
        for cache_file in cache_dir.rglob("cache.jsonl"):
            if dry_run:
                print(f"  Would clean {cache_file}")
                count += 1
            else:
                # Rewrite cache file, keeping only fresh entries
                from collectors.cache import CollectorCache
                parent_name = cache_file.parent.name
                col_cfg = cfg(f"collectors.{parent_name}", {})
                ttl = col_cfg.get("cache_ttl", 3600)
                cc = CollectorCache(cache_file.parent, ttl=ttl)
                cc.clear_expired()
                count += 1
        print(f"  Cleaned {count} cache directories")

    if dry_run:
        print("DRY RUN — no changes made")


# ── Watch ──────────────────────────────────────────────────────────────

def cmd_watch(args):
    """hpc_batch.py watch [--poll SEC] [--type TYPE] [--source SOURCE] [--json]"""
    poll = args.poll
    seen: set[str] = set()

    print(f"[watch] Polling every {poll}s...")
    if args.type:
        print(f"  Filter: type={args.type}")
    if args.source:
        print(f"  Filter: source={args.source}")

    try:
        while True:
            db = args.db or str(DEFAULT_DB)
            if not os.path.exists(db):
                time.sleep(poll)
                continue

            conn = sqlite3.connect(db)
            conn.row_factory = sqlite3.Row

            query = """
                SELECT id, status, result, error, completed_at,
                       json_extract(metadata, '$.type') as task_type,
                       json_extract(metadata, '$.source') as task_source,
                       json_extract(metadata, '$.title') as task_title
                FROM tasks
                WHERE status IN ('completed', 'failed')
                ORDER BY completed_at DESC
                LIMIT 50
            """
            rows = conn.execute(query).fetchall()
            conn.close()

            for row in rows:
                tid = row["id"]
                if tid in seen:
                    continue
                seen.add(tid)

                if args.type and row["task_type"] != args.type:
                    continue
                if args.source and row["task_source"] != args.source:
                    continue

                completed = row["completed_at"]
                ts = datetime.fromtimestamp(completed, tz=timezone.utc).strftime("%H:%M:%S") if completed else "?"
                task_type = row["task_type"] or "?"
                title = (row["task_title"] or tid)[:50]
                status = row["status"]

                if args.json:
                    print(json.dumps(dict(row)))
                else:
                    if status == "completed":
                        content_preview = ""
                        if row["result"]:
                            try:
                                r = json.loads(row["result"])
                                content_preview = r.get("content", "")[:80]
                            except json.JSONDecodeError:
                                content_preview = row["result"][:80]
                        print(f"[{ts}] {tid[:12]}  {task_type:<18} {status:>9}  {title:<40}  {content_preview}")
                    else:
                        error_preview = (row["error"] or "")[:60]
                        print(f"[{ts}] {tid[:12]}  {task_type:<18} {status:>9}  {title:<40}  FAIL: {error_preview}")

            time.sleep(poll)
    except KeyboardInterrupt:
        print("\n[watch] Stopped")


# ── Export ─────────────────────────────────────────────────────────────

def cmd_export(args):
    """hpc_batch.py export <task-id> [--format markdown|json|csv] [--output FILE]
       hpc_batch.py export --type TYPE --source SOURCE --format markdown --dir ./reports/
    """
    db = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    if args.dir:
        # Batch export
        Path(args.dir).mkdir(parents=True, exist_ok=True)
        where_clauses = ["status IN ('completed', 'failed')"]
        params: list[Any] = []
        if args.type:
            where_clauses.append("json_extract(metadata, '$.type') = ?")
            params.append(args.type)
        if args.source:
            where_clauses.append("json_extract(metadata, '$.source') = ?")
            params.append(args.source)

        rows = conn.execute(
            f"SELECT * FROM tasks WHERE {' AND '.join(where_clauses)}",
            params,
        ).fetchall()
        count = 0
        for row in rows:
            filepath = Path(args.dir) / f"{row['id'][:12]}.md"
            _export_task(filepath, row, args.format)
            count += 1
        print(f"Exported {count} tasks to {args.dir}/")
        conn.close()
        return

    # Single task export
    if not args.task_id:
        print("USAGE: hpc_batch.py export <task-id> [--format markdown] [--output FILE]")
        print("       hpc_batch.py export --type TYPE --source SOURCE --format markdown --dir ./reports/")
        conn.close()
        return

    row = conn.execute("SELECT * FROM tasks WHERE id = ?", (args.task_id,)).fetchone()
    conn.close()

    if not row:
        print(f"Task not found: {args.task_id}")
        return

    if args.output:
        _export_task(Path(args.output), row, args.format)
        print(f"Exported to {args.output}")
    else:
        # Print to stdout
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            _export_task(Path(f.name), row, args.format)
            print(Path(f.name).read_text())
        Path(f.name).unlink()


def _export_task(filepath: Path, row: sqlite3.Row, fmt: str = "markdown"):
    """Write a single task to a file in the specified format."""
    task_id = row["id"]
    status = row["status"]
    result_json = row["result"]
    error = row["error"] or ""
    messages = json.loads(row["messages"]) if row["messages"] else []
    metadata = json.loads(row["metadata"]) if row["metadata"] else {}
    model = row["model"]
    completed_at = row["completed_at"]

    # Extract prompt
    prompt = ""
    for msg in messages if isinstance(messages, list) else []:
        if msg.get("role") in ("user", "system"):
            prompt += msg.get("content", "") + "\n"

    # Extract result content
    content = ""
    usage = {}
    if result_json:
        try:
            r = json.loads(result_json)
            content = r.get("content", "")
            usage = r.get("usage", {})
        except json.JSONDecodeError:
            content = result_json

    if fmt == "json":
        filepath.write_text(json.dumps({
            "id": task_id,
            "status": status,
            "model": model,
            "prompt": prompt,
            "content": content,
            "usage": usage,
            "error": error,
            "metadata": metadata,
            "completed_at": completed_at,
        }, indent=2, default=str))
    elif fmt == "csv":
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "status", "model", "type", "source", "prompt_length", "content_length",
                     "prompt_tokens", "completion_tokens", "error"])
        w.writerow([
            task_id, status, model,
            metadata.get("type", ""), metadata.get("source", ""),
            len(prompt), len(content),
            usage.get("prompt_tokens", ""), usage.get("completion_tokens", ""),
            error,
        ])
        filepath.write_text(buf.getvalue())
    else:  # markdown (default)
        completed_str = datetime.fromtimestamp(completed_at, tz=timezone.utc).isoformat() if completed_at else "?"
        md = f"""# Analysis: {metadata.get('title', task_id[:24])}

**Task:** `{task_id}`
**Status:** {status}
**Model:** {model}
**Type:** {metadata.get('type', '?')}
**Source:** {metadata.get('source', '?')}
**Completed:** {completed_str}
"""
        if usage:
            md += f"""**Tokens:** {usage.get('prompt_tokens', 0)} prompt / {usage.get('completion_tokens', 0)} completion

"""
        if error:
            md += f"""**Error:** {error}

"""
        md += f"""## Prompt

{prompt.strip()[:2000]}

## Result

{content.strip()[:5000]}

## Metadata

```json
{json.dumps(metadata, indent=2, default=str)}
```
"""
        filepath.write_text(md)


# ── Pipeline ───────────────────────────────────────────────────────────

def cmd_pipeline(args):
    """hpc_batch.py pipeline run <yaml_path>
       hpc_batch.py pipeline status <pipeline-uuid>
    """
    import yaml

    if args.run:
        yaml_path = Path(args.run)
        if not yaml_path.exists():
            print(f"Pipeline file not found: {yaml_path}")
            return

        pipe = yaml.safe_load(yaml_path.read_text())
        if not pipe:
            print("Empty pipeline file")
            return

        name = pipe.get("name", "unnamed")
        pipeline_id = str(uuid.uuid4())
        print(f"[pipeline] Running '{name}' (id: {pipeline_id})")

        all_task_ids: list[str] = []
        prev_step_task_ids: list[str] = []

        for step_idx, step in enumerate(pipe.get("steps", [])):
            collector_name = step.get("collector", "")
            if not collector_name:
                print(f"  Step {step_idx}: no collector specified, skipping")
                continue

            print(f"  Step {step_idx}: {collector_name}...")

            # Run collector
            import collectors
            _import_collectors()
            try:
                collector_cls = collectors.get_collector(collector_name)
                collector = collector_cls()
                results = collector.collect(step.get("params", {}))
            except Exception as e:
                print(f"    ERROR: {e}")
                continue

            if not results:
                print(f"    No results")
                continue

            # Submit tasks for this step
            step_task_ids: list[str] = []
            for task_def in step.get("tasks", []):
                task_type = task_def.get("type", "generic")
                task_model = task_def.get("model", args.model)
                queries = task_def.get("queries", [f"Analyze this {collector_name} result"])

                for r in results:
                    for q in queries:
                        meta = {
                            "type": task_type,
                            "source": collector_name,
                            "source_id": r.source_id,
                            "source_url": r.url,
                            "title": r.title,
                            "tags": task_def.get("tags", []),
                            "pipeline_run": pipeline_id,
                            "pipeline_name": name,
                            "pipeline_step": step_idx,
                            "provenance": {
                                "collector": f"{collector_name}/1.0",
                                "collected_at": time.time(),
                                "source": collector_name,
                            },
                        }
                        tid = submit_task(
                            prompt=f"{q}\n\nTitle: {r.title}\n\n{r.text}",
                            db_path=args.db,
                            model=task_model,
                            metadata=meta,
                            depends_on=prev_step_task_ids if step_idx > 0 else None,
                        )
                        step_task_ids.append(tid)

            print(f"    Submitted {len(step_task_ids)} tasks")
            all_task_ids.extend(step_task_ids)
            prev_step_task_ids = step_task_ids

        print(f"[pipeline] Done — {len(all_task_ids)} total tasks submitted")
        print(f"Pipeline ID: {pipeline_id}")
        for tid in all_task_ids:
            print(f"  {tid}")

    elif args.status:
        pipeline_id = args.status
        db = args.db or str(DEFAULT_DB)
        if not os.path.exists(db):
            print(f"Database not found: {db}")
            return

        conn = sqlite3.connect(db)
        cursor = conn.execute(
            "SELECT id, status, error, json_extract(metadata, '$.pipeline_step') as step "
            "FROM tasks WHERE json_extract(metadata, '$.pipeline_run') = ?"
            "ORDER BY json_extract(metadata, '$.pipeline_step')",
            (pipeline_id,),
        )
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print(f"No tasks found for pipeline: {pipeline_id}")
            return

        print(f"Pipeline: {pipeline_id}")
        print(f"{'STEP':<6} {'ID':<40} {'STATUS':<12} {'ERROR'}")
        print("-" * 80)
        for r in rows:
            err = (r[2] or "")[:40]
            print(f"{str(r[3] or '?'):<6} {r[0]:<40} {r[1]:<12} {err}")

    else:
        print("USAGE: hpc_batch.py pipeline run <yaml_path>")
        print("       hpc_batch.py pipeline status <pipeline-uuid>")


def cmd_fetch(args):
    """hpc_batch.py fetch <source> [--query Q] [--max N] [--model M] [--tags TAGS]
    
    Fetches data from the specified source and submits analysis tasks.
    """
    import collectors

    if args.list:
        _import_collectors()
        print("Available collectors:")
        for name in sorted(collectors.list_collectors()):
            print(f"  {name}")
        return

    collector_cls = collectors.get_collector(args.source)
    collector = collector_cls()

    # Build params
    params = {}
    if args.query:
        params["query"] = args.query
    if args.max:
        params["max_results"] = args.max
    if args.id:
        params["id_list"] = args.id
    if args.url:
        params["url"] = args.url
    if args.files:
        params["files"] = args.files.split(",")
    if args.competition:
        params["competition"] = args.competition
    if args.dataset:
        params["dataset"] = args.dataset
    if args.refresh:
        params["refresh"] = True

    print(f"[fetch] Running {args.source} collector...")
    try:
        results = collector.collect(params)
    except ImportError as e:
        print(f"[fetch] ERROR: {e}")
        return
    except Exception as e:
        print(f"[fetch] ERROR: {e}")
        return

    if not results:
        print("[fetch] No results found")
        return

    print(f"[fetch] Got {len(results)} results, submitting tasks...")

    # Convert to tasks and submit
    task_kwargs = {"model": args.model}
    if args.tags:
        task_kwargs["tags"] = args.tags.split(",")

    tasks = collector.to_tasks(results, **task_kwargs)

    # Validate metadata
    from collectors.base import validate_metadata
    task_ids = []
    for task in tasks:
        meta = task.get("metadata", {})
        warnings = validate_metadata(meta)
        for w in warnings:
            print(f"  WARNING: {w}")

        tid = submit_task(
            prompt=task["prompt"],
            db_path=args.db,
            model=task.get("model", args.model),
            metadata=task.get("metadata", {}),
            depends_on=task.get("depends_on"),
        )
        task_ids.append(tid)

    print(f"[fetch] Submitted {len(task_ids)} tasks")
    if args.wait:
        print(f"[fetch] Waiting for results (poll every {args.wait}s)...")
        for tid in task_ids:
            deadline = time.time() + args.timeout
            db = args.db or str(DEFAULT_DB)
            while time.time() < deadline:
                conn = sqlite3.connect(db)
                row = conn.execute(
                    "SELECT status, result, error FROM tasks WHERE id = ?", (tid,)
                ).fetchone()
                conn.close()
                if row is None:
                    break
                if row[0] == "completed":
                    print(f"  {tid[:12]}... completed")
                    break
                elif row[0] == "failed":
                    print(f"  {tid[:12]}... FAILED: {row[2][:80]}")
                    break
                time.sleep(args.wait)
    else:
        for tid in task_ids:
            print(f"  {tid}")


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
            if args.cycle is not None:
                meta["cycle"] = args.cycle
            tid = submit_task(
                prompt=item.get("prompt", item.get("messages", "")),
                db_path=args.db,
                model=item.get("model", args.model),
                metadata=meta,
                depends_on=args.depends_on,
            )
            ids.append(tid)

        print(f"Submitted {len(ids)} tasks")
        return ids

    else:
        meta = {"type": args.type}
        if args.cycle is not None:
            meta["cycle"] = args.cycle
        tid = submit_task(
            prompt=args.prompt,
            db_path=args.db,
            model=args.model,
            metadata=meta,
            depends_on=args.depends_on,
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


def get_blocked_tasks(db_path: str = "") -> list[dict]:
    """Find pending tasks whose dependencies are not all completed.

    Returns a list of dicts with task info and the blocking task IDs.
    """
    db = db_path or str(DEFAULT_DB)
    if not os.path.exists(db):
        return []

    conn = init_db(db)
    conn.row_factory = sqlite3.Row

    # Find pending tasks with unmet deps
    rows = conn.execute("""
        SELECT t.id, t.messages, t.model, t.created_at,
               td.depends_on AS blocker_id,
               bt.status AS blocker_status
        FROM tasks t
        JOIN task_deps td ON td.task_id = t.id
        LEFT JOIN tasks bt ON bt.id = td.depends_on
        WHERE t.status = 'pending'
          AND (bt.status IS NULL OR bt.status != 'completed')
        ORDER BY t.created_at ASC
    """).fetchall()

    # Group by task
    blocked_map: dict[str, dict] = {}
    for r in rows:
        tid = r["id"]
        if tid not in blocked_map:
            messages = json.loads(r["messages"]) if r["messages"] else []
            preview = messages[0]["content"][:80] if messages else ""
            blocked_map[tid] = {
                "id": tid,
                "prompt_preview": preview,
                "model": r["model"],
                "created_at": datetime.fromtimestamp(r["created_at"], tz=timezone.utc)
                    .isoformat() if r["created_at"] else "",
                "blocked_by": [],
            }
        blocker = {
            "id": r["blocker_id"],
            "status": r["blocker_status"] or "unknown",
        }
        if blocker not in blocked_map[tid]["blocked_by"]:
            blocked_map[tid]["blocked_by"].append(blocker)

    conn.close()
    return list(blocked_map.values())


def expire_old_tasks(db_path: str = "", older_than: int = 86400) -> int:
    """Mark pending tasks older than `older_than` seconds as failed.

    Args:
        db_path: Path to SQLite DB.
        older_than: Age threshold in seconds (default 24h).

    Returns:
        Number of tasks expired.
    """
    db = db_path or str(DEFAULT_DB)
    if not os.path.exists(db):
        return 0

    conn = init_db(db)
    cutoff = time.time() - older_than
    error_msg = f"Expired (older than {older_than}s)"

    conn.execute(
        """
        UPDATE tasks SET status = 'failed', error = ?,
            completed_at = ?
        WHERE status = 'pending' AND created_at < ?
        """,
        (error_msg, time.time(), cutoff),
    )
    count = conn.total_changes
    conn.commit()
    conn.close()

    if count:
        print(f"[expire] Expired {count} tasks older than {older_than}s")
    return count


def cmd_expire(args):
    """hpc_batch.py expire --older-than SECONDS"""
    count = expire_old_tasks(args.db, older_than=args.older_than)
    print(count)


def cmd_blocked(args):
    """hpc_batch.py blocked [--json]"""
    blocked = get_blocked_tasks(args.db)

    if args.json:
        print(json.dumps(blocked, indent=2))
        return blocked

    if not blocked:
        print("No blocked tasks")
        return blocked

    print(f"{'ID':<40} {'PROMPT':<50} {'BLOCKED BY':<30}")
    print("-" * 120)
    for t in blocked:
        blockers = ", ".join(
            f"{b['id'][:12]}...({b['status']})"
            for b in t["blocked_by"]
        )
        print(f"{t['id']:<40} {t['prompt_preview']:<50} {blockers:<30}")
    return blocked


def cmd_route(args):
    """hpc_batch.py route <task-id> or hpc_batch.py route --pending"""
    from infra.routing import route_task_by_id, route_pending_breakdown

    if args.pending:
        breakdown = route_pending_breakdown(args.db)
        print(f"Pending routing breakdown ({breakdown['total']} total):")
        print(f"  Local: {breakdown['local']}")
        print(f"  HPC:   {breakdown['hpc']}")
        return

    if args.task_id:
        try:
            decision = route_task_by_id(args.task_id, args.db)
            print(f"Task {args.task_id} -> {decision}")
        except ValueError as e:
            print(f"Error: {e}")
            return

    print("Usage: hpc_batch.py route <task-id> | --pending")


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
    p_submit.add_argument("--cycle", type=int, default=None,
                          help="FA pipeline cycle number (stored in metadata)")
    p_submit.add_argument("--depends-on", action="append", default=[],
                          dest="depends_on",
                          help="Task ID this submission depends on (repeatable)")
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

    # blocked
    p_blocked = sub.add_parser("blocked", help="Show tasks blocked by unmet dependencies")
    p_blocked.add_argument("--json", action="store_true", help="JSON output")
    p_blocked.set_defaults(func=cmd_blocked)

    # expire
    p_expire = sub.add_parser("expire", help="Expire old pending tasks")
    p_expire.add_argument("--older-than", type=int, default=86400,
                          dest="older_than",
                          help="Age threshold in seconds (default: 86400 = 24h)")
    p_expire.set_defaults(func=cmd_expire)

    # fetch (data source collectors)
    p_fetch = sub.add_parser("fetch", help="Fetch data from a source and submit analysis tasks")
    p_fetch.add_argument("source", nargs="?", default="",
                         help="Collector name (arxiv, github, kaggle, web). Run 'hpc_batch.py fetch --list' to see all.")
    p_fetch.add_argument("--list", action="store_true", help="List available collectors")
    p_fetch.add_argument("--query", help="Search query")
    p_fetch.add_argument("--id", help="Source-specific ID (e.g. arXiv paper ID)")
    p_fetch.add_argument("--url", help="URL to fetch (for web/github collectors)")
    p_fetch.add_argument("--max", type=int, default=None, help="Max results")
    p_fetch.add_argument("--tags", help="Comma-separated tags for metadata")
    p_fetch.add_argument("--competition", help="Kaggle competition name")
    p_fetch.add_argument("--dataset", help="Kaggle dataset path")
    p_fetch.add_argument("--files", help="Comma-separated file paths (for github)")
    p_fetch.add_argument("--refresh", action="store_true", help="Bypass cache")
    p_fetch.add_argument("--wait", type=int, default=0,
                         help="Poll interval in seconds (0 = no wait)")
    p_fetch.add_argument("--timeout", type=int, default=86400,
                         help="Max wait time in seconds (default 24h)")
    p_fetch.set_defaults(func=cmd_fetch)

    # preprocess
    p_pre = sub.add_parser("preprocess", help="Preprocess content and submit as chunked DAG")
    p_pre.add_argument("paper", nargs="?", default="",
                       help="'paper' for PDF preprocessing")
    p_pre.add_argument("--pdf", help="Path to PDF file")
    p_pre.add_argument("--url", help="URL (for repo preprocessing)")
    p_pre.add_argument("--chunk", type=int, default=4000, help="Chunk size in tokens (default: 4000)")
    p_pre.add_argument("--overlap", type=int, default=200, help="Chunk overlap in tokens (default: 200)")
    p_pre.add_argument("--type", default="paper_analysis", help="Task type for analysis")
    p_pre.add_argument("--query", help="Custom analysis query for each chunk")
    p_pre.set_defaults(func=cmd_preprocess)

    # validate
    p_val = sub.add_parser("validate", help="Validate task metadata")
    p_val.add_argument("--task", help="Task ID to validate")
    p_val.add_argument("--all", action="store_true", help="Validate all pending tasks")
    p_val.add_argument("--cache-stats", action="store_true", help="Show collector cache statistics")
    p_val.set_defaults(func=cmd_validate)

    # count
    p_count = sub.add_parser("count", help="Count tasks by status and type")
    p_count.add_argument("--type", help="Filter by task type")
    p_count.add_argument("--status", default="pending", help="Filter by status (default: pending)")
    p_count.set_defaults(func=cmd_count)

    # watch
    p_watch = sub.add_parser("watch", help="Watch for completed/failed tasks in real-time")
    p_watch.add_argument("--poll", type=int, default=30, help="Poll interval in seconds (default: 30)")
    p_watch.add_argument("--type", help="Filter by task type")
    p_watch.add_argument("--source", help="Filter by source")
    p_watch.add_argument("--json", action="store_true", help="JSON output")
    p_watch.set_defaults(func=cmd_watch)

    # export
    p_export = sub.add_parser("export", help="Export task results to file")
    p_export.add_argument("task_id", nargs="?", help="Task UUID")
    p_export.add_argument("--type", help="Filter by task type (batch export)")
    p_export.add_argument("--source", help="Filter by source (batch export)")
    p_export.add_argument("--format", default="markdown", choices=["markdown", "json", "csv"],
                          help="Output format (default: markdown)")
    p_export.add_argument("--output", help="Output file path (single task)")
    p_export.add_argument("--dir", help="Output directory (batch export)")
    p_export.set_defaults(func=cmd_export)

    # pipeline
    p_pipe = sub.add_parser("pipeline", help="Run or inspect YAML research pipelines")
    p_pipe.add_argument("run", nargs="?", default="", help="Run a pipeline YAML file")
    p_pipe.add_argument("--status", help="Show status of a pipeline by ID")
    p_pipe.set_defaults(func=cmd_pipeline)

    # archive
    p_archive = sub.add_parser("archive", help="Archive old tasks or manage archive database")
    p_archive.add_argument("--older-than", type=int, default=90,
                           help="Archive tasks older than N days (default: 90)")
    p_archive.add_argument("--status", default="", help="Archive only this status (completed/failed)")
    p_archive.add_argument("--list", action="store_true", help="List archived tasks")
    p_archive.add_argument("--query", help="Query archived tasks by type")
    p_archive.add_argument("restore", nargs="?", default="", help="Restore a task by ID")
    p_archive.set_defaults(func=cmd_archive)

    # cleanup
    p_clean = sub.add_parser("cleanup", help="Clean old tasks, proxy files, and cache entries")
    p_clean.add_argument("--completed-days", type=int, default=30,
                         help="Keep completed tasks for N days (default: 30)")
    p_clean.add_argument("--failed-days", type=int, default=7,
                         help="Keep failed tasks for N days (default: 7)")
    p_clean.add_argument("--dry-run", action="store_true",
                         help="Show what would be deleted without deleting")
    p_clean.set_defaults(func=_run_cleanup)

    # route
    p_route = sub.add_parser("route", help="Show routing decision for a task or breakdown")
    p_route.add_argument("task_id", nargs="?", help="Task UUID")
    p_route.add_argument("--pending", action="store_true",
                         help="Show routing breakdown of all pending tasks")
    p_route.set_defaults(func=cmd_route)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
