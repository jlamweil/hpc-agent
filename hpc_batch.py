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
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
import threading
from pathlib import Path
from typing import Any

# ── Defaults from config.yaml or env ─────────────────────────────────────────
HPC_AGENT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = HPC_AGENT_DIR / "config.yaml"
DEFAULT_DB = HPC_AGENT_DIR / "tasks.db"
DEFAULT_DATA_DIR = HPC_AGENT_DIR / "data"
DEFAULT_PARTITION = "your-partition"
DEFAULT_ACCOUNT = "your-account"
DEFAULT_QOS = "defaultqos"

# Rate-limit auto-dispatch checks: at most once per 60s
_last_auto_dispatch = 0.0
_AUTO_DISPATCH_COOLDOWN = 60.0

# P9: Trusted task sources. submit_task() validates against these.
# The source is stored in both the metadata dict and the dedicated column.
# This is a governance boundary, not a security sandbox — the AIAgent's
# execute_code tool can call submit_task() directly and bypass validation.
VALID_SOURCES = frozenset({"orchestrator", "fill-agent", "cron", "cli", "test"})


def log_db_event(event: str, db_path: str | None = None, **kwargs):
    """Insert a structured event into the task_events table."""
    db = db_path or str(DEFAULT_DB)
    try:
        conn = sqlite3.connect(db, timeout=3)
        conn.execute("""
            INSERT INTO task_events (ts, event, mode, task_id, job_id, priority,
                                     duration, status, reason, fill_id, backoff,
                                     saved_msgs, retry_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            time.time(), event,
            kwargs.get('mode'), kwargs.get('task_id'), kwargs.get('job_id'),
            kwargs.get('priority'), kwargs.get('duration'), kwargs.get('status'),
            kwargs.get('reason'), kwargs.get('fill_id'), kwargs.get('backoff'),
            kwargs.get('saved_msgs'), kwargs.get('retry_count'),
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass  # Non-critical


# ── MCP Wait: fill thread blocks on submit_task while daemon processes MCP ──
_fill_context = threading.local()  # Per-thread; set by HermesRunner in agent thread
_fill_context_task_id: str | None = None
_mcp_wait_events: dict[str, threading.Event] = {}
_mcp_wait_lock = threading.Lock()


def _auto_dispatch_if_needed(db_path: str, qos: str | None = None):
    """If real tasks are pending and HPC is cold, dispatch in background.

    Opens its own DB connection briefly for the count check, then spawns
    a thread for the SSH/SLURM work. Rate-limited to once per 60s.
    Retries up to 3 times with 10s backoff on failure (D9).
    Writes state file /tmp/hpc-dispatch-state.json for visibility.

    Args:
        db_path: Path to tasks database.
        qos: Override SLURM QOS. Passed through to ensure_hpc_worker and
             sbatch_on_hpc. 'preemptqos' for keepwarm mode, None for normal.
    """
    global _last_auto_dispatch
    now = time.time()
    if now - _last_auto_dispatch < _AUTO_DISPATCH_COOLDOWN:
        return
    _last_auto_dispatch = now

    # Count check — retry loop on locked DB with longer timeout
    for attempt in range(3):
        try:
            conn = sqlite3.connect(db_path, timeout=10)
            real_pending = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND priority >= 0"
            ).fetchone()[0]
            fill_pending = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND priority < 0"
            ).fetchone()[0]
            conn.close()
            if real_pending == 0 and fill_pending == 0:
                print(f"[auto-dispatch] No pending tasks (real={real_pending} fill={fill_pending}), skipping",
                      file=sys.stderr)
                return
            print(f"[auto-dispatch] Pending: real={real_pending} fill={fill_pending} — dispatching",
                  file=sys.stderr)
            break  # Have count, continue to dispatch
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < 2:
                time.sleep(1.0 * (attempt + 1))
                continue
            return  # Non-lock error — try again next cycle
        except Exception:
            return  # DB corrupt or missing — try again next time

    import threading
    def _do_dispatch():
        try:
            result = ensure_hpc_worker(db_path, qos=qos)
            if result["status"] == "submitted":
                print(f"[auto-dispatch] Job {result['job_id']} submitted (qos={qos or 'default'})", file=sys.stderr)
            elif result["status"] == "warm_reused":
                print("[auto-dispatch] HPC worker already warm — rsynced", file=sys.stderr)
        except Exception as e:
            print(f"[auto-dispatch] Dispatch failed: {e}", file=sys.stderr)

    t = threading.Thread(target=_do_dispatch, daemon=True)
    t.start()


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
    conn = sqlite3.connect(db_path, timeout=30)
    # Use DELETE journal mode to avoid WAL checkpoint race between
    # daemon and provider (WAL checkpoint can lose uncheckpointed writes).
    conn.execute("PRAGMA journal_mode=DELETE")
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

    # Priority and task_type columns for fill task queueing
    for col, dtype in [("priority", "INTEGER DEFAULT 0"),
                       ("task_type", "TEXT DEFAULT 'llm'")]:
        try:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {dtype}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    # P9: source column for origin tracking (backfill from metadata)
    try:
        conn.execute("ALTER TABLE tasks ADD COLUMN source TEXT DEFAULT 'orchestrator'")
        # Backfill existing tasks that have source in metadata
        conn.execute("""
            UPDATE tasks SET source = json_extract(metadata, '$.source')
            WHERE source = 'orchestrator' AND json_extract(metadata, '$.source') IS NOT NULL
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON tasks(source)")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Structured event store for traceability and debugging.
    # Every lifecycle event (job_submitted, task_completed, breaker_open, etc.)
    # goes here. Queryable with standard SQL — no NDJSON parsing needed.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            event TEXT NOT NULL,
            mode TEXT,
            task_id TEXT,
            job_id TEXT,
            priority INTEGER,
            duration REAL,
            status TEXT,
            reason TEXT,
            fill_id TEXT,
            backoff REAL,
            saved_msgs INTEGER,
            retry_count INTEGER
        )
    """)
    for col in ["ts", "event", "task_id", "job_id", "mode"]:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_events_{col} ON task_events({col})")
        except sqlite3.OperationalError:
            pass

    # Schema version — checked by worker before processing
    conn.execute("PRAGMA user_version = 1")

    conn.commit()
    return conn


INJECT_RE = re.compile(r'\{\{INJECT:([^}]+)\}\}')


def resolve_injections(
    messages: list[dict],
    data_dir: str = "",
) -> list[dict]:
    """Expand {{INJECT:relative/path}} markers in message content using files from data_dir.

    The markers are replaced with the contents of the referenced file.
    This allows large data (dashboard, listings, etc.) to be stored as files
    rather than embedded inline in the prompt, keeping prompts small enough
    for the model's context window.

    Args:
        messages: List of message dicts (OpenAI format) potentially containing
                  {{INJECT:path/to/file.json}} markers.
        data_dir: Base directory for resolving relative paths.
                  Defaults to HPC_AGENT_DIR/data/.

    Returns:
        Messages with all markers expanded to file contents.
    """
    base = Path(data_dir) if data_dir else DEFAULT_DATA_DIR

    def _expand(content: str) -> str:
        def _replacer(m: re.Match) -> str:
            rel_path = m.group(1)
            full_path = base / rel_path
            try:
                return full_path.read_text()
            except (OSError, IOError) as e:
                return f'[INJECT ERROR: {rel_path} — {e}]'

        return INJECT_RE.sub(_replacer, content)

    resolved = []
    for msg in messages:
        if isinstance(msg.get("content"), str):
            msg = dict(msg)
            msg["content"] = _expand(msg["content"])
        resolved.append(msg)

    return resolved


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
    priority: int = 0,
    task_type: str = "llm",
    source: str = "orchestrator",
) -> str:
    """Add a task to the local SQLite queue.

    Args:
        prompt: Task prompt text (used when messages is not provided).
        db_path: Path to SQLite DB (default: tasks.db in HPC_AGENT_DIR).
        model: Model name for inference.
        agent_id: Identifier for the submitting agent.
        metadata: Arbitrary key-value metadata.
        task_id: Explicit task ID (auto-generated if empty).
        depends_on: List of task IDs this task depends on.
                     All must exist in the DB at submission time.
        messages: Full message array (list of dicts with role/content).
                  If provided, takes precedence over prompt.

    Returns:
        The task ID.
    """
    db = db_path or str(DEFAULT_DB)
    task_id = task_id or str(uuid.uuid4())
    created_at = time.time()
    depends_on = depends_on or []

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

    # Store source in metadata for idle time tracking
    metadata = dict(metadata or {})
    # P9: validate source against trusted origins
    if source not in VALID_SOURCES:
        raise ValueError(
            f"Invalid source '{source}'. Must be one of: {', '.join(sorted(VALID_SOURCES))}"
        )
    metadata["source"] = source

    # Use messages array if provided, otherwise wrap prompt as single user message
    if messages:
        messages_json = json.dumps(messages)
    else:
        messages_json = json.dumps([{"role": "user", "content": prompt}])

    # Retry INSERT on "database is locked" (DELETE mode contention)
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            conn.execute("""
                INSERT INTO tasks (
                    id, agent_id, messages, tools, model, api_config,
                    metadata, created_at, status, priority, task_type, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                priority,
                task_type,
                source,
            ))
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                conn.close()
                conn = init_db(db)
            else:
                raise

    # Insert dependency edges
    for dep_id in depends_on:
        conn.execute("""
            INSERT OR IGNORE INTO task_deps (task_id, depends_on, created_at)
            VALUES (?, ?, ?)
        """, (task_id, dep_id, created_at))

    conn.commit()
    conn.close()

    # Auto-dispatch to HPC if this is a real task (not fill)
    if priority >= 0:
        _auto_dispatch_if_needed(db)

    # Notify daemon of new task via local sentinel file.
    # Works for all priorities (both real and fill tasks need fast pickup).
    # /tmp/ avoids filesystem latency on BeeGFS (tasks.db may be on shared
    # storage, but the sentinel must be local to the daemon).
    try:
        Path("/tmp/.hpc-task-ready").touch()
    except Exception:
        pass  # Non-critical — daemon falls back to 60s poll

    # ── MCP Wait: block if called from a fill thread ──────────────────────
    # The fill thread stays alive (blocked on Event) while the daemon
    # processes the MCP task. After completion, signal_mcp_done() unblocks.
    fill_id = getattr(_fill_context, 'task_id', None)
    if fill_id is not None:
        ev = threading.Event()
        with _mcp_wait_lock:
            _mcp_wait_events[task_id] = ev
        ev.wait(timeout=300)
        with _mcp_wait_lock:
            _mcp_wait_events.pop(task_id, None)

    return task_id


def signal_mcp_done(task_id: str):
    """Signal the exact fill thread waiting for this task's completion.

    Called by the daemon after processing a task that was submitted
    from within a fill thread's AIAgent context. The fill thread is
    blocked on an Event in submit_task() — this Event.set() unblocks it.
    """
    with _mcp_wait_lock:
        ev = _mcp_wait_events.get(task_id)
    if ev is not None:
        ev.set()


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
            ORDER BY priority DESC, created_at ASC
        """, (task_type,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, messages, model, created_at
            FROM tasks WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
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
    """Get HPC connection details from config.yaml (or env-based defaults)."""
    return {
        "user": cfg("hpc.user", os.environ.get("HPC_USER", "your-hpc-user")),
        "host": cfg("hpc.host", os.environ.get("HPC_HOST", "your-hpc-login-host")),
        "dir": cfg("hpc.dir", "~/hpc-agent"),
        "partition": cfg("hpc.partition", DEFAULT_PARTITION),
        "account": cfg("hpc.account", DEFAULT_ACCOUNT),
        "qos": cfg("hpc.qos", DEFAULT_QOS),
        "banned_nodes": cfg("hpc.banned_nodes", []),
    }


def _recover_needs_rsync(db_path: str):
    """Retry rsync for tasks flagged with needs_rsync metadata."""
    import json
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id, metadata FROM tasks WHERE status = 'pending'"
    ).fetchall()
    needs_rsync_ids = []
    for tid, meta_json in rows:
        meta = json.loads(meta_json) if meta_json else {}
        if meta.get("needs_rsync"):
            needs_rsync_ids.append(tid)
    if not needs_rsync_ids:
        conn.close()
        return
    hpc_cfg = _get_hpc_config()
    rsync_ok = rsync_to_hpc(db_path, hpc_cfg)
    if rsync_ok:
        for tid in needs_rsync_ids:
            meta = json.loads(
                conn.execute("SELECT metadata FROM tasks WHERE id = ?", (tid,)).fetchone()[0]
            )
            meta["needs_rsync"] = False
            conn.execute(
                "UPDATE tasks SET metadata = ? WHERE id = ?",
                (json.dumps(meta), tid),
            )
        conn.commit()
        print(f"[recover] Retried rsync for {len(needs_rsync_ids)} needs_rsync tasks")
    conn.close()


def rsync_to_hpc(db_path: str, hpc_config: dict[str, str] | None = None) -> bool:
    """Rsync the local DB and data files to HPC login node.

    Uses staging path + atomic rename to prevent inode replacement races
    with a running worker that has the DB file open.
    """
    cfg_hpc = hpc_config or _get_hpc_config()

    # Rsync the DB to a staging path, then atomic rename
    src = db_path
    dst_staging = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/tasks.db.staging"
    dst_final = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/tasks.db"
    print(f"[dispatch] rsync {src} -> {dst_staging}")
    result = subprocess.run(
        ["rsync", "-az", "--progress", src, dst_staging],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"[dispatch] rsync DB FAILED: {result.stderr.strip()}")
        return False

    # Atomic rename on BeeGFS (same filesystem — mv is instant)
    rename = subprocess.run(
        ["ssh", f"{cfg_hpc['user']}@{cfg_hpc['host']}",
         f"mv {cfg_hpc['dir']}/tasks.db.staging {cfg_hpc['dir']}/tasks.db"],
        capture_output=True, text=True, timeout=15,
    )
    if rename.returncode != 0:
        print(f"[dispatch] atomic rename FAILED: {rename.stderr.strip()}")
        return False

    # Rsync the data directory (contains files referenced by {{INJECT:...}} markers)
    local_data = str(DEFAULT_DATA_DIR)
    if os.path.isdir(local_data) and os.listdir(local_data):
        remote_data = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/data/"
        print(f"[dispatch] rsync {local_data}/ -> {remote_data}")
        result = subprocess.run(
            ["rsync", "-az", "--delete", f"{local_data}/", remote_data],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync data WARN: {result.stderr.strip()}")
            # Non-fatal — tasks will fail individually if files are missing
        else:
            print(f"[dispatch] rsync data OK")

    # Rsync the slurm scripts (so fixes like --enable-auto-tool-choice take effect)
    local_slurm = os.path.join(os.path.dirname(db_path) or str(HPC_AGENT_DIR), "slurm")
    remote_slurm = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/slurm/"
    if os.path.isdir(local_slurm):
        print(f"[dispatch] rsync {local_slurm}/ -> {remote_slurm}")
        result = subprocess.run(
            ["rsync", "-az", "--delete", f"{local_slurm}/", remote_slurm],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync slurm WARN: {result.stderr.strip()}")

    # Rsync worker modules (daemon, tunnel, model runner)
    local_worker = os.path.join(os.path.dirname(db_path) or str(HPC_AGENT_DIR), "worker")
    if os.path.isdir(local_worker):
        remote_worker = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/worker/"
        print(f"[dispatch] rsync worker/ -> {remote_worker}")
        result = subprocess.run(
            ["rsync", "-az", "--delete", f"{local_worker}/", remote_worker],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync worker/ WARN: {result.stderr.strip()}")

    # Rsync MCP server modules
    local_mcps = os.path.join(os.path.dirname(db_path) or str(HPC_AGENT_DIR), "mcps")
    if os.path.isdir(local_mcps):
        remote_mcps = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/mcps/"
        print(f"[dispatch] rsync mcps/ -> {remote_mcps}")
        result = subprocess.run(
            ["rsync", "-az", "--delete", f"{local_mcps}/", remote_mcps],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync mcps/ WARN: {result.stderr.strip()}")

    # Rsync utility scripts
    local_scripts = os.path.join(os.path.dirname(db_path) or str(HPC_AGENT_DIR), "scripts")
    if os.path.isdir(local_scripts):
        remote_scripts = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/scripts/"
        print(f"[dispatch] rsync scripts/ -> {remote_scripts}")
        result = subprocess.run(
            ["rsync", "-az", "--delete", f"{local_scripts}/", remote_scripts],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync scripts/ WARN: {result.stderr.strip()}")

    # Rsync hpc-startup.sh (daemon launcher) — single file, no --delete
    local_startup = os.path.join(os.path.dirname(db_path) or str(HPC_AGENT_DIR), "hpc-startup.sh")
    if os.path.isfile(local_startup):
        remote_startup = f"{cfg_hpc['user']}@{cfg_hpc['host']}:{cfg_hpc['dir']}/"
        print(f"[dispatch] rsync hpc-startup.sh -> {remote_startup}")
        result = subprocess.run(
            ["rsync", "-az", local_startup, remote_startup],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[dispatch] rsync hpc-startup.sh WARN: {result.stderr.strip()}")

    print(f"[dispatch] rsync OK")
    return True


def _parse_slurm_excludes(script_name: str) -> list[str]:
    """Parse #SBATCH --exclude directives from a SLURM script.

    The script's hardcoded --exclude lists known-bad nodes. When we
    add our own --exclude at the CLI level, it OVERRIDES the script's
    directive instead of merging. We must read them and merge explicitly.
    """
    import re
    script_path = os.path.join(os.path.dirname(__file__) or ".", script_name)
    try:
        with open(script_path) as f:
            for line in f:
                m = re.match(r'#SBATCH\s+--exclude\s*=\s*(.+)', line)
                if m:
                    return [n.strip() for n in m.group(1).split(',') if n.strip()]
    except (OSError, IOError):
        pass
    return []


def sbatch_on_hpc(hpc_config: dict[str, str] | None = None, server_mode: bool = False,
                  exclude_nodes: list[str] | None = None) -> dict[str, Any]:
    """Submit the DS V4 Flash batch job to HPC via SSH.

    Args:
        hpc_config: HPC connection config. Defaults to _get_hpc_config().
        server_mode: If True, set long idle timeout for daemon mode.
        exclude_nodes: List of node hostnames to exclude from SLURM
                        allocation (e.g. keepwarm node). Merged with
                        any #SBATCH --exclude from the slurm script.
    """
    cfg_hpc = hpc_config or _get_hpc_config()
    ssh = _ssh_cmd(cfg_hpc["host"], cfg_hpc["user"])

    # Build sbatch command — use dsv4_fast.slurm (HermesWorker loop)
    sbatch_script = "slurm/dsv4_fast.slurm"
    sbatch_cmd = (
        f"cd {cfg_hpc['dir']} && "
        f"sbatch --partition={cfg_hpc['partition']} "
        f"--account={cfg_hpc['account']} "
        f"--qos={cfg_hpc['qos']} "
    )
    # Merge banned nodes from config + script-level --exclude + runtime
    # exclude_nodes (keepwarm node). CLI --exclude overrides #SBATCH
    # --exclude, so we must combine explicitly to avoid losing them.
    config_banned = cfg_hpc.get("banned_nodes", []) or []
    script_banned = _parse_slurm_excludes(sbatch_script)
    all_excludes = list(config_banned) + script_banned
    if exclude_nodes:
        all_excludes.extend(exclude_nodes)
    if all_excludes:
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for n in all_excludes:
            if n not in seen:
                seen.add(n)
                unique.append(n)
        sbatch_cmd += f"--exclude={','.join(unique)} "
    sbatch_cmd += f"{sbatch_script}"
    if server_mode:
        sbatch_cmd += " --export=IDLE_TIMEOUT=1800,POLL_INTERVAL=10"
    # Note: --dependency=singleton was removed because it caused a race:
    # when a job is preempted, the singleton blocks new submissions until
    # the old job fully clears SLURM's queue (can take minutes in COMPLETING
    # state). Without singleton, concurrent jobs are possible but harmless —
    # the tunnel connects to whichever starts first, and SLURM's time limit
    # eventually cleans up duplicative ones.

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


def ensure_hpc_worker(db_path: str, qos: str | None = None) -> dict:
    """Ensure an HPC worker is running to process pending tasks.
    
    Optimistic concurrency (no lock):
    1. Rsync recovery for needs_rsync flagged tasks
    2. Check squeue for running dsv4-batch jobs
    3. If warm → rsync only
    4. If cold → rsync + sbatch in server mode
    
    Args:
        db_path: Path to tasks database.
        qos: Override SLURM QOS (e.g. 'preemptqos' for keepwarm mode).
              If None, uses config value.
    
    Returns {"status": "warm_reused"|"submitted"|"error", ...}
    """
    hpc_cfg = _get_hpc_config()
    if qos is not None:
        hpc_cfg['qos'] = qos
    _recover_needs_rsync(db_path)
    jobs = check_hpc_jobs(hpc_cfg)

    # Categorize active dsv4- jobs by QoS for keepwarm detection.
    # normal_jobs = defaultqos (stable, user-facing)
    # preempt_jobs = preemptqos (cheaper, preemptible — keepwarm)
    _active_states = ("RUNNING", "PENDING", "CONFIGURING")
    _dsv4_filter = lambda j: "dsv4-fast" in j.get("name", "") or "dsv4-" in j.get("name", "")
    normal_jobs = [
        j for j in jobs
        if j.get("state") in _active_states and _dsv4_filter(j)
        and j.get("qos") != "preemptqos"
    ]
    preempt_jobs = [
        j for j in jobs
        if j.get("state") in _active_states and _dsv4_filter(j)
        and j.get("qos") == "preemptqos"
    ]

    if not rsync_to_hpc(db_path, hpc_cfg):
        return {"status": "error", "error": "rsync failed"}

    # Case 1: Normal QoS job exists + real tasks need it → reuse it
    if normal_jobs and qos != "preemptqos":
        return {"status": "warm_reused", "job_id": normal_jobs[0]["id"]}

    # Case 2: Keepwarm (preemptqos) running
    if preempt_jobs:
        # Sub-case 2a: fill-only mode → keepwarm is fine, no replacement needed
        if qos == "preemptqos":
            return {"status": "warm_reused", "job_id": preempt_jobs[0]["id"]}
        # Sub-case 2b: real tasks need stable QoS → submit replacement
        # on a different node so keepwarm isn't disrupted.
        nodes = [
            j["node"] for j in preempt_jobs
            if j.get("state") == "RUNNING" and j.get("node")
            and not j["node"].startswith("(")
        ]
        print(f"[dispatch] Keepwarm job(s) on nodes={nodes}, submitting replacement on different node",
              file=sys.stderr)
        result = sbatch_on_hpc(hpc_cfg, server_mode=True, exclude_nodes=nodes or None)
        if result["success"]:
            mark_dispatched(db_path, result["job_id"])
            return {"status": "submitted", "job_id": result["job_id"], "replacement": True}
        return {"status": "error", "error": result.get("error", "sbatch failed")}

    # Case 3: Fill-only mode, no preemptqos job, but a normal QoS job exists.
    # Submit a preemptqos keepwarm alongside it. The defaultqos job will expire
    # on wall time; the keepwarm takes over fill tasks.
    if qos == "preemptqos":
        nodes = [
            j["node"] for j in normal_jobs
            if j.get("state") == "RUNNING" and j.get("node")
            and not j["node"].startswith("(")
        ]
        print(f"[dispatch] Normal QoS job(s) on nodes={nodes}, submitting keepwarm on different node",
              file=sys.stderr)
        result = sbatch_on_hpc(hpc_cfg, server_mode=True, exclude_nodes=nodes or None)
        if result["success"]:
            mark_dispatched(db_path, result["job_id"])
            return {"status": "submitted", "job_id": result["job_id"]}
        return {"status": "error", "error": result.get("error", "sbatch failed")}

    # Case 4: Cold — no jobs at all
    result = sbatch_on_hpc(hpc_cfg, server_mode=True)
    if result["success"]:
        mark_dispatched(db_path, result["job_id"])
        return {"status": "submitted", "job_id": result["job_id"]}
    return {"status": "error", "error": result.get("error", "sbatch failed")}


def check_hpc_jobs(hpc_config: dict[str, str] | None = None) -> list[dict]:
    """Check SLURM job status on HPC."""
    cfg_hpc = hpc_config or _get_hpc_config()
    ssh = _ssh_cmd(cfg_hpc["host"], cfg_hpc["user"])

    result = subprocess.run(
        ssh + [f"squeue -u {cfg_hpc['user']} -o '%i %T %N %M %S %j %q' --noheader"],
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
                    "name": parts[5] if len(parts) > 5 else "",
                    "qos": parts[6] if len(parts) > 6 else "",
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
            WHERE id = ? AND status IN ('pending', 'running')
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
                priority=args.priority,
                source="cli",
            )
            ids.append(tid)

        print(f"Submitted {len(ids)} tasks")
        if args.priority >= 0:
            _auto_dispatch_if_needed(args.db)
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
            priority=args.priority,
            source="cli",
        )
        print(tid)
        if args.priority >= 0:
            _auto_dispatch_if_needed(args.db)
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
        host = cfg('hpc.host', 'your-hpc-host')
        print(f"\nHPC cluster ({host}):")
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
    result = ensure_hpc_worker(args.db)
    if result["status"] == "warm_reused":
        print(f"[dispatch] Warm vLLM session reused. Tasks rsynced.")
    elif result["status"] == "submitted":
        print(f"[dispatch] Server job {result['job_id']} submitted.")
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
        source="cli",
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


# ── Fill Task Helpers ─────────────────────────────────────────────────────────

_FILL_CYCLE_FILE = HPC_AGENT_DIR / "prompts" / "fill" / ".cycle-counter"


def _random_repo() -> str:
    """Pick a random target repo name from config."""
    repos = cfg("fill_tasks.target_repos", ["hpc-agent"])
    import random
    return random.choice(repos)


def _available_fill_prompts() -> list[str]:
    """Scan prompts/fill/ directory for available fill prompt types.

    Returns sorted list of prompt type names (filename stems) excluding
    non-prompt files (.cycle-counter) and disabled prompts (prefixed with _).
    The filesystem IS the registry. To disable a fill task, prefix its
    filename with underscore (e.g. _experimental-thing.txt).
    """
    fill_dir = HPC_AGENT_DIR / "prompts" / "fill"
    if not fill_dir.exists():
        return []
    stems = sorted(
        f.stem for f in fill_dir.iterdir()
        if f.suffix == ".txt"
        and f.name != ".cycle-counter"
    )
    return stems


def _next_fill_prompt(prompt_types: list[str] | None = None) -> str:
    """Pick next fill prompt in round-robin order + random target repo.

    If prompt_types is provided (for testing), use it directly.
    Otherwise scan prompts/fill/ directory — the filesystem IS the registry.
    Adding a .txt file to prompts/fill/ is the only registration step needed.

    Returns the prompt text with {TARGET_REPO} and {TARGET_REPO_DIR}
    substituted for the target repository name and absolute path.
    """
    if prompt_types is None:
        prompt_types = _available_fill_prompts()
    if not prompt_types:
        return "No fill prompts available."

    idx = 0
    try:
        idx = int(Path(_FILL_CYCLE_FILE).read_text().strip())
    except (FileNotFoundError, ValueError):
        pass
    idx = (idx + 1) % len(prompt_types)
    Path(_FILL_CYCLE_FILE).write_text(str(idx))
    prompt_file = HPC_AGENT_DIR / "prompts" / "fill" / f"{prompt_types[idx]}.txt"
    if not prompt_file.exists():
        return f"Analyze the current state of the {{TARGET_REPO}} project. Focus on {prompt_types[idx]} aspects. Do not modify any files."

    raw = prompt_file.read_text().strip()
    # Parse #!target_repo: directive — overrides random repo selection
    repo = _random_repo()
    if raw.startswith("#!target_repo:"):
        first_line, _, raw = raw.partition("\n")
        pinned = first_line.split(":", 1)[1].strip()
        if pinned:
            repo = pinned
        raw = raw.strip()
    # Substitute target repo placeholders
    base = os.path.expanduser(cfg("fill_tasks.base_repo_dir", "~/Documents/GAIA"))
    # Check for per-repo path override (e.g. lessons-content is at ~/projects/)
    repo_overrides = cfg("fill_tasks.repo_paths", {})
    repo_dir = os.path.expanduser(repo_overrides.get(repo, os.path.join(base, repo)))
    return raw.replace("{TARGET_REPO}", repo).replace("{TARGET_REPO_DIR}", repo_dir)


def cmd_submit_fill(args):
    """hpc_batch.py submit-fill [--prompt PROMPT] [--type TYPE]

    Creates a low-priority fill task in the queue.
    Prompt types starting with _ get priority -2 (run only when
    no priority -1 fill tasks are available).
    """
    prompt_type = args.fill_type
    prompt = args.prompt or _next_fill_prompt([prompt_type])
    priority = -2 if prompt_type.startswith("_") else -1
    metadata = {
        "type": "fill",
        "fill_type": prompt_type,
        "source_file": f"prompts/fill/{prompt_type}.txt",
    }
    tid = submit_task(
        prompt=prompt,
        db_path=args.db,
        model=args.model,
        metadata=metadata,
        task_type="fill",
        priority=priority,
        source="fill-agent",
    )
    print(tid)
    return tid


def cmd_submit_fill_noop(args):
    """hpc_batch.py submit-fill (as subcommand stub)."""
    return cmd_submit_fill(args)


def expire_old_tasks(db_path: str = "", older_than: int = 86400) -> int:
    """Mark pending tasks older than `older_than` seconds as failed.

    Also cascades: if a task's dependency has failed, expire the
    dependent task immediately (don't wait for the age threshold).

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
    now = time.time()
    cutoff = now - older_than
    error_msg = f"Expired (older than {older_than}s)"
    dep_error_msg = f"Expired (dependency failed)"

    total = 0

    # 1. Expire old pending tasks
    cur = conn.execute(
        """
        UPDATE tasks SET status = 'failed', error = ?,
            completed_at = ?
        WHERE status = 'pending' AND created_at < ?
        """,
        (error_msg, now, cutoff),
    )
    total += cur.rowcount

    # 2. Cascade-expire tasks whose dependencies have failed (recursive:
    #    keep cascading until no more tasks are affected — handles chains)
    while True:
        cur = conn.execute(
            """
            UPDATE tasks SET status = 'failed', error = ?,
                completed_at = ?
            WHERE status = 'pending' AND id IN (
                SELECT td.task_id FROM task_deps td
                JOIN tasks bt ON bt.id = td.depends_on
                WHERE bt.status = 'failed'
            )
            """,
            (dep_error_msg, now),
        )
        affected = cur.rowcount
        total += affected
        if affected <= 0:
            break

    conn.commit()
    conn.close()

    if total:
        print(f"[expire] Expired {total} tasks (time-based + cascade)")
    return total


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


def cmd_recover_stuck(args):
    """hpc_batch.py recover-stuck

    Find tasks stuck in 'running' status whose SLURM job is no longer active,
    and reset them to 'pending' (or 'failed' if max retries exceeded).
    """
    db_path = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db_path)

    running = conn.execute(
        "SELECT id, metadata, retry_count, slurm_job_id FROM tasks WHERE status = 'running'"
    ).fetchall()

    max_retries = getattr(args, 'max_retries', 3)
    recovered = 0
    failed = 0
    skipped = 0

    for row in running:
        task_id = row[0]
        meta = json.loads(row[1]) if row[1] else {}
        retry_count = row[2] or 0
        slurm_job_id = row[3]

        # Check if SLURM job is still active
        job_alive = False
        if slurm_job_id:
            try:
                hpc = _get_hpc_config()
                result = subprocess.run(
                    ["ssh", f"{hpc['user']}@{hpc['host']}", "squeue", "-j", str(slurm_job_id), "-h"],
                    capture_output=True, text=True, timeout=15,
                )
                job_alive = bool(result.stdout.strip())
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
                skipped += 1
                continue

        if job_alive:
            skipped += 1
            continue

        # Job is dead — recover
        new_retry = retry_count + 1
        if new_retry > max_retries:
            conn.execute(
                "UPDATE tasks SET status = 'failed', error = ? WHERE id = ?",
                (f"Failed after {new_retry} retries (preempted)", task_id),
            )
            failed += 1
        else:
            conn.execute(
                "UPDATE tasks SET status = 'pending', retry_count = ?, started_at = NULL WHERE id = ?",
                (new_retry, task_id),
            )
            recovered += 1

    conn.commit()
    conn.close()

    print(json.dumps({
        "recovered": recovered,
        "failed_after_max_retries": failed,
        "skipped": skipped,
        "total_examined": len(running),
    }, indent=2))


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


def cmd_events(args):
    """hpc_batch.py events [--event TYPE] [--mode MODE] [--task PREFIX] [--limit N] [--json] [--stats]

    Query the structured task_events table for debugging and analysis.
    """
    db = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    if args.stats:
        # Aggregate stats
        rows = conn.execute("""
            SELECT event, mode, COUNT(*) as cnt,
                   ROUND(AVG(duration), 1) as avg_dur,
                   ROUND(MIN(duration), 1) as min_dur,
                   ROUND(MAX(duration), 1) as max_dur
            FROM task_events
            GROUP BY event, mode
            ORDER BY event, mode
        """).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], indent=2))
        else:
            print(f"{'Event':25s} {'Mode':12s} {'Count':6s} {'AvgDur':8s} {'Min':8s} {'Max':8s}")
            print("-" * 70)
            for r in rows:
                print(f"{r['event']:25s} {r['mode']:12s} {r['cnt']:6d} {r['avg_dur'] or 0:8.1f} {r['min_dur'] or 0:8.1f} {r['max_dur'] or 0:8.1f}")
        conn.close()
        return

    # Raw event query
    where = []
    params = []
    if args.event:
        where.append("event = ?")
        params.append(args.event)
    if args.mode:
        where.append("mode = ?")
        params.append(args.mode)
    if args.task:
        where.append("task_id LIKE ?")
        params.append(f"{args.task}%")
    where_clause = " AND ".join(where) if where else "1"

    rows = conn.execute(f"""
        SELECT ts, event, mode, task_id, job_id, duration, status, reason,
               fill_id, backoff, saved_msgs, retry_count, priority
        FROM task_events
        WHERE {where_clause}
        ORDER BY ts DESC
        LIMIT ?
    """, params + [args.limit]).fetchall()

    if args.json:
        print(json.dumps([{k: r[k] for k in r.keys()} for r in rows], indent=2, default=str))
    else:
        for r in rows:
            from datetime import datetime, timezone
            ts_str = datetime.fromtimestamp(r['ts'], tz=timezone.utc).strftime("%H:%M:%S")
            parts = [f"[{ts_str}] {r['event']:25s} mode={r['mode'] or '?':10s}"]
            if r['task_id']: parts.append(f"task={r['task_id'][:16]}")
            if r['job_id']: parts.append(f"job={r['job_id']}")
            if r['duration'] is not None: parts.append(f"dur={r['duration']:.0f}s")
            if r['status']: parts.append(f"status={r['status']}")
            if r['reason']: parts.append(f"reason={r['reason']}")
            if r['fill_id']: parts.append(f"fill={r['fill_id'][:12]}")
            if r['backoff'] is not None: parts.append(f"backoff={r['backoff']:.0f}s")
            if r['saved_msgs'] is not None: parts.append(f"saved={r['saved_msgs']}msgs")
            if r['retry_count'] is not None and r['retry_count'] > 0: parts.append(f"retry={r['retry_count']}")
            print(" | ".join(parts))

    conn.close()


def cmd_kpi(args):
    """hpc_batch.py kpi [--last N] [--fill] [--json]

    KPI dashboard for task health, performance, and fill convergence.
    """
    db = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db)

    kpi = {}

    # ── 1. System Health ──────────────────────────────────────────────
    import json, os, glob
    from collections import Counter
    from pathlib import Path

    stats = conn.execute("""
        SELECT status, COUNT(*) FROM tasks GROUP BY status
    """).fetchall()
    status_counts = {r[0]: r[1] for r in stats}
    real_pending = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status='pending' AND priority>=0"
    ).fetchone()[0]
    fill_pending = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status='pending' AND priority<0"
    ).fetchone()[0]

    daemon_info = {}
    try:
        hb = json.loads(Path("/tmp/hermes-worker.heartbeat").read_text())
        daemon_info = {
            "pid": hb.get("pid"), "breaker": hb.get("breaker_state"),
            "working": hb.get("working"), "detail": hb.get("detail","")[:50],
            "mode": hb.get("mode", "?"),
        }
    except Exception:
        daemon_info = {"status": "no heartbeat (daemon dead?)"}

    hpc_job = ""
    try:
        import subprocess
        cfg = _get_hpc_config()
        r = subprocess.run(
            ["ssh", f"{cfg['user']}@{cfg['host']}",
             f"squeue -u {cfg['user']} -o '%i %T %N %M %j' --noheader 2>/dev/null | head -1"],
            capture_output=True, text=True, timeout=10,
        )
        hpc_job = r.stdout.strip()
    except Exception:
        hpc_job = "(ssh failed)"

    kpi["system"] = {
        "queue": status_counts,
        "real_pending": real_pending,
        "fill_pending": fill_pending,
        "daemon": daemon_info,
        "hpc_job": hpc_job,
    }

    # ── 2. Event Summary ──────────────────────────────────────────────
    events = conn.execute("""
        SELECT event, mode, COUNT(*),
               ROUND(AVG(duration),1), ROUND(MIN(duration),1), ROUND(MAX(duration),1)
        FROM task_events GROUP BY event, mode ORDER BY event
    """).fetchall()
    event_summary = []
    for r in events:
        event_summary.append({
            "event": r[0], "mode": r[1], "count": r[2],
            "avg_dur": r[3], "min_dur": r[4], "max_dur": r[5],
        })
    kpi["events"] = event_summary

    # ── 3. Task Performance ──────────────────────────────────────────
    task_events = conn.execute("""
        SELECT event, status, duration, reason, mode FROM task_events
        WHERE event IN ('task.complete', 'task_completed')
    """).fetchall()
    completed = [r for r in task_events if r[1] == 'completed']
    failed = [r for r in task_events if r[1] == 'failed']
    orphaned = [r for r in task_events if r[1] == 'orphaned']
    preempted = [r for r in failed if r[3] == 'preempted']
    timedout = [r for r in failed if r[3] == 'timeout']

    # Permanently failed (genuine errors that exhausted retries)
    permanently_failed = conn.execute("""
        SELECT COUNT(*) FROM task_events WHERE event IN ('task.fail.permanent', 'task_permanently_failed')
    """).fetchone()[0]

    kpi["tasks"] = {
        "total": len(task_events),
        "completed": len(completed),
        "failed": len(failed),
        "permanently_failed": permanently_failed,
        "orphaned": len(orphaned),
        "preempted": len(preempted),
        "avg_duration_completed": round(sum(r[2] or 0 for r in completed) / max(len(completed),1), 1),
        "avg_duration_failed": round(sum(r[2] or 0 for r in failed) / max(len(failed),1), 1),
    }

    # vLLM load times
    vllm_ready_events = [r for r in event_summary
                         if r["event"] in ("vllm.ready", "vllm_ready")]
    vllm_timeout_events = [r for r in event_summary
                           if r["event"] in ("vllm.timeout", "vllm_timeout")]
    kpi["vllm"] = {"ready_events": vllm_ready_events, "timeout_events": vllm_timeout_events}

    # Job kills
    job_killed_events = conn.execute("""
        SELECT reason, mode, COUNT(*), ROUND(AVG(duration))
        FROM task_events WHERE event IN ('hpc.kill', 'job_killed') GROUP BY reason, mode
    """).fetchall()
    kpi["jobs_killed"] = [{"reason": r[0], "mode": r[1], "count": r[2], "avg_duration": r[3]} for r in job_killed_events]

    # ── 4. Fill Analysis ─────────────────────────────────────────────
    fill_events = conn.execute("""
        SELECT event, COUNT(*) FROM task_events
        WHERE (event LIKE 'fill_%' OR event LIKE 'fill.%') GROUP BY event
    """).fetchall()
    kpi["fills"] = {r[0]: r[1] for r in fill_events}
    kpi["fills_pending"] = fill_pending

    # ── 5. Session Quality (last N) ──────────────────────────────────
    session_dir = Path.home() / ".hermes" / "sessions"
    sessions = sorted(
        session_dir.glob("session_2026*.json"),
        key=lambda p: p.stat().st_mtime, reverse=True
    )[:args.last]

    session_stats = []
    outputs = []
    usefulness_high = 0
    usefulness_low = 0
    total_bytes_written = 0
    total_diffs = 0
    modified_files = set()
    path_errors = 0
    for sp in sessions:
        try:
            sdata = json.loads(sp.read_text())
            msgs = sdata.get("messages", [])
            if not msgs:
                continue
            last_role = msgs[-1].get("role", "")
            orphaned = last_role == "tool"
            tc = Counter()
            wrote_files = False
            session_bytes = 0
            session_diffs = 0
            session_files = set()
            # Track tool calls AND their results
            for i, m in enumerate(msgs):
                # Tool CALLS (assistant messages)
                if m.get("role") == "assistant" and m.get("tool_calls"):
                    for t in m["tool_calls"]:
                        fn = t["function"]["name"]
                        tc[fn] += 1
                        if fn in ("write_file", "patch", "apply_diff"):
                            wrote_files = True
                            # Extract file path from arguments
                            try:
                                tool_args = json.loads(t["function"]["arguments"])
                                fp = tool_args.get("path") or tool_args.get("file_path") or ""
                                if fp:
                                    session_files.add(fp)
                            except (json.JSONDecodeError, TypeError):
                                pass
                # Tool RESULTS (tool role messages) — get bytes/diff stats
                if m.get("role") == "tool":
                    try:
                        result = json.loads(m.get("content", "{}"))
                        if isinstance(result, dict):
                            session_bytes += result.get("bytes_written", 0) or 0
                            if "diff" in result and result.get("success"):
                                diff_text = result["diff"]
                                session_diffs += diff_text.count("\n") if isinstance(diff_text, str) else 0
                                # Extract file path from diff header
                                if isinstance(diff_text, str):
                                    for line in diff_text.split("\n"):
                                        if line.startswith("+++ b/") or line.startswith("--- a/"):
                                            session_files.add(line.split("/", 2)[-1] if "/" in line else line)
                    except (json.JSONDecodeError, TypeError):
                        pass
            total_bytes_written += session_bytes
            total_diffs += session_diffs
            modified_files.update(session_files)

            # Extract final output
            final_output = ""
            for m in reversed(msgs):
                if m.get("role") == "assistant" and not m.get("tool_calls"):
                    final_output = m.get("content", "")
                    break
            ol = len(final_output)

            # Usefulness classification
            useful = False
            if wrote_files:
                usefulness_high += 1
                useful = True
            elif ol > 200:
                usefulness_high += 1
                useful = True
            elif "not found" in final_output.lower() or "does not exist" in final_output.lower():
                usefulness_low += 1
            elif ol < 50 and sum(tc.values()) == 0:
                usefulness_low += 1
            elif not orphaned and ol > 50:
                usefulness_high += 1
                useful = True

            if "not found" in str(msgs[-1].get("content","")) if msgs else False:
                path_errors += 1

            session_stats.append({
                "name": sp.name, "size": sp.stat().st_size,
                "messages": len(msgs), "tool_calls": sum(tc.values()),
                "tool_types": len(tc), "orphaned": orphaned,
                "output_len": ol, "wrote_files": wrote_files,
                "useful": useful, "bytes_written": session_bytes,
                "diff_lines": session_diffs,
                "files": list(session_files),
            })
            if final_output:
                outputs.append(final_output)
        except (json.JSONDecodeError, OSError):
            continue

    if session_stats:
        completed_sessions = [s for s in session_stats if not s["orphaned"]]
        orphaned_sessions = [s for s in session_stats if s["orphaned"]]
        useful_sessions = [s for s in session_stats if s.get("useful")]
        useless_sessions = [s for s in session_stats if not s.get("useful") and not s["orphaned"]]
        kpi["sessions"] = {
            "total": len(session_stats),
            "completed": len(completed_sessions),
            "orphaned": len(orphaned_sessions),
            "orphaned_rate": round(len(orphaned_sessions) / max(len(session_stats),1) * 100, 1),
            "useful": len(useful_sessions),
            "useless": len(useless_sessions),
            "useful_rate": round(len(useful_sessions) / max(len(completed_sessions),1) * 100, 1) if completed_sessions else 0,
            "total_bytes_written": total_bytes_written,
            "total_diff_lines": total_diffs,
            "files_modified": len(modified_files),
            "modified_files": sorted(modified_files)[:20],  # Top 20
            "path_errors": path_errors,
            "avg_output_chars": round(sum(s["output_len"] for s in completed_sessions) / max(len(completed_sessions),1)),
            "avg_tool_calls": round(sum(s["tool_calls"] for s in session_stats) / max(len(session_stats),1), 1),
            "avg_tool_types": round(sum(s["tool_types"] for s in session_stats) / max(len(session_stats),1), 1),
            "largest_session": max(session_stats, key=lambda s: s["size"]) if session_stats else None,
            "most_tools": max(session_stats, key=lambda s: s["tool_calls"]) if session_stats else None,
            "orphaned_list": [s["name"] for s in orphaned_sessions[:5]],
            "useless_list": [s["name"] for s in useless_sessions[:3]],
        }
    else:
        kpi["sessions"] = {"total": 0}

    # ── 6. Convergence (fill output similarity) ──────────────────────
    if args.fill and len(outputs) >= 2:
        similarities = []
        for i in range(len(outputs) - 1):
            a = set(w for w in outputs[i].lower().split() if len(w) > 3)
            b = set(w for w in outputs[i+1].lower().split() if len(w) > 3)
            if not a or not b:
                continue
            intersection = len(a & b)
            union = len(a | b)
            similarities.append(intersection / max(union, 1))
        if similarities:
            avg_sim = sum(similarities) / len(similarities)
            kpi["convergence"] = {
                "sessions_compared": len(similarities) + 1,
                "avg_similarity": round(avg_sim, 3),
                "min_similarity": round(min(similarities), 3),
                "max_similarity": round(max(similarities), 3),
                "interpretation": (
                    "⚠️ CONVERGING — fills may be stuck (same output each cycle)"
                    if avg_sim > 0.6
                    else "✅ DIVERGING — diverse work, healthy"
                    if avg_sim < 0.3
                    else "→ MODERATE — mixed output, normal"
                ),
            }

    conn.close()

    # ── BY-PROMPT EVOLUTION ──────────────────────────────────────────
    if args.by_prompt:
        _analyze_prompt_evolution(args.last, args.json)
        return

    # ── Health Score ────────────────────────────────────────────────
    warnings = []
    daemon_ok = kpi["system"]["daemon"].get("breaker") in ("CLOSED",) and kpi["system"]["daemon"].get("working") is not False
    if not daemon_ok:
        warnings.append("Daemon offline or breaker not CLOSED")
    if kpi["system"]["hpc_job"] == "(none)":
        warnings.append("No active HPC job")
    orphan_rate = kpi["sessions"].get("orphaned_rate", 0)
    if orphan_rate > 25:
        warnings.append(f"High orphan rate ({orphan_rate}%)")
    elif orphan_rate > 15:
        warnings.append(f"Elevated orphan rate ({orphan_rate}%)")

    useful_rate = kpi["sessions"].get("useful_rate", 100)
    if useful_rate < 50 and kpi["sessions"].get("completed", 0) > 3:
        warnings.append(f"Low useful output rate ({useful_rate}%)")
    if kpi["sessions"].get("useless", 0) > kpi["sessions"].get("completed", 0) * 0.3:
        warnings.append(f"Many low-value sessions ({kpi['sessions']['useless']}/{kpi['sessions']['completed']} completed)")

    score = "✅ PASS" if not warnings else ("⚠️ WARN" if len(warnings) <= 2 else "❌ FAIL")
    kpi["health"] = {"score": score, "warnings": warnings}

    # ── OUTPUT ────────────────────────────────────────────────────────
    if args.json:
        print(json.dumps(kpi, indent=2, default=str))
        return

    print("=" * 70)
    print("  HPC-AGENT KPI DASHBOARD")
    print("=" * 70)

    # Health score
    h = kpi["health"]
    print(f"\n🏥 HEALTH: {h['score']}")
    for w in h['warnings']:
        print(f"  ⚠️  {w}")
    if not h['warnings']:
        print(f"  All subsystems nominal")

    # System
    d = kpi["system"]
    print(f"\n📊 SYSTEM HEALTH")
    print(f"  Daemon: PID={d['daemon'].get('pid','?')} | Mode={d['daemon'].get('mode','?')} | "
          f"Breaker={d['daemon'].get('breaker','?')} | Working={d['daemon'].get('working','?')}")
    print(f"  Detail: {d['daemon'].get('detail','')}")
    print(f"  HPC: {d['hpc_job'] or '(none)'}")
    print(f"  Queue: {d['real_pending']} real pending | {d['fill_pending']} fill pending")
    print(f"  Total: {sum(d['queue'].values())} tasks ({d['queue'].get('completed',0)} completed, "
          f"{d['queue'].get('failed',0)} failed, {d['queue'].get('pending',0)} pending)")

    # Task performance
    t = kpi["tasks"]
    print(f"\n📈 TASK PERFORMANCE")
    print(f"  Completed: {t['completed']} | Failed: {t['failed']} | "
          f"PermFailed: {t.get('permanently_failed', 0)} | Preempted: {t['preempted']}")
    print(f"  Avg duration: {t['avg_duration_completed']}s completed | {t['avg_duration_failed']}s failed")
    if kpi.get("vllm"):
        for e in kpi["vllm"]["ready_events"]:
            print(f"  vLLM load (mode={e['mode']}): {e['count']}x, avg {e['avg_dur']}s")

    # Fill analysis
    print(f"\n🔄 FILL ANALYSIS")
    print(f"  Pending: {kpi['fills_pending']}")
    for ev, cnt in sorted(kpi['fills'].items()):
        print(f"  {ev}: {cnt}")
    if kpi.get("jobs_killed"):
        print(f"  Jobs killed:")
        for jk in kpi["jobs_killed"]:
            print(f"    {jk['reason']:15s} mode={jk['mode']:10s} {jk['count']}x avg_dur={jk['avg_duration']}s")
    if kpi.get("vllm") and kpi["vllm"]["timeout_events"]:
        for e in kpi["vllm"]["timeout_events"]:
            print(f"  vLLM timeouts (mode={e['mode']}): {e['count']}x at {e['avg_dur']}s")

    # Session quality
    s = kpi["sessions"]
    print(f"\n💬 SESSIONS (last {s.get('total',0)})")
    print(f"  Completed: {s.get('completed',0)} | Orphaned: {s.get('orphaned',0)} ({s.get('orphaned_rate',0)}%)")
    print(f"  Useful: {s.get('useful',0)} ({s.get('useful_rate',0)}% of completed) | "
          f"Low-value: {s.get('useless',0)} | Path errors: {s.get('path_errors',0)}")
    print(f"  Code produced: {s.get('total_bytes_written',0)} bytes written | {s.get('total_diff_lines',0)} diff lines | "
          f"{s.get('files_modified',0)} files modified")
    print(f"  Avg output: {s.get('avg_output_chars',0)} chars | Avg tools: {s.get('avg_tool_calls',0)} "
          f"({s.get('avg_tool_types',0)} types)")
    if s.get('modified_files'):
        print(f"  Modified files ({len(s['modified_files'])}):")
        for fp in s['modified_files'][:8]:
            print(f"    📄 {fp[:70]}")
    if s.get("orphaned_list"):
        print(f"  Orphaned sessions:")
        for name in s["orphaned_list"]:
            print(f"    ⚠️ {name[:45]}")
    if s.get("largest_session"):
        ls = s["largest_session"]
        print(f"  Largest: {ls['name'][:40]} ({ls['size']//1024}K, {ls['messages']}msgs, {ls['tool_calls']}tools)")

    # Convergence
    if kpi.get("convergence"):
        c = kpi["convergence"]
        print(f"\n🎯 FILL CONVERGENCE ({c['sessions_compared']} consecutive outputs)")
        print(f"  Similarity: avg={c['avg_similarity']:.1%} min={c['min_similarity']:.1%} max={c['max_similarity']:.1%}")
        print(f"  → {c['interpretation']}")


def _analyze_prompt_evolution(last_n: int = 20, json_output: bool = False):
    """Group session files by prompt text (first user message) and show evolution.

    For each prompt group, shows metrics across consecutive runs:
    - Output length trend, tool call trend, orphan rate, files modified
    Helps detect convergence, degradation, or stuck fills.
    """
    import os, glob, json
    from collections import Counter, defaultdict
    from pathlib import Path

    session_dir = Path.home() / ".hermes" / "sessions"
    sessions = sorted(
        session_dir.glob("session_2026*.json"),
        key=lambda p: p.stat().st_mtime, reverse=True
    )[:max(last_n * 3, 100)]

    # Group by prompt text (first user message)
    prompt_groups = defaultdict(list)
    for sp in sessions:
        try:
            sdata = json.loads(sp.read_text())
            msgs = sdata.get("messages", [])
            if not msgs:
                continue
            # Extract first user message as prompt signature
            prompt = ""
            for m in msgs:
                if m.get("role") == "user":
                    prompt = m.get("content", "")[:200]
                    break
            if not prompt:
                continue

            # Compute session metrics
            last_role = msgs[-1].get("role", "") if msgs else ""
            orphaned = last_role == "tool"
            tc = Counter()
            wrote_files = False
            session_bytes = 0
            session_files = set()
            for m in msgs:
                if m.get("role") == "assistant" and m.get("tool_calls"):
                    for t in m["tool_calls"]:
                        fn = t["function"]["name"]
                        tc[fn] += 1
                        if fn in ("write_file", "patch", "apply_diff"):
                            wrote_files = True
                            try:
                                ta = json.loads(t["function"]["arguments"])
                                fp = ta.get("path") or ta.get("file_path") or ""
                                if fp:
                                    session_files.add(fp)
                            except (json.JSONDecodeError, TypeError):
                                pass
                if m.get("role") == "tool":
                    try:
                        result = json.loads(m.get("content", "{}"))
                        if isinstance(result, dict):
                            session_bytes += result.get("bytes_written", 0) or 0
                    except (json.JSONDecodeError, TypeError):
                        pass
            final_output = ""
            for m in reversed(msgs):
                if m.get("role") == "assistant" and not m.get("tool_calls"):
                    final_output = m.get("content", "")
                    break
            output_len = len(final_output)
            tool_total = sum(tc.values())
            tool_types = len(tc)

            prompt_groups[prompt].append({
                "name": sp.name,
                "ts": sp.stat().st_mtime,
                "orphaned": orphaned,
                "output_len": output_len,
                "tool_calls": tool_total,
                "tool_types": tool_types,
                "wrote_files": wrote_files,
                "bytes_written": session_bytes,
                "files": sorted(session_files),
                "prompt_short": prompt[:80],
            })
        except (json.JSONDecodeError, OSError):
            continue

    # Sort each group by timestamp (oldest first = evolution order)
    for prompt in prompt_groups:
        prompt_groups[prompt].sort(key=lambda s: s["ts"])

    # Show results
    if json_output:
        result = {}
        for prompt, runs in sorted(prompt_groups.items(), key=lambda x: -len(x[1])):
            result[prompt[:100]] = runs
        print(json.dumps(result, indent=2, default=str))
        return

    print("=" * 70)
    print("  PER-PROMPT EVOLUTION (last {} runs)".format(max(len(v) for v in prompt_groups.values()) if prompt_groups else 0))
    print("=" * 70)

    for prompt, runs in sorted(prompt_groups.items(), key=lambda x: -len(x[1])):
        # Determine a short label for the prompt
        label = runs[0]["prompt_short"].rstrip(".").strip()[:60]
        
        # Count orphans, compute trends
        total_runs = len(runs)
        orphan_count = sum(1 for r in runs if r["orphaned"])
        first_output = runs[0]["output_len"]
        last_output = runs[-1]["output_len"]
        first_tools = runs[0]["tool_calls"]
        last_tools = runs[-1]["tool_calls"]
        total_bytes = sum(r["bytes_written"] for r in runs)
        file_count = sum(len(r["files"]) for r in runs)

        # Trend direction
        output_trend = "↑" if last_output > first_output + 10 else ("↓" if last_output < first_output - 10 else "→")
        tool_trend = "↑" if last_tools > first_tools + 2 else ("↓" if last_tools < first_tools - 2 else "→")

        # Summary line
        print(f"\n📋 \"{label}\"")
        print(f"  Runs: {total_runs} | Orphaned: {orphan_count}/{total_runs} ({100*orphan_count//max(total_runs,1)}%) | "
              f"Written: {total_bytes} bytes | Files: {file_count}")
        print(f"  First run: {first_output}c output | {first_tools} tools"
              f"  →  Latest: {last_output}c {output_trend} | {last_tools}tools {tool_trend}")

        # Show per-run timeline (compact)
        timeline = []
        for i, r in enumerate(runs[:20]):  # Show at most 20 runs
            icon = "⚠️" if r["orphaned"] else "✅"
            timeline.append(f"{icon}{r['output_len']}c/{r['tool_calls']}t")
        if timeline:
            print(f"  Timeline: {' → '.join(timeline[:10])}")
            if len(timeline) > 10:
                print(f"            {' → '.join(timeline[10:20])}")
        if runs[-1]["files"]:
            print(f"  Latest files: {', '.join(runs[-1]['files'][:5])}")

        # Quick assessment
        if orphan_count > total_runs // 2 and total_runs >= 3:
            print(f"  ⚠️  Assessment: HIGH ORPHAN RATE — consider revising prompt")
        elif total_bytes == 0 and total_runs >= 3 and last_output < 100:
            print(f"  ⚠️  Assessment: LOW OUTPUT — fill may be stuck")
        elif output_trend == "↑" and tool_trend == "↑":
            print(f"  ✅ Assessment: Growing — good trajectory")
        elif output_trend == "→" and tool_trend == "→":
            print(f"  → Assessment: Stable — normal operation")
        else:
            print(f"  → Assessment: Mixed — review if concerning")


def cmd_cleanup(args):
    """hpc_batch.py cleanup [--sessions DAYS] [--events DAYS] [--dry-run]

    Clean up stale data: session files and old task_events.
    """
    from pathlib import Path
    import os, shutil, glob

    db = args.db or str(DEFAULT_DB)
    now = time.time()
    dry_run = args.dry_run

    # ── Session files ──────────────────────────────────────────────
    if args.sessions:
        session_dir = Path.home() / ".hermes" / "sessions"
        cutoff = now - args.sessions * 86400
        removed = 0
        kept = 0
        if session_dir.is_dir():
            for sp in sorted(session_dir.glob("session_*.json"),
                              key=lambda p: p.stat().st_mtime):
                if sp.stat().st_mtime < cutoff:
                    removed += 1
                    if not dry_run:
                        sp.unlink()
                else:
                    kept += 1
        print(f"Session files: {removed} removed, {kept} kept "
              f"(cutoff: {args.sessions}d)" + (" (dry-run)" if dry_run else ""))

    # ── task_events ─────────────────────────────────────────────────
    if args.events:
        import sqlite3
        if not dry_run:
            conn = sqlite3.connect(db)
            cutoff = now - args.events * 86400
            deleted = conn.execute(
                "DELETE FROM task_events WHERE ts < ?", (cutoff,)
            ).rowcount
            conn.commit()
            conn.close()
            print(f"task_events: {deleted} rows deleted (cutoff: {args.events}d)")
        else:
            conn = sqlite3.connect(db)
            cutoff = now - args.events * 86400
            count = conn.execute(
                "SELECT COUNT(*) FROM task_events WHERE ts < ?", (cutoff,)
            ).fetchone()[0]
            conn.close()
            print(f"task_events: {count} rows would be deleted (cutoff: {args.events}d)")


def cmd_migrate_events(args):
    """hpc_batch.py migrate-events [--dry-run] [--event EVENT]

    Backfill migration: rename old underscore event names to new dot-separated
    names (e.g., fill_claimed → fill.claim). Runs in a single transaction.

    After migration, KPI queries can drop the backward-compat IN ('new', 'old').
    """
    db_path = args.db
    import sqlite3

    # Old → new event name mapping
    EVENT_RENAME_MAP = {
        "fill_claimed": "fill.claim",
        "fill_completed": "fill.complete",
        "fill_yielded": "fill.yield",
        "vllm_monitoring_start": "vllm.monitor.start",
        "vllm_ready": "vllm.ready",
        "vllm_timeout": "vllm.timeout",
        "vllm_job_vanished": "vllm.job.lost",
        "vllm_premature": "vllm.tunnel.stale",
        "job_killed": "hpc.kill",
        "job_submitted": "hpc.submit",
        "breaker_closed": "breaker.close",
        "breaker_open": "breaker.open",
        "breaker_half_open": "breaker.half_open",
        "state_restored": "daemon.state.restore",
        "task_claimed": "task.claim",
        "task_completed": "task.complete",
        "task_permanently_failed": "task.fail.permanent",
        "task_recovered": "task.recover",
    }

    renames = EVENT_RENAME_MAP
    if args.event:
        if args.event not in renames:
            print(f"Unknown event: {args.event}")
            print(f"Known old names: {', '.join(sorted(renames.keys()))}")
            return
        renames = {args.event: renames[args.event]}

    if args.dry_run:
        conn = sqlite3.connect(db_path, timeout=3)
        total = 0
        for old_name, new_name in renames.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM task_events WHERE event = ?", (old_name,)
            ).fetchone()[0]
            if count > 0:
                print(f"Would rename {count} rows: {old_name} → {new_name}")
                total += count
        conn.close()
        print(f"Total: {total} rows would be renamed")
        return

    # Actual migration in a single transaction
    conn = sqlite3.connect(db_path, timeout=10)
    try:
        total = 0
        for old_name, new_name in renames.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM task_events WHERE event = ?", (old_name,)
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    "UPDATE task_events SET event = ? WHERE event = ?",
                    (new_name, old_name),
                )
                print(f"Renamed {count} rows: {old_name} → {new_name}")
                total += count
        conn.commit()
        print(f"Done: {total} rows renamed")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── System Diagnostics (extracted from scripts/diag.py) ────────────────

_DIAG_EMOJI = {
    "ok": "  ✅ ",
    "warn": "  ⚠️  ",
    "fail": "  ❌ ",
}


def _diag_ok(msg: str) -> str:
    return f"{_DIAG_EMOJI['ok']}{msg}"

def _diag_warn(msg: str) -> str:
    return f"{_DIAG_EMOJI['warn']}{msg}"

def _diag_fail(msg: str) -> str:
    return f"{_DIAG_EMOJI['fail']}{msg}"

def _diag_check(condition: bool, msg_pass: str, msg_fail: str) -> str:
    return _diag_ok(msg_pass) if condition else _diag_fail(msg_fail)


def _diag_check_db(db_path: str) -> list[str]:
    """Check DB integrity and task counts."""
    results = ["\n📦 Database"]
    if not os.path.exists(db_path):
        return results + [_diag_fail(f"DB not found: {db_path}")]
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.execute("SELECT 1")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0]
        running = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running'").fetchone()[0]
        failed = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='failed'").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='completed'").fetchone()[0]
        fills = conn.execute("SELECT COUNT(*) FROM tasks WHERE priority < 0").fetchone()[0]
        conn.close()
        results.append(_diag_ok(f"DB: {db_path}"))
        results.append(_diag_ok(f"Integrity: {integrity}"))
        results.append(_diag_ok(
            f"Total: {total} (pending={pending} running={running} "
            f"completed={completed} failed={failed} fill={fills})"))
        if running > 0:
            results.append(_diag_warn(
                f"{running} task(s) in 'running' status — daemon auto-recovery (#5) handles "
                f"stale started_at, but tasks with NULL started_at may need manual reset"))
        if failed > 0:
            results.append(_diag_warn(
                f"{failed} failed task(s) — check with: python3 hpc_batch.py pending"))
    except Exception as e:
        results.append(_diag_fail(f"DB error: {e}"))
    return results


def _diag_check_daemon() -> list[str]:
    """Check daemon heartbeat, PID, processes."""
    results = ["\n🤖 Daemon (HermesWorker)"]
    try:
        pid_file = "/tmp/hermes-worker.pid"

        # Check fcntl lock
        try:
            import fcntl
            fd = os.open(pid_file, os.O_RDONLY | os.O_CREAT)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fd, fcntl.LOCK_UN)
                results.append(_diag_warn("No fcntl lock — daemon not running"))
            except IOError:
                results.append(_diag_ok("fcntl lock held (singleton guard active)"))
            finally:
                os.close(fd)
        except ImportError:
            results.append(_diag_warn("fcntl not available — using PID file fallback"))

        # PID file
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                expected_pid = int(f.read().strip())
            try:
                os.kill(expected_pid, 0)
                results.append(_diag_ok(f"PID file OK ({expected_pid} running)"))
            except OSError:
                results.append(_diag_warn(f"PID file stale ({expected_pid} not alive)"))
        else:
            results.append(_diag_warn("No PID file"))

        # Scan for HermesWorker processes
        proc = subprocess.run(
            ["pgrep", "-a", "python"], capture_output=True, text=True, timeout=5)
        hermes_pids = []
        for line in proc.stdout.strip().split("\n"):
            if "HermesWorker" in line:
                hermes_pids.append(line.split()[0])
        if len(hermes_pids) == 0:
            results.append(_diag_fail("No HermesWorker process found"))
        elif len(hermes_pids) > 1:
            results.append(_diag_warn(
                f"Multiple HermesWorker instances ({', '.join(hermes_pids)})"))
        else:
            results.append(_diag_ok(f"Running PID {hermes_pids[0]} (singleton OK)"))
    except Exception as e:
        results.append(_diag_warn(f"Could not check: {e}"))

    # tmux session
    try:
        r = subprocess.run(["tmux", "has-session", "-t", "hpc-worker"],
                          capture_output=True, timeout=3)
        results.append(_diag_ok("tmux session hpc-worker exists")
                       if r.returncode == 0 else _diag_fail("no tmux session hpc-worker"))
    except FileNotFoundError:
        results.append(_diag_fail("tmux not found"))
    except Exception:
        pass

    # Heartbeat
    hb_file = "/tmp/hermes-worker.heartbeat"
    if os.path.exists(hb_file):
        try:
            with open(hb_file) as f:
                hb = json.load(f)
            age = time.time() - hb.get("time", 0)
            working = hb.get("working", False)
            detail = hb.get("detail", "")
            pending = hb.get("pending_real", "?")
            if age < 300:
                status = "fresh" if working else "idle"
                results.append(_diag_ok(
                    f"Heartbeat: {status}, {age:.0f}s old, {detail} ({pending} pending)"))
            else:
                results.append(_diag_warn(
                    f"Heartbeat stale: {age:.0f}s old (>300s) — daemon may be stuck"))
        except (json.JSONDecodeError, OSError) as e:
            results.append(_diag_warn(f"Heartbeat unreadable: {e}"))
    else:
        results.append(_diag_warn("No heartbeat file"))

    # Daemon log
    log_dir = HPC_AGENT_DIR / "logs"
    daemon_log = log_dir / "worker-daemon.log"
    if daemon_log.exists():
        size = daemon_log.stat().st_size
        results.append(_diag_ok(f"worker-daemon.log: {size} bytes"))
        if size > 0:
            with open(daemon_log) as f:
                all_lines = f.readlines()
                last_line = all_lines[-1].strip() if all_lines else ""
            results.append(_diag_ok(f"Last log: {last_line[:120]}"))
    else:
        results.append(_diag_warn("worker-daemon.log not found"))
    return results


def _diag_check_tunnel(tunnel_debug: bool = False) -> list[str]:
    """Check SSH tunnel to HPC vLLM."""
    import socket
    results = ["\n🔌 Tunnel (port 8100 → HPC vLLM)"]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        result = s.connect_ex(("127.0.0.1", 8100))
        s.close()
        results.append(_diag_ok("Port 8100 is listening") if result == 0
                       else _diag_fail("Port 8100 not listening"))
    except Exception as e:
        results.append(_diag_warn(f"Port check error: {e}"))

    try:
        r = subprocess.run(["lsof", "-ti", "tcp:8100"],
                          capture_output=True, text=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            pids = r.stdout.strip().split("\n")
            results.append(_diag_ok(f"Tunnel process(es): {', '.join(pids)}"))
        else:
            results.append(_diag_fail("No tunnel process found on port 8100"))
    except Exception as e:
        results.append(_diag_warn(f"lsof error: {e}"))

    try:
        r = subprocess.run(["curl", "-s", "--max-time", "5",
                           "http://localhost:8100/v1/models"],
                          capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            try:
                data = json.loads(r.stdout)
                models = [m["id"] for m in data.get("data", [])]
                results.append(_diag_ok(f"vLLM responds through tunnel: models={models}"))
            except Exception:
                results.append(_diag_ok("vLLM responds through tunnel (raw)"))
        else:
            results.append(_diag_fail(f"Tunnel: curl failed (rc={r.returncode})"))
    except Exception as e:
        results.append(_diag_warn(f"Tunnel vLLM test skipped: {e}"))
    return results


def _diag_check_mcp() -> list[str]:
    """Check MCP server status."""
    results = ["\n🖥️  MCP Server (mcps/hpc_hermes.py)"]
    mcp_path = HPC_AGENT_DIR / "mcps" / "hpc_hermes.py"
    results.append(_diag_check(mcp_path.exists(), "mcps/hpc_hermes.py exists", "MISSING"))
    try:
        proc = subprocess.Popen(
            [sys.executable, str(mcp_path)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True,
        )
        init_msg = json.dumps({
            "jsonrpc": "2.0", "method": "initialize", "id": 1,
            "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                       "clientInfo": {"name": "diag", "version": "1.0"}}
        })
        out, _ = proc.communicate(input=init_msg + "\n", timeout=5)
        resp = json.loads(out.strip())
        if resp.get("id") == 1 and "result" in resp:
            results.append(_diag_ok("MCP server initializes (protocol 2024-11-05)"))

        # List tools
        proc2 = subprocess.Popen(
            [sys.executable, str(mcp_path)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True,
        )
        batch = (
            json.dumps({"jsonrpc": "2.0", "method": "initialize", "id": 1,
                        "params": {"protocolVersion": "2024-11-05",
                                   "capabilities": {},
                                   "clientInfo": {"name": "diag", "version": "1.0"}}})
            + "\n" +
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"})
            + "\n" +
            json.dumps({"jsonrpc": "2.0", "method": "tools/list", "id": 2})
            + "\n"
        )
        out2, _ = proc2.communicate(input=batch, timeout=5)
        for line in out2.strip().split("\n"):
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
                if msg.get("id") == 2 and "result" in msg:
                    tools = msg["result"]["tools"]
                    names = [t["name"] for t in tools]
                    if all(n in names for n in ["run_on_hpc_tool", "submit_hpc_task",
                                                  "run_hpc_agent_task", "check_hpc_task_status"]):
                        results.append(_diag_ok(f"MCP tools visible ({len(tools)} tools)"))
                    else:
                        results.append(_diag_warn(f"Missing tools: expected 4, got {names}"))
            except json.JSONDecodeError:
                pass
    except Exception as e:
        results.append(_diag_warn(f"MCP protocol test: {e}"))
    return results


def _diag_check_hpc() -> list[str]:
    """Check HPC SLURM job."""
    results = ["\n🖧  HPC Job"]
    try:
        cfg_file = HPC_AGENT_DIR / "config.yaml"
        if cfg_file.exists():
            import yaml
            cfg = yaml.safe_load(cfg_file.read_text()) or {}
            hpc_host = cfg.get("hpc", {}).get("host", "your-hpc-host")
            hpc_user = cfg.get("hpc", {}).get("user", "your-hpc-user")
        else:
            hpc_host = "your-hpc-host"
            hpc_user = "your-hpc-user"

        r = subprocess.run(
            ["ssh", f"{hpc_user}@{hpc_host}",
             "squeue -u $(whoami) -o '%i %T %N %M %j' --noheader 2>/dev/null"],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode == 0 and r.stdout.strip():
            for line in r.stdout.strip().split("\n"):
                parts = line.split()
                if len(parts) >= 3:
                    results.append(_diag_ok(
                        f"Job {parts[0]}: {parts[1]} on {parts[2]} "
                        f"({parts[3] if len(parts) > 3 else '?'})"))
        else:
            results.append(_diag_warn("No active HPC jobs"))
            results.append(_diag_warn("Auto-dispatch will submit one when real tasks arrive"))
    except Exception as e:
        results.append(_diag_warn(f"HPC check error: {e}"))
    return results


def _diag_check_crontab() -> list[str]:
    """Check crontab for hpc-worker entries."""
    results = ["\n⏰ Crontab"]
    try:
        r = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            lines = r.stdout.strip().split("\n")
            worker_lines = [l for l in lines if "hpc-worker" in l]
            results.append(_diag_ok(f"Crontab has {len(worker_lines)} hpc-worker line(s)")
                           if worker_lines else _diag_fail("No hpc-worker crontab line"))
        else:
            results.append(_diag_fail("No crontab"))
    except Exception as e:
        results.append(_diag_warn(f"Crontab check error: {e}"))
    return results


def cmd_diag_default(db_path: str | None = None,
                     use_json: bool = False,
                     tunnel_debug: bool = False) -> str | dict:
    """Run all 6 health checks and return formatted output.

    Args:
        db_path: Path to tasks.db (default: DEFAULT_DB).
        use_json: Return dict instead of formatted string.
        tunnel_debug: Include verbose tunnel diagnostics.

    Returns:
        Formatted string (default) or dict (use_json=True).
    """
    if db_path is None:
        db_path = str(DEFAULT_DB)

    sections = [
        _diag_check_db(db_path),
        _diag_check_daemon(),
        _diag_check_tunnel(tunnel_debug),
        _diag_check_mcp(),
        _diag_check_hpc(),
        _diag_check_crontab(),
    ]

    if use_json:
        output: dict = {"healthy": True, "checks": {}}
        for section in sections:
            if not section:
                continue
            header = section[0].strip()
            for emoji in ["📦", "🤖", "🔌", "🖥️", "🖧", "⏰"]:
                header = header.replace(emoji + " ", "", 1) if header.startswith(emoji) else header
            items = section[1:]
            output["checks"][header] = items
            for item in items:
                if _DIAG_EMOJI["fail"] in item:
                    output["healthy"] = False
        return output

    lines = []
    for section in sections:
        lines.extend(section)
    lines.append(f"\n{'='*50}")
    errors = sum(1 for s in sections for l in s[1:] if _DIAG_EMOJI["fail"] in l)
    warnings = sum(1 for s in sections for l in s[1:] if _DIAG_EMOJI["warn"] in l)
    if errors == 0 and warnings == 0:
        lines.append("✅ ALL CHECKS PASSED")
    else:
        lines.append(f"📊 {errors} failures, {warnings} warnings")
    return "\n".join(lines)


def cmd_diag(args):
    """hpc_batch.py diag [--job JOB_ID] [--recent] [--fills]

    System diagnostics and job lifecycle tracing.

    --job JOB_ID   Show chronological event sequence for a specific HPC job.
    --recent       Show last 10 job lifecycle summaries.
    --fills        Show fill task performance per prompt type.
    """
    db = args.db or str(DEFAULT_DB)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    from datetime import datetime, timezone

    # ── Job trace ──────────────────────────────────────────────────
    if args.job:
        rows = conn.execute("""
            SELECT ts, event, mode, fill_id, duration, reason, status
            FROM task_events
            WHERE job_id = ?
            ORDER BY ts ASC
        """, (args.job,)).fetchall()

        if not rows:
            print(f"No events found for job {args.job}")
            conn.close()
            return

        # Determine job mode from first event
        mode = rows[0]["mode"] or "?"
        print(f"Job {args.job}  (mode={mode})")
        print("-" * 60)

        start_ts = rows[0]["ts"]
        for r in rows:
            ts_str = datetime.fromtimestamp(r["ts"], tz=timezone.utc).strftime("%H:%M:%S")
            offset = ""
            if start_ts:
                offset = f"  +{r['ts'] - start_ts:.0f}s"
            fill = f"  fill={r['fill_id'][:12]}" if r["fill_id"] else ""
            dur = f"  dur={r['duration']:.0f}s" if r["duration"] is not None else ""
            reason = f"  ({r['reason']})" if r["reason"] else ""
            print(f"  {ts_str} {r['event']:25s}{offset}{fill}{dur}{reason}")

        conn.close()
        return

    # ── Recent jobs ────────────────────────────────────────────────
    if args.recent:
        # Find distinct job_ids from task_events (most recent first)
        job_ids = conn.execute("""
            SELECT job_id FROM task_events
            WHERE job_id IS NOT NULL AND job_id != ''
            GROUP BY job_id
            ORDER BY MAX(ts) DESC
            LIMIT 10
        """).fetchall()

        for jr in job_ids:
            jid = jr["job_id"]
            events = conn.execute("""
                SELECT event, ts, duration FROM task_events
                WHERE job_id = ? ORDER BY ts ASC
            """, (jid,)).fetchall()

            if not events:
                continue
            start = events[0]["ts"]
            end = events[-1]["ts"]
            total = end - start
            outcomes = [e["event"] for e in events]
            status = "✅" if "vllm.ready" in outcomes else ("❌" if "vllm.timeout" in outcomes else "👻")
            print(f"  {status} {jid[:20]:20s}  {outcomes[0]:15s} → {outcomes[-1]:15s}  ({total:.0f}s)")

        conn.close()
        return

    # ── Fill performance ───────────────────────────────────────────
    if args.fills:
        rows = conn.execute("""
            SELECT substr(fill_id, 1, 12) as fid,
                   event, COUNT(*) as cnt,
                   ROUND(AVG(duration)) as avg_dur
            FROM task_events
            WHERE (event LIKE 'fill.%' OR event LIKE 'fill_%')
            GROUP BY fid, event
            ORDER BY fid, event
        """).fetchall()
        for r in rows:
            print(f"  {r['fid']:12s} {r['event']:20s} {r['cnt']:3d}x  avg_dur={r['avg_dur'] or '?'}s")
        conn.close()
        return

    # ── Default: quick health summary ─────────────────────────────
    result = cmd_diag_default(db_path=args.db, use_json=getattr(args, 'json_output', False))
    if isinstance(result, dict):
        print(json.dumps(result, indent=2))
    else:
        print(result)
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="HPC Batch — accumulate LLM tasks, dispatch to HPC GPU, collect results",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB),
                        help=f"SQLite database path (default: {DEFAULT_DB})")
    parser.add_argument("--model", default="deepseek-ai/DeepSeek-V4-Flash",
                        help="Model name")
    sub = parser.add_subparsers(dest="command", required=True)

    # submit-fill
    p_submit_fill = sub.add_parser("submit-fill", help="Submit a low-priority fill task")
    p_submit_fill.add_argument("--prompt", help="Explicit prompt (omit for auto-generated)")
    p_submit_fill.add_argument("--type", dest="fill_type", default="stress-test",
                               help="Fill task type (stress-test, spec-gap, data-explore, cross-ref, warmup)")
    p_submit_fill.set_defaults(func=cmd_submit_fill)

    # submit
    p_submit = sub.add_parser("submit", help="Submit task(s) to the queue")
    p_submit.add_argument("--prompt", help="Single prompt text")
    p_submit.add_argument("--prompt-file", help="File containing prompt text (avoids argv limits)")
    p_submit.add_argument("--file", help="File with prompts (one per line or JSONL)")
    p_submit.add_argument("--type", default="generic", help="Task type tag for filtering")
    p_submit.add_argument("--cycle", type=int, default=None,
                          help="FA pipeline cycle number (stored in metadata)")
    p_submit.add_argument("--priority", type=int, default=0,
                          help="Task priority (default: 0, fill tasks use -1)")
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
    p_dispatch.add_argument("--batch", action="store_true",
                           help="One-shot batch mode (short idle timeout)")
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

    # recover-stuck
    p_rs = sub.add_parser("recover-stuck", help="Recover tasks stuck in running status (preempted jobs)")
    p_rs.add_argument("--max-retries", type=int, default=3,
                      dest="max_retries",
                      help="Max retries before marking as failed (default: 3)")
    p_rs.set_defaults(func=cmd_recover_stuck)

    # count
    p_count = sub.add_parser("count", help="Count tasks by status and type")
    p_count.add_argument("--type", help="Filter by task type")
    p_count.add_argument("--status", default="pending", help="Filter by status (default: pending)")
    p_count.set_defaults(func=cmd_count)

    # events
    p_ev = sub.add_parser("events", help="Query structured event log")
    p_ev.add_argument("--event", help="Filter by event type (e.g. vllm_ready, job_killed)")
    p_ev.add_argument("--mode", help="Filter by daemon mode (normal, keepwarm)")
    p_ev.add_argument("--task", help="Filter by task ID prefix")
    p_ev.add_argument("--limit", type=int, default=20, help="Max events to show")
    p_ev.add_argument("--json", action="store_true", help="JSON output")
    p_ev.add_argument("--stats", action="store_true",
                      help="Show aggregate stats instead of raw events")
    p_ev.set_defaults(func=cmd_events)

    # migrate-events
    p_migrate = sub.add_parser("migrate-events",
                                help="Backfill old underscore event names to dot-separated names")
    p_migrate.add_argument("--dry-run", action="store_true",
                           help="Show what would be renamed without modifying data")
    p_migrate.add_argument("--event", help="Target a specific old event name (e.g., fill_claimed)")
    p_migrate.set_defaults(func=cmd_migrate_events)

    # cleanup
    p_cleanup = sub.add_parser("cleanup", help="Clean up stale session files and task_events")
    p_cleanup.add_argument("--sessions", type=int, default=7,
                           help="Remove session files older than N days (default: 7)")
    p_cleanup.add_argument("--events", type=int, default=30,
                           help="Delete task_events older than N days (default: 30)")
    p_cleanup.add_argument("--dry-run", action="store_true",
                           help="Show what would be deleted without deleting")
    p_cleanup.set_defaults(func=cmd_cleanup)

    # diag
    p_diag = sub.add_parser("diag", help="System diagnostics + job lifecycle tracing")
    p_diag.add_argument("--job", help="Trace a specific HPC job by ID")
    p_diag.add_argument("--recent", action="store_true",
                        help="Show last 10 job lifecycle summaries")
    p_diag.add_argument("--fills", action="store_true",
                        help="Show fill task performance")
    p_diag.add_argument("--json", dest="json_output", action="store_true",
                        help="JSON output (machine-readable)")
    p_diag.set_defaults(func=cmd_diag)

    # kpi
    p_kpi = sub.add_parser("kpi", help="Task KPI dashboard — health, performance, convergence")
    p_kpi.add_argument("--last", type=int, default=20, help="Analyze last N sessions (default: 20)")
    p_kpi.add_argument("--fill", action="store_true", help="Show fill-specific KPIs and convergence")
    p_kpi.add_argument("--by-prompt", action="store_true",
                       help="Group sessions by prompt type and show evolution over repetitions")
    p_kpi.add_argument("--json", action="store_true", help="JSON output")
    p_kpi.set_defaults(func=cmd_kpi)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
