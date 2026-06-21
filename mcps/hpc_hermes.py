"""MCP server: run_on_hpc — submit prompt to HPC queue, return result.

Usage:
    python3 mcps/hpc_hermes.py        # stdio MCP server (for OpenCode)
    HPC_DB_PATH=/path/to/tasks.db python3 mcps/hpc_hermes.py
"""

import contextlib
import json
import os
import sqlite3
import sys
import time
from mcp.server.fastmcp import FastMCP

# ── Stall-based polling thresholds ──────────────────────────────────────
# Phase 1 — PENDING: max wait for daemon to pick up task.
# Covers vLLM cold start (vllm_ready p90=432s, max=526s) + SLURM queue.
_PENDING_STALL = 600       # 10 min
# Phase 2 — RUNNING: max time for a task to execute.
# Covers task processing (task.complete max=1042s, p99=301s).
_RUNNING_TIMEOUT = 1800    # 30 min
# Absolute safety valve — no connection should last this long.
_HARD_CEILING = 3600       # 1 hour


def _poll(task_id: str, db_path: str, stall_timeout: int = None) -> str:
    if stall_timeout is None:
        stall_timeout = _PENDING_STALL
    """Poll DB until task completes or stalls.

    Two-phase stall detection:
    - PENDING phase: times out if task sits pending >stall_timeout
    - RUNNING phase: times out if task runs >_RUNNING_TIMEOUT

    As long as the task's status keeps changing (progressing),
    the connection stays open. A hard ceiling prevents infinite loops.
    """
    start = time.time()
    last_change = start
    last_status = None

    while time.time() - start < _HARD_CEILING:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            row = conn.execute(
                "SELECT status, result, error FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        finally:
            conn.close()

        if not row:
            time.sleep(0.5)
            continue

        status, result_json, error = row

        # Track progress — reset stall timer on status change
        if status != last_status:
            last_change = time.time()
            print(f"[MCP _poll] task={task_id[:16]}... status={status}",
                  file=sys.stderr, flush=True)
            last_status = status

        if status == "completed":
            try:
                return json.loads(result_json).get("content", "")
            except (json.JSONDecodeError, TypeError):
                return result_json or ""
        elif status == "failed":
            return f"ERROR: {error or 'task failed'}"

        # Choose threshold by current phase
        threshold = stall_timeout if status == "pending" else _RUNNING_TIMEOUT
        stalled_for = time.time() - last_change
        if stalled_for >= threshold:
            return (f"ERROR: task stalled at '{status}' for "
                    f"{stalled_for:.0f}s (threshold={threshold}s)")

        time.sleep(1.0)

    return f"ERROR: hard ceiling reached ({_HARD_CEILING}s)"


def run_on_hpc(prompt: str, stall_timeout: int = None) -> str:
    if stall_timeout is None:
        stall_timeout = _PENDING_STALL
    """Submit a prompt to the HPC queue and wait for the result.

    Uses stall-based polling: waits as long as the task keeps making
    progress. Only times out if the task is stuck (no status change)
    for an extended period.

    Returns the model's text response, or an ERROR: prefixed string.
    """
    import sys
    try:
        from hpc_batch import submit_task

        db_path = _resolve_db()
        print(f"[MCP] Submitting task (prompt={prompt[:60]}..., db={db_path})",
              file=sys.stderr, flush=True)
        tid = submit_task(
            prompt=prompt, db_path=db_path, priority=0, task_type="subagent"
        )
        print(f"[MCP] Task {tid[:16]}... submitted (stall_timeout={stall_timeout}s)",
              file=sys.stderr, flush=True)
        result = _poll(tid, db_path, stall_timeout)
        print(f"[MCP] Task {tid[:16]}... result: {result[:100] if result else 'empty'}",
              file=sys.stderr, flush=True)
        return result
    except Exception as e:
        print(f"[MCP] Error: {e}", file=sys.stderr, flush=True)
        return f"ERROR: {e}"


def _resolve_db() -> str:
    """Resolve tasks.db path."""
    return os.environ.get(
        "HPC_DB_PATH",
        os.path.join(os.path.dirname(__file__), "..", "tasks.db"),
    )


# ── FastMCP Server ────────────────────────────────────────────────────────────

mcp = FastMCP(
    "hpc-hermes",
    description=(
        "Run prompts through HPC vLLM (Qwen2.5-72B) via the Hermes queue. "
        "Submits a task to the local queue and waits for the model's response."
    ),
)


@mcp.tool()
def run_on_hpc_tool(prompt: str) -> str:
    """Run a prompt through HPC vLLM (Qwen2.5-72B) and return the response.

    Uses stall-based polling: waits as long as the task keeps making
    progress. Only times out if the task is stuck (no status change)
    for an extended period (currently 10 min pending, 30 min running).

    Use this INSTEAD of spawning hpc-test subagent. This is the primary
    text generation path.

    For role-specific tasks (code review, research, implementation),
    use run_hpc_agent_task with the appropriate system_prompt instead.
    For extremely long tasks, use submit_hpc_task (fire-and-forget) +
    check_hpc_task_status for manual polling.

    Args:
        prompt: The prompt to send to the model.
    """
    # Redirect stdout → stderr during hpc_batch calls so auto-dispatch
    # noise doesn't corrupt the MCP JSON-RPC protocol stream.
    with contextlib.redirect_stdout(sys.stderr):
        return run_on_hpc(prompt=prompt)


@mcp.tool()
def submit_hpc_task(prompt: str) -> str:
    """Submit a prompt to the HPC queue and return immediately (fire-and-forget).

    The daemon processes tasks asynchronously. Returns the task ID so you
    can check the result later with:
        python3 hpc_batch.py view <task-id>
        python3 hpc_batch.py view <task-id> --json

    Args:
        prompt: The prompt to send to the model.
    """
    import sys
    from hpc_batch import submit_task
    db_path = _resolve_db()
    tid = submit_task(prompt=prompt, db_path=db_path, priority=0, task_type="subagent")
    print(f"[MCP] Submitted {tid[:16]}... (fire-and-forget)", file=sys.stderr, flush=True)
    return tid


@mcp.tool()
def run_hpc_agent_task(
    messages: list[dict],
    system_prompt: str = "",
) -> str:
    """Submit a full conversation to HPC and return the model's response.

    Uses stall-based polling: waits as long as the task keeps making
    progress. Only times out if the task is stuck (no status change)
    for an extended period (currently 10 min pending, 30 min running).

    Accepts the same message format as the OpenAI Chat Completion API:
    [{"role": "system", "content": "..."},
     {"role": "user", "content": "..."}]

    Use for multi-turn conversations and complex prompts. For simple
    string prompts, run_on_hpc_tool(prompt) is more convenient.
    For extremely long tasks, use submit_hpc_task (fire-and-forget) +
    check_hpc_task_status for manual polling.

    Args:
        messages: Full OpenAI message array (list of {role, content} dicts).
        system_prompt: Optional system instruction prepended (overrides
                       any existing system message).
    """
    with contextlib.redirect_stdout(sys.stderr):
        from hpc_batch import submit_task
        db_path = _resolve_db()

        # Prepend system prompt if provided (before any existing messages)
        if system_prompt:
            messages = [{"role": "system", "content": system_prompt}] + messages

        tid = submit_task(
            db_path=db_path,
            messages=messages,
            priority=0,
            task_type="subagent",
        )
        return _poll(tid, db_path)


@mcp.tool()
def check_hpc_task_status(task_id: str) -> dict:
    """Check the status of a previously-submitted HPC task (fire-and-forget).

    Use with submit_hpc_task (which returns a task ID). Poll this tool
    to check if the result is ready.

    Returns:
        {"status": "pending"|"running"|"completed"|"failed"|"unknown"}
        Result is included when status is "completed".
    """
    db_path = _resolve_db()
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status, result, error FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    conn.close()

    if not row:
        return {"status": "unknown", "error": "Task ID not found"}

    result: dict = {"status": row[0]}
    if row[1]:
        try:
            parsed = json.loads(row[1])
            content = parsed.get("content", row[1])
            if content:
                result["result"] = content
        except json.JSONDecodeError:
            result["result"] = row[1]
    if row[2]:
        result["error"] = row[2]
    return result


def main():
    """Entry point — run the stdio MCP server.

    Singleton guard via fcntl.flock at /tmp/hpc-hermes-mcp.lock
    prevents multiple MCP server instances. Stale locks from orphaned
    processes (no PDEATHSIG) are detected and cleaned up.

    PDEATHSIG ensures the MCP server dies when the parent OpenCode
    process exits, preventing orphaned processes.
    """
    # ── PDEATHSIG: die when parent (OpenCode) dies ─────────────────────
    try:
        import ctypes
        _libc = ctypes.CDLL("libc.so.6")
        _libc.prctl(1, 15)  # PR_SET_PDEATHSIG = 1, SIGTERM = 15
    except Exception:
        pass  # Non-critical

    # ── Singleton guard with stale lock detection ──────────────────────
    _lock_path = "/tmp/hpc-hermes-mcp.lock"
    try:
        import fcntl
        _fd = os.open(_lock_path, os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            # Lock is held — check if holder is alive
            try:
                _pid_data = os.read(_fd, 32)
                _old_pid = int(_pid_data.strip()) if _pid_data.strip() else None
                if _old_pid:
                    # Check if process is alive (kill 0 = test only)
                    os.kill(_old_pid, 0)
                    # Process is alive — exit gracefully
                    print(f"[MCP] Another MCP server (PID {_old_pid}) is running — exiting",
                          file=sys.stderr, flush=True)
                    sys.exit(0)
            except (OSError, ValueError):
                print(f"[MCP] Stale lock detected (PID {_old_pid}) — removing and re-acquiring",
                      file=sys.stderr, flush=True)
            # Stale lock: close old fd, remove, retry
            try:
                os.close(_fd)
                os.unlink(_lock_path)
            except Exception:
                pass
            _fd = os.open(_lock_path, os.O_RDWR | os.O_CREAT)
            fcntl.flock(_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Write our PID to the lock file so stale lock detection works
        os.write(_fd, str(os.getpid()).encode())
        os.fsync(_fd)
    except ImportError:
        pass  # No fcntl — run without guard

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
