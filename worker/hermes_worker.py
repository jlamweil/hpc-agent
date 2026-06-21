"""Hermes Worker — inline Python Worker loop for dsv4_fast.slurm.

Runs alongside vLLM in the same SLURM job, dequeues tasks from hpc_batch.py DB,
processes them through HermesRunner, monitors idle time, and pushes fill tasks
when the GPU would otherwise sit idle.
"""
import json
import os
import sys
import time
import sqlite3
import subprocess
from pathlib import Path

# ── Structured event log (shared by HermesWorker and standalone functions) ──
_STATS_FILE = "/tmp/hpc-daemon-stats.ndjson"

def log_event(event: str, mode: str = "unknown", db_path: str | None = None, **kwargs):
    """Log a structured event to the task_events DB table."""
    import hpc_batch
    hpc_batch.log_db_event(event, db_path=db_path, mode=mode, **kwargs)

from hermes_runtime.task_queue import LLMTask, TaskStatus
from worker.model_runner import HermesRunner

_HPC_LAST_KILLED = 0.0  # Rate-limit: at most once per 60s
_HPC_KILL_COOLDOWN = 60.0


# ── Standalone functions (testable seams) ─────────────────────────────────────

def get_pending_real_count(db_path: str) -> int:
    """Count pending tasks with priority >= 0 (real, not fill tasks)."""
    import hpc_batch
    conn = hpc_batch.init_db(db_path)
    row = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND priority >= 0"
    ).fetchone()
    conn.close()
    return row[0]


def process_next_task(db_path: str, runner: HermesRunner | None = None,
                      min_priority: int | None = None,
                      random_order: bool = False,
                      preempt_check=None,
                      job_id: str | None = None) -> bool:
    # preempt_check is an optional callable: () -> bool.
    # Used by P6-A priority preemption. Not type-annotated to avoid
    # import issues across Python versions.
    """Claim and process one pending task.

    Args:
        db_path: Path to SQLite database.
        runner: HermesRunner (or MockRunner for testing).
        min_priority: If set, only dequeue tasks with priority >= this value.
                       Use min_priority=0 to skip fill tasks.
        random_order: If True, pick randomly among matching tasks (for fill
                       tasks) instead of priority-FIFO ordering.

    Returns True if a task was processed, False if queue is empty.
    """
    import hpc_batch
    conn = hpc_batch.init_db(db_path)
    where = "status = 'pending'"
    if min_priority is not None:
        where += f" AND priority >= {min_priority}"
    order_clause = "RANDOM()" if random_order else "priority DESC, created_at ASC"
    row = conn.execute(f"""
        SELECT id, messages, priority, task_type, metadata
        FROM tasks
        WHERE {where}
        ORDER BY {order_clause} LIMIT 1
    """).fetchone()

    if not row:
        conn.close()
        return False

    task_id, messages_json, priority, task_type, metadata_json = row

    # Claim the task (atomic update) with retry on locked DB
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            conn.execute(
                "UPDATE tasks SET status = 'running', started_at = ? WHERE id = ?",
                (time.time(), task_id),
            )
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                conn.close()
                conn = hpc_batch.init_db(db_path)
            else:
                raise
    conn.close()

    # Build a short preview for logging (first line of user message)
    try:
        msgs = json.loads(messages_json)
        first_user = next((m.get("content", "") for m in msgs if m.get("role") == "user"), "")
        preview = first_user.strip().split("\n")[0][:80]
    except Exception:
        preview = "(unknown)"

    started_at = time.time()
    print(f"[PROCESS] claim task={task_id[:16]}... priority={priority} type={task_type} preview={preview}",
          file=sys.stderr, flush=True)
    log_event("task.claim", task_id=task_id[:16], priority=priority, task_type=task_type,
              job_id=job_id, db_path=db_path)

    # Process
    _runner = runner or HermesRunner()
    try:
        messages = json.loads(messages_json) if not msgs else msgs
        try:
            task_metadata = json.loads(metadata_json) if metadata_json else {}
        except (json.JSONDecodeError, TypeError):
            task_metadata = {}
        # P7+P8: Extract saved conversation from metadata for retry continuity.
        # saved_conversation is set by HermesRunner on timeout; stripped from
        # metadata before passing to LLMTask to avoid leaking into DB on success.
        saved_conversation = task_metadata.pop('saved_conversation', None)
        if saved_conversation:
            print(f"[PROCESS] task={task_id[:16]}... restoring {len(saved_conversation)} messages from saved conversation",
                  file=sys.stderr, flush=True)
            # If last message is a tool result (orphaned session), nudge the
            # agent to complete its response instead of restarting the task.
            if saved_conversation[-1].get('role') == 'tool':
                # Replace the prompt with a completion nudge so the agent
                # reviews the history and produces the missing final response.
                messages = [{"role": "user", "content":
                    "[The previous conversation was interrupted right after a tool call completed. "
                    "Review the full conversation above and complete your response. "
                    "Do NOT restart from scratch — just respond to the last tool result.]"
                }]
                print(f"[PROCESS] task={task_id[:16]}... orphaned session — injected completion nudge",
                      file=sys.stderr, flush=True)
        task = LLMTask(
            task_id=task_id,
            agent_id="hermes-worker",
            messages=messages,
            tools=[],
            model=task_metadata.get('model', getattr(_runner, 'model', 'unknown')),
            api_config={},
            metadata=task_metadata,
            priority=priority,
        )
        result = _runner.run(task, conversation_history=saved_conversation, preempt_check=preempt_check, db_path=db_path)
        result_content = json.dumps({"content": result.content})
        result_status = "completed" if result.status == TaskStatus.COMPLETED else "failed"
        result_error = result.error if hasattr(result, 'error') and result.error else None
    except Exception as e:
        result_content = json.dumps({"content": None})
        result_status = "failed"
        result_error = str(e)

    elapsed = time.time() - started_at
    print(f"[PROCESS] done task={task_id[:16]}... status={result_status} elapsed={elapsed:.1f}s error={result_error or 'none'}",
          file=sys.stderr, flush=True)
    # Log structured event with failure reason classification
    fail_reason = None
    if result_error:
        if "Preempted" in result_error:
            fail_reason = "preempted"
        elif "timeout" in result_error.lower():
            fail_reason = "timeout"
        else:
            fail_reason = "error"
    log_event("task.complete", task_id=task_id[:16], priority=priority,
             status=result_status, duration=round(elapsed, 1),
             fail_reason=fail_reason, job_id=job_id, db_path=db_path)

    # Store result with retry on locked DB
    conn = hpc_batch.init_db(db_path)
    for attempt in range(MAX_RETRIES):
        try:
            if result_error:
                conn.execute(
                    "UPDATE tasks SET status = ?, result = ?, error = ?, completed_at = ? WHERE id = ?",
                    (result_status, result_content, result_error, time.time(), task_id),
                )
            else:
                conn.execute(
                    "UPDATE tasks SET status = ?, result = ?, completed_at = ? WHERE id = ?",
                    (result_status, result_content, time.time(), task_id),
                )
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                conn.close()
                conn = hpc_batch.init_db(db_path)
            else:
                raise
    conn.close()
    return True


# ── vLLM Progress Parser (standalone, testable) ───────────────────────────────

def _parse_vllm_progress(log_text: str | None) -> dict:
    """Parse vLLM startup log text and return progress information.

    Args:
        log_text: Raw vLLM log content, or None if log file doesn't exist yet.

    Returns:
        dict with keys:
            progress (int): 0-100 estimated progress percentage
            stage (str): current stage name
            stages (dict): all detected stages with their line context
            details (dict): stage-specific details (shard_pct, etc.)
            stalled (bool): whether log appears stalled (default False;
                            caller should compare hashes across calls)
    """
    import re
    result = {
        "progress": 0,
        "stage": "unknown",
        "stages": {},
        "details": {},
        "stalled": False,
    }
    if not log_text:
        return result

    lines = log_text.split("\n")
    last_line = ""
    for line in lines:
        stripped = line.strip()
        if stripped:
            last_line = stripped

        # Check stage markers in priority order (most advanced first)
        if re.search(r"Application startup complete", line):
            result["stages"]["app_ready"] = line[:80]
        elif re.search(r"Started server process", line):
            result["stages"]["server_process"] = line[:80]
        elif re.search(r"Starting vLLM server on http", line):
            result["stages"]["server_start"] = line[:80]
        elif re.search(r"shm_broadcast", line):
            result["stages"]["engine_init"] = line[:80]
        elif re.search(r"torch\.compile took", line):
            result["stages"]["torch_compile"] = line[:80]
        elif re.search(r"Loading weights took", line):
            result["stages"]["weights_loaded"] = line[:80]
        elif re.search(r"Loading safetensors checkpoint shards:\s+(\d+)%", line):
            m = re.search(r"Loading safetensors checkpoint shards:\s+(\d+)%", line)
            result["stages"]["shard_loading"] = line[:80]
            result["details"]["shard_pct"] = int(m.group(1))

    # Map latest stage to progress percentage
    stage_order = [
        ("app_ready", 100), ("server_process", 95), ("server_start", 90),
        ("engine_init", 35), ("torch_compile", 30), ("weights_loaded", 25),
        ("shard_loading", 10),
    ]
    best_progress = 0
    best_stage = "unknown"
    for stage, pct in stage_order:
        if stage in result["stages"]:
            if pct >= best_progress:
                best_progress = pct
                best_stage = stage
    result["progress"] = best_progress
    result["stage"] = best_stage
    return result


# ── Worker Class ──────────────────────────────────────────────────────────────

class HermesWorker:
    """Hermes Worker loop — dequeue, process, idle monitor, fill dispatch."""

    _PID_FILE = "/tmp/hermes-worker.pid"
    _HEARTBEAT_FILE = "/tmp/hermes-worker.heartbeat"
    _STATS_FILE = "/tmp/hpc-daemon-stats.ndjson"
    _STATE_FILE = "/tmp/hermes-worker.state"  # Persisted state for crash recovery
    _STARTED_AT = time.time()  # Daemon process start time — used to detect stale tasks from previous runs

    def __init__(
        self,
        db_path: str = "tasks.db",
        poll_interval: float = 60.0,
        idle_before_fill: int = 3,
        idle_timeout: float = 600.0,
        shutdown_wait_max: float = 300.0,
        runner=None,
        fill_timeout: float = 300.0,
        stale_threshold: float = 600.0,
        mode: str = "normal",
    ):
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.idle_before_fill = idle_before_fill
        self.idle_timeout = idle_timeout
        self.shutdown_wait_max = shutdown_wait_max
        self.idle_cycles = 0
        self.fill_in_flight = False
        self.fill_timeout = fill_timeout
        self.stale_threshold = stale_threshold
        self.runner = runner or HermesRunner()
        self._mode = mode
        self._lock_fd = None  # Set by _acquire_singleton_lock()
        self._last_real_time = time.time()  # When last real task was processed
        self._hpc_idle_timeout = 300.0  # Kill HPC job after 5 min no real work
        self._hpc_job_seen_at = 0.0  # When we first detected current HPC job
        self._monitoring_vllm = False  # D3: vLLM startup monitoring state
        self._monitor_started_at = 0.0
        self._vllm_stall_count = 0     # Consecutive stalled log reads
        self._vllm_last_log_hash = ""  # MD5 of last vLLM log read
        self._fill_thread_ref: threading.Thread | None = None  # MCP Wait: running fill thread
        self._fill_yielded_id: str | None = None  # Fill task ID that yielded (for cleanup)
        # ── Circuit breaker (D2) ─────────────────────────────────────────
        self._breaker_state = "CLOSED"  # CLOSED | OPEN | HALF_OPEN
        self._breaker_failures = 0
        self._breaker_max_failures = 3       # Consecutive failures before OPEN
        self._breaker_backoff = 30.0          # Current backoff (doubles each time)
        self._breaker_backoff_max = 300.0     # Max backoff (5 min)
        self._breaker_next_retry = 0.0        # Earliest time for HALF_OPEN probe
        self._breaker_consecutive_successes = 0
        self._breaker_reset_threshold = 5     # Consecutive successes to reset backoff
        # Track last-picked fill task ID so we cycle through them
        # (fill tasks stay pending forever — ORDER BY priority DESC, created_at ASC
        # sorts freshest fills first, and last-pick exclusion prevents starvation).
        self._last_fill_task_id = None
        # ── Convergence tracking for fill auto-rotation (D3) ─────────────
        # When a fill converges (output is too similar run-to-run), its priority
        # in the DB is decremented (e.g., -1 → -2 → -3), making it sink below
        # fresher fills in the ORDER BY priority DESC query. This ensures:
        #   (a) non-converged fills are always picked first
        #   (b) converged fills still get re-tested after fresher fills cycle
        #   (c) there is NEVER an idle dead end — some fill is always chosen
        self._fill_output_history: dict[str, list[str]] = {}  # fill_id → last N output signatures
        self._FILL_CONVERGENCE_THRESHOLD = 0.70     # >70% similarity = converged
        self._FILL_CONVERGENCE_WINDOW = 5           # look at last 5 outputs
        self._FILL_HISTORY_MAX = 10                 # keep max 10 outputs per fill
        # Convergence sentinel — written when ANY fill converges so it's LOUD
        self._FILL_CONVERGENCE_SENTINEL = "/tmp/hpc-fill-converged"
        # Note: both real tasks (priority>=0) and fill tasks (priority<0)
        # use self.runner (AIAgent) — all tasks need tool execution.

    def get_pending_real_count(self) -> int:
        return get_pending_real_count(self.db_path)

    # ── Dynamic workload detection (replaces fixed daemon mode) ────────
    # The daemon no longer has a single "mode" (normal vs keepwarm).
    # Instead it dynamically chooses QOS based on what's pending:
    #   - Real tasks (priority >= 0) → normal QOS (stable, user-facing)
    #   - Only fill tasks (priority < 0) → preemptqos (cheaper, preemptible)
    #   - Nothing pending → no dispatch, kill HPC on idle timeout
    # This eliminates the deadlock where fill tasks exist but no HPC job.

    def _any_pending(self) -> bool:
        """True if any task (real or fill) is waiting. Uses cached cycle counts."""
        return self._cycle_real > 0 or self._cycle_fill > 0

    def _only_fills_pending(self) -> bool:
        """True if fill tasks exist but NO real tasks. Uses cached cycle counts."""
        return self._cycle_fill > 0 and self._cycle_real == 0

    def _active_qos(self) -> str | None:
        """Choose SLURM QOS based on current workload.
        
        Returns:
            "preemptqos" — only fill tasks pending (cheaper, preemptible)
            None — real tasks pending or nothing pending (default/normal QOS)
        """
        if self._only_fills_pending():
            return "preemptqos"
        return None

    # ── Per-cycle cached counts (set at start of each _idle_cycle) ─────
    # Redundant DB connections are expensive in tests. Cache the counts
    # for the current cycle so callers don't re-query the DB.
    _cycle_real: int = 0
    _cycle_fill: int = 0
    _cycle_job_id: str | None = None

    def _active_qos_label(self) -> str:
        """Human-readable QOS label for logging. Uses cached cycle counts."""
        if self._cycle_real > 0:
            return "qos=normal"
        if self._cycle_fill > 0:
            return "qos=preemptqos"
        return "qos=none (idle)"

    def _log_event(self, event: str, **kwargs):
        """Log a structured event to the task_events DB table with mode context.
        Events are queryable via: hpc_batch.py events [--event TYPE] [--mode MODE]
        """
        import hpc_batch
        hpc_batch.log_db_event(event, db_path=self.db_path, mode=self._active_qos_label(), **kwargs)

    def _log(self, msg: str):
        """Log with workload-aware prefix for grep-ability."""
        print(f"[HermesWorker/{self._active_qos_label()}] {msg}", file=sys.stderr, flush=True)

    def _check_task_ready_sentinel(self) -> bool:
        """Check if a new task sentinel exists (fast wakeup signal)."""
        sentinel = "/tmp/.hpc-task-ready"
        if os.path.exists(sentinel):
            try:
                os.unlink(sentinel)
            except OSError:
                pass
            return True
        return False

    MAX_TASK_RETRIES = 3  # Max retries for tasks WITHOUT saved context (real errors)

    def _recover_recoverable_failed(self, job_id: str | None = None) -> int:
        """Reset failed tasks that have saved_conversation in metadata back to pending.

        P7+P8: When a task times out or is preempted, HermesRunner saves the
        session messages to the task's metadata. This method finds those tasks
        and resets them to pending so they can retry with context restored.

        Preemption and timeout are EXTERNAL interruptions (HPC job death,
        higher-priority task arrival) — they don't count against the task.
        Tasks with saved_conversation are always retried regardless of
        retry_count. Only tasks without saved_conversation (genuine errors
        like invalid prompts or tool failures) hit the retry limit.

        Returns count of tasks recovered.
        """
        import json, time
        import hpc_batch
        conn = hpc_batch.init_db(self.db_path)
        rows = conn.execute("""
            SELECT id, metadata, retry_count FROM tasks
            WHERE status = 'failed'
        """).fetchall()
        recovered = 0
        now = time.time()
        for task_id, meta_json, retry_count in rows:
            try:
                meta = json.loads(meta_json) if meta_json else {}
                if 'saved_conversation' in meta and isinstance(meta['saved_conversation'], list) and len(meta['saved_conversation']) > 0:
                    # External interruption (preempt/timeout) — always retry, no limit
                    conn.execute(
                        "UPDATE tasks SET status = 'pending', started_at = NULL, "
                        "error = 'Retrying with saved conversation' WHERE id = ? AND status = 'failed'",
                        (task_id,),
                    )
                    conn.commit()  # Commit before _log_event to release lock
                    recovered += 1
                    self._log_event("task.recover", task_id=task_id[:16],
                                   retry_count=retry_count or 0,
                                   saved_msgs=len(meta['saved_conversation']),
                                   job_id=job_id)
                elif (retry_count or 0) < self.MAX_TASK_RETRIES:
                    # Genuine error (no saved context) — limited retries
                    conn.execute(
                        "UPDATE tasks SET status = 'pending', retry_count = ?, started_at = NULL, "
                        "error = 'Retrying (genuine error)' WHERE id = ? AND status = 'failed'",
                        ((retry_count or 0) + 1, task_id),
                    )
                    conn.commit()
                    recovered += 1
                else:
                    # Genuine error exhausted retries — flag once (check _genuine_error)
                    if not meta.get('_genuine_error'):
                        meta['_genuine_error'] = True
                        conn.execute(
                            "UPDATE tasks SET metadata = ? WHERE id = ?",
                            (json.dumps(meta), task_id),
                        )
                        conn.commit()
                        self._log_event("task.fail.permanent", task_id=task_id[:16],
                                       retry_count=retry_count,
                                       job_id=job_id)
            except (json.JSONDecodeError, TypeError):
                continue
        conn.close()
        if recovered > 0:
            print(f"[HermesWorker] Recovered {recovered} failed task(s) with saved conversation",
                  file=sys.stderr, flush=True)
        return recovered

    def _find_recent_session(self, started_at: float | None) -> list | None:
        """Find a session file that started near the given timestamp.

        Scans the 5 most recent session files in ~/.hermes/sessions/
        and returns messages from the one whose mtime is within 120s
        of started_at. Returns None if no match found.
        """
        if started_at is None:
            return None
        import json
        session_dir = Path.home() / ".hermes" / "sessions"
        if not session_dir.is_dir():
            return None
        sessions = sorted(session_dir.glob("session_*.json"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        for sp in sessions[:5]:
            try:
                mtime = sp.stat().st_mtime
                if abs(mtime - started_at) < 120:
                    sdata = json.loads(sp.read_text())
                    msgs = sdata.get("messages", [])
                    if msgs and len(msgs) > 1:
                        return msgs
            except (json.JSONDecodeError, OSError):
                continue
        return None

    def _recover_stuck_tasks(self, stale_threshold: float | None = None,
                             job_id: str | None = None) -> int:
        """Reset tasks stuck in 'running' state — tasks started by a previous
        daemon instance (before _STARTED_AT).

        Also attempts to recover session files for tasks whose daemon died
        abruptly (SIGTERM) — scans ~/.hermes/sessions/ for files newer than
        the task's started_at and saves messages as saved_conversation.

        Note: stale_threshold parameter is accepted for backward compatibility
        but NOT used. The real check is _STARTED_AT — tasks started before the
        current daemon booted belong to a previous instance and are stale.

        Called on daemon startup and during idle cycles. Returns count
        of tasks recovered.
        """
        import hpc_batch
        import json
        conn = hpc_batch.init_db(self.db_path)
        count = 0

        # Find stale running tasks — tasks started by a previous daemon
        # instance (before this daemon's _STARTED_AT).
        # The old PID check was wrong: after restart, the new daemon has a
        # different PID but the PID file is overwritten, so the check always
        # sees the current PID as alive and never recovers stale tasks.
        rows = conn.execute("""
            SELECT id, started_at, metadata FROM tasks
            WHERE status='running' AND started_at IS NOT NULL AND started_at < ?
        """, (self._STARTED_AT,)).fetchall()

        for task_id, started_at, meta_json in rows:
            # Try to find a session file that started after this task
            saved_msgs = self._find_recent_session(started_at)
            if saved_msgs:
                meta = json.loads(meta_json) if meta_json else {}
                meta['saved_conversation'] = saved_msgs
                conn.execute(
                    "UPDATE tasks SET status='pending', started_at=NULL, "
                    "metadata=?, error='Recovered from stuck running (session restored)' "
                    "WHERE id=? AND status='running'",
                    (json.dumps(meta), task_id),
                )
                conn.commit()  # Release lock before _log_event
                self._log_event("task.recover", task_id=task_id[:16],
                               saved_msgs=len(saved_msgs), reason="sigterm_session_restore",
                               job_id=job_id)
            else:
                conn.execute(
                    "UPDATE tasks SET status='pending', started_at=NULL, "
                    "error='Recovered from stuck running state (daemon auto-recovery)' "
                    "WHERE id=? AND status='running'",
                    (task_id,),
                )
                conn.commit()  # Release lock before _log_event
            count += 1
            self._log_event("task.recover", task_id=task_id[:16],
                           saved_msgs=len(saved_msgs) if saved_msgs else 0,
                           reason="stuck_task_sigterm",
                           job_id=job_id)

        # Category 2: started_at IS NULL but created_at is very old
        # (tasks left in running state from a crashed daemon that never set started_at)
        cur = conn.execute(
            "UPDATE tasks SET status='pending', started_at=NULL, "
            "error='Recovered from stuck running state (NULL started_at, daemon auto-recovery)' "
            "WHERE status='running' AND started_at IS NULL AND created_at < ?",
            (self._STARTED_AT,)
        )
        count += cur.rowcount

        conn.commit()
        conn.close()
        if count > 0:
            print(f"[HermesWorker] Recovered {count} stuck task(s)",
                  file=sys.stderr, flush=True)
        return count

    def _process_next(self) -> bool:
        """Process next real task (priority >= 0) via AIAgent with tools.

        Uses the same HermesRunner (AIAgent) as fill tasks — all tasks
        need tool execution (read_file, write, bash, etc.).

        Checks for higher-priority tasks (priority >= 5) waiting longer
        than 30s before processing a lower-priority task (D6).

        Also installs a preempt_check callback (P6-A) that polls every 10s
        during task execution. If a higher-priority task arrives while a
        low-priority task is running, the running task is preempted, its
        session saved (P7+P8), and the higher-priority task runs next.
        """
        _priority_threshold = 5
        _age_threshold = 30  # seconds

        # Capture current HPC job_id for task event correlation
        _current_job_id: str | None = None
        if hasattr(self, '_has_hpc_job'):
            try:
                _has, _jid = self._has_hpc_job()
                if _has and _jid:
                    _current_job_id = _jid
            except Exception:
                pass

        # D6: priority reorder before starting
        def _check_higher_priority() -> bool:
            import sqlite3
            try:
                _c = sqlite3.connect(self.db_path)
                _row = _c.execute(
                    "SELECT id FROM tasks WHERE status = 'pending' AND priority >= ? "
                    "AND created_at < ? ORDER BY priority DESC, created_at ASC LIMIT 1",
                    (_priority_threshold, time.time() - _age_threshold)
                ).fetchone()
                _c.close()
                return _row is not None
            except Exception:
                return False

        if _check_higher_priority():
            return process_next_task(self.db_path, self.runner,
                                     min_priority=_priority_threshold,
                                     job_id=_current_job_id)

        # Install preempt_check for the running task (P6-A).
        # Only preempt if a higher-priority task has been waiting > age_threshold.
        # Pass db_path via closure so it uses the HermesWorker's path.
        _db_path = self.db_path
        def _preempt_check() -> bool:
            return _check_higher_priority()

        return process_next_task(self.db_path, self.runner, min_priority=0,
                                preempt_check=_preempt_check,
                                job_id=_current_job_id)

    def _push_fill_task(self):
        """Submit a fill task via hpc_batch.py CLI."""
        _h = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        hpc_batch_path = os.path.join(_h, "hpc_batch.py")
        # --db must come BEFORE the subcommand (argparse global arg)
        subprocess.run(
            [sys.executable, hpc_batch_path, "--db", self.db_path, "submit-fill"],
            capture_output=True,
            timeout=30,
        )

    def _has_pending_fill(self) -> bool:
        """Check if any fill tasks exist in the queue."""
        import hpc_batch
        conn = hpc_batch.init_db(self.db_path)
        row = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND priority < 0"
        ).fetchone()
        conn.close()
        return row[0] > 0

    @staticmethod
    def _is_orphaned_fill(task_id: str, messages: list) -> bool:
        """Check if a fill task's source prompt file still exists on disk.

        Fill tasks seeded via hpc_batch submit-fill record their source
        file path in metadata. If that file was deleted, the task is
        orphaned.

        Content comparison is normalized to handle {TARGET_REPO} and
        {TARGET_REPO_DIR} substitution — the DB stores the resolved form
        (e.g. "/home/lam/frontier-arbiter") while the prompt files on disk
        have the template form ("{TARGET_REPO_DIR}").
        """
        import hpc_batch as _hpc
        repo = getattr(_hpc, 'HPC_AGENT_DIR', None)
        if not repo:
            return False
        fill_dir = repo / "prompts" / "fill"
        if not fill_dir.is_dir():
            return False

        # Find the first user message content
        content = ""
        for msg in messages:
            if msg.get("role") == "user":
                content = (msg.get("content") or "").strip()
                break
        if not content:
            return False

        # Check if any active .txt file matches (with substitution tolerance)
        for f in fill_dir.iterdir():
            if f.suffix == ".txt" and not f.name.startswith("_"):
                file_text = f.read_text().strip()
                # Exact match (no substitution in prompt)
                if content == file_text:
                    return False
                # Prefix match: first 80 chars should be identical.
                # Handles prompts where only {TARGET_REPO_DIR} differs later.
                if len(content) > 80 and len(file_text) > 80:
                    if content[:80] == file_text[:80]:
                        return False
                # Content contains the file text with placeholders resolved:
                # Check if removing known repo paths reveals the template.
                # E.g., "/home/lam/frontier-arbiter" → "{TARGET_REPO_DIR}"
                normalized = content
                # Try all configured repos and their resolved paths
                repos = _hpc.cfg("fill_tasks.target_repos", [])
                base = _hpc.cfg("fill_tasks.base_repo_dir", "~/Documents/GAIA")
                overrides = _hpc.cfg("fill_tasks.repo_paths", {})
                for r in repos:
                    rd = _hpc.os.path.expanduser(overrides.get(r, _hpc.os.path.join(
                        _hpc.os.path.expanduser(base), r)))
                    normalized = normalized.replace(rd, "{TARGET_REPO_DIR}")
                    normalized = normalized.replace(r, "{TARGET_REPO}")
                if normalized == file_text:
                    return False
        # No match found — orphaned only if the content looks like
        # a seeded prompt (multi-line, task-oriented). Short test
        # prompts like "say hi" pass through.
        return len(content) > 200  # heuristic: seeded prompts are long

    def _process_random_fill(self) -> bool:
        """Pick a pending fill task in FIFO order and process it.

        The DB row is NEVER modified — the fill task stays pending
        permanently. The worker just reads the prompt and runs it.
        Returns True if a fill was processed.

        Cycles through fill tasks by excluding the last-picked ID,
        so no single fill task starves newer ones.

        Fill tasks have a hard timeout (fill_timeout) to prevent
        long-running AIAgent conversations from blocking real tasks.
        """
        import hpc_batch
        import concurrent.futures, threading

        # ── Check if a previous fill thread is still running ────────────
        # If the fill yielded (MCP Wait), the thread is alive but blocked.
        # Don't start a new fill until it finishes. Check if it's done now.
        if self._fill_thread_ref is not None:
            if self._fill_thread_ref.is_alive():
                return True  # Still running — skip this cycle
            # Thread finished — clean up. Fill stays pending for re-use.
            self._fill_thread_ref = None

        # Get current HPC job_id for event correlation
        _fill_job_id = ""
        try:
            _has, _jid = self._has_hpc_job()
            if _has and _jid:
                _fill_job_id = _jid
        except Exception:
            pass

        conn = hpc_batch.init_db(self.db_path)
        # Exclude the last-picked fill task so we cycle through them.
        # Order by priority DESC so non-converged fills (higher priority)
        # are picked before converged fills (which degrade their priority).
        if self._last_fill_task_id:
            row = conn.execute("""
                SELECT id, messages, priority FROM tasks
                WHERE status = 'pending' AND priority < 0 AND id != ?
                ORDER BY priority DESC, created_at ASC LIMIT 1
            """, (self._last_fill_task_id,)).fetchone()
        else:
            row = conn.execute("""
                SELECT id, messages, priority FROM tasks
                WHERE status = 'pending' AND priority < 0
                ORDER BY priority DESC, created_at ASC LIMIT 1
            """).fetchone()
        # If only one fill task exists, fall back to picking it
        if not row and self._last_fill_task_id:
            row = conn.execute("""
                SELECT id, messages, priority FROM tasks
                WHERE status = 'pending' AND priority < 0
                ORDER BY priority DESC, created_at ASC LIMIT 1
            """).fetchone()
        if not row:
            conn.close()
            return False

        self._last_fill_task_id = row[0]
        messages = json.loads(row[1])
        fill_priority = row[2]
        conn.close()

        self._log_event("fill.claim", fill_id=row[0][:16], priority=fill_priority, job_id=_fill_job_id)

        # Check if this fill task is orphaned (source file was deleted).
        # Orphaned fills are removed from the queue and skipped.
        if self._is_orphaned_fill(row[0], messages):
            import hpc_batch as _hpc
            _conn = _hpc.init_db(self.db_path)
            _conn.execute("DELETE FROM tasks WHERE id = ?", (row[0],))
            _conn.commit()
            _conn.close()
            print(f"[HermesWorker] Removed orphaned fill task {row[0][:16]}...",
                  file=sys.stderr, flush=True)
            return True  # Report work done so daemon cycles to the next fill

        # Process the prompt — no DB updates (fill task stays pending)
        # Wrap in timeout so fill tasks can't block the daemon
        from hermes_runtime.task_queue import LLMTask, TaskResult
        task = LLMTask(
            task_id=row[0], agent_id="hermes-worker",
            messages=messages, tools=[], model=getattr(self.runner, 'model', 'unknown'),
            api_config={}, metadata={}, priority=fill_priority,
        )

        fill_timeout = getattr(self, 'fill_timeout', 300)  # 5 min max per fill task
        result_container = []

        def _do_fill():
            try:
                r = self.runner.run(task, fill_task_id=row[0])
                result_container.append(r)
            except Exception as e:
                result_container.append(e)

        t = threading.Thread(target=_do_fill, daemon=True)
        t.start()
        self._fill_thread_ref = t

        # While fill task is running, check every 5s for new real tasks.
        # If a real task appears (MCP task created by fill), the fill
        # thread is blocked on an Event inside submit_task(). The daemon
        # returns to process the MCP task, signals the Event, and the
        # fill thread unblocks and continues.
        # P6-A: also check for higher-priority tasks (priority >= 5
        # waiting > 30s) to preempt low-priority fill work.
        remaining = fill_timeout
        while remaining > 0:
            t.join(timeout=min(5.0, remaining))
            remaining -= 5.0
            if not t.is_alive():
                self._fill_thread_ref = None
                break  # Fill completed
            # Check for MCP tasks (from fill's own tool calls)
            if self.get_pending_real_count() > 0:
                print(f"[HermesWorker] MCP task detected, fill {row[0][:16]}... yielded",
                      file=sys.stderr, flush=True)
                self._log_event("fill.yield", reason="mcp_task", fill_id=row[0][:16], job_id=_fill_job_id)
                self._fill_yielded_id = row[0]
                return True
            # P6-A: check for higher-priority real tasks that arrived
            # from outside (orchestrator, not fill-generated MCP).
            # Only check periodically (every ~30s) to avoid thrashing.
            if remaining % 30 < 5:
                try:
                    import sqlite3
                    _c = sqlite3.connect(self.db_path)
                    _hi = _c.execute(
                        "SELECT id FROM tasks WHERE status='pending' AND priority >= 5 "
                        "AND created_at < ? LIMIT 1",
                        (time.time() - 30,)
                    ).fetchone()
                    _c.close()
                    if _hi:
                        print(f"[HermesWorker] Higher-priority task detected, yielding fill {row[0][:16]}...",
                              file=sys.stderr, flush=True)
                        self._log_event("fill.yield", reason="higher_priority", fill_id=row[0][:16], job_id=_fill_job_id)
                        self._fill_yielded_id = row[0]
                        return True
                except Exception:
                    pass

        if t.is_alive():
            print(f"[HermesWorker] Fill task {row[0][:16]}... timed out after {fill_timeout}s — marked done",
                  file=sys.stderr, flush=True)
            self._log_event("fill.complete", fill_id=row[0][:16], status="timeout",
                           duration=fill_timeout, job_id=_fill_job_id)
            # Fill extends idle timer so HPC stays alive for next fill
            self._last_real_time = time.time()
            return True
        if result_container:
            r = result_container[0]
            if isinstance(r, TaskResult):
                print(f"[HermesWorker] Fill task {row[0][:16]}... status={r.status} — marked done",
                      file=sys.stderr, flush=True)
            elif isinstance(r, Exception):
                print(f"[HermesWorker] Fill task {row[0][:16]}... exception: {r} — marked done",
                      file=sys.stderr, flush=True)
        self._log_event("fill.complete", fill_id=row[0][:16],
                       status=isinstance(r, TaskResult) and r.status.value or "exception",
                       job_id=_fill_job_id)
        # Fill extends idle timer so HPC stays alive for next fill
        self._last_real_time = time.time()

        # ── Convergence detection: compare output to previous runs ──────
        try:
            out_text = ""
            if isinstance(r, TaskResult) and r.content:
                out_text = r.content if isinstance(r.content, str) else str(r.content)
            elif isinstance(r, Exception):
                out_text = str(r)
            if out_text:
                history = self._fill_output_history.setdefault(row[0], [])
                history.append(out_text)
                if len(history) > self._FILL_HISTORY_MAX:
                    history.pop(0)  # Trim to max
                # Compute Jaccard similarity with last output
                if len(history) >= 2:
                    recent = history[-self._FILL_CONVERGENCE_WINDOW:]
                    similarities = []
                    for i in range(len(recent) - 1):
                        a = set(w for w in recent[i].lower().split() if len(w) > 3)
                        b = set(w for w in recent[i+1].lower().split() if len(w) > 3)
                        if a and b:
                            similarities.append(len(a & b) / max(len(a | b), 1))
                    if similarities:
                        avg_sim = sum(similarities) / len(similarities)
                        if avg_sim > self._FILL_CONVERGENCE_THRESHOLD:
                            # ── LOUD CONVERGENCE ALERT ─────────────────
                            new_priority = fill_priority - 1
                            # Persist priority decay to DB so this fill sinks
                            try:
                                _c = hpc_batch.init_db(self.db_path)
                                _c.execute(
                                    "UPDATE tasks SET priority = ? WHERE id = ?",
                                    (new_priority, row[0]),
                                )
                                _c.commit()
                                _c.close()
                            except Exception:
                                pass
                            # Stderr: all-caps, prominent, unmistakable
                            print(
                                f"\n"
                                f"═══ ⚠️  FILL CONVERGED ⚠️ ═══\n"
                                f"  Fill:        {row[0][:16]}...\n"
                                f"  Similarity:  {avg_sim:.1%}\n"
                                f"  Priority:    {fill_priority} → {new_priority}\n"
                                f"  Next pick:   higher-priority fills first\n"
                                f"═══════════════════════════════════\n",
                                file=sys.stderr, flush=True,
                            )
                            self._log_event("fill.converge", fill_id=row[0][:16],
                                           reason=f"sim={avg_sim:.2f}",
                                           priority=new_priority)
                            # Write convergence sentinel file (persistent alert)
                            try:
                                import json as _json
                                _sentinel = {
                                    "ts": time.time(),
                                    "fill_id": row[0][:16],
                                    "similarity": avg_sim,
                                    "old_priority": fill_priority,
                                    "new_priority": new_priority,
                                }
                                with open(self._FILL_CONVERGENCE_SENTINEL, "w") as _sf:
                                    _sf.write(_json.dumps(_sentinel))
                            except Exception:
                                pass
        except Exception as conv_err:
            print(f"[HermesWorker] Convergence check error: {conv_err}",
                  file=sys.stderr, flush=True)
        return True

    @staticmethod
    def _hpc_cfg():
        """Get HPC config dict (cached import to avoid circular issues)."""
        import hpc_batch as _hpc
        return _hpc._get_hpc_config()

    def _has_hpc_job(self) -> tuple[bool, str]:
        """Check if an HPC SLURM job is running.

        Returns (exists, job_id_or_empty).
        """
        import subprocess
        try:
            cfg = self._hpc_cfg()
            host = f"{cfg['user']}@{cfg['host']}"
            r = subprocess.run(
                ["ssh", host,
                 f"squeue -u {cfg['user']} -o '%i %T %j' --noheader 2>/dev/null | grep 'dsv4-' | head -1"],
                capture_output=True, text=True, timeout=10,
            )
        except Exception:
            return False, ""
        if not r.stdout.strip():
            return False, ""
        return True, r.stdout.strip().split()[0]

    def _kill_hpc_job(self) -> bool:
        """Kill the HPC SLURM job (vLLM) when the queue has been idle too long.

        vLLM startup grace: any HPC job gets a 5 min window before we
        consider killing it. This gives vLLM time to load the model
        (~4 min). Uses _hpc_job_seen_at to track when we first noticed
        the current job.

        Rate-limited to once per 60s. Returns True if a job was killed.
        """
        global _HPC_LAST_KILLED
        now = time.time()

        has_job, job_id = self._has_hpc_job()
        if not has_job or not job_id:
            self._hpc_job_seen_at = 0  # No job — reset grace timer
            return False

        # Get job state
        import subprocess
        try:
            cfg = self._hpc_cfg()
            host = f"{cfg['user']}@{cfg['host']}"
            r = subprocess.run(
                ["ssh", host,
                 f"squeue -j {job_id} -o '%i %T %j' --noheader"],
                capture_output=True, text=True, timeout=10,
            )
        except Exception:
            return False
        job_state = r.stdout.strip().split()[1] if r.stdout.strip() else "?"

        if job_state == "PENDING":
            return False  # Don't kill pending jobs

        # Track when we first saw THIS job (for vLLM startup grace)
        if self._hpc_job_seen_at == 0:
            self._hpc_job_seen_at = now
            return False  # Just noticed it — give 5 min to start vLLM

        # Don't kill during vLLM startup window
        job_age = now - self._hpc_job_seen_at
        if job_age < 300.0:
            return False

        # Rate limit
        if now - _HPC_LAST_KILLED < _HPC_KILL_COOLDOWN:
            return False
        _HPC_LAST_KILLED = now

        # Kill it
        try:
            cfg = self._hpc_cfg()
            subprocess.run(
                ["ssh", f"{cfg['user']}@{cfg['host']}", f"scancel {job_id}"],
                capture_output=True, timeout=10,
            )
            idle_secs = time.time() - self._last_real_time
            print(f"[HermesWorker] Killed idle HPC job {job_id} ({job_state}) — {idle_secs:.0f}s since last real task",
                  file=sys.stderr, flush=True)
            self._log_event("hpc.kill", job_id=job_id, reason="idle_timeout",
                           duration=round(time.time() - self._hpc_job_seen_at),
                           idle_secs=round(idle_secs))
            self._hpc_job_seen_at = 0  # Reset so next job gets a 5 min grace
            return True
        except Exception as e:
            print(f"[HermesWorker] Failed to kill HPC job: {e}",
                  file=sys.stderr, flush=True)
            return False

    def _vllm_available(self) -> bool:
        """Check if vLLM is actually responding (not just port open).

        SSH tunnel keeps port 8100 listening even when the remote
        vLLM is dead. We need an actual HTTP request to verify.
        """
        try:
            import urllib.request
            req = urllib.request.Request("http://localhost:8100/v1/models",
                                         method="GET")
            with urllib.request.urlopen(req, timeout=3) as resp:
                return resp.status == 200
        except Exception:
            return False

    def _check_health_and_alert(self):
        """Compute daemon health and write alert sentinel on WARN/FAIL.

        Written to /tmp/hpc-daemon-alert as JSON. External watchdog/cron
        can check this file and act when health is not PASS.
        The sentinel is removed when health recovers to PASS.
        """
        import json as _json
        _sentinel = "/tmp/hpc-daemon-alert"
        _warnings = []
        _score = "PASS"

        # 1. Circuit breaker
        if self._breaker_state != "CLOSED":
            _warnings.append(f"Breaker {self._breaker_state}")
            _score = "WARN" if self._breaker_state == "HALF_OPEN" else "FAIL"

        # 2. vLLM / runner availability
        if getattr(self.runner, 'needs_vllm', False) and not self._vllm_available():
            _warnings.append("vLLM unavailable")
            _score = "FAIL" if _score == "PASS" else _score

        # 3. Converged fills (all at -3 or lower = likely stuck)
        try:
            import hpc_batch as _hb
            _c = _hb.init_db(self.db_path)
            _fill_prios = _c.execute(
                "SELECT priority FROM tasks WHERE status='pending' AND priority < 0"
            ).fetchall()
            _c.close()
            if _fill_prios:
                _min_prio = min(r[0] for r in _fill_prios)
                _converged_count = sum(1 for r in _fill_prios if r[0] < -2)
                if _converged_count >= len(_fill_prios):
                    _warnings.append(f"ALL fills converged (min priority={_min_prio})")
                    if _score == "PASS":
                        _score = "WARN"
                elif _converged_count > 0:
                    _warnings.append(f"{_converged_count}/{len(_fill_prios)} fills converged")
                    if _score == "PASS":
                        _score = "WARN"
        except Exception:
            pass

        health = {
            "ts": time.time(),
            "ts_str": time.ctime(time.time()),
            "mode": self._active_qos_label(),
            "breaker": self._breaker_state,
            "score": _score,
            "warnings": _warnings,
        }
        try:
            if _score == "PASS":
                # Remove sentinel when healthy
                if os.path.exists(_sentinel):
                    os.unlink(_sentinel)
            else:
                with open(_sentinel, "w") as _f:
                    _f.write(_json.dumps(health))
        except Exception:
            pass  # Non-critical

    def _write_heartbeat(self, working: bool, detail: str = ""):
        """Write heartbeat file with current state.

        Used by external watchdog to detect stuck daemon.
        """
        import json
        try:
            now = time.time()
            real_count = self.get_pending_real_count()
            fill_count = self._count_fill_pending()
            hb = {
                "pid": os.getpid(),
                "time": now,
                "time_str": time.ctime(now),
                "mode": self._active_qos_label(),
                "working": working,
                "detail": detail,
                "pending_real": real_count,
                "pending_fill": fill_count,
                "idle_since_real": now - self._last_real_time,
                "breaker_state": self._breaker_state,
                "breaker_backoff": self._breaker_backoff,
            }
            with open(self._HEARTBEAT_FILE, "w") as f:
                f.write(json.dumps(hb))
        except Exception:
            pass  # Non-critical — heartbeat is advisory

    def _save_state(self):
        """Persist daemon state to /tmp/hermes-worker.state for crash recovery.

        On restart, _load_state() restores in-memory state so the daemon
        picks up exactly where it left off instead of starting from cold.
        """
        import json
        try:
            state = {
                "version": 1,
                "ts": time.time(),
                "started_at": self._STARTED_AT,
                "mode": self._active_qos_label(),
                "last_fill_task_id": self._last_fill_task_id,
                # Convergence history: trim to last 3 entries per fill for size
                "fill_output_history": {
                    fid: hist[-3:] for fid, hist in self._fill_output_history.items()
                },
                "breaker_state": self._breaker_state,
                "breaker_failures": self._breaker_failures,
                "breaker_backoff": self._breaker_backoff,
                "breaker_next_retry": self._breaker_next_retry,
                "breaker_consecutive_successes": self._breaker_consecutive_successes,
                "last_real_time": self._last_real_time,
                "hpc_job_seen_at": self._hpc_job_seen_at,
            }
            with open(self._STATE_FILE, "w") as f:
                f.write(json.dumps(state))
        except Exception:
            pass  # Non-critical

    def _load_state(self):
        """Load persisted daemon state from /tmp/hermes-worker.state.

        Called at startup after __init__ but before the main loop.
        Restores breaker state, fill cycling, convergence history, timers.
        Silently ignores missing/corrupt state files.
        """
        import json
        try:
            if not os.path.exists(self._STATE_FILE):
                return False
            with open(self._STATE_FILE) as f:
                state = json.loads(f.read())
            if not isinstance(state, dict) or state.get("version") != 1:
                return False
            self._last_fill_task_id = state.get("last_fill_task_id")
            history = state.get("fill_output_history", {})
            if isinstance(history, dict):
                self._fill_output_history = {
                    k: v for k, v in history.items() if isinstance(v, list)
                }
            # Restore breaker state only if it was OPEN (CLOSED is the default
            # that __init__ already set — no need to overwrite).
            bstate = state.get("breaker_state")
            if bstate in ("OPEN", "HALF_OPEN"):
                self._breaker_state = bstate
                self._breaker_failures = state.get("breaker_failures", 0)
                self._breaker_backoff = state.get("breaker_backoff", 30.0)
                self._breaker_next_retry = state.get("breaker_next_retry", 0.0)
                self._breaker_consecutive_successes = state.get(
                    "breaker_consecutive_successes", 0)
            # Restore timers — clamp to reasonable values (within past hour)
            now = time.time()
            lrt = state.get("last_real_time", 0)
            if now - 3600 < lrt <= now:
                self._last_real_time = lrt
            hjs = state.get("hpc_job_seen_at", 0)
            if now - 3600 < hjs <= now:
                self._hpc_job_seen_at = hjs
            return True
        except Exception:
            return False

    def _sweep_stuck_running(self):
        """Background sweep: recover tasks stuck in 'running' state (D10).

        Runs every 300s. Recovers tasks whose started_at is before the
        current daemon's _STARTED_AT (i.e., they were started by a previous
        daemon instance that was killed or crashed).

        The old PID-based check was wrong: after restart the PID file is
        overwritten, so the check always sees the current PID as alive.

        Daemon thread — dies with process.
        """
        import time as _t
        while True:
            _t.sleep(300)
            try:
                import hpc_batch as _hpc
                import json
                conn = _hpc.init_db(self.db_path)
                rows = conn.execute(
                    "SELECT id, started_at, metadata FROM tasks WHERE status = 'running' "
                    "AND started_at IS NOT NULL AND started_at < ?",
                    (self._STARTED_AT,)
                ).fetchall()
                recovered = 0
                for task_id, started_at, meta_json in rows:
                    # Try to restore session file before recovering
                    saved_msgs = self._find_recent_session(started_at)
                    if saved_msgs:
                            _meta_row = conn.execute(
                                "SELECT metadata FROM tasks WHERE id=?", (task_id,)
                            ).fetchone()
                            if _meta_row:
                                import json
                                meta = json.loads(_meta_row[0]) if _meta_row[0] else {}
                                meta['saved_conversation'] = saved_msgs
                                conn.execute(
                                    "UPDATE tasks SET status='pending', started_at=NULL, "
                                    "metadata=?, error='Recovered from stale running (session restored)' "
                                    "WHERE id=? AND status='running'",
                                    (json.dumps(meta), task_id),
                                )
                                recovered += 1
                                self._log_event("task.recover", task_id=task_id[:16],
                                               saved_msgs=len(saved_msgs),
                                               reason="sweep_session_restore")
                                continue
                    conn.execute(
                        "UPDATE tasks SET status = 'pending', started_at = NULL, "
                        "error = 'Recovered from stale running state (owner daemon not found)' "
                        "WHERE id = ? AND status = 'running'",
                        (task_id,)
                    )
                    recovered += 1
                conn.commit()
                conn.close()
                if recovered > 0:
                    print(f"[HermesWorker] Sweep: recovered {recovered} stuck task(s)",
                          file=sys.stderr, flush=True)
            except Exception as e:
                print(f"[HermesWorker] Sweep error: {e}",
                      file=sys.stderr, flush=True)

    def _breaker_record_failure(self):
        """Record a task failure, possibly opening the breaker."""
        self._breaker_failures += 1
        self._breaker_consecutive_successes = 0
        if self._breaker_failures >= self._breaker_max_failures:
            self._breaker_state = "OPEN"
            self._breaker_next_retry = time.time() + self._breaker_backoff
            self._breaker_backoff = min(self._breaker_backoff * 2,
                                        self._breaker_backoff_max)
            print(f"[HermesWorker] Breaker OPEN — {self._breaker_failures} failures, "
                  f"retry in {self._breaker_backoff:.0f}s",
                  file=sys.stderr, flush=True)
            # Reset so next OPEN cycle re-accumulates
            self._breaker_failures = 0

    def _breaker_record_success(self, job_id: str | None = None):
        """Record a task success, possibly closing the breaker."""
        self._breaker_failures = 0
        if self._breaker_state == "HALF_OPEN":
            self._breaker_state = "CLOSED"
            self._breaker_consecutive_successes = 0
            self._breaker_backoff = 30.0  # Reset to min
            print(f"[HermesWorker] Breaker CLOSED — probe succeeded",
                  file=sys.stderr, flush=True)
            self._log_event("breaker.close", reason="probe_succeeded",
                           job_id=job_id)
        else:
            self._breaker_consecutive_successes += 1
            if self._breaker_consecutive_successes >= self._breaker_reset_threshold:
                self._breaker_backoff = 30.0  # Reset to min
                self._breaker_consecutive_successes = 0

    def _read_vllm_log(self) -> str | None:
        """Read the vLLM startup log from the shared filesystem via HPC login node.

        The vLLM log is on BeeGFS (shared filesystem), accessible from
        the login node at ~/hpc-agent/logs/vllm-fast-{JOB_ID}.log.

        Returns:
            Log content as string, empty string if file exists but empty,
            None if no HPC job or SSH fails.
        """
        import subprocess
        try:
            has_job, job_id = self._has_hpc_job()
            if not has_job or not job_id:
                return None
            cfg = self._hpc_cfg()
            cmd = [
                "ssh", f"{cfg['user']}@{cfg['host']}",
                f"cat {cfg['dir']}/logs/vllm-fast-{job_id}.log 2>/dev/null || true"
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if r.returncode == 0:
                return r.stdout or ""
            return None
        except Exception:
            return None

    def _force_tunnel_refresh(self) -> bool:
        """Force SSH tunnel to reconnect to the current HPC compute node.

        If the tunnel is stale (pointing to a previous job's node), this
        forces it to reconnect. Returns True if tunnel is now pointing
        to the current job, False if refresh failed.
        """
        try:
            if hasattr(self.runner, '_ensure_vllm_endpoint'):
                tunneled = self.runner._ensure_vllm_endpoint()
                if tunneled:
                    self.runner.endpoint = tunneled
                    self.runner._tunnel_established = True
                    print(f"[HermesWorker] Tunnel refreshed: {tunneled}",
                          file=sys.stderr, flush=True)
                    return True
            return False
        except Exception as e:
            print(f"[HermesWorker] Tunnel refresh failed: {e}",
                  file=sys.stderr, flush=True)
            return False

    def _check_vllm_startup_monitor(self):
        """If HPC job is running but vLLM not responding, enter monitoring.

        During monitoring, no tasks are processed. The daemon polls
        /v1/models every cycle. After monitoring starts, it tracks
        vLLM startup progress via log parsing and kills only if stalled.

        Tunnel freshness: if vLLM responds immediately but a new HPC job
        was submitted recently (<5 min), the response may be from a stale
        tunnel. Force a tunnel refresh to verify.
        """
        if not getattr(self, '_monitoring_vllm', False):
            # Not monitoring yet — check if we should start
            vllm_up = self._vllm_available()
            if vllm_up:
                # vLLM IS available — but tunnel might be stale from a
                # previous job. If a new HPC job was submitted recently,
                # force a tunnel refresh before accepting the fast response.
                if self._hpc_job_seen_at > 0:
                    job_age = time.time() - self._hpc_job_seen_at
                    if job_age < 300:  # Job seen within last 5 min
                        # Fetch job_id for structured events
                        _, tunnel_job_id = self._has_hpc_job()
                        print(f"[HermesWorker] vLLM responded after {job_age:.0f}s — "
                              f"new HPC job exists, forcing tunnel refresh",
                              file=sys.stderr, flush=True)
                        self._log_event("vllm.tunnel.suspect", duration=round(job_age),
                                       job_id=tunnel_job_id)
                        if self._force_tunnel_refresh():
                            # Re-check with fresh tunnel
                            vllm_up = self._vllm_available()
                            if vllm_up:
                                print(f"[HermesWorker] vLLM confirmed ready after tunnel refresh ({job_age:.0f}s)",
                                      file=sys.stderr, flush=True)
                                self._log_event("vllm.ready", duration=round(job_age),
                                               job_id=tunnel_job_id)
                                return
                            print(f"[HermesWorker] Tunnel was stale — vLLM not ready, entering monitoring",
                                  file=sys.stderr, flush=True)
                            self._log_event("vllm.tunnel.refresh", job_id=tunnel_job_id)
                        else:
                            return  # Refresh failed — keep stale tunnel
                    else:
                        return  # Old job — tunnel is fine
                else:
                    return  # No job known — tunnel is fine
            else:
                # vLLM not available
                if not getattr(self.runner, 'needs_vllm', False):
                    return  # MockRunner — no vLLM dependency
            # If we get here, vLLM isn't available (or was stale).
            # Check if there's an HPC job that could be loading vLLM
            has_job, job_id = self._has_hpc_job() if hasattr(self, '_has_hpc_job') else (False, '')
            if has_job:
                self._log_event("hpc.submit", job_id=job_id)
                # P4+P5: Reset breaker when new HPC job appears.
                # Without this, the breaker stays OPEN (exponential backoff)
                # even after a fresh job starts. The tunnel refresh below
                # handles stale tunnels; the breaker reset unblocks tasks.
                if self._breaker_state != "CLOSED":
                    print(f"[HermesWorker] New HPC job detected — resetting breaker from {self._breaker_state}",
                          file=sys.stderr, flush=True)
                    self._log_event("breaker.close", reason="new_hpc_job", previous_state=self._breaker_state,
                                   job_id=job_id)
                    self._breaker_state = "CLOSED"
                    self._breaker_failures = 0
                    self._breaker_backoff = 30.0

                self._monitoring_vllm = True
                self._monitor_started_at = time.time()
                self._vllm_stall_count = 0
                self._vllm_last_log_hash = ""
                self._log_event("vllm.monitor.start", job_id=job_id)
                # Sync the HPC job kill timer so _kill_hpc_job()
                # recognizes this job as existing (not "just noticed").
                self._hpc_job_seen_at = time.time()
                # Immediately try to refresh tunnel — may be stale from
                # a previous HPC job on a different compute node
                if hasattr(self.runner, '_ensure_vllm_endpoint'):
                    try:
                        tunneled = self.runner._ensure_vllm_endpoint()
                        if tunneled:
                            self.runner.endpoint = tunneled
                            self.runner._tunnel_established = True
                            print(f"[HermesWorker] Tunnel refreshed on monitor start: {tunneled}",
                                  file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[HermesWorker] Initial tunnel refresh failed: {e}",
                              file=sys.stderr, flush=True)
                print(f"[HermesWorker] vLLM startup monitoring — HPC job exists but vLLM not responding",
                      file=sys.stderr, flush=True)
        else:
            # Already monitoring — check progress
            elapsed = time.time() - self._monitor_started_at

            # ── Early exit if HPC job disappeared ─────────────────────────
            # If the job was killed/preempted during monitoring, don't wait
            # the full timeout. Exit immediately so the daemon can
            # auto-dispatch a replacement in the next cycle.
            has_job, job_id = self._has_hpc_job()
            if not has_job:
                # Jobs that vanish during monitoring are either preempted
                # (SLURM killed them) or the node crashed. Log separately
                # from the daemon's own idle_timeout kills.
                hint = "preempted" if elapsed < 60 else "vanished"
                print(f"[HermesWorker] vLLM startup monitoring — HPC job {hint} after {elapsed:.0f}s",
                      file=sys.stderr, flush=True)
                self._log_event("vllm.job.lost", duration=round(elapsed),
                               reason=hint, job_id=job_id)
                self._monitoring_vllm = False
                return

            # Tunnel keepalive is handled by SSH ServerAliveInterval (set in
            # worker/tunnel.py). No periodic refresh needed — the tunnel
            # self-destructs within ~45s of the remote side dying.

            if self._vllm_available():
                # Minimum 30s monitoring window prevents tunnel false positives.
                # If vLLM responds immediately (<30s), it might be a stale SSH
                # tunnel to a previous job's vLLM still running on the old node.
                # Require at least 30s to pass before accepting readiness.
                if elapsed < 30.0:
                    print(f"[HermesWorker] vLLM responded after {elapsed:.0f}s — too soon, suspect stale tunnel, continuing monitoring",
                          file=sys.stderr, flush=True)
                    self._log_event("vllm.tunnel.stale", duration=round(elapsed),
                                   job_id=job_id)
                else:
                    self._monitoring_vllm = False
                    print(f"[HermesWorker] vLLM startup monitoring — ready after {elapsed:.0f}s",
                          file=sys.stderr, flush=True)
                    self._log_event("vllm.ready", duration=round(elapsed),
                                   job_id=job_id)
                    return
            # ── Log-based progress tracking ───────────────────────────────
            # Read and parse the vLLM startup log to estimate progress.
            # If the log hasn't advanced for 3+ consecutive checks (~300s),
            # the job is stalled and should be killed.
            # Falls back to wall-clock timeout if SSH is unavailable.
            import hashlib
            log_text = self._read_vllm_log()
            if log_text is not None:
                progress = _parse_vllm_progress(log_text)
                log_hash = hashlib.md5(log_text.encode()).hexdigest()
                if log_hash == self._vllm_last_log_hash:
                    self._vllm_stall_count += 1
                else:
                    self._vllm_stall_count = 0
                    self._vllm_last_log_hash = log_hash

                # If log says ready but HTTP doesn't respond yet, wait
                if progress["progress"] >= 100:
                    print(f"[HermesWorker] vLLM log shows ready after {elapsed:.0f}s — waiting for HTTP",
                          file=sys.stderr, flush=True)
                else:
                    # Log progress every 60s
                    if int(elapsed) % 60 < 10 and int(elapsed) > 10:
                        print(f"[HermesWorker] vLLM startup — {progress['progress']}% ({progress['stage']}) "
                              f"after {elapsed:.0f}s",
                              file=sys.stderr, flush=True)

                # Kill if stalled for 3+ consecutive checks
                if self._vllm_stall_count >= 3:
                    print(f"[HermesWorker] vLLM startup — STALLED at {progress['progress']}% "
                          f"({progress['stage']}) for {self._vllm_stall_count} checks, killing job",
                          file=sys.stderr, flush=True)
                    self._log_event("vllm.timeout", duration=round(elapsed),
                                   reason=f"stalled_pct={progress['progress']}",
                                   job_id=job_id)
                    self._kill_hpc_job()
                    self._monitoring_vllm = False
                    return
            else:
                # SSH failed — fall back to wall-clock timeout
                _vllm_timeout = float(os.environ.get("VLLM_STARTUP_TIMEOUT", "720"))
                if int(elapsed) % 60 < 10 and int(elapsed) > 10:
                    print(f"[HermesWorker] vLLM startup monitoring — still waiting ({elapsed:.0f}s / {_vllm_timeout:.0f}s max)",
                          file=sys.stderr, flush=True)
                if elapsed > _vllm_timeout:
                    # Check if port is listening (tunnel up vs vLLM down)
                    _tunnel_up = False
                    try:
                        import socket
                        _s = socket.create_connection(("localhost", 8100), timeout=2)
                        _s.close()
                        _tunnel_up = True
                    except Exception:
                        pass
                    _vllm_resp = "unknown"
                    try:
                        import urllib.request
                        _req = urllib.request.Request("http://localhost:8100/v1/models", method="GET")
                        _resp = urllib.request.urlopen(_req, timeout=3)
                        _vllm_resp = f"http {_resp.status}"
                    except Exception as e:
                        _vllm_resp = str(e)[:80]
                    print(f"[HermesWorker] vLLM startup monitoring — TIMEOUT after {elapsed:.0f}s"
                          f" (tunnel_up={_tunnel_up}, vllm_response={_vllm_resp}), killing job",
                          file=sys.stderr, flush=True)
                    self._log_event("vllm.timeout", duration=round(elapsed),
                                   reason=f"tunnel={_tunnel_up} resp={_vllm_resp}",
                                   job_id=job_id)
                    self._kill_hpc_job()
                    self._monitoring_vllm = False

    def _check_hpc_idle_kill(self):
        """Check if HPC job should be killed due to idle.

        Uses cached cycle counts (_cycle_real, _cycle_fill) set by
        _idle_cycle(). Only kills if:
        - No tasks are pending (real or fill — fills keep HPC alive)
        - _hpc_idle_timeout seconds have passed since last task
        - Not currently in vLLM startup monitoring mode
        """
        # Don't kill if real tasks are pending (fills are cheap — keepwarm handles them)
        if self._cycle_real > 0:
            return
        if self._hpc_idle_timeout <= 0:
            return
        if getattr(self, '_monitoring_vllm', False):
            return  # Don't kill during startup monitoring
        idle_since = time.time() - self._last_real_time
        if idle_since >= self._hpc_idle_timeout:
            self._kill_hpc_job()

    def _idle_cycle(self) -> tuple[str | None, bool]:
        """Run one idle cycle."""
        import sys
        # Cache counts for the cycle so helpers don't re-query the DB
        self._cycle_real = self.get_pending_real_count()
        self._cycle_fill = self._count_fill_pending()
        real_count = self._cycle_real
        fill_count = self._cycle_fill

        # HPC idle kill check — uses cached cycle counts
        self._check_hpc_idle_kill()

        # vLLM startup monitoring (D3): if an HPC job is running but vLLM
        # is not responding, defer all task processing until vLLM is ready.
        self._check_vllm_startup_monitor()
        if getattr(self, '_monitoring_vllm', False):
            elapsed = time.time() - self._monitor_started_at
            self._write_heartbeat(False, detail=f"waiting for vLLM ({elapsed:.0f}s)")
            return None, False

        # ── Auto-dispatch: if pending tasks, ensure an HPC job exists ────
        # Rate-limited to once per 60s (via _last_auto_dispatch in hpc_batch).
        # ensure_hpc_worker handles three cases:
        #   - Normal QoS job running → warm_reused (no sbatch)
        #   - Only preemptqos (keepwarm) + real tasks → submit replacement
        #   - Fill-only with keepwarm → warm_reused (keepwarm is fine)
        #   - Cold → submit fresh
        # After idle timeout (_hpc_idle_timeout since last real task),
        # suppress auto-dispatch. The HPC job was killed by _check_hpc_idle_kill.
        # Fill tasks on keepwarm (preemptqos) continue naturally. When a real
        # task arrives, _last_real_time resets and dispatch resumes.
        has_job, self._cycle_job_id = self._has_hpc_job()
        _hpc_job_exists = has_job
        _idle_secs = time.time() - self._last_real_time
        if self._any_pending() and _idle_secs < self._hpc_idle_timeout:
            import hpc_batch as _hpc
            _qos = self._active_qos()
            print(f"[HermesWorker] Dispatching: real={real_count} fill={fill_count} hpc={_hpc_job_exists} qos={_qos or 'normal'}",
                  file=sys.stderr, flush=True)
            _hpc._auto_dispatch_if_needed(self.db_path, qos=_qos)

        # ── Circuit breaker: skip processing if vLLM is down ───────────
        # Only applies to HermesRunner/VLLMRunner (need vLLM endpoint).
        # MockRunner and TransformersRunner bypass the breaker.
        if getattr(self.runner, 'needs_vllm', False) and not self._vllm_available():
            # If no HPC job exists, the breaker has nothing to protect.
            # Skip entirely so auto-dispatch and keepwarm fill push can work.
            if not _hpc_job_exists:
                pass
            elif self._breaker_state == "CLOSED":
                self._breaker_failures += 1
                if self._breaker_failures >= self._breaker_max_failures:
                    self._breaker_state = "OPEN"
                    self._breaker_next_retry = time.time() + self._breaker_backoff
                    self._breaker_backoff = min(self._breaker_backoff * 2,
                                                self._breaker_backoff_max)
                    self._breaker_failures = 0
                    print(f"[HermesWorker] Breaker OPEN — retry in {self._breaker_backoff:.0f}s",
                          file=sys.stderr, flush=True)
                    self._log_event("breaker.open", backoff=self._breaker_backoff,
                                   job_id=self._cycle_job_id)
                    self._write_heartbeat(False, detail=f"breaker OPEN")
                else:
                    print(f"[HermesWorker] vLLM down ({self._breaker_failures}/{self._breaker_max_failures})",
                          file=sys.stderr, flush=True)
                    self._write_heartbeat(False, detail=f"vLLM down")
                return None, False  # Don't process this cycle
            elif self._breaker_state == "OPEN":
                # If no HPC job exists, the backoff is pointless — vLLM won't
                # magically appear. Reset to CLOSED so auto-dispatch can fire.
                has_job, _open_job_id = self._has_hpc_job()
                if not has_job:
                    print(f"[HermesWorker] Breaker OPEN but no HPC job — resetting to CLOSED",
                          file=sys.stderr, flush=True)
                    self._log_event("breaker.close", reason="open_no_hpc_job",
                                   job_id=_open_job_id)
                    self._breaker_state = "CLOSED"
                    self._breaker_failures = 0
                    self._breaker_backoff = 30.0
                elif time.time() >= self._breaker_next_retry:
                    self._breaker_state = "HALF_OPEN"
                    print(f"[HermesWorker] Breaker HALF_OPEN — probe allowed",
                          file=sys.stderr, flush=True)
                    self._log_event("breaker.half_open", job_id=_open_job_id)
                else:
                    remaining = self._breaker_next_retry - time.time()
                    print(f"[HermesWorker] Breaker OPEN — {remaining:.0f}s remaining",
                          file=sys.stderr, flush=True)
                    self._write_heartbeat(False, detail=f"breaker OPEN")
                    return None, False
            elif self._breaker_state == "HALF_OPEN":
                # Probe is still in-flight — don't process more tasks
                # until the probe completes. Next cycle checks probe outcome
                # via _breaker_record_success/failure.
                # But if no HPC job exists, no probe can complete — reset to
                # CLOSED so auto-dispatch can fire and start a new job.
                has_job, _hj_id = self._has_hpc_job()
                if not has_job:
                    print(f"[HermesWorker] Breaker HALF_OPEN but no HPC job — resetting to CLOSED",
                          file=sys.stderr, flush=True)
                    self._log_event("breaker.close", reason="half_open_no_hpc_job",
                                   job_id=_hj_id)
                    self._breaker_state = "CLOSED"
                    self._breaker_failures = 0
                    self._breaker_backoff = 30.0
                else:
                    self._write_heartbeat(False, detail="breaker HALF_OPEN — probe in flight")
                    return None, False
        else:
            # vLLM is up — close the breaker if it was open
            if self._breaker_state != "CLOSED":
                self._breaker_state = "CLOSED"
                self._breaker_failures = 0
                self._breaker_backoff = 30.0
                print(f"[HermesWorker] Breaker CLOSED — vLLM recovered",
                      file=sys.stderr, flush=True)
                self._log_event("breaker.close", reason="vllm_recovered",
                               job_id=self._cycle_job_id)

        # ── Real tasks: process all, reset idle timer ──────────────────
        if real_count > 0:
            print(f"[HermesWorker] Cycle: {real_count} real tasks, {fill_count} fill tasks → processing real tasks",
                  file=sys.stderr, flush=True)
            self._write_heartbeat(True, detail=f"processing {real_count} real tasks")
            self._last_real_time = time.time()  # Real work — reset idle timer
            processed_any = False
            while self._process_next():
                processed_any = True
            self.idle_cycles = 0
            self.fill_in_flight = False
            # Update breaker based on actual task outcome
            if getattr(self.runner, 'needs_vllm', False) and not self._vllm_available():
                self._breaker_record_failure()
            else:
                self._breaker_record_success(job_id=self._cycle_job_id)
            # Check for recoverable tasks after real-task processing.
            if self._recover_recoverable_failed(job_id=self._cycle_job_id) > 0:
                self._last_real_time = time.time()
            # Signal MCP waiters (D5): if any completed task has a blocked
            # fill thread waiting on it, signal the event to unblock.
            if processed_any:
                import hpc_batch as _hb
                import sqlite3 as _sqlite3
                _conn = _sqlite3.connect(self.db_path)
                for _tid in list(_hb._mcp_wait_events.keys()):
                    _row = _conn.execute("SELECT status FROM tasks WHERE id=?", (_tid,)).fetchone()
                    if _row and _row[0] in ('completed', 'failed'):
                        _hb.signal_mcp_done(_tid)
                _conn.close()
            return None, processed_any

        # ── Fill tasks: skip if vLLM is down ────────────────────────────
        # Fill tasks run a coding agent that calls MCP tools, which in
        # turn submit real tasks. Fill tasks do NOT reset the idle timer
        # (_last_real_time) — only actual real tasks do. If a fill task
        # generates a real task via MCP, the next cycle will process it
        # and reset the timer then.
        if fill_count > 0:
            if getattr(self.runner, 'needs_vllm', False) and not self._vllm_available():
                print(f"[HermesWorker] vLLM down, skipping fill tasks (will retry next cycle)",
                      file=sys.stderr, flush=True)
                self._write_heartbeat(False, detail="vLLM down, fill skipped")
                return None, False  # Sleep normally, check again next cycle
            print(f"[HermesWorker] Cycle: idle, processing fill task (timeout={getattr(self, 'fill_timeout', 120)}s)",
                  file=sys.stderr, flush=True)
            self._write_heartbeat(True, detail="processing fill task")
            self._process_random_fill()
            # Check for recoverable tasks after each fill cycle.
            # Fills stay pending forever, so the truly-idle recovery
            # path is never reached — we must check here instead.
            if self._recover_recoverable_failed(job_id=self._cycle_job_id) > 0:
                self._last_real_time = time.time()
            if self.idle_timeout != 0 and self.idle_cycles >= (self.idle_timeout // self.poll_interval):
                print("[HermesWorker] Idle timeout reached, shutting down",
                      file=sys.stderr, flush=True)
                return "shutdown", False
            if self.idle_timeout != 0:
                self.idle_cycles += 1
            self.fill_in_flight = False
            return None, True  # work_done=True → don't sleep

        # ── Truly idle ──────────────────────────────────────────────────
        # Fills stay pending forever (never marked completed), so this
        # branch is only reached when no fill tasks exist at all (fresh
        # DB or manual cleanup). Normal mode: idle check → shutdown.
        # keepwarm mode: never shut down; next start will seed fills.
        if self.idle_cycles == 0:
            print(f"[HermesWorker] Cycle: truly idle (no fills in queue)",
              file=sys.stderr, flush=True)
        self._write_heartbeat(False, detail=f"idle {time.time()-self._last_real_time:.0f}s since last real task")

        # Check for stuck tasks before deciding to shutdown
        recovered = self._recover_stuck_tasks(job_id=self._cycle_job_id)
        recovered += self._recover_recoverable_failed(job_id=self._cycle_job_id)
        if recovered > 0:
            print(f"[HermesWorker] Recovered {recovered} task(s) (stuck + failed), "
                  f"will process next cycle", file=sys.stderr, flush=True)
            self._last_real_time = time.time()  # May need HPC for retries
            return None, True  # Skip sleep, process recovered tasks next cycle

        # Daemon self-termination (separate from HPC kill)
        if self.idle_timeout != 0 and self.idle_cycles >= (self.idle_timeout // self.poll_interval):
            print("[HermesWorker] Idle timeout reached, shutting down",
                  file=sys.stderr, flush=True)
            return "shutdown", False

        if self.idle_timeout != 0:
            self.idle_cycles += 1
        return None, False

    def _count_fill_pending(self) -> int:
        """Count pending fill tasks."""
        import hpc_batch
        conn = hpc_batch.init_db(self.db_path)
        row = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND priority < 0"
        ).fetchone()
        conn.close()
        return row[0] if row else 0

    def _wait_for_fill_or_cancel(self):
        """Wait for in-flight fill task, checking periodically for new real tasks."""
        deadline = time.time() + self.shutdown_wait_max
        while time.time() < deadline:
            time.sleep(10)
            if self.get_pending_real_count() > 0:
                break  # Real task arrived, cancel shutdown
            # Check if fill task completed (no longer in-flight)
            conn = None
            try:
                import hpc_batch
                conn = hpc_batch.init_db(self.db_path)
                fill_count = conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE task_type = 'fill' AND status = 'running'"
                ).fetchone()[0]
                if fill_count == 0:
                    self.fill_in_flight = False
                    break  # Fill task completed
            finally:
                if conn:
                    conn.close()

    def _acquire_singleton_lock(self) -> bool:
        """Ensure only one HermesWorker runs against this DB.

        Uses fcntl.flock for an atomic kernel-enforced file lock.
        This prevents race conditions where two processes start
        simultaneously and both pass the PID file check.

        Fallback: if flock is not available (e.g., some network FS),
        uses PID file with /proc check.
        """
        pid_file = self._PID_FILE
        my_pid = os.getpid()
        try:
            import fcntl
            fd = os.open(pid_file, os.O_RDWR | os.O_CREAT)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Lock acquired — write our PID
                os.ftruncate(fd, 0)
                os.write(fd, str(my_pid).encode())
                # Store fd so it stays open for the process lifetime
                self._lock_fd = fd
                return True
            except IOError:
                # Another process holds the lock
                os.close(fd)
                try:
                    with open(pid_file) as f:
                        old_pid = int(f.read().strip())
                    # Check if the lock holder is still alive
                    os.kill(old_pid, 0)
                    # Silent exit — don't pollute daemon log with guard noise
                    return False
                except (OSError, IOError, ValueError):
                    pass  # Stale lock — retry
                # Stale lock: try to acquire again
                return self._acquire_singleton_lock()
        except ImportError:
            # Fallback: PID file with /proc check (no flock)
            try:
                if os.path.exists(pid_file):
                    with open(pid_file) as f:
                        old_pid = int(f.read().strip())
                    if old_pid != my_pid:
                        try:
                            os.kill(old_pid, 0)
                            return False  # Silent — don't pollute log
                        except (OSError, IOError, ValueError):
                            pass  # Stale PID — claim it
                with open(pid_file, "w") as f:
                    f.write(str(my_pid))
                return True
            except Exception as e:
                print(f"[HermesWorker] PID lock fallback failed ({e}), starting anyway",
                      file=sys.stderr, flush=True)
                return True

    def run(self):
        """Main worker loop. Runs until idle timeout triggers shutdown."""
        import sys

        # Singleton guard — prevents duplicate daemon instances
        if not self._acquire_singleton_lock():
            return

        # ── PDEATHSIG: die when parent (tmux/shell) dies ────────────────
        # Without this, if tmux session is killed/restarted, the daemon
        # survives as an orphan and keeps running. PDEATHSIG ensures clean
        # death when the parent process group terminates.
        try:
            import ctypes
            libc = ctypes.CDLL("libc.so.6")
            # PR_SET_PDEATHSIG = 1
            libc.prctl(1, 15)  # SIGTERM = 15
            print("[HermesWorker] PDEATHSIG set — daemon dies with parent",
                  file=sys.stderr, flush=True)
        except Exception:
            pass  # Non-critical

        # ── SIGTERM handler: graceful shutdown (D7) ───────────────────────
        # Sets a flag checked by the main loop after each task cycle.
        # In-flight task gets up to shutdown_wait_max to drain.
        import signal as _signal
        self._shutting_down = False
        def _handle_sigterm(signum, frame):
            self._shutting_down = True
            print("[HermesWorker] SIGTERM received — draining in-flight task",
                  file=sys.stderr, flush=True)
        _signal.signal(_signal.SIGTERM, _handle_sigterm)

        # Pre-warm: import AIAgent on the main thread so all 30+ transitive
        # modules (run_agent, openai, hermes_*, agent.*, tools.*) are cached
        # in sys.modules before any daemon thread touches them. This
        # eliminates the transient import failure on the first task cycle.
        try:
            from run_agent import AIAgent
            print("[HermesWorker] AIAgent import OK, tool chain ready",
                  file=sys.stderr, flush=True)
        except Exception as e:
            print(f"[HermesWorker] AIAgent import FAILED: {e}",
                  file=sys.stderr, flush=True)
            import traceback
            traceback.print_exc()
            sys.exit(1)

        print(f"[HermesWorker] Daemon started (db={self.db_path}, poll={self.poll_interval}s, "
              f"idle_timeout={self.idle_timeout}, mode=dynamic)",
              file=sys.stderr, flush=True)

        # Restore persisted state (breaker, fill cycling, convergence history)
        if self._load_state():
            print(f"[HermesWorker] Restored daemon state — last_fill={str(self._last_fill_task_id)[:16] if self._last_fill_task_id else 'none'}, "
                  f"breaker={self._breaker_state}, fills_tracked={len(self._fill_output_history)}",
                  file=sys.stderr, flush=True)
            self._log_event("daemon.state.restore", reason="crash_recovery",
                           breaker=self._breaker_state)

        # Recover any stuck tasks from a previous crash
        recovered = self._recover_stuck_tasks()
        if recovered > 0:
            print(f"[HermesWorker] Recovered {recovered} stuck task(s) on startup",
                  file=sys.stderr, flush=True)

        # Start stuck-task sweep thread (D10)
        import threading as _t
        _sweep = _t.Thread(target=self._sweep_stuck_running, daemon=True)
        _sweep.start()

        cycle = 0
        while True:
            cycle += 1
            # Graceful shutdown check (D7)
            if self._shutting_down:
                print("[HermesWorker] Shutting down (SIGTERM received)",
                      file=sys.stderr, flush=True)
                action = "shutdown"
                break
            action, work_done = self._idle_cycle()
            self._save_state()  # Persist state for crash recovery
            self._check_health_and_alert()
            if action == "shutdown":
                print("[HermesWorker] Shutdown signal received, exiting",
                      file=sys.stderr, flush=True)
                break
        # ── Cleanup on exit (shared by shutdown + SIGTERM) ─────────────
        if self._lock_fd is not None:
            try:
                import fcntl
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
            except Exception:
                pass
            self._lock_fd = None
        # Stop heartbeat
        try:
            os.unlink(self._HEARTBEAT_FILE)
        except Exception:
            pass

        # Sleep only when idle — skip sleep if we just did work
            if not work_done:
                if cycle % 10 == 0:
                    print(f"[HermesWorker] Sleeping {self.poll_interval}s (cycle {cycle})",
                          file=sys.stderr, flush=True)
                # Fast-start: check sentinel or any pending task every 1s for 5s
                woke_early = False
                for _ in range(min(5, int(self.poll_interval))):
                    if self._check_task_ready_sentinel():
                        woke_early = True
                        break
                    if self.get_pending_real_count() > 0:
                        woke_early = True
                        break
                    if self._count_fill_pending() > 0:
                        woke_early = True
                        break
                    time.sleep(1)
                if not woke_early:
                    # Fallback: check periodically during remaining time
                    remaining = self.poll_interval - 5
                    while remaining > 0:
                        if self._check_task_ready_sentinel():
                            break
                        if self.get_pending_real_count() > 0:
                            break
                        if self._count_fill_pending() > 0:
                            break
                        chunk = min(10.0, remaining)
                        time.sleep(chunk)
                        remaining -= chunk

    def run_once(self) -> tuple[str | None, bool]:
        """Run a single cycle (for testing). Returns (action, work_done)."""
        return self._idle_cycle()
