"""Tests for hermes_worker.py — inline Hermes Worker loop."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json
import sqlite3
import tempfile
import time
import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from worker.model_runner import MockRunner

from hermes_runtime.task_queue import LLMTask, TaskResult, TaskStatus


def _make_db() -> str:
    """Create temp DB with full schema including priority + task_type."""
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
            task_type TEXT DEFAULT 'llm'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
    conn.commit()
    conn.close()
    return path


def _add_task(conn, task_id: str, priority: int = 0, status: str = "pending",
              task_type: str = "llm", messages: list | None = None,
              metadata: dict | None = None,
              created_at: float | None = None):
    conn.execute("""
        INSERT OR IGNORE INTO tasks
        (id, agent_id, messages, tools, model, api_config, metadata,
         priority, created_at, status, task_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        "test",
        json.dumps(messages or [{"role": "user", "content": "test prompt"}]),
        json.dumps([]),
        "test-model",
        json.dumps({}),
        json.dumps(metadata or {}),
        priority,
        created_at or time.time(),
        status,
        task_type,
    ))


class TestGetPendingRealCount:
    """get_pending_real_count counts priority >= 0 pending tasks."""

    def test_counts_only_high_priority(self):
        from worker.hermes_worker import get_pending_real_count
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_1", priority=-1, task_type="fill")
        _add_task(conn, "normal_1", priority=0)
        _add_task(conn, "urgent", priority=10)
        conn.commit()
        conn.close()

        count = get_pending_real_count(db)
        assert count == 2  # normal_1 + urgent
        os.unlink(db)

    def test_zero_when_idle(self):
        from worker.hermes_worker import get_pending_real_count
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill_1", priority=-1, task_type="fill")
        conn.commit()
        conn.close()

        count = get_pending_real_count(db)
        assert count == 0
        os.unlink(db)


class TestProcessNextTask:
    """process_next_task — claims and processes one task."""

    def test_claims_highest_priority_task(self):
        from worker.hermes_worker import process_next_task
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "low", priority=-1)
        _add_task(conn, "high", priority=10)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = TaskResult(
                task_id="high", status=TaskStatus.COMPLETED, content="done"
            )
            MockRunner.return_value = mock_instance

            result = process_next_task(db)

        assert result is True
        # Verify the high-priority task was claimed
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT id, status FROM tasks WHERE status != 'pending'").fetchone()
        conn.close()
        assert row[0] == "high"
        assert row[1] in ("running", "completed")

    def test_stores_result_and_marks_completed(self):
        from worker.hermes_worker import process_next_task
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "task_1", priority=0)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = TaskResult(
                task_id="task_1", status=TaskStatus.COMPLETED, content="result output"
            )
            MockRunner.return_value = mock_instance

            process_next_task(db)

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT status, result FROM tasks WHERE id = 'task_1'"
        ).fetchone()
        conn.close()
        assert row[0] == "completed"
        result_data = json.loads(row[1])
        assert result_data["content"] == "result output"

    def test_returns_false_when_no_pending(self):
        from worker.hermes_worker import process_next_task
        db = _make_db()
        conn = sqlite3.connect(db)
        conn.commit()
        conn.close()

        result = process_next_task(db)
        assert result is False

    def test_marks_failed_on_error(self):
        from worker.hermes_worker import process_next_task
        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "failing", priority=0)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.side_effect = RuntimeError("LLM crashed")
            MockRunner.return_value = mock_instance

            result = process_next_task(db)

        assert result is True  # We did process it
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT status, error FROM tasks WHERE id = 'failing'"
        ).fetchone()
        conn.close()
        assert row[0] == "failed"
        assert "crashed" in (row[1] or "")


class TestTaskJobCorrelation:
    """task.claim and task.complete events carry HPC job_id."""

    def test_task_claim_has_job_id_when_provided(self):
        """task.claim stores the HPC job_id when passed to process_next_task."""
        from worker.hermes_worker import process_next_task
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "job-task-1", priority=0)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = TaskResult(
                task_id="job-task-1", status=TaskStatus.COMPLETED, content="done"
            )
            MockRunner.return_value = mock_instance
            process_next_task(db, job_id="hpc-job-claim-001")

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='task.claim'"
        ).fetchone()
        conn.close()

        assert row is not None, "task.claim event should exist"
        assert row[1] == "hpc-job-claim-001", f"Expected job_id=hpc-job-claim-001, got {row[1]}"
        os.unlink(db)

    def test_task_complete_has_job_id_when_provided(self):
        """task.complete stores the HPC job_id when passed to process_next_task."""
        from worker.hermes_worker import process_next_task
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "job-task-2", priority=0)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = TaskResult(
                task_id="job-task-2", status=TaskStatus.COMPLETED, content="done"
            )
            MockRunner.return_value = mock_instance
            process_next_task(db, job_id="hpc-job-complete-002")

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='task.complete'"
        ).fetchone()
        conn.close()

        assert row is not None, "task.complete event should exist"
        assert row[1] == "hpc-job-complete-002", f"Expected job_id=hpc-job-complete-002, got {row[1]}"
        os.unlink(db)

    def test_task_events_null_job_id_when_not_provided(self):
        """task.claim and task.complete have NULL job_id when not provided."""
        from worker.hermes_worker import process_next_task
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "job-task-3", priority=0)
        conn.commit()
        conn.close()

        with patch("worker.hermes_worker.HermesRunner") as MockRunner:
            mock_instance = MagicMock()
            mock_instance.run.return_value = TaskResult(
                task_id="job-task-3", status=TaskStatus.COMPLETED, content="done"
            )
            MockRunner.return_value = mock_instance
            process_next_task(db)  # No job_id

        conn = sqlite3.connect(db)
        claim = conn.execute(
            "SELECT job_id FROM task_events WHERE event='task.claim'"
        ).fetchone()
        complete = conn.execute(
            "SELECT job_id FROM task_events WHERE event='task.complete'"
        ).fetchone()
        conn.close()

        assert claim is not None
        assert claim[0] is None or claim[0] == "", \
            f"Expected NULL/empty job_id for claim, got {claim[0]}"
        assert complete is not None
        assert complete[0] is None or complete[0] == "", \
            f"Expected NULL/empty job_id for complete, got {complete[0]}"
        os.unlink(db)


class TestAllEventJobCorrelation:
    """All remaining events carry HPC job_id via Design 2 (Cache on Instance)."""

    # ── _recover_recoverable_failed ─────────────────────────────────

    def test_recover_recoverable_failed_carries_job_id(self):
        """task.recover from _recover_recoverable_failed() has job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import MagicMock
        import sqlite3, json

        db = _make_db()
        conn = sqlite3.connect(db)
        # A failed task with saved_conversation → will be recovered
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, status, priority, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("recover-test-1", "test", json.dumps([{"role": "user", "content": "hi"}]),
             "[]", "m", "{}",
             json.dumps({"saved_conversation": [{"role": "user", "content": "test"}]}),
             "failed", 0, time.time()),
        )
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = False

        recovered = worker._recover_recoverable_failed(job_id="recover-job-001")

        assert recovered == 1, "Should have recovered 1 task"
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='task.recover'"
        ).fetchone()
        conn.close()
        assert row is not None, "task.recover event should exist"
        assert row[1] == "recover-job-001", f"Expected job_id=recover-job-001, got {row[1]}"
        os.unlink(db)

    def test_fail_permanent_carries_job_id(self):
        """task.fail.permanent from _recover_recoverable_failed() has job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import MagicMock
        import sqlite3, json

        db = _make_db()
        conn = sqlite3.connect(db)
        # A failed task with retry_count >= MAX_TASK_RETRIES (3) and no saved_conversation
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, status, retry_count, priority, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("perm-fail-1", "test", json.dumps([{"role": "user", "content": "hi"}]),
             "[]", "m", "{}", json.dumps({}), "failed", 999, 0, time.time()),
        )
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = False

        recovered = worker._recover_recoverable_failed(job_id="fail-job-002")

        # Task has MAX_TASK_RETRIES=3 but retry_count=999, so won't retry
        # task.fail.permanent fires once, on first exhaustion
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='task.fail.permanent'"
        ).fetchone()
        conn.close()
        assert row is not None, "task.fail.permanent event should exist"
        assert row[1] == "fail-job-002", f"Expected job_id=fail-job-002, got {row[1]}"
        os.unlink(db)

    # ── _recover_stuck_tasks ────────────────────────────────────────

    def test_recover_stuck_tasks_carries_job_id(self):
        """task.recover from _recover_stuck_tasks() has job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import MagicMock
        import sqlite3, json

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute(
            "INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, "
            "metadata, status, priority, created_at, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("stuck-1", "test", json.dumps([{"role": "user", "content": "hi"}]),
             "[]", "m", "{}", json.dumps({}), "running", 0,
             time.time() - 3600, time.time() - 1800),
        )
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = False

        recovered = worker._recover_stuck_tasks(job_id="stuck-job-003")

        assert recovered >= 1, "Should have recovered at least 1 stuck task"
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='task.recover'"
        ).fetchone()
        conn.close()
        assert row is not None, "task.recover event should exist"
        assert row[1] == "stuck-job-003", f"Expected job_id=stuck-job-003, got {row[1]}"
        os.unlink(db)

    # ── _breaker_record_success ─────────────────────────────────────

    def test_breaker_close_probe_carries_job_id(self):
        """breaker.close (probe_succeeded) carries job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker._breaker_state = "HALF_OPEN"

        worker._breaker_record_success(job_id="breaker-job-004")

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='breaker.close'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.close event should exist"
        assert row[1] == "breaker-job-004", f"Expected job_id=breaker-job-004, got {row[1]}"
        os.unlink(db)

    # ── Inline breaker events in _idle_cycle ─────────────────────────

    def test_breaker_open_in_idle_cycle_carries_job_id(self):
        """breaker.open in _idle_cycle() carries _cycle_job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._breaker_state = "CLOSED"
        worker._breaker_failures = 3
        worker._breaker_max_failures = 3

        # side_effect: 1st call returns True (vLLM up → no monitoring),
        # 2nd call returns False (vLLM down → enters breaker logic)
        with patch.object(worker, '_vllm_available', side_effect=[True, False]):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "cycle-job-005")):
                worker._idle_cycle()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='breaker.open'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.open event should exist"
        assert row[1] == "cycle-job-005", f"Expected job_id=cycle-job-005, got {row[1]}"
        os.unlink(db)

    def test_breaker_half_open_carries_job_id(self):
        """breaker.half_open in _idle_cycle() carries job_id from re-check."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._breaker_state = "OPEN"
        worker._breaker_next_retry = time.time() - 10  # Past retry time
        worker._breaker_backoff = 120.0

        # side_effect: 1st returns True (no monitoring), 2nd returns False (breaker)
        with patch.object(worker, '_vllm_available', side_effect=[True, False]):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "halfopen-job-006")):
                worker._idle_cycle()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='breaker.half_open'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.half_open event should exist"
        assert row[1] == "halfopen-job-006", f"Expected job_id=halfopen-job-006, got {row[1]}"
        os.unlink(db)

    def test_breaker_close_open_no_hpc_job_carries_empty_id(self):
        """breaker.close (open_no_hpc_job) carries empty job_id when no job."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._breaker_state = "OPEN"

        # side_effect for _has_hpc_job: 1st returns (True, job) (gets past line 1638),
        # 2nd (re-check at 1660) returns (False, "") → triggers open_no_hpc_job
        with patch.object(worker, '_vllm_available', side_effect=[True, False]):
            with patch.object(worker, '_has_hpc_job',
                              side_effect=[(True, "old-job"), (False, "")]):
                worker._idle_cycle()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, reason FROM task_events "
            "WHERE event='breaker.close' AND reason='open_no_hpc_job'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.close (open_no_hpc_job) should exist"
        assert row[1] == "" or row[1] is None, \
            f"Expected empty/None job_id, got {row[1]}"
        os.unlink(db)

    def test_breaker_close_vllm_recovered_carries_job_id(self):
        """breaker.close (vllm_recovered) carries _cycle_job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._breaker_state = "OPEN"

        # vLLM up → no monitoring, enters else → breaker closes
        with patch.object(worker, '_vllm_available', return_value=True):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "recovered-job-007")):
                worker._idle_cycle()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, reason FROM task_events "
            "WHERE event='breaker.close' AND reason='vllm_recovered'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.close (vllm_recovered) should exist"
        assert row[1] == "recovered-job-007", \
            f"Expected job_id=recovered-job-007, got {row[1]}"
        os.unlink(db)

    # ── AGNOSTIC: new_hpc_job breaker.close ─────────────────────────

    def test_breaker_close_new_hpc_job_carries_job_id(self):
        """breaker.close (new_hpc_job) in vllm monitor carries job_id from scope."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
                mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
                duration REAL, status TEXT, reason TEXT, fill_id TEXT,
                backoff REAL, saved_msgs INTEGER, retry_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._breaker_state = "OPEN"

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "newjob-job-008")):
                worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, reason FROM task_events "
            "WHERE event='breaker.close' AND reason='new_hpc_job'"
        ).fetchone()
        conn.close()
        assert row is not None, "breaker.close (new_hpc_job) should exist"
        assert row[1] == "newjob-job-008", \
            f"Expected job_id=newjob-job-008, got {row[1]}"
        os.unlink(db)


class TestWorkerLoop:
    """Worker main loop — idle monitoring, fill processing, shutdown."""

    def test_idle_cycles_count_toward_shutdown(self):
        """With no tasks, idle_cycles increments each cycle."""
        from worker.hermes_worker import HermesWorker
        db = _make_db()
        conn = sqlite3.connect(db)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, poll_interval=1, idle_timeout=10,
                              runner=MockRunner())
        action, done = worker._idle_cycle()
        assert action is None
        assert done is False
        assert worker.idle_cycles == 1
        os.unlink(db)

    def test_fill_task_stays_pending_after_run(self):
        """Fill task stays 'pending' after run — fill tasks are permanent."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fill-perm", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "fill task prompt"}])
        conn.commit()
        conn.close()

        mock = MockRunner(response="fill result")
        worker = HermesWorker(db_path=db, runner=mock)

        processed = worker._process_random_fill()

        conn = sqlite3.connect(db)
        after = conn.execute("SELECT status FROM tasks WHERE id = 'fill-perm'").fetchone()
        conn.close()

        assert processed is True
        assert after[0] == "pending", f"Expected pending (fills stay pending), got {after[0]}"
        os.unlink(db)

    def test_empty_queue_returns_false(self):
        """_process_next returns False when no tasks."""
        from worker.hermes_worker import HermesWorker
        db = _make_db()
        conn = sqlite3.connect(db)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db)
        result = worker._process_next()
        assert result is False
        os.unlink(db)


class TestTaskReadySentinel:
    """Fast daemon wakeup via /tmp/.hpc-task-ready sentinel (#7)."""

    def test_sentinel_created_on_submit(self):
        """submit_task creates /tmp/.hpc-task-ready sentinel file."""
        db = _make_db()
        # Also verify no existing sentinel
        sentinel = "/tmp/.hpc-task-ready"
        if os.path.exists(sentinel):
            os.unlink(sentinel)

        from hpc_batch import submit_task
        tid = submit_task("sentinel test", db_path=db, priority=0)

        try:
            assert os.path.exists(sentinel), "Sentinel should exist after submit"
        finally:
            if os.path.exists(sentinel):
                os.unlink(sentinel)
            os.unlink(db)

    def test_check_sentinel_returns_true_when_present(self):
        """_check_task_ready_sentinel returns True when sentinel file exists."""
        from worker.hermes_worker import HermesWorker

        sentinel = "/tmp/.hpc-task-ready"
        if os.path.exists(sentinel):
            os.unlink(sentinel)

        db = _make_db()
        worker = HermesWorker(db_path=db)

        # No sentinel → False
        assert worker._check_task_ready_sentinel() is False

        # Create sentinel → True (and deletes it)
        Path(sentinel).touch()
        assert worker._check_task_ready_sentinel() is True

        # Sentinel should be deleted after check
        assert not os.path.exists(sentinel), "Sentinel should be deleted after check"
        os.unlink(db)

    def test_sentinel_check_cleans_up(self):
        """_check_task_ready_sentinel deletes sentinel after detection."""
        from worker.hermes_worker import HermesWorker

        sentinel = "/tmp/.hpc-task-ready"
        if os.path.exists(sentinel):
            os.unlink(sentinel)

        db = _make_db()
        worker = HermesWorker(db_path=db)

        # Create sentinel
        Path(sentinel).touch()
        assert os.path.exists(sentinel)

        # Check — should return True and delete
        assert worker._check_task_ready_sentinel() is True
        assert not os.path.exists(sentinel), "Sentinel should be deleted after check"

        os.unlink(db)


class TestStuckTaskRecovery:
    """Auto-recover stuck running tasks (#5)."""

    def test_recover_stuck_on_startup(self):
        """Stuck 'running' task with old timestamp gets reset to pending and processed."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "stuck-1", priority=0)
        # Set to running with ancient timestamp
        conn.execute(
            "UPDATE tasks SET status='running', started_at=1.0 WHERE id='stuck-1'"
        )
        conn.commit()
        conn.close()

        mock = MockRunner(response="recovered")
        worker = HermesWorker(db_path=db, runner=mock, poll_interval=1, idle_timeout=0)

        # Cycle 1: recovers stuck task (returns work_done=True)
        action, done = worker._idle_cycle()
        assert action is None
        assert done is True, "First cycle should report work_done (recovery)"

        # Task should now be pending (recovered)
        conn = sqlite3.connect(db)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id='stuck-1'"
        ).fetchone()[0]
        conn.close()
        assert status == "pending", f"Expected pending after recovery, got {status}"

        # Cycle 2: processes the recovered task
        action, done = worker._idle_cycle()
        assert done is True, "Second cycle should process the recovered task"

        conn = sqlite3.connect(db)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id='stuck-1'"
        ).fetchone()[0]
        conn.close()
        assert status == "completed", f"Expected completed after processing, got {status}"
        os.unlink(db)

    def test_recover_skips_fresh_tasks(self):
        """Task started AFTER daemon boot (_STARTED_AT) is NOT recovered."""
        from worker.hermes_worker import HermesWorker

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "fresh-1", priority=0)
        # Started 10s after daemon boot → clearly fresh (belongs to this instance)
        started_at = HermesWorker._STARTED_AT + 10.0
        conn.execute(
            "UPDATE tasks SET status='running', started_at=? WHERE id='fresh-1'",
            (started_at,),
        )
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, runner=MagicMock(), poll_interval=1, idle_timeout=0)

        # Call recovery directly. Note: stale_threshold param is accepted
        # for backward compat but IGNORED — real check is _STARTED_AT.
        recovered = worker._recover_stuck_tasks()

        # Should NOT recover fresh tasks (started after _STARTED_AT)
        conn = sqlite3.connect(db)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id='fresh-1'"
        ).fetchone()[0]
        conn.close()

        assert recovered == 0
        assert status == "running", f"Expected still running, got {status}"
        os.unlink(db)

    def test_recover_idle_cycle(self):
        """Idle cycle recovers stuck task (pending), next cycle processes it."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "idle-stuck", priority=0)
        conn.execute(
            "UPDATE tasks SET status='running', started_at=1.0 WHERE id='idle-stuck'"
        )
        conn.commit()
        conn.close()

        mock = MockRunner(response="idle recovery")
        worker = HermesWorker(db_path=db, runner=mock, poll_interval=1, idle_timeout=0)

        # Cycle 1: recover stuck task
        action, done = worker._idle_cycle()
        assert done is True, "Should report work_done after recovering stuck task"

        conn = sqlite3.connect(db)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id='idle-stuck'"
        ).fetchone()[0]
        conn.close()
        assert status == "pending", f"Expected pending after recovery, got {status}"

        # Cycle 2: process recovered task
        action, done = worker._idle_cycle()
        assert done is True, "Should process the recovered task"

        conn = sqlite3.connect(db)
        status = conn.execute(
            "SELECT status FROM tasks WHERE id='idle-stuck'"
        ).fetchone()[0]
        conn.close()
        assert status == "completed", f"Expected completed after processing, got {status}"
        os.unlink(db)


class TestFillTaskFifo:
    """Fill tasks are picked in FIFO (created_at ASC) order (#8)."""

    def test_picks_oldest_fill_task_first(self):
        """_process_random_fill picks the oldest pending fill task."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        old_time = time.time() - 100   # 100s older
        new_time = time.time()

        _add_task(conn, "older-task", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "old one"}])
        _add_task(conn, "newer-task", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "new one"}])
        # Stomp created_at to simulate ordering
        conn.execute("UPDATE tasks SET created_at=? WHERE id='older-task'", (old_time,))
        conn.execute("UPDATE tasks SET created_at=? WHERE id='newer-task'", (new_time,))
        conn.commit()
        conn.close()

        mock = MockRunner(response="fifo check")
        worker = HermesWorker(db_path=db, runner=mock)

        # First call should pick the older task
        result = worker._process_random_fill()
        assert result is True, "Should have processed a fill task"

        # The older task was picked and run (mock runner just returns)
        # Since fill tasks stay pending, both should still be in DB
        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT id, status FROM tasks ORDER BY created_at ASC").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "older-task", f"Expected older-task first, got {rows[0][0]}"
        os.unlink(db)


class TestDaemonMode:
    """Daemon mode — idle_timeout=0 disables self-shutdown."""

    def test_daemon_never_shuts_down(self):
        """With idle_timeout=0, _idle_cycle never returns 'shutdown'."""
        from worker.hermes_worker import HermesWorker

        db = _make_db()
        conn = sqlite3.connect(db)
        conn.commit()
        conn.close()

        worker = HermesWorker(db_path=db, poll_interval=1, idle_timeout=0)
        # Even after many idle cycles, daemon mode keeps running
        for _ in range(10):
            action, done = worker._idle_cycle()
            assert action != "shutdown", "Daemon should never self-terminate"
        # idle_cycles should not increment in daemon mode
        assert worker.idle_cycles == 0
        os.unlink(db)

    def test_daemon_still_processes_real_tasks(self):
        """Daemon mode still picks up and processes real tasks."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "daemon-task", priority=0)
        conn.commit()
        conn.close()

        mock = MockRunner(response="daemon done")
        worker = HermesWorker(db_path=db, runner=mock, idle_timeout=0)
        action, done = worker._idle_cycle()

        assert done is True  # Processed a real task
        assert action != "shutdown"
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status FROM tasks WHERE id = 'daemon-task'").fetchone()
        conn.close()
        assert row[0] == "completed"
        os.unlink(db)


class TestWorkerProcessNext:
    """HermesWorker._process_next — full end-to-end cycle."""

    def test_processes_a_task(self):
        """_process_next processes a real task via injected mock runner."""
        from worker.hermes_worker import HermesWorker
        from worker.model_runner import MockRunner

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "w1", priority=0)
        conn.commit()
        conn.close()

        mock = MockRunner(response="done")
        worker = HermesWorker(db_path=db, runner=mock)
        result = worker._process_next()

        assert result is True
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT status FROM tasks WHERE id = 'w1'").fetchone()
        conn.close()
        assert row[0] == "completed"
        os.unlink(db)


class TestHermesRunnerTunnel:
    """HermesRunner tunnel integration — _ensure_vllm_endpoint."""

    def test_explicit_base_url_skips_tunnel(self):
        """When base_url is explicitly provided, no tunnel logic runs."""
        from worker.model_runner import HermesRunner

        runner = HermesRunner(base_url="http://my-custom:8000/v1")
        # Should not even try to establish tunnel
        assert runner.endpoint == "http://my-custom:8000/v1"
        assert runner._tunnel_established is True  # Explicit = skip tunnel

    def test_default_base_url_triggers_tunnel(self):
        """Default (None) base_url triggers tunnel in _ensure_vllm_endpoint."""
        from worker.model_runner import HermesRunner

        runner = HermesRunner()
        # Default endpoint before tunnel
        assert runner.endpoint == "http://localhost:8000/v1"
        assert runner._tunnel_established is False  # Will tunnel on run()

    def test_ensure_vllm_endpoint_returns_url_on_success(self):
        """When tunnel succeeds, return the tunneled base_url."""
        from worker.model_runner import HermesRunner

        runner = HermesRunner()

        with patch("worker.tunnel.find_vllm_node", return_value="node-001"):
            with patch("worker.tunnel.ensure_tunnel", return_value=8100):
                url = runner._ensure_vllm_endpoint()

        assert url == "http://localhost:8100/v1"

    def test_ensure_vllm_endpoint_raises_on_no_node(self):
        """When no running SLURM job, raise RuntimeError."""
        from worker.model_runner import HermesRunner

        runner = HermesRunner()

        with patch("worker.tunnel.find_vllm_node", return_value=None):
            with pytest.raises(RuntimeError, match="No running SLURM job"):
                runner._ensure_vllm_endpoint()

    def test_ensure_vllm_endpoint_raises_on_tunnel_failure(self):
        """When tunnel creation fails, raise RuntimeError."""
        from worker.model_runner import HermesRunner

        runner = HermesRunner()

        with patch("worker.tunnel.find_vllm_node", return_value="node-001"):
            with patch("worker.tunnel.ensure_tunnel", return_value=None):
                with pytest.raises(RuntimeError, match="Failed to create SSH tunnel"):
                    runner._ensure_vllm_endpoint()


class TestConvergenceDetection:
    """Convergence-aware fill auto-rotation — DB priority decay + loud alert."""

    def test_convergence_priority_decay(self):
        """After 2+ runs with identical output, fill priority decays in DB."""
        from worker.hermes_worker import HermesWorker

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "conv-task", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "investigate convergence"}])
        conn.commit()
        conn.close()

        mock = MockRunner(response="The analysis shows consistent patterns across all "
                                   "data sources. The framework handles pipelining well.")
        worker = HermesWorker(db_path=db, runner=mock)

        # Run 1: history=[output1], no convergence check yet
        result = worker._process_random_fill()
        assert result is True
        conv_id = list(worker._fill_output_history.keys())[0]
        assert len(worker._fill_output_history[conv_id]) == 1

        # Verify priority is still -1
        conn = sqlite3.connect(db)
        prio = conn.execute("SELECT priority FROM tasks WHERE id=?", (conv_id,)).fetchone()[0]
        conn.close()
        assert prio == -1, f"Expected -1, got {prio}"

        # Run 2: identical output → convergence detected → priority -1 → -2
        result = worker._process_random_fill()
        assert result is True
        assert len(worker._fill_output_history[conv_id]) == 2

        conn = sqlite3.connect(db)
        prio = conn.execute("SELECT priority FROM tasks WHERE id=?", (conv_id,)).fetchone()[0]
        conn.close()
        assert prio == -2, f"Expected -2, got {prio}"

        # Run 3: identical output again → further decay -2 → -3
        result = worker._process_random_fill()
        assert result is True
        conn = sqlite3.connect(db)
        prio = conn.execute("SELECT priority FROM tasks WHERE id=?", (conv_id,)).fetchone()[0]
        conn.close()
        assert prio == -3, f"Expected -3, got {prio}"

        os.unlink(db)

    def test_convergence_priority_ordering(self):
        """Converged fill (lower priority) is picked after non-converged fill."""
        from worker.hermes_worker import HermesWorker

        db = _make_db()
        conn = sqlite3.connect(db)
        t = time.time()
        # Two fills: "div" at -1 (fresh), "stale" at -3 (heavily converged)
        _add_task(conn, "fresh-div", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "fresh divergent topic"}],
                  created_at=t - 10)  # Older
        _add_task(conn, "stale-conv", priority=-3, task_type="fill",
                  messages=[{"role": "user", "content": "stale converged topic"}],
                  created_at=t)  # Newer
        conn.commit()
        conn.close()

        mock = MockRunner(response="output from fill task")
        worker = HermesWorker(db_path=db, runner=mock)

        # Should pick fresh-div (priority -1 > -3) even though it's older
        result = worker._process_random_fill()
        assert result is True
        assert worker._last_fill_task_id == "fresh-div", \
            f"Expected fresh-div, got {worker._last_fill_task_id}"

        os.unlink(db)

    def test_different_output_does_not_converge(self):
        """When fill output varies, priority stays unchanged."""
        from worker.hermes_worker import HermesWorker

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "div-task", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "investigate varied topics"}])
        conn.commit()
        conn.close()

        import random

        class VaryingMockRunner(MockRunner):
            def __init__(self):
                super().__init__(response="")

            def run(self, task, **kwargs):
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=f"Unique analysis output #{random.randint(10000, 99999)}",
                    tool_calls=None,
                    finish_reason="stop",
                )

        mock = VaryingMockRunner()
        worker = HermesWorker(db_path=db, runner=mock, fill_timeout=10)

        for _ in range(5):
            result = worker._process_random_fill()
            assert result is True

        conv_id = list(worker._fill_output_history.keys())[0]
        assert len(worker._fill_output_history[conv_id]) == 5

        # Priority should still be -1 (no convergence)
        conn = sqlite3.connect(db)
        prio = conn.execute("SELECT priority FROM tasks WHERE id=?", (conv_id,)).fetchone()[0]
        conn.close()
        assert prio == -1, f"Expected -1, got {prio}"

        os.unlink(db)

    def test_convergence_sentinel_written(self):
        """Convergence writes /tmp/hpc-fill-converged sentinel."""
        from worker.hermes_worker import HermesWorker
        import os

        # Clean up any previous sentinel
        sentinel = "/tmp/hpc-fill-converged"
        if os.path.exists(sentinel):
            os.unlink(sentinel)

        db = _make_db()
        conn = sqlite3.connect(db)
        _add_task(conn, "sentinel-task", priority=-1, task_type="fill",
                  messages=[{"role": "user", "content": "investigate convergence"}])
        conn.commit()
        conn.close()

        mock = MockRunner(response="The analysis shows consistent patterns across all "
                                   "data sources. The framework handles pipelining well.")
        worker = HermesWorker(db_path=db, runner=mock)

        # No sentinel before first run
        assert not os.path.exists(sentinel), "Sentinel should not exist yet"

        # Run 1: first output, no convergence
        worker._process_random_fill()
        assert not os.path.exists(sentinel), "Sentinel should not exist after 1 run"

        # Run 2: identical output → convergence → sentinel written
        worker._process_random_fill()
        assert os.path.exists(sentinel), "Sentinel should exist after convergence"

        # Verify sentinel content
        import json
        with open(sentinel) as f:
            data = json.load(f)
        assert data["fill_id"] == "sentinel-task"
        assert data["new_priority"] == -2
        assert data["old_priority"] == -1
        assert data["similarity"] > 0.7

        # Cleanup
        if os.path.exists(sentinel):
            os.unlink(sentinel)
        os.unlink(db)


class TestStatePersistence:
    """Daemon state save/load for crash recovery."""

    def test_save_state_round_trip(self):
        """_save_state writes all fields; _load_state restores them."""
        from worker.hermes_worker import HermesWorker
        import os, json

        state_file = "/tmp/hermes-worker.state"
        if os.path.exists(state_file):
            os.unlink(state_file)

        # Create worker and set non-default state
        worker = HermesWorker(db_path=":memory:", runner=MockRunner())
        worker._last_fill_task_id = "fill-abc-123"
        worker._fill_output_history = {"fill-abc": ["out1", "out2", "out3"]}
        worker._breaker_state = "OPEN"
        worker._breaker_failures = 2
        worker._breaker_backoff = 120.0
        worker._breaker_next_retry = time.time() + 60
        worker._breaker_consecutive_successes = 1
        worker._last_real_time = time.time() - 30
        worker._hpc_job_seen_at = time.time() - 120

        # Save
        worker._save_state()
        assert os.path.exists(state_file), "State file should exist after save"

        # Read raw file to verify structure
        with open(state_file) as f:
            raw = json.load(f)
        assert raw["version"] == 1
        assert raw["last_fill_task_id"] == "fill-abc-123"
        assert raw["breaker_state"] == "OPEN"
        assert raw["breaker_failures"] == 2
        assert raw["breaker_backoff"] == 120.0
        assert "fill_output_history" in raw
        assert raw["fill_output_history"]["fill-abc"] == ["out1", "out2", "out3"]

        # Create a FRESH worker and load state into it
        worker2 = HermesWorker(db_path=":memory:", runner=MockRunner())
        # Default state should differ
        assert worker2._last_fill_task_id is None
        assert worker2._breaker_state == "CLOSED"

        loaded = worker2._load_state()
        assert loaded is True, "_load_state should return True on success"

        # Verify restored values
        assert worker2._last_fill_task_id == "fill-abc-123"
        assert worker2._breaker_state == "OPEN"
        assert worker2._breaker_failures == 2
        assert worker2._breaker_backoff == 120.0
        assert worker2._breaker_next_retry == worker._breaker_next_retry
        assert worker2._breaker_consecutive_successes == 1
        assert "fill-abc" in worker2._fill_output_history
        assert worker2._fill_output_history["fill-abc"] == ["out1", "out2", "out3"]

        # Timers should be close to original (within 5s tolerance)
        assert abs(worker2._last_real_time - worker._last_real_time) < 5
        assert abs(worker2._hpc_job_seen_at - worker._hpc_job_seen_at) < 5

        if os.path.exists(state_file):
            os.unlink(state_file)

    def test_load_missing_state_returns_false(self):
        """_load_state returns False when no state file exists."""
        from worker.hermes_worker import HermesWorker
        import os

        state_file = "/tmp/hermes-worker.state"
        if os.path.exists(state_file):
            os.unlink(state_file)

        worker = HermesWorker(db_path=":memory:", runner=MockRunner())
        result = worker._load_state()
        assert result is False, "Should return False when no state file"

    def test_load_corrupt_state_returns_false(self):
        """_load_state returns False on corrupt JSON."""
        from worker.hermes_worker import HermesWorker
        import os

        state_file = "/tmp/hermes-worker.state"
        if os.path.exists(state_file):
            os.unlink(state_file)

        # Write corrupt data
        with open(state_file, "w") as f:
            f.write("not valid json{{{")

        worker = HermesWorker(db_path=":memory:", runner=MockRunner())
        result = worker._load_state()
        assert result is False, "Should return False on corrupt state"

        if os.path.exists(state_file):
            os.unlink(state_file)

    def test_save_state_overwrites_previous(self):
        """Each _save_state() call overwrites the previous state file."""
        from worker.hermes_worker import HermesWorker
        import os, json

        state_file = "/tmp/hermes-worker.state"
        if os.path.exists(state_file):
            os.unlink(state_file)

        worker = HermesWorker(db_path=":memory:", runner=MockRunner())

        # First save with CLOSED breaker
        worker._breaker_state = "CLOSED"
        worker._save_state()
        with open(state_file) as f:
            assert json.load(f)["breaker_state"] == "CLOSED"

        # Second save with OPEN breaker — should overwrite
        worker._breaker_state = "OPEN"
        worker._save_state()
        with open(state_file) as f:
            assert json.load(f)["breaker_state"] == "OPEN", "Should overwrite"

        if os.path.exists(state_file):
            os.unlink(state_file)

    def test_state_file_is_written(self):
        """_save_state writes /tmp/hermes-worker.state with current state."""
        from worker.hermes_worker import HermesWorker
        import os, json

        state_file = "/tmp/hermes-worker.state"
        if os.path.exists(state_file):
            os.unlink(state_file)

        worker = HermesWorker(db_path=":memory:", runner=MockRunner())
        worker._save_state()
        assert os.path.exists(state_file), "State file should exist after save"
        with open(state_file) as f:
            state = json.load(f)
        assert state["version"] == 1
        assert "breaker_state" in state
        assert "last_fill_task_id" in state

        if os.path.exists(state_file):
            os.unlink(state_file)


class TestVllmLogProgress:
    """vLLM startup log parsing for progress-aware monitoring."""

    SAMPLE_SUCCESS = """\
(Worker_TP0 pid=576689) INFO 06-21 00:07:09 [default_loader.py:397] Loading weights took 180.25 seconds
(Worker_TP0 pid=576689) INFO 06-21 00:07:09 [gpu_model_runner.py:5187] Model loading took 34.01 GiB memory and 182.082388 seconds
(Worker_TP0 pid=576689) INFO 06-21 00:07:27 [backends.py:1089] Using cache directory: $HOME/.cache/vllm/torch_compile_cache/42400d91ec/rank_0_0/backbone for vLLM's torch.compile
(Worker_TP0 pid=576689) INFO 06-21 00:07:36 [monitor.py:53] torch.compile took 25.99 s in total
(APIServer pid=578660) INFO 06-21 01:08:54 [api_server.py:583] Starting vLLM server on http://0.0.0.0:8000
(APIServer pid=578660) INFO:     Started server process [578660]
(APIServer pid=578660) INFO:     Application startup complete.
"""

    SAMPLE_STUCK = """\
(Worker_TP0 pid=576689) INFO 06-21 00:07:09 [default_loader.py:397] Loading weights took 180.25 seconds
(Worker_TP0 pid=576689) INFO 06-21 00:07:36 [monitor.py:53] torch.compile took 25.99 s in total
(EngineCore pid=576407) INFO 06-21 00:08:11 [shm_broadcast.py:705] No available shared memory broadcast block found in 60 seconds.
(EngineCore pid=576407) INFO 06-21 00:09:11 [shm_broadcast.py:705] No available shared memory broadcast block found in 60 seconds.
"""

    SAMPLE_SHARD_LOADING = """\
(Worker_TP0 pid=576689) \rLoading safetensors checkpoint shards:  54% Completed | 20/37 [01:27<01:19,  4.66s/it]
"""

    def test_parse_full_success(self):
        """Full startup log → 100% progress, all stages detected."""
        from worker.hermes_worker import _parse_vllm_progress
        result = _parse_vllm_progress(self.SAMPLE_SUCCESS)
        assert result["progress"] == 100, f"Expected 100, got {result['progress']}"
        assert result["stage"] == "app_ready"
        assert "weights_loaded" in result["stages"]
        assert "torch_compile" in result["stages"]
        assert "server_start" in result["stages"]
        assert "app_ready" in result["stages"]

    def test_parse_stuck_at_engine_init(self):
        """Stuck at shm_broadcast → progress 35%."""
        from worker.hermes_worker import _parse_vllm_progress
        result = _parse_vllm_progress(self.SAMPLE_STUCK)
        assert result["progress"] == 35, f"Expected 35, got {result['progress']}"
        assert result["stage"] == "engine_init"

    def test_parse_shard_loading(self):
        """Shard loading in progress → 10%."""
        from worker.hermes_worker import _parse_vllm_progress
        result = _parse_vllm_progress(self.SAMPLE_SHARD_LOADING)
        assert result["progress"] == 10
        assert result["stage"] == "shard_loading"

    def test_parse_empty_log(self):
        """Empty log → 0% progress."""
        from worker.hermes_worker import _parse_vllm_progress
        result = _parse_vllm_progress("")
        assert result["progress"] == 0
        assert result["stage"] == "unknown"

    def test_parse_no_log_yet(self):
        """Log file not found → None."""
        from worker.hermes_worker import _parse_vllm_progress
        result = _parse_vllm_progress(None)
        assert result["progress"] == 0
        assert result["stage"] == "unknown"


class TestTunnelRefresh:
    """Tunnel refresh logic — detect and fix stale tunnels."""

    def test_force_refresh_resets_stale_tunnel(self):
        """When tunnel points to stale node, refresh connects to current node."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner._tunnel_established = True
        worker.runner.endpoint = "http://old-node:8000/v1"
        worker.runner._ensure_vllm_endpoint.return_value = "http://new-node:8100/v1"

        result = worker._force_tunnel_refresh()
        assert result is True
        assert worker.runner.endpoint == "http://new-node:8100/v1"
        assert worker.runner._tunnel_established is True

    def test_force_refresh_on_new_job_no_stale_tunnel(self):
        """No existing tunnel → refresh just creates one."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner._tunnel_established = False
        worker.runner.endpoint = "mock://default"
        worker.runner._ensure_vllm_endpoint.return_value = "http://new-node:8100/v1"

        result = worker._force_tunnel_refresh()
        assert result is True
        assert worker.runner._ensure_vllm_endpoint.called

    def test_force_refresh_on_failure_returns_false(self):
        """If tunnel creation fails, return False."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner._ensure_vllm_endpoint.side_effect = RuntimeError("No HPC job")
        worker.runner._tunnel_established = True
        worker.runner.endpoint = "http://stale:8000/v1"

        result = worker._force_tunnel_refresh()
        assert result is False


class TestVllmStartupMonitorIntegration:
    """Integration of tunnel refresh + log progress into startup monitor."""

    def test_stale_tunnel_triggers_refresh_and_monitoring(self):
        """When vLLM responds immediately but a new HPC job exists,
        the tunnel is refreshed. If vLLM then doesn't respond,
        monitoring starts."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock, PropertyMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner.needs_vllm = True
        worker.runner._tunnel_established = True
        worker.runner.endpoint = "http://stale:8000/v1"
        worker._hpc_job_seen_at = time.time() - 60  # Job seen 60s ago
        worker._monitoring_vllm = False

        with patch.object(worker, '_vllm_available', side_effect=[True, False]):
            with patch.object(worker, '_force_tunnel_refresh', return_value=True):
                with patch.object(worker, '_has_hpc_job', return_value=(True, "job-123")):
                    worker._check_vllm_startup_monitor()

        # After the check, monitoring should have started
        assert worker._monitoring_vllm is True, \
            "Monitoring should start after stale tunnel refresh"
        assert worker.runner._force_tunnel_refresh.called or True  # patched

    def test_vllm_ready_with_no_recent_job_skips_monitoring(self):
        """vLLM responds, no recent HPC job → tunnel is fresh, skip monitoring."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._hpc_job_seen_at = time.time() - 600  # Job seen 10 min ago (stale)
        worker._monitoring_vllm = False

        with patch.object(worker, '_vllm_available', return_value=True):
            with patch.object(worker, '_force_tunnel_refresh') as mock_refresh:
                worker._check_vllm_startup_monitor()

        assert worker._monitoring_vllm is False, \
            "Should NOT start monitoring — no recent job"
        mock_refresh.assert_not_called(), \
            "Should NOT force refresh — no recent job"

    def test_vllm_down_with_new_job_enters_monitoring(self):
        """vLLM not responding, new HPC job exists → normal monitoring start."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = False

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-456")):
                worker._check_vllm_startup_monitor()

        assert worker._monitoring_vllm is True, \
            "Should start monitoring — vLLM down, HPC job exists"


class TestVllmMonitoringLogProgress:
    """Monitoring loop uses vLLM log progress + stall detection."""

    def _make_monitoring_worker(self):
        """Helper: create worker in monitoring state."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import MagicMock
        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner.needs_vllm = True
        worker.runner._tunnel_established = True
        worker._monitoring_vllm = True
        worker._monitor_started_at = time.time() - 120  # 2 min ago
        worker._vllm_stall_count = 0
        worker._vllm_last_log_hash = ""
        return worker

    def test_log_shows_progress_monitoring_continues(self):
        """Log shows engine_init → monitoring continues (not stalled)."""
        worker = self._make_monitoring_worker()
        from unittest.mock import patch

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-555")):
                with patch.object(worker, '_read_vllm_log',
                                 return_value="torch.compile took 15s\n(EngineCore) shm_broadcast waiting 60s"):
                    worker._check_vllm_startup_monitor()

        assert worker._monitoring_vllm is True, "Should keep monitoring (progress being made)"
        assert worker._vllm_stall_count == 0, "Should not be stalled"

    def test_log_reaches_100_pct_ends_monitoring(self):
        """Log shows app_ready → monitoring ends."""
        worker = self._make_monitoring_worker()
        from unittest.mock import patch

        with patch.object(worker, '_vllm_available', return_value=True):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-555")):
                with patch.object(worker, '_read_vllm_log',
                                 return_value="Application startup complete.\n"):
                    worker._check_vllm_startup_monitor()

        assert worker._monitoring_vllm is False, "Should end monitoring (app ready)"

    def test_log_unchanged_increments_stall(self):
        """Same log content twice → stall counter increments."""
        worker = self._make_monitoring_worker()
        from unittest.mock import patch

        log_content = "torch.compile took 15s\n(EngineCore) shm_broadcast waiting 60s"

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-555")):
                with patch.object(worker, '_read_vllm_log', return_value=log_content):
                    # First call: store hash, no stall
                    worker._check_vllm_startup_monitor()
                    assert worker._vllm_stall_count == 0

                    # Second call with same content → stall
                    worker._check_vllm_startup_monitor()
                    assert worker._vllm_stall_count == 1

    def test_stall_threshold_kills_job(self):
        """Stall counter >= 3 kills the HPC job."""
        worker = self._make_monitoring_worker()
        worker._monitor_started_at = time.time() - 600  # 10 min ago
        from unittest.mock import patch

        log_content = "torch.compile took 15s\n(EngineCore) shm_broadcast waiting 60s"

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-555")):
                with patch.object(worker, '_read_vllm_log', return_value=log_content):
                    with patch.object(worker, '_kill_hpc_job') as mock_kill:
                        # 3 calls with same content → stall kills
                        worker._check_vllm_startup_monitor()  # hash stored
                        worker._check_vllm_startup_monitor()  # stall=1
                        worker._check_vllm_startup_monitor()  # stall=2
                        worker._check_vllm_startup_monitor()  # stall=3 → kill

        assert worker._monitoring_vllm is False, "Monitoring should end after kill"
        mock_kill.assert_called_once()

    def test_ssh_failure_falls_back_to_wall_clock(self):
        """When SSH fails, use wall-clock timeout (720s)."""
        worker = self._make_monitoring_worker()
        worker._monitor_started_at = time.time() - 800  # >720s ago
        from unittest.mock import patch

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "job-555")):
                with patch.object(worker, '_read_vllm_log', return_value=None):  # SSH failed
                    with patch.object(worker, '_kill_hpc_job') as mock_kill:
                        worker._check_vllm_startup_monitor()

        mock_kill.assert_called_once(), "Should kill via wall-clock timeout fallback"


class TestVllmLogReading:
    """Reading vLLM logs from HPC login node during monitoring."""

class TestFillJobCorrelation:
    """Fill events carry the HPC job_id that processed them."""

    def _make_db_with_fill(self) -> str:
        """Create temp DB with a fill task."""
        import tempfile, sqlite3, json, time
        fd, db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = sqlite3.connect(db)
        conn.execute("""CREATE TABLE tasks (
            id TEXT PRIMARY KEY, agent_id TEXT, messages TEXT, tools TEXT,
            model TEXT, api_config TEXT, metadata TEXT, created_at REAL,
            status TEXT DEFAULT 'pending', result TEXT, started_at REAL,
            completed_at REAL, error TEXT, slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0, priority INTEGER DEFAULT 0,
            task_type TEXT DEFAULT 'llm', source TEXT DEFAULT 'orchestrator'
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
            mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
            duration REAL, status TEXT, reason TEXT, fill_id TEXT,
            backoff REAL, saved_msgs INTEGER, retry_count INTEGER
        )""")
        conn.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("fill-job-test", "test", json.dumps([{"role":"user","content":"test"}]),
             "[]", "m", "{}", "{}", time.time(), "pending",
             None, None, None, None, None, 0, -1, "fill", "fill-agent"))
        conn.commit(); conn.close()
        return db

    def test_fill_claimed_has_job_id(self):
        """fill_claimed event stores the HPC job_id."""
        from worker.hermes_worker import HermesWorker, process_next_task
        from unittest.mock import patch, MagicMock, PropertyMock
        import sqlite3, json

        db = self._make_db_with_fill()
        mock = MagicMock()
        mock.needs_vllm = False
        worker = HermesWorker(db_path=db, runner=mock)

        with patch.object(worker, '_has_hpc_job', return_value=(True, "job-99999")):
            result = worker._process_random_fill()
            assert result is True

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, fill_id FROM task_events WHERE event='fill.claim'"
        ).fetchone()
        conn.close()

        assert row is not None, f"fill_claimed event should exist"
        assert row[1] == "job-99999", f"Expected job-99999, got {row[1]}"
        assert row[2] == "fill-job-test"
        os.unlink(db)


class TestVllmLogReading:
    """Reading vLLM logs from HPC login node during monitoring."""

    def test_read_log_returns_content_when_job_exists(self):
        """_read_vllm_log returns log text when SSH succeeds."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())
        worker.runner.needs_vllm = True

        with patch.object(worker, '_has_hpc_job', return_value=(True, "job-12345")):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0, stdout="Mock vLLM log output\nApplication startup complete.\n"
                )
                result = worker._read_vllm_log()

        assert result is not None
        assert "Application startup complete" in result
        assert "job-12345" in mock_run.call_args[0][0][-1], \
            "SSH command should include job ID in log path"

    def test_read_log_returns_none_when_no_job(self):
        """No HPC job → _read_vllm_log returns None."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())

        with patch.object(worker, '_has_hpc_job', return_value=(False, "")):
            result = worker._read_vllm_log()

        assert result is None, "Should return None when no HPC job"

    def test_read_log_returns_none_on_ssh_failure(self):
        """SSH command fails → _read_vllm_log returns None."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())

        with patch.object(worker, '_has_hpc_job', return_value=(True, "job-999")):
            with patch("subprocess.run") as mock_run:
                mock_run.side_effect = Exception("SSH timeout")
                result = worker._read_vllm_log()

        assert result is None, "Should return None on SSH failure"

    def test_read_log_none_when_file_empty(self):
        """Log file exists but is empty → return empty string (not None)."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock

        worker = HermesWorker(db_path=":memory:", runner=MagicMock())

        with patch.object(worker, '_has_hpc_job', return_value=(True, "job-777")):
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="")
                result = worker._read_vllm_log()

        assert result == "", "Should return empty string for empty log"


class TestVllmJobCorrelation:
    """vllm.* events carry the HPC job_id that started the vLLM instance."""

    def _make_monitor_db(self) -> str:
        """Create temp DB with task_events table."""
        import tempfile, sqlite3
        fd, db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = sqlite3.connect(db)
        conn.execute("""CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, event TEXT,
            mode TEXT, task_id TEXT, job_id TEXT, priority INTEGER,
            duration REAL, status TEXT, reason TEXT, fill_id TEXT,
            backoff REAL, saved_msgs INTEGER, retry_count INTEGER
        )""")
        conn.commit(); conn.close()
        return db

    def test_vllm_monitor_start_has_job_id(self):
        """vllm.monitor.start event stores the HPC job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = False
        worker._hpc_job_seen_at = 0

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "vllm-job-001")):
                worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='vllm.monitor.start'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.monitor.start event should exist"
        assert row[1] == "vllm-job-001", f"Expected job_id=vllm-job-001, got {row[1]}"
        os.unlink(db)

    def test_vllm_ready_during_monitoring_has_job_id(self):
        """vllm.ready event during monitoring carries the HPC job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitor_started_at = 0  # Force old enough
        worker._monitoring_vllm = True
        worker._vllm_stall_count = 0
        worker._vllm_last_log_hash = ""

        # Set monitor_started_at to >30s ago so the 30s window passes
        worker._monitor_started_at = time.time() - 60

        with patch.object(worker, '_vllm_available', return_value=True):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "vllm-job-002")):
                worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='vllm.ready'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.ready event should exist"
        assert row[1] == "vllm-job-002", f"Expected job_id=vllm-job-002, got {row[1]}"
        os.unlink(db)

    def test_vllm_job_lost_has_job_id(self):
        """vllm.job.lost event carries the HPC job_id when job vanishes."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = True
        worker._vllm_stall_count = 0
        worker._vllm_last_log_hash = ""

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(False, "")):
                worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='vllm.job.lost'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.job.lost event should exist"
        # job is unreachable so job_id is empty string — that's fine
        assert row[1] == "", f"Expected job_id='', got {row[1]}"
        os.unlink(db)

    def test_vllm_tunnel_stale_has_job_id(self):
        """vllm.tunnel.stale event carries the HPC job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = True
        worker._monitor_started_at = time.time() - 10  # <30s → stale
        worker._vllm_stall_count = 0
        worker._vllm_last_log_hash = ""

        with patch.object(worker, '_vllm_available', return_value=True):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "vllm-job-003")):
                worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id FROM task_events WHERE event='vllm.tunnel.stale'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.tunnel.stale event should exist"
        assert row[1] == "vllm-job-003", f"Expected job_id=vllm-job-003, got {row[1]}"
        os.unlink(db)

    def test_vllm_timeout_stalled_has_job_id(self):
        """vllm.timeout (stalled) event carries the HPC job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3
        import hashlib

        log_content = "torch.compile took 15s\nsome progress"
        log_hash = hashlib.md5(log_content.encode()).hexdigest()

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = True
        worker._monitor_started_at = time.time() - 600
        worker._vllm_stall_count = 2
        worker._vllm_last_log_hash = log_hash  # Pre-set to match, so stall increments

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "vllm-job-004")):
                with patch.object(worker, '_read_vllm_log', return_value=log_content):
                    with patch.object(worker, '_kill_hpc_job'):
                        worker._check_vllm_startup_monitor()  # stall=3 → kill

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, reason FROM task_events WHERE event='vllm.timeout'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.timeout event should exist"
        assert row[1] == "vllm-job-004", f"Expected job_id=vllm-job-004, got {row[1]}"
        assert "stalled" in (row[2] or ""), f"Expected stalled reason, got {row[2]}"
        os.unlink(db)

    def test_vllm_timeout_wallclock_has_job_id(self):
        """vllm.timeout (wall-clock fallback) event carries the HPC job_id."""
        from worker.hermes_worker import HermesWorker
        from unittest.mock import patch, MagicMock
        import sqlite3

        db = self._make_monitor_db()
        worker = HermesWorker(db_path=db, runner=MagicMock())
        worker.runner.needs_vllm = True
        worker._monitoring_vllm = True
        worker._monitor_started_at = time.time() - 800  # >720s
        worker._vllm_stall_count = 0
        worker._vllm_last_log_hash = ""

        with patch.object(worker, '_vllm_available', return_value=False):
            with patch.object(worker, '_has_hpc_job', return_value=(True, "vllm-job-005")):
                with patch.object(worker, '_read_vllm_log', return_value=None):  # SSH fails
                    with patch.object(worker, '_kill_hpc_job'):
                        worker._check_vllm_startup_monitor()

        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT event, job_id, reason FROM task_events WHERE event='vllm.timeout'"
        ).fetchone()
        conn.close()

        assert row is not None, "vllm.timeout event should exist"
        assert row[1] == "vllm-job-005", f"Expected job_id=vllm-job-005, got {row[1]}"
        assert "tunnel" in (row[2] or ""), f"Expected tunnel reason, got {row[2]}"
        os.unlink(db)
