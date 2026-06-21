"""CLI tests for DAG dependency system."""
import pytest
from argparse import Namespace
from hpc_batch import cmd_submit, submit_task, init_db
from conftest import dequeue_pending, complete_task
import json


class TestCliDependsOn:
    """Test the --depends-on CLI flag."""

    def test_cli_depends_on_creates_dep(self, db_path):
        """Submit with --depends-on creates task_deps entries."""
        # First, use the API to submit task A to get its ID
        task_a = submit_task("A prompt", db_path=db_path)

        # Now simulate CLI submit for B with --depends-on
        args = Namespace(
            db=db_path,
            prompt="B prompt",
            prompt_file=None,
            file=None,
            type="generic",
            model="test-model",
            cycle=None,
            priority=0,
            depends_on=[task_a],
        )
        ids = cmd_submit(args)

        # Verify B is blocked by A
        ready = dequeue_pending(db_path)
        ready_ids = [t["id"] for t in ready]
        assert task_a in ready_ids
        assert ids[0] not in ready_ids, "B should be blocked by A"

    def test_cli_depends_on_multiple(self, db_path):
        """Submit with multiple --depends-on flags."""
        a = submit_task("A", db_path=db_path)
        b = submit_task("B", db_path=db_path)

        args = Namespace(
            db=db_path,
            prompt="C prompt",
            prompt_file=None,
            file=None,
            type="generic",
            model="test-model",
            cycle=None,
            priority=0,
            depends_on=[a, b],
        )
        ids = cmd_submit(args)
        task_c = ids[0]

        # Complete A only → C still blocked
        complete_task(db_path, a)
        assert task_c not in [t["id"] for t in dequeue_pending(db_path)]

        # Complete B → C ready
        complete_task(db_path, b)
        assert task_c in [t["id"] for t in dequeue_pending(db_path)]

    def test_cli_depends_on_none_by_default(self, db_path):
        """Submit without --depends-on should work as before."""
        a = submit_task("A", db_path=db_path)

        args = Namespace(
            db=db_path,
            prompt="B prompt",
            prompt_file=None,
            file=None,
            type="generic",
            model="test-model",
            cycle=None,
            priority=0,
            depends_on=[],
        )
        ids = cmd_submit(args)

        # Both should be ready (no deps)
        ready = dequeue_pending(db_path)
        ready_ids = [t["id"] for t in ready]
        assert a in ready_ids
        assert ids[0] in ready_ids


class TestDiagCommand:
    """hpc_batch.py diag command — system diagnostics + job tracing."""

    def test_diag_job_trace_shows_events(self, db_path):
        """diag --job <id> shows chrono event sequence for a job."""
        import sqlite3, json, time
        from hpc_batch import log_db_event, cmd_diag
        from argparse import Namespace
        import io, sys

        # Init DB schema + insert test events for a known job
        init_db(db_path)
        ts = time.time()
        conn = sqlite3.connect(db_path)
        conn.executescript(f"""
            INSERT INTO task_events (ts, event, mode, job_id, duration, reason)
            VALUES ({ts}, 'hpc.submit', 'qos=preemptqos', 'diag-job-1', NULL, NULL);
            INSERT INTO task_events (ts, event, mode, job_id, duration, reason)
            VALUES ({ts+60}, 'vllm.monitor.start', 'qos=preemptqos', 'diag-job-1', NULL, NULL);
            INSERT INTO task_events (ts, event, mode, job_id, duration, reason)
            VALUES ({ts+368}, 'vllm.ready', 'qos=preemptqos', 'diag-job-1', 368.0, NULL);
            INSERT INTO task_events (ts, event, mode, fill_id, job_id, duration, reason)
            VALUES ({ts+370}, 'fill.claim', 'qos=preemptqos', 'fill-abc', 'diag-job-1', NULL, NULL);
            INSERT INTO task_events (ts, event, mode, fill_id, job_id, duration, reason)
            VALUES ({ts+700}, 'fill.complete', 'qos=preemptqos', 'fill-abc', 'diag-job-1', 330.0, NULL);
            INSERT INTO task_events (ts, event, mode, job_id, duration, reason)
            VALUES ({ts+912}, 'hpc.kill', 'qos=preemptqos', 'diag-job-1', 542.0, 'idle_timeout');
        """)
        conn.commit(); conn.close()

        # Capture stdout
        out = io.StringIO()
        old = sys.stdout
        sys.stdout = out
        try:
            cmd_diag(Namespace(db=db_path, job="diag-job-1", recent=False, fills=False))
        finally:
            sys.stdout = old

        output = out.getvalue()
        # Check key events appear in order
        assert "diag-job-1" in output, "Job ID should be shown"
        assert "hpc.submit" in output, "hpc.submit event should appear"
        assert "vllm.ready" in output, "vllm.ready event should appear"
        assert "fill.claim" in output, "fill.claim event should appear"
        assert "+368s" in output, "Duration should be shown"
        assert "idle_timeout" in output, "Reason should be shown"

    def test_diag_default_returns_string(self, db_path):
        """diag default path returns health check output without crashing."""
        from hpc_batch import cmd_diag_default
        import io, sys

        # Init DB so checks have something to work with
        init_db(db_path)

        result = cmd_diag_default(db_path=db_path)

        assert isinstance(result, str), f"Expected string, got {type(result)}"
        assert len(result) > 100, "Health check output too short"
        # Should contain common markers
        assert "Database" in result, "Should include DB check"
        assert "Daemon" in result, "Should include daemon check"
        assert "ALL CHECKS PASSED" in result or "failures" in result, \
            "Should include pass/fail summary"

    def test_diag_default_json(self, db_path):
        """diag default --json returns a dict."""
        from hpc_batch import cmd_diag_default

        init_db(db_path)

        result = cmd_diag_default(db_path=db_path, use_json=True)

        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        assert "checks" in result, "JSON output should have 'checks' key"
        assert "healthy" in result, "JSON output should have 'healthy' key"


class TestMigrateEvents:
    """hpc_batch.py migrate-events — backfill old event names."""

    EVENT_MAP = {
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

    def _seed_old_events(self, db_path: str):
        """Insert one row per old event name."""
        import sqlite3, time
        conn = sqlite3.connect(db_path)
        for old_name in self.EVENT_MAP:
            conn.execute(
                "INSERT INTO task_events (ts, event, mode) VALUES (?, ?, ?)",
                (time.time(), old_name, "qos=test"),
            )
        conn.commit()
        conn.close()

    def _count_events(self, db_path: str, event_name: str) -> int:
        import sqlite3
        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM task_events WHERE event = ?", (event_name,)
        ).fetchone()[0]
        conn.close()
        return count

    def test_migrate_all_events_renames_correctly(self, db_path):
        """migrate-events renames all old event names to new dot-separated names."""
        import sqlite3, io, sys
        from hpc_batch import cmd_migrate_events, init_db
        from argparse import Namespace

        init_db(db_path)
        total_old = len(self.EVENT_MAP)
        self._seed_old_events(db_path)

        # Count old event names before migration
        old_count_before = self._count_events(db_path, "fill_claimed")
        new_count_before = self._count_events(db_path, "fill.claim")
        assert old_count_before == 1, "Should have 1 old-style event"
        assert new_count_before == 0, "Should have 0 new-style events"

        # Run migration
        cmd_migrate_events(Namespace(db=db_path, dry_run=False, event=None))

        # Verify all old names renamed to new names
        for old_name, new_name in self.EVENT_MAP.items():
            old_count = self._count_events(db_path, old_name)
            new_count = self._count_events(db_path, new_name)
            assert old_count == 0, f"{old_name} should be 0 after migration, got {old_count}"
            assert new_count == 1, f"{new_name} should be 1 after migration, got {new_count}"

        # Total count unchanged
        conn = sqlite3.connect(db_path)
        total = conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
        conn.close()
        assert total == total_old, f"Total event count should be {total_old}, got {total}"

    def test_migrate_dry_run_does_not_modify(self, db_path):
        """migrate-events --dry-run prints counts but does not rename."""
        import sqlite3, io, sys
        from hpc_batch import cmd_migrate_events, init_db
        from argparse import Namespace

        init_db(db_path)
        self._seed_old_events(db_path)

        old_count_before = self._count_events(db_path, "fill_claimed")

        # Capture stdout
        out = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = out
        try:
            cmd_migrate_events(Namespace(db=db_path, dry_run=True, event=None))
        finally:
            sys.stdout = old_stdout

        output = out.getvalue()
        assert "Would rename" in output, "Dry-run should show 'Would rename'"

        # Verify no changes
        assert self._count_events(db_path, "fill_claimed") == old_count_before
        assert self._count_events(db_path, "fill.claim") == 0

    def test_migrate_single_event(self, db_path):
        """migrate-events --event renames only the specified event type."""
        import sqlite3, io, sys
        from hpc_batch import cmd_migrate_events, init_db
        from argparse import Namespace

        init_db(db_path)
        self._seed_old_events(db_path)

        # Migrate only fill_claimed → fill.claim
        cmd_migrate_events(Namespace(db=db_path, dry_run=False, event="fill_claimed"))

        assert self._count_events(db_path, "fill_claimed") == 0
        assert self._count_events(db_path, "fill.claim") == 1
        # Other events unchanged
        assert self._count_events(db_path, "job_killed") == 1
        assert self._count_events(db_path, "hpc.kill") == 0

    def test_migrate_handles_empty_db(self, db_path):
        """migrate-events on empty DB completes without error."""
        from hpc_batch import cmd_migrate_events, init_db
        from argparse import Namespace

        init_db(db_path)
        cmd_migrate_events(Namespace(db=db_path, dry_run=False, event=None))
        # No crash is the pass condition

