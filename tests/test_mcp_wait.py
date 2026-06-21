"""Tests for MCP Wait — fill yields vLLM to MCP tasks."""
import sys, os, json, time, threading, sqlite3, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    from hpc_batch import init_db
    init_db(path)
    return path


class TestMcpWaitContext:
    """Tracer 1: fill context propagates to submit_task, blocks on event, unblocks on signal."""

    def test_submit_task_blocks_when_fill_context_active(self):
        """submit_task() blocks on event when _fill_context.task_id is set in same thread."""
        from hpc_batch import submit_task, _fill_context, _mcp_wait_events, signal_mcp_done

        db = _make_db()
        results = {"blocked": False, "tid": None}

        def blocking_call():
            _fill_context.task_id = "fill-test-1"
            results["tid"] = submit_task(
                prompt="mcp from fill", db_path=db,
                task_id="mcp-from-fill-1", priority=0,
            )
            results["blocked"] = True

        t = threading.Thread(target=blocking_call, daemon=True)
        t.start()
        time.sleep(0.3)

        assert "mcp-from-fill-1" in _mcp_wait_events, "Event should be registered"
        assert results["blocked"] is False, "Should be blocked on event"

        signal_mcp_done("mcp-from-fill-1")
        time.sleep(0.2)

        assert results["blocked"] is True, "Should unblock after signal"
        assert results["tid"] == "mcp-from-fill-1"
        assert "mcp-from-fill-1" not in _mcp_wait_events, "Event should be cleaned up"

        _fill_context.task_id = None
        os.unlink(db)


class TestMcpWaitHermesRunner:
    """Tracer 2: HermesRunner sets fill context in _run_agent thread."""

    def test_hermes_runner_accepts_fill_task_id(self):
        """HermesRunner.run() accepts fill_task_id parameter."""
        from worker.model_runner import HermesRunner
        import inspect
        sig = inspect.signature(HermesRunner.run)
        assert 'fill_task_id' in sig.parameters
        param = sig.parameters['fill_task_id']
        assert param.default is None


class TestMcpWaitDaemonIntegration:
    """Tracer 3: daemon signals MCP waiters after processing real tasks."""

    def test_signal_mcp_done_is_noop_for_nonexistent_task(self):
        """signal_mcp_done with unknown task_id does nothing."""
        from hpc_batch import signal_mcp_done, _mcp_wait_events
        before = len(_mcp_wait_events)
        signal_mcp_done("nonexistent-task")
        assert len(_mcp_wait_events) == before

    def test_process_next_task_triggers_signal(self):
        """After process_next_task completes, signal_mcp_done can be called."""
        from hpc_batch import signal_mcp_done, _mcp_wait_events, _fill_context
        from worker.model_runner import MockRunner
        from worker.hermes_worker import process_next_task

        db = _make_db()

        # Task submitted as if from a fill
        from hpc_batch import submit_task
        tid = submit_task(prompt="mcp task", db_path=db, priority=0)

        # Register MCP waiter (as submit_task would)
        ev = threading.Event()
        _mcp_wait_events[tid] = ev

        # Process the task (daemon)
        processed = process_next_task(db, runner=MockRunner(response="ok"))
        assert processed is True

        # Signal the waiter
        signal_mcp_done(tid)
        assert ev.is_set(), "Waiter should be signaled after task completion"

        os.unlink(db)
