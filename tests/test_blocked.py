"""Tests for Slice 3: blocked command."""
import json
import pytest
from argparse import Namespace

from hpc_batch import submit_task, cmd_blocked
from conftest import complete_task


class TestBlocked:
    """blocked command shows pending tasks with unmet deps."""

    def test_no_blocked_tasks(self, db_path):
        """No pending tasks → blocked is empty."""
        args = Namespace(db=db_path, json=False)
        result = cmd_blocked(args)
        assert result == [], "No tasks → no blocked tasks"

    def test_no_deps_no_blocked(self, db_path):
        """Tasks without deps are not blocked."""
        submit_task("A", db_path=db_path)
        submit_task("B", db_path=db_path)

        args = Namespace(db=db_path, json=False)
        result = cmd_blocked(args)
        assert result == [], "Tasks without deps should not be blocked"

    def test_blocked_task_shown(self, db_path):
        """Task with unmet dep should appear in blocked."""
        a = submit_task("A", db_path=db_path)
        b = submit_task("B", db_path=db_path, depends_on=[a])

        args = Namespace(db=db_path, json=True)
        result = cmd_blocked(args)

        assert len(result) == 1, "One task should be blocked"
        assert result[0]["id"] == b, "B should be blocked"
        blocker_ids = [blk["id"] for blk in result[0]["blocked_by"]]
        assert a in blocker_ids, "A should be the blocker"

    def test_blocked_lists_blockers(self, db_path):
        """Blocked task should list all its blockers."""
        a = submit_task("A", db_path=db_path)
        b = submit_task("B", db_path=db_path)
        c = submit_task("C", db_path=db_path, depends_on=[a, b])

        args = Namespace(db=db_path, json=True)
        result = cmd_blocked(args)

        assert len(result) == 1, "Only C should be blocked"
        blocker_ids = [blk["id"] for blk in result[0]["blocked_by"]]
        assert a in blocker_ids
        assert b in blocker_ids
        assert len(blocker_ids) == 2

    def test_blocked_clears_after_completion(self, db_path):
        """Task should not appear in blocked after deps complete."""
        a = submit_task("A", db_path=db_path)
        b = submit_task("B", db_path=db_path, depends_on=[a])

        complete_task(db_path, a)

        args = Namespace(db=db_path, json=True)
        result = cmd_blocked(args)
        assert result == [], "B should no longer be blocked after A completes"

    def test_multiple_blocked_tasks(self, db_path):
        """Multiple blocked tasks should all appear."""
        a = submit_task("A", db_path=db_path)
        b = submit_task("B", db_path=db_path, depends_on=[a])
        c = submit_task("C", db_path=db_path, depends_on=[a])
        d = submit_task("D", db_path=db_path, depends_on=[b, c])

        args = Namespace(db=db_path, json=True)
        result = cmd_blocked(args)
        result_ids = [r["id"] for r in result]

        assert b in result_ids
        assert c in result_ids
        assert d in result_ids
        assert a not in result_ids  # A has no deps
