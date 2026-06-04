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
            depends_on=[],
        )
        ids = cmd_submit(args)

        # Both should be ready (no deps)
        ready = dequeue_pending(db_path)
        ready_ids = [t["id"] for t in ready]
        assert a in ready_ids
        assert ids[0] in ready_ids
