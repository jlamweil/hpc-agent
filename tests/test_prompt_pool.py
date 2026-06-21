"""Tests for fill task prompt pool and round-robin cycling."""
import sys
import os
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import patch

import hpc_batch


class TestNextFillPrompt:
    """Round-robin fill prompt selector with repo substitution."""

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_returns_string(self, mock_repo):
        """_next_fill_prompt returns a non-empty string, substitutes placeholders if present."""
        prompt = hpc_batch._next_fill_prompt(["handoff"])
        assert isinstance(prompt, str)
        assert len(prompt) > 0
        # {TARGET_REPO} may not be in every prompt file, but if it is, it must be substituted
        assert "{TARGET_REPO}" not in prompt, "Placeholder should be substituted"

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_cycles_through_types(self, mock_repo):
        """Consecutive calls return different prompts in round-robin."""
        types = ["handoff", "frontier-agent", "frontier-improve"]
        prompts = []
        for _ in range(3):
            p = hpc_batch._next_fill_prompt(types)
            prompts.append(p)

        # All 3 should be different (each type file has different content)
        assert len(set(prompts)) == 3

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_cycle_counter_rounds(self, mock_repo):
        """After last type, wraps back to first."""
        types = ["handoff", "frontier-agent", "frontier-improve"]
        first = hpc_batch._next_fill_prompt(types)
        _ = hpc_batch._next_fill_prompt(types)
        _ = hpc_batch._next_fill_prompt(types)
        fourth = hpc_batch._next_fill_prompt(types)
        # After wrapping, should be same as first (same type, same repo)
        assert fourth == first

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_uses_config_defaults(self, mock_repo):
        """Without argument, loads types from config."""
        prompt = hpc_batch._next_fill_prompt()
        assert isinstance(prompt, str) and len(prompt) > 0
        assert "{TARGET_REPO}" not in prompt

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_directive_stripped_from_output(self, mock_repo):
        """#!target_repo: directive is not present in returned prompt."""
        prompt = hpc_batch._next_fill_prompt(["handoff"])
        assert not prompt.startswith("#!"), f"Directive leaked into output: {prompt[:40]}"

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_directive_overrides_random_repo(self, mock_repo):
        """Prompt with #!target_repo: uses pinned repo, not _random_repo()."""
        prompt = hpc_batch._next_fill_prompt(["handoff"])
        # handoff.txt has #!target_repo: autoresearch-factory
        assert "/autoresearch-factory/" in prompt, \
            f"Expected pinned repo path, got: {prompt[:100]}"
        # _random_repo was mocked to return hpc-agent, so if directive
        # were ignored, we'd see /hpc-agent/ in the path
        assert "/hpc-agent/" not in prompt, \
            f"Directive was ignored — output uses mocked repo: {prompt[:100]}"

    @patch("hpc_batch._random_repo", return_value="hpc-agent")
    def test_no_directive_uses_random_repo(self, mock_repo):
        """Prompt without #!target_repo: uses _random_repo() result."""
        prompt = hpc_batch._next_fill_prompt(["lesson-mining"])
        # lesson-mining.txt has no directive → uses mocked _random_repo
        assert "/hpc-agent/" in prompt, \
            f"Expected random repo path, got: {prompt[:100]}"

    def test_random_repo_returns_valid(self):
        """_random_repo returns one of the configured repos."""
        repo = hpc_batch._random_repo()
        valid = hpc_batch.cfg("fill_tasks.target_repos", ["hpc-agent"])
        assert repo in valid, f"{repo} not in {valid}"

    def test_disabled_prompt_gets_priority_minus_2(self):
        """cmd_submit_fill uses priority -2 for _-prefixed prompt types."""
        from hpc_batch import cmd_submit_fill, submit_task
        from argparse import Namespace
        import tempfile, sqlite3

        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        from hpc_batch import init_db
        init_db(db_path)

        args = Namespace(
            db=db_path, prompt=None, fill_type="_stress-test",
            model="test-model",
        )
        with patch("hpc_batch._random_repo", return_value="hpc-agent"):
            tid = cmd_submit_fill(args)

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT priority, task_type FROM tasks WHERE id = ?", (tid,)).fetchone()
        conn.close()
        os.unlink(db_path)

        assert row is not None, "Task should exist"
        assert row[0] == -2, f"Expected priority=-2 for _-prefixed prompt, got {row[0]}"
        assert row[1] == "fill"

    def test_active_prompt_gets_priority_minus_1(self):
        """cmd_submit_fill uses priority -1 for non-prefixed prompt types."""
        from hpc_batch import cmd_submit_fill
        from argparse import Namespace
        import tempfile, sqlite3

        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        from hpc_batch import init_db
        init_db(db_path)

        args = Namespace(
            db=db_path, prompt=None, fill_type="handoff",
            model="test-model",
        )
        with patch("hpc_batch._random_repo", return_value="hpc-agent"):
            tid = cmd_submit_fill(args)

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT priority, task_type FROM tasks WHERE id = ?", (tid,)).fetchone()
        conn.close()
        os.unlink(db_path)

        assert row is not None, "Task should exist"
        assert row[0] == -1, f"Expected priority=-1 for active prompt, got {row[0]}"
        assert row[1] == "fill"


class TestPromptFiles:
    """Fill prompt files exist and have content."""

    def test_all_prompts_have_content(self):
        """ALL fill prompts (including _-prefixed) have content > 50 chars."""
        names = hpc_batch._available_fill_prompts()
        assert len(names) > 0, "No fill prompts found"
        base = hpc_batch.HPC_AGENT_DIR / "prompts" / "fill"
        for name in names:
            content = (base / f"{name}.txt").read_text().strip()
            assert len(content) > 50, f"Prompt {name}.txt too short ({len(content)} chars)"

    def test_disabled_prompts_exist_on_disk(self):
        """Disabled prompts (_*.txt) still exist for future re-enable."""
        base = hpc_batch.HPC_AGENT_DIR / "prompts" / "fill"
        disabled = sorted(
            f.stem for f in base.iterdir()
            if f.suffix == ".txt" and f.name.startswith("_")
        )
        for name in disabled:
            content = (base / f"{name}.txt").read_text().strip()
            assert len(content) > 50, f"Disabled prompt {name}.txt too short ({len(content)} chars)"

    def test_all_prompts_in_cycle(self):
        """ALL .txt files (including _-prefixed) are in the cycle."""
        names = hpc_batch._available_fill_prompts()
        disabled = [n for n in names if n.startswith("_")]
        active = [n for n in names if not n.startswith("_")]
        assert len(disabled) >= 1, "Expected at least one _-prefixed prompt in cycle"
        assert len(active) >= 1, "Expected at least one non-prefixed prompt in cycle"
