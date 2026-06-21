"""Tests for worker/model_runner.py — HermesRunner."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pre-seed a mock run_agent module so the tests don't trigger the real
# module's deep dependency chain (openai, fire, browser_tool, etc.).
# The tests mock AIAgent immediately after importing, so only the module
# skeleton is needed — not the real 12K-line module with all its deps.
import types
from unittest.mock import MagicMock
_mock_ra = types.ModuleType('run_agent')
_mock_ra.AIAgent = MagicMock()
sys.modules['run_agent'] = _mock_ra

import json
import time
import concurrent.futures
import pytest
from unittest.mock import patch, MagicMock

from hermes_runtime.task_queue import LLMTask, TaskResult, TaskStatus
from worker.model_runner import HermesRunner


class TestHermesRunnerExtractPrompt:
    """Test _extract_prompt — the string extraction from LLMTask."""

    def test_extract_from_messages(self):
        """Messages list of dicts → concatenated content string."""
        task = LLMTask(
            task_id="test-1",
            agent_id="test",
            messages=[{"role": "user", "content": "Hello world"}],
            tools=[],
            model="test-model",
            api_config={},
            metadata={},
        )
        runner = HermesRunner()
        prompt = runner._extract_prompt(task)
        assert prompt == "Hello world"

    def test_extract_from_multiple_messages(self):
        """Multiple messages → all content concatenated."""
        task = LLMTask(
            task_id="test-2",
            agent_id="test",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello world"},
            ],
            tools=[],
            model="test-model",
            api_config={},
            metadata={},
        )
        runner = HermesRunner()
        prompt = runner._extract_prompt(task)
        # Should concatenate all content
        assert "helpful assistant" in prompt
        assert "Hello world" in prompt


class TestHermesRunnerRun:
    """Test HermesRunner.run() — the main execution method."""

    def test_run_returns_completed_with_content(self):
        """run() returns COMPLETED with AIAgent response content."""
        from unittest.mock import patch, MagicMock
        import run_agent as _ra

        mock_agent_instance = MagicMock()
        mock_agent_instance.run_conversation.return_value = {"content": "Hello from agent"}
        _ra.AIAgent = MagicMock(return_value=mock_agent_instance)

        task = LLMTask(
            task_id="run-test-1",
            agent_id="test",
            messages=[{"role": "user", "content": "Hi"}],
            tools=[],
            model="test-model",
            api_config={},
            metadata={},
        )
        with patch("worker.tunnel.find_vllm_node", return_value="node-001"):
            with patch("worker.tunnel.ensure_tunnel", return_value=8100):
                runner = HermesRunner()
                result = runner.run(task)

        assert result.status == TaskStatus.COMPLETED
        assert result.content == "Hello from agent"
        assert result.task_id == "run-test-1"

    def test_run_handles_empty_response(self):
        """Empty AIAgent response → COMPLETED with empty content."""
        import run_agent as _ra

        class _MockAIAgent:
            """Pickle-safe AIAgent mock that returns empty content."""
            def __init__(self, base_url="", model="", **kw):
                self.base_url = base_url
                self.model = model
                self.session_id = "mock-session"
            def run_conversation(self, prompt, conversation_history=None):
                return {"content": None}

        task = LLMTask(
            task_id="run-test-2",
            agent_id="test",
            messages=[{"role": "user", "content": "Say nothing"}],
            tools=[],
            model="test-model",
            api_config={},
            metadata={},
        )
        _ra.AIAgent = _MockAIAgent
        runner = HermesRunner(base_url="http://localhost:8000/v1")
        result = runner.run(task)

        assert result.status == TaskStatus.COMPLETED
        assert result.content == ""

    def test_run_handles_timeout(self):
        """Timeout during run_conversation → FAILED status."""
        from unittest.mock import patch, MagicMock
        import run_agent as _ra

        def _slow(prompt, conversation_history=None, **kwargs):
            import time
            time.sleep(5)  # Will be interrupted by 0.1s runner timeout

        mock_agent_instance = MagicMock()
        mock_agent_instance.run_conversation.side_effect = _slow
        _ra.AIAgent = MagicMock(return_value=mock_agent_instance)

        task = LLMTask(
            task_id="run-test-3",
            agent_id="test",
            messages=[{"role": "user", "content": "Sleep"}],
            tools=[],
            model="test-model",
            api_config={},
            metadata={},
        )
        with patch("worker.tunnel.find_vllm_node", return_value="node-001"):
            with patch("worker.tunnel.ensure_tunnel", return_value=8100):
                runner = HermesRunner(timeout=0.1)
                result = runner.run(task)

        assert result.status == TaskStatus.FAILED
        assert "timeout" in result.finish_reason.lower()
