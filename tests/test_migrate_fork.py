"""Migration test: hermes_runtime package exposes the same API as fork's runtime.

This test verifies the vendored hermes_runtime/ package works correctly
before we switch imports away from the fork. After migration, it stays
as a regression guard.
"""
import pytest
from hermes_runtime import (
    TaskQueue, LLMTask, TaskResult, TaskStatus,
    StateStore, AgentState, MemoryStateStore,
    AsyncAIAgent, AsyncAgentResult, AgentStatus,
)
from hermes_runtime.exceptions import (
    StateStoreError, StateNotFoundError, StateCorruptedError,
    AgentTimeoutError, AgentRetryExhaustedError,
)
from abc import ABC


class TestApiSurface:
    """Every symbol we import from the fork is present in hermes_runtime."""

    def test_task_queue_abstraction(self):
        """TaskQueue is an abstract base class."""
        assert issubclass(type(TaskQueue), type)
        assert TaskQueue.__bases__ == (ABC,)

    def test_llm_task_dataclass(self):
        """LLMTask has all required fields."""
        task = LLMTask(
            task_id="t1", agent_id="a1",
            messages=[{"role": "user", "content": "hi"}],
            tools=[], model="m", api_config={}, metadata={},
        )
        assert task.task_id == "t1"
        assert task.agent_id == "a1"
        assert task.messages == [{"role": "user", "content": "hi"}]
        assert task.priority == 0
        assert task.created_at > 0

    def test_task_result_dataclass(self):
        """TaskResult has all required fields."""
        r = TaskResult(task_id="t1", status=TaskStatus.COMPLETED, content="ok")
        assert r.task_id == "t1"
        assert r.status == TaskStatus.COMPLETED
        assert r.content == "ok"
        assert r.tool_calls is None
        assert r.error is None

    def test_task_status_enum(self):
        """TaskStatus has expected values."""
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"

    def test_agent_status_enum(self):
        """AgentStatus has expected values."""
        assert AgentStatus.IDLE.value == "idle"
        assert AgentStatus.RUNNING.value == "running"
        assert AgentStatus.WAITING.value == "waiting"
        assert AgentStatus.COMPLETED.value == "completed"
        assert AgentStatus.FAILED.value == "failed"

    def test_async_agent_result(self):
        """AsyncAgentResult has expected fields."""
        r = AsyncAgentResult(status=AgentStatus.COMPLETED, final_response="done")
        assert r.status == AgentStatus.COMPLETED
        assert r.final_response == "done"
        assert r.api_calls == 0

    def test_async_ai_agent_constructs(self):
        """AsyncAIAgent can be constructed with mocks."""
        from unittest.mock import MagicMock
        agent = AsyncAIAgent(
            task_queue=MagicMock(),
            state_store=MagicMock(),
            agent_id="test-agent",
        )
        assert agent.agent_id == "test-agent"
        assert agent.status == AgentStatus.IDLE

    def test_agent_state_dataclass(self):
        """AgentState has expected fields."""
        import time
        state = AgentState(agent_id="a1", session_id="s1")
        assert state.agent_id == "a1"
        assert state.session_id == "s1"
        assert state.api_call_count == 0
        assert state.version == 1

    def test_memory_state_store(self):
        """MemoryStateStore implements save/load."""
        store = MemoryStateStore()
        state = AgentState(agent_id="a1", session_id="s1")
        assert store.save(state) is True
        loaded = store.load("a1")
        assert loaded is not None
        assert loaded.agent_id == "a1"
        assert loaded.session_id == "s1"

    def test_state_store_error(self):
        """Exception classes are importable."""
        assert issubclass(StateStoreError, Exception)
        assert issubclass(StateNotFoundError, StateStoreError)
        assert issubclass(AgentTimeoutError, Exception)

    def test_load_by_task(self):
        """MemoryStateStore.load_by_task works."""
        store = MemoryStateStore()
        state = AgentState(agent_id="a1", session_id="s1", pending_task_ids=["t1"])
        store.save(state)
        loaded = store.load_by_task("t1")
        assert loaded is not None
        assert loaded.agent_id == "a1"


class TestAIAgentImport:
    """AIAgent resolves from installed hermes-agent pip package."""

    def test_aiagent_imports(self):
        """from run_agent import AIAgent works without sys.path hacks."""
        from run_agent import AIAgent
        assert AIAgent is not None

    def test_aiagent_constructs(self):
        """AIAgent can be constructed with base_url override."""
        from run_agent import AIAgent
        agent = AIAgent(base_url="http://localhost:8000/v1", model="test")
        # Constructor works — upstream sets base_url from config, but override
        # path from model_runner.py should be compatible
        assert agent.base_url is not None
        assert hasattr(agent, '_client_kwargs')
        assert hasattr(agent, '_create_openai_client')


@pytest.mark.skip(reason="Run manually: verifies old imports still resolve from fork")
class TestOldImportsStillWork:
    """Verify backward compat while fork still exists (pre-cleanup)."""

    def test_old_imports(self):
        import sys
        sys.path.insert(0, "/home/lam/Documents/GAIA/async-hermes-agent")
        from runtime.task_queue import LLMTask as OldTask
        task = OldTask(task_id="t", agent_id="a", messages=[], tools=[], model="m", api_config={}, metadata={})
        assert task.task_id == "t"
