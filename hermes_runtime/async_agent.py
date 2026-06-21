import json
import uuid
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional

from .task_queue import TaskQueue, LLMTask, TaskResult, TaskStatus
from .state_store import StateStore, AgentState
from .exceptions import AgentTimeoutError, StateNotFoundError


class AgentStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    WAITING = "waiting"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AsyncAgentResult:
    status: AgentStatus
    final_response: Optional[str] = None
    messages: list[dict] = field(default_factory=list)
    api_calls: int = 0
    completed: bool = False
    wait_task_id: Optional[str] = None
    error: Optional[str] = None


MAX_CONCURRENT_SUBAGENTS = 5
DEFAULT_SUBAGENT_BUDGET = 50


class AsyncAIAgent:
    def __init__(
        self,
        task_queue: TaskQueue,
        state_store: StateStore,
        agent_id: str = None,
        max_iterations: int = 90,
        max_wait_time: float = 300.0,
        poll_interval: float = 1.0,
        **agent_kwargs
    ):
        self.task_queue = task_queue
        self.state_store = state_store
        self.agent_id = agent_id or str(uuid.uuid4())
        self.max_iterations = max_iterations
        self.max_wait_time = max_wait_time
        self.poll_interval = poll_interval
        self.agent_kwargs = agent_kwargs

        self._status = AgentStatus.IDLE
        self._current_task_id: Optional[str] = None
        self._messages: list[dict] = []
        self._api_call_count = 0
        self._iteration_budget = max_iterations

    @property
    def status(self) -> AgentStatus:
        return self._status

    def run_conversation(
        self,
        user_message: str,
        system_message: str = None,
        conversation_history: list[dict] = None,
        resume_task_id: str = None,
        stream_callback: callable = None,
    ) -> AsyncAgentResult:
        if resume_task_id:
            return self._resume_from_task(resume_task_id)
        
        return self._run_fresh(user_message, system_message, conversation_history)

    def _run_fresh(
        self,
        user_message: str,
        system_message: str,
        conversation_history: list[dict],
    ) -> AsyncAgentResult:
        self._status = AgentStatus.RUNNING
        self._messages = list(conversation_history) if conversation_history else []
        self._api_call_count = 0
        self._iteration_budget = self.max_iterations

        user_msg = {"role": "user", "content": user_message}
        self._messages.append(user_msg)

        return self._execute_loop()

    def _resume_from_task(self, task_id: str) -> AsyncAgentResult:
        state = self.state_store.load(self.agent_id)
        if state is None:
            raise StateNotFoundError(f"No state found for agent {self.agent_id}")

        self._restore_state(state)

        result = self.task_queue.get_result_nowait(task_id)
        if result is None or result.status != TaskStatus.COMPLETED:
            return AsyncAgentResult(
                status=AgentStatus.WAITING,
                wait_task_id=task_id,
                messages=self._messages,
                api_calls=self._api_call_count,
                error="Task not ready"
            )

        return self._continue_after_llm(result)

    def _execute_loop(self) -> AsyncAgentResult:
        while self._iteration_budget > 0:
            if not self._iteration_budget_consume():
                return AsyncAgentResult(
                    status=AgentStatus.COMPLETED,
                    final_response="Iteration budget exhausted",
                    messages=self._messages,
                    api_calls=self._api_call_count,
                    completed=True
                )

            self._api_call_count += 1
            task = self._create_llm_task()
            task_id = self.task_queue.enqueue(task)
            self._current_task_id = task_id

            self._save_state([task_id])

            self._status = AgentStatus.WAITING
            return AsyncAgentResult(
                status=AgentStatus.WAITING,
                wait_task_id=task_id,
                messages=self._messages,
                api_calls=self._api_call_count,
                completed=False
            )

        return AsyncAgentResult(
            status=AgentStatus.COMPLETED,
            messages=self._messages,
            api_calls=self._api_call_count,
            completed=True
        )

    def _continue_after_llm(self, result: TaskResult) -> AsyncAgentResult:
        self._status = AgentStatus.RUNNING

        if result.status == TaskStatus.FAILED:
            self._status = AgentStatus.FAILED
            return AsyncAgentResult(
                status=AgentStatus.FAILED,
                error=result.error,
                messages=self._messages,
                api_calls=self._api_call_count
            )

        assistant_msg = {"role": "assistant", "content": result.content or ""}
        if result.reasoning:
            assistant_msg["reasoning"] = result.reasoning
        if result.tool_calls:
            assistant_msg["tool_calls"] = result.tool_calls
        self._messages.append(assistant_msg)

        if result.tool_calls:
            tool_results = self._execute_tools_local(result.tool_calls)
            self._messages.extend(tool_results)
            return self._execute_loop()

        self._status = AgentStatus.COMPLETED
        return AsyncAgentResult(
            status=AgentStatus.COMPLETED,
            final_response=result.content,
            messages=self._messages,
            api_calls=self._api_call_count,
            completed=True
        )

    def _create_llm_task(self) -> LLMTask:
        return LLMTask(
            task_id=str(uuid.uuid4()),
            agent_id=self.agent_id,
            messages=self._messages.copy(),
            tools=self.agent_kwargs.get("tools", []),
            model=self.agent_kwargs.get("model", "claude-3-5-sonnet-20241022"),
            api_config=self.agent_kwargs.get("api_config", {}),
            metadata={
                "session_id": self.agent_kwargs.get("session_id"),
                "api_call_count": self._api_call_count,
                "created_at": time.time(),
            },
            priority=0
        )

    def _execute_tools_local(self, tool_calls: list[dict]) -> list[dict]:
        results = []
        for tc in tool_calls:
            name = tc.get("function", {}).get("name", "")
            args = tc.get("function", {}).get("arguments", "{}")
            try:
                args_dict = json.loads(args)
            except json.JSONDecodeError:
                args_dict = {}

            from model_tools import handle_function_call
            result = handle_function_call(name, args_dict, task_id=None)
            results.append({
                "role": "tool",
                "tool_call_id": tc.get("id", str(uuid.uuid4())),
                "content": result
            })
        return results

    def _iteration_budget_consume(self) -> bool:
        if self._iteration_budget <= 0:
            return False
        self._iteration_budget -= 1
        return True

    def _save_state(self, pending_task_ids: list[str]) -> None:
        state = AgentState(
            agent_id=self.agent_id,
            session_id=self.agent_id,
            messages=self._messages.copy(),
            api_call_count=self._api_call_count,
            iteration_budget_remaining=self._iteration_budget,
            pending_task_ids=pending_task_ids,
            created_at=time.time(),
            updated_at=time.time(),
        )
        self.state_store.save(state)

    def _restore_state(self, state: AgentState) -> None:
        self._messages = state.messages.copy()
        self._api_call_count = state.api_call_count
        self._iteration_budget = state.iteration_budget_remaining

    def poll_for_result(self, task_id: str = None) -> Optional[AsyncAgentResult]:
        task_id = task_id or self._current_task_id
        if not task_id:
            return None

        # Check if already processed (idempotency)
        if hasattr(self, '_processed_tasks') and task_id in self._processed_tasks:
            return None
        
        result = self.task_queue.get_result_nowait(task_id)
        if result is None:
            return None

        # Mark as processed
        if not hasattr(self, '_processed_tasks'):
            self._processed_tasks = set()
        self._processed_tasks.add(task_id)

        if result.status == TaskStatus.COMPLETED:
            return self._continue_after_llm(result)
        elif result.status == TaskStatus.FAILED:
            return AsyncAgentResult(
                status=AgentStatus.FAILED,
                error=result.error,
                messages=self._messages,
                api_calls=self._api_call_count
            )

        return None

    def wait_for_result(self, task_id: str = None, timeout: float = None) -> AsyncAgentResult:
        task_id = task_id or self._current_task_id
        timeout = timeout or self.max_wait_time
        start = time.time()

        while time.time() - start < timeout:
            result = self.poll_for_result(task_id)
            if result is not None:
                return result
            time.sleep(self.poll_interval)

        raise AgentTimeoutError(f"Timeout waiting for task {task_id}")

    def _can_spawn_subagent(self) -> bool:
        state = self.state_store.load(self.agent_id)
        if state is None:
            return True
        active = len(state.active_subagents)
        return active < MAX_CONCURRENT_SUBAGENTS