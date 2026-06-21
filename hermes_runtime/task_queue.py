from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import time


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class LLMTask:
    task_id: str
    agent_id: str
    messages: list[dict]
    tools: list[dict]
    model: str
    api_config: dict
    metadata: dict
    priority: int = 0
    created_at: float = field(default_factory=time.time)


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    content: Optional[str] = None
    tool_calls: Optional[list] = None
    reasoning: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[dict] = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = field(default_factory=time.time)


class TaskQueue(ABC):
    @abstractmethod
    def enqueue(self, task: LLMTask) -> str:
        pass

    @abstractmethod
    def get_result(self, task_id: str, timeout: float) -> TaskResult:
        pass

    @abstractmethod
    def get_result_nowait(self, task_id: str) -> Optional[TaskResult]:
        pass

    @abstractmethod
    def mark_running(self, task_id: str) -> bool:
        pass

    @abstractmethod
    def submit_result(self, result: TaskResult) -> bool:
        pass

    @abstractmethod
    def dequeue_batch(self, count: int, worker_id: str) -> list[LLMTask]:
        pass

    @abstractmethod
    def get_queue_size(self) -> int:
        pass

    @abstractmethod
    def ping(self) -> bool:
        pass

    @abstractmethod
    def get_metrics(self) -> dict:
        pass