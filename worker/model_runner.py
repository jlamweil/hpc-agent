"""Model runner - calls LLM (vLLM, OpenAI, etc.) or mocks for testing."""
import time
import random
from typing import Optional
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime import LLMTask, TaskResult, TaskStatus


class ModelRunner:
    def __init__(self, endpoint: str, api_key: str = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def run(self, task: LLMTask) -> TaskResult:
        raise NotImplementedError


class MockRunner(ModelRunner):
    """Mock runner for testing - returns fixed response with optional latency."""
    
    def __init__(self, response: str = "Mock response from worker",
                 latency_min: float = 0.0, latency_max: float = 0.0):
        super().__init__(endpoint="mock://", api_key=None)
        self.response = response
        self.latency_min = latency_min
        self.latency_max = latency_max

    def run(self, task: LLMTask) -> TaskResult:
        # Simulate latency
        if self.latency_max > 0:
            delay = random.uniform(self.latency_min, self.latency_max)
            time.sleep(delay)
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            content=self.response,
            tool_calls=None,
            finish_reason="stop",
        )


class VLLMRunner(ModelRunner):
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0
    
    def run(self, task: LLMTask) -> TaskResult:
        import requests
        last_error = None
        
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.endpoint}/v1/chat/completions",
                    json={
                        "model": task.model,
                        "messages": task.messages,
                        "tools": task.tools,
                    },
                    headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=data["choices"][0]["message"].get("content"),
                    tool_calls=data["choices"][0]["message"].get("tool_calls"),
                    usage=data.get("usage"),
                )
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            error=str(last_error),
        )