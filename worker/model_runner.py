"""Model runner - calls LLM (vLLM, OpenAI, etc.) or mocks for testing."""
import time
import random
import sys
import os

HERO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERO, "async-hermes-agent"))

from runtime.task_queue import LLMTask, TaskResult, TaskStatus


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
    
    def __init__(self, endpoint: str, api_key: str = None, model: str = None):
        super().__init__(endpoint, api_key)
        self.model = model
    
    def run(self, task: LLMTask) -> TaskResult:
        import requests
        last_error = None
        
        model_name = self.model or task.model
        
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.endpoint}/v1/chat/completions",
                    json={
                        "model": model_name,
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


class OllamaRunner(VLLMRunner):
    def __init__(self, endpoint: str = "http://localhost:11434", model: str = "llama3:8b"):
        super().__init__(endpoint=endpoint, api_key=None, model=model)
        self._api_base = endpoint.rstrip("/")
    
    def run(self, task: LLMTask) -> TaskResult:
        import requests
        last_error = None
        
        model_name = self.model
        
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self._api_base}/v1/chat/completions",
                    json={
                        "model": model_name,
                        "messages": task.messages,
                        "temperature": 0.7,
                        "max_tokens": 2048,
                    },
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
                
                content = data["choices"][0]["message"].get("content", "")
                
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=content,
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