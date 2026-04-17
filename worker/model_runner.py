"""Model runner - calls LLM (vLLM, OpenAI, etc.)."""
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


class VLLMRunner(ModelRunner):
    def run(self, task: LLMTask) -> TaskResult:
        import requests
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
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=str(e),
            )