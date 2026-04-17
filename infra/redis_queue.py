"""Redis task queue - infrastructure implementation (hpc-agent)."""
from dataclasses import asdict
from typing import Optional, Callable
import json
import time
import uuid
import threading
import redis as redis_lib
import sys
sys.path.insert(0, ".")
sys.path.insert(0, "/home/lam/Documents/GAIA/hpc-agent")
import _setup
from runtime.task_queue import TaskQueue, LLMTask, TaskResult, TaskStatus


class RedisTaskQueue(TaskQueue):
    TASK_COMPLETED_CHANNEL = "hpc:task:completed"

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 1,
                 prefix: str = "llm:task:", result_prefix: str = "llm:result:",
                 queue_key: str = "llm:queue", **config):
        self._client = redis_lib.Redis(host=host, port=port, db=db, decode_responses=False)
        self._pubsub = redis_lib.Redis(host=host, port=port, db=db, decode_responses=True)
        self.prefix = prefix
        self.result_prefix = result_prefix
        self.queue_key = queue_key
        self.timeout = config.get("timeout", 300)
        self._listener_thread = None
        self._callbacks = {}

    def _task_key(self, task_id: str) -> str:
        return f"{self.prefix}{task_id}"

    def _result_key(self, task_id: str) -> str:
        return f"{self.result_prefix}{task_id}"

    def enqueue(self, task: LLMTask) -> str:
        task_id = task.task_id or str(uuid.uuid4())
        task.task_id = task_id
        
        task_data = json.dumps(asdict(task)).encode()
        self._client.hset(self._task_key(task_id), mapping={
            "data": task_data,
            "status": TaskStatus.PENDING.value,
            "created_at": str(task.created_at)
        })
        self._client.rpush(self.queue_key, task_id)
        
        return task_id

    def get_result(self, task_id: str, timeout: float) -> TaskResult:
        start = time.time()
        while time.time() - start < timeout:
            result = self.get_result_nowait(task_id)
            if result is not None:
                return result
            time.sleep(0.5)
        
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.PENDING,
            error="Timeout waiting for result"
        )

    def get_result_nowait(self, task_id: str) -> Optional[TaskResult]:
        data = self._client.hgetall(self._result_key(task_id))
        if not data:
            return None
        
        try:
            status = data.get(b"status", b"").decode()
            return TaskResult(
                task_id=task_id,
                status=TaskStatus(status),
                content=data.get(b"content", b"").decode() or None,
                tool_calls=json.loads(data.get(b"tool_calls", b"[]").decode()) if data.get(b"tool_calls") else None,
                reasoning=data.get(b"reasoning", b"").decode() or None,
                finish_reason=data.get(b"finish_reason", b"").decode() or None,
                usage=json.loads(data.get(b"usage", b"{}").decode()) if data.get(b"usage") else None,
                error=data.get(b"error", b"").decode() or None,
                started_at=float(data.get(b"started_at", b"0").decode()) or None,
                completed_at=float(data.get(b"completed_at", b"0").decode()) or None,
            )
        except Exception:
            return None

    def mark_running(self, task_id: str) -> bool:
        return bool(self._client.hset(self._task_key(task_id), "status", TaskStatus.RUNNING.value))

    def submit_result(self, result: TaskResult) -> bool:
        result.completed_at = result.completed_at or time.time()
        data = {
            "status": result.status.value,
            "completed_at": str(result.completed_at),
        }
        if result.content is not None:
            data["content"] = result.content
        if result.tool_calls is not None:
            data["tool_calls"] = json.dumps(result.tool_calls)
        if result.reasoning is not None:
            data["reasoning"] = result.reasoning
        if result.finish_reason is not None:
            data["finish_reason"] = result.finish_reason
        if result.usage is not None:
            data["usage"] = json.dumps(result.usage)
        if result.error is not None:
            data["error"] = result.error
        if result.started_at is not None:
            data["started_at"] = str(result.started_at)
        
        self._client.hset(self._result_key(result.task_id), mapping=data)
        self._client.hset(self._task_key(result.task_id), "status", result.status.value)
        
        # Publish completion event
        self._client.publish(self.TASK_COMPLETED_CHANNEL, result.task_id)
        
        return True

    def subscribe_completed(self, callback: Callable[[str], None]) -> None:
        """Subscribe to task completion events."""
        def _listen():
            pubsub = self._pubsub.pubsub()
            pubsub.subscribe(self.TASK_COMPLETED_CHANNEL)
            for msg in pubsub.listen():
                if msg["type"] == "message":
                    task_id = msg["data"]
                    if task_id in self._callbacks:
                        self._callbacks[task_id](task_id)
        
        self._listener_thread = threading.Thread(target=_listen, daemon=True)
        self._listener_thread.start()

    def on_task_completed(self, task_id: str, callback: Callable[[str], None]) -> None:
        """Register a callback for a specific task."""
        self._callbacks[task_id] = callback

    def dequeue_batch(self, count: int, worker_id: str) -> list[LLMTask]:
        tasks = []
        for _ in range(count):
            task_id = self._client.lpop(self.queue_key)
            if task_id is None:
                break
            task_id = task_id.decode() if isinstance(task_id, bytes) else task_id
            
            task_data = self._client.hget(self._task_key(task_id), "data")
            if task_data is None:
                continue
            
            try:
                task_dict = json.loads(task_data.decode())
                task = LLMTask(**task_dict)
                self.mark_running(task_id)
                tasks.append(task)
            except Exception:
                continue
        
        return tasks

    def get_queue_size(self) -> int:
        return self._client.llen(self.queue_key)

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False

    def get_metrics(self) -> dict:
        try:
            info = self._client.info()
            return {
                "queue_size": self.get_queue_size(),
                "total_keys": self._client.dbsize(),
                "memory_used_mb": info.get("used_memory", 0) / (1024 * 1024),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception:
            return {"error": "Failed to get metrics"}