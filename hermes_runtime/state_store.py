from dataclasses import dataclass, field, asdict
from typing import Optional
import json
import hashlib
import time


@dataclass
class AgentState:
    agent_id: str
    session_id: str

    messages: list[dict] = field(default_factory=list)
    system_prompt: str = ""

    api_call_count: int = 0
    iteration_budget_remaining: int = 90
    pending_task_ids: list[str] = field(default_factory=list)

    retry_counters: dict[str, int] = field(default_factory=dict)

    context_compressor_state: dict = field(default_factory=dict)

    memory_state: dict = field(default_factory=dict)

    todo_state: dict = field(default_factory=dict)

    active_subagents: list[dict] = field(default_factory=list)
    subagent_budgets: dict[str, int] = field(default_factory=dict)

    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    last_api_call_time: float = 0.0

    version: int = 1


class StateStore:
    def __init__(self, backend: str = "memory", **config):
        self.backend = backend
        self.config = config

    def save(self, state: AgentState) -> bool:
        raise NotImplementedError

    def load(self, agent_id: str) -> Optional[AgentState]:
        raise NotImplementedError

    def load_by_task(self, task_id: str) -> Optional[AgentState]:
        raise NotImplementedError

    def exists(self, agent_id: str) -> bool:
        raise NotImplementedError

    def delete(self, agent_id: str) -> bool:
        raise NotImplementedError


class MemoryStateStore(StateStore):
    def __init__(self, **config):
        super().__init__("memory", **config)
        self._store: dict[str, bytes] = {}
        self._task_map: dict[str, str] = {}

    def _serialize(self, state: AgentState) -> bytes:
        data = asdict(state)
        data["updated_at"] = time.time()
        payload = json.dumps(data).encode()
        checksum = hashlib.sha256(payload).hexdigest()[:16]
        return payload + b"|" + checksum.encode()

    def _deserialize(self, data: bytes) -> Optional[AgentState]:
        if b"|" not in data:
            return None
        payload, checksum = data.rsplit(b"|", 1)
        expected = hashlib.sha256(payload).hexdigest()[:16]
        if checksum.decode() != expected:
            return None
        try:
            data = json.loads(payload.decode())
            return AgentState(**data)
        except Exception:
            return None

    def save(self, state: AgentState) -> bool:
        self._store[state.agent_id] = self._serialize(state)
        for task_id in state.pending_task_ids:
            self._task_map[task_id] = state.agent_id
        return True

    def load(self, agent_id: str) -> Optional[AgentState]:
        data = self._store.get(agent_id)
        if data is None:
            return None
        return self._deserialize(data)

    def load_by_task(self, task_id: str) -> Optional[AgentState]:
        agent_id = self._task_map.get(task_id)
        if agent_id is None:
            return None
        return self.load(agent_id)

    def exists(self, agent_id: str) -> bool:
        return agent_id in self._store

    def delete(self, agent_id: str) -> bool:
        if agent_id in self._store:
            del self._store[agent_id]
            return True
        return False