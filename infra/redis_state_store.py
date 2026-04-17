"""Redis state store - infrastructure implementation (hpc-agent)."""
from dataclasses import asdict
from typing import Optional
import json
import hashlib
import time
import redis as redis_lib
import sys
sys.path.insert(0, ".")
sys.path.insert(0, "/home/lam/Documents/GAIA/hpc-agent")
import _setup
from runtime.state_store import StateStore, AgentState
from runtime.exceptions import StateStoreError


class RedisStateStore(StateStore):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, 
                 prefix: str = "agent:state:", task_map_prefix: str = "agent:task:",
                 **config):
        super().__init__("redis", host=host, port=port, db=db, prefix=prefix, **config)
        self._client = redis_lib.Redis(host=host, port=port, db=db, decode_responses=False)
        self.prefix = prefix
        self.task_map_prefix = task_map_prefix

    def _key(self, agent_id: str) -> str:
        return f"{self.prefix}{agent_id}"

    def _task_key(self, task_id: str) -> str:
        return f"{self.task_map_prefix}{task_id}"

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
        try:
            self._client.set(self._key(state.agent_id), self._serialize(state))
            for task_id in state.pending_task_ids:
                self._client.set(self._task_key(task_id), state.agent_id)
            return True
        except Exception as e:
            raise StateStoreError(f"Redis save failed: {e}")

    def load(self, agent_id: str) -> Optional[AgentState]:
        try:
            data = self._client.get(self._key(agent_id))
            if data is None:
                return None
            return self._deserialize(data)
        except Exception as e:
            raise StateStoreError(f"Redis load failed: {e}")

    def exists(self, agent_id: str) -> bool:
        try:
            return bool(self._client.exists(self._key(agent_id)))
        except Exception:
            return False

    def delete(self, agent_id: str) -> bool:
        try:
            state = self.load(agent_id)
            if state:
                for task_id in state.pending_task_ids:
                    self._client.delete(self._task_key(task_id))
            return bool(self._client.delete(self._key(agent_id)))
        except Exception:
            return False

    def load_by_task(self, task_id: str) -> Optional[AgentState]:
        try:
            agent_id = self._client.get(self._task_key(task_id))
            if agent_id is None:
                return None
            agent_id = agent_id.decode() if isinstance(agent_id, bytes) else agent_id
            return self.load(agent_id)
        except Exception:
            return None