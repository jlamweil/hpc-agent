"""Redis state store - infrastructure implementation (hpc-agent)."""
from dataclasses import asdict
from typing import Optional
import json
import hashlib
import time
import redis as redis_lib
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime.state_store import StateStore, AgentState
from runtime.exceptions import StateStoreError


class RedisStateStore(StateStore):
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, 
                 prefix: str = "agent:state:", **config):
        super().__init__("redis", host=host, port=port, db=db, prefix=prefix, **config)
        self._client = redis_lib.Redis(host=host, port=port, db=db, decode_responses=False)
        self.prefix = prefix

    def _key(self, agent_id: str) -> str:
        return f"{self.prefix}{agent_id}"

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
            return bool(self._client.delete(self._key(agent_id)))
        except Exception:
            return False