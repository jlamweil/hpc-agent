"""Task Router — decide whether a task should run locally or on HPC."""

import json
import sqlite3
from pathlib import Path
from typing import Optional


def _load_routing_config() -> dict:
    """Load routing section from config.yaml."""
    config_path = Path(__file__).resolve().parent.parent / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}
            return config.get("routing", {})
        except Exception:
            pass
    return {}


class TaskRouter:
    """Decide whether a task should run locally or on HPC."""

    LOCAL_MODELS = {"qwen2.5:0.5b", "llama3.2:1b", "tinyllama:1.1b"}
    HPC_MODELS = {"deepseek-ai/DeepSeek-V4-Flash"}

    def __init__(self, config: Optional[dict] = None):
        self.routing_config = config or _load_routing_config()
        self.default_route = self.routing_config.get("default", "hpc")
        local_models = self.routing_config.get("local_models")
        if local_models:
            self.LOCAL_MODELS = set(local_models)
        hpc_models = self.routing_config.get("hpc_models")
        if hpc_models:
            self.HPC_MODELS = set(hpc_models)

    @staticmethod
    def load_config() -> dict:
        """Load routing config from config.yaml."""
        return _load_routing_config()

    def route(self, task: dict) -> str:
        """Return 'local' or 'hpc' based on rules.

        Rules (priority order):
        1. Explicit override in metadata.route
        2. Model name match
        3. Task type match
        4. Context size heuristic
        5. Default (hpc)
        """
        metadata = task.get("metadata") or {}

        # 1. Explicit override in metadata.route
        override = metadata.get("route")
        if override in ("local", "hpc"):
            return override

        # 2. Model name match
        model = (task.get("model") or "").strip()
        if model in self.LOCAL_MODELS:
            return "local"
        if model in self.HPC_MODELS:
            return "hpc"

        # 3. Task type match
        task_type = metadata.get("type", "")
        local_types = self.routing_config.get("local_types", [])
        hpc_types = self.routing_config.get("hpc_types", [])
        if task_type in local_types:
            return "local"
        if task_type in hpc_types:
            return "hpc"

        # 4. Context size heuristic — small prompts are good for local
        messages = task.get("messages", [])
        total_chars = 0
        for m in messages if isinstance(messages, list) else []:
            if isinstance(m, dict):
                total_chars += len(str(m.get("content", "")))
        context_threshold = self.routing_config.get("context_threshold", 2000)
        if total_chars < context_threshold:
            return "local"

        return self.default_route


def route_task_by_id(task_id: str, db_path: str = "tasks.db") -> str:
    """Load task from DB, route it, return 'local' or 'hpc'.

    Raises ValueError if task_id is not found.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id, model, messages, metadata FROM tasks WHERE id = ?",
        (task_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise ValueError(f"Task not found: {task_id}")

    task = {
        "id": row["id"],
        "model": row["model"],
        "messages": json.loads(row["messages"]),
        "metadata": json.loads(row["metadata"]),
    }
    router = TaskRouter()
    return router.route(task)


def route_pending_breakdown(db_path: str = "tasks.db") -> dict:
    """Return counts of local vs HPC pending tasks::

        {"local": N, "hpc": M, "total": N+M}
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT model, messages, metadata FROM tasks WHERE status = 'pending'"
    ).fetchall()
    conn.close()

    router = TaskRouter()
    local_count = 0
    hpc_count = 0
    for row in rows:
        task = {
            "model": row["model"],
            "messages": json.loads(row["messages"]),
            "metadata": json.loads(row["metadata"]),
        }
        if router.route(task) == "local":
            local_count += 1
        else:
            hpc_count += 1

    return {"local": local_count, "hpc": hpc_count, "total": local_count + hpc_count}
