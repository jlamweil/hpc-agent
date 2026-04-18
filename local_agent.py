#!/usr/bin/env python3
"""Local async agent for submitting tasks to SQLite."""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.sqlite_queue import SQLiteTaskQueue
from runtime.task_queue import LLMTask

DEFAULT_DB = "tasks.db"


class LocalSubmitterAgent:
    """Async agent that adds tasks to SQLite."""
    
    def __init__(self, db_path: str = DEFAULT_DB):
        self.db_path = db_path
        self.queue = SQLiteTaskQueue(db_path=db_path)
    
    async def run(self, task: dict) -> dict:
        """Submit a task to SQLite asynchronously."""
        llm_task = LLMTask(
            task_id=None,
            agent_id=",".join(task.get("agents", ["default"])),
            messages=[{"role": "user", "content": task.get("prompt", "")}],
            tools=[],
            model=task.get("model", "TinyLlama/TinyLlama-1.1B-Chat-v1.0"),
            api_config={},
            metadata={"prompt": task.get("prompt", ""), "agents": task.get("agents", ["default"])},
            priority=task.get("priority", 0)
        )
        
        task_id = self.queue.enqueue(llm_task)
        
        return {"task_id": task_id, "status": "pending", "prompt": task.get("prompt", "")[:50]}


async def submit_task(prompt: str, agents: list = None, model: str = None, db_path: str = DEFAULT_DB) -> dict:
    """Submit a single task asynchronously."""
    agent = LocalSubmitterAgent(db_path)
    task = {"prompt": prompt, "agents": agents or ["default"]}
    if model:
        task["model"] = model
    return await agent.run(task)


async def submit_batch(prompts: list, agents: list = None, model: str = None, db_path: str = DEFAULT_DB) -> list:
    """Submit multiple tasks asynchronously."""
    agent = LocalSubmitterAgent(db_path)
    tasks = []
    for prompt in prompts:
        task = {"prompt": prompt, "agents": agents or ["default"]}
        if model:
            task["model"] = model
        tasks.append(task)
    
    results = await asyncio.gather(*[agent.run(t) for t in tasks])
    return results


def init_db(db_path: str = DEFAULT_DB):
    """Initialize SQLite database if not exists."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            messages TEXT NOT NULL,
            tools TEXT NOT NULL,
            model TEXT NOT NULL,
            api_config TEXT NOT NULL,
            metadata TEXT NOT NULL,
            priority INTEGER DEFAULT 0,
            created_at REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            result TEXT,
            started_at REAL,
            completed_at REAL,
            error TEXT,
            slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
    conn.commit()
    conn.close()


async def demo():
    """Demo of async task submission."""
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB
    
    # Use SQLiteTaskQueue to init DB (it creates tables automatically)
    queue = SQLiteTaskQueue(db_path=db_path)
    
    prompts = [
        "Write a Python function to calculate factorial",
        "Write a Python function to sort a list",
        "Write a Python class for a linked list",
        "Write a Python function to reverse a string",
        "Write a Python class for a stack"
    ]
    
    print(f"Submitting {len(prompts)} tasks asynchronously...")
    results = await submit_batch(prompts, agents=["coder"], db_path=db_path)
    
    for r in results:
        print(f"  Submitted: {r['task_id'][:8]}... - {r['status']}")
    
    queue = SQLiteTaskQueue(db_path=db_path)
    pending = queue.get_queue_size()
    print(f"\nTotal pending tasks: {pending}")


if __name__ == "__main__":
    asyncio.run(demo())