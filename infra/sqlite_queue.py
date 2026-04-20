"""SQLite task queue - local persistence layer for HPC architecture."""
import sqlite3
import json
import time
import uuid
import threading
from dataclasses import asdict
from typing import Optional
import os
import sys

HERO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASYNC_HERMES = os.path.join(os.path.dirname(HERO_DIR), "async-hermes-agent")
sys.path.insert(0, ASYNC_HERMES)

from runtime.task_queue import TaskQueue, LLMTask, TaskResult, TaskStatus


class SQLiteTaskQueue(TaskQueue):
    """SQLite-based task queue with file-based sync support."""
    
    def __init__(self, db_path: str = "tasks.db", 
                 sync_dir: str = None,  # For rsync to HPC
                 **config):
        self.db_path = db_path
        self.sync_dir = sync_dir
        self._conn = None
        self._init_db()
        self._callbacks = {}
    
    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def _init_db(self):
        conn = self._get_conn()
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
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                task_id TEXT,
                job_id INTEGER,
                timestamp REAL NOT NULL,
                metadata TEXT
            )
        """)
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_created ON tasks(created_at)")
        conn.commit()
    
    def enqueue(self, task: LLMTask) -> str:
        """Add a task to the queue."""
        task_id = task.task_id or str(uuid.uuid4())
        task.task_id = task_id
        
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO tasks (
                id, agent_id, messages, tools, model, api_config, metadata,
                priority, created_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task_id,
            task.agent_id,
            json.dumps(task.messages),
            json.dumps(task.tools),
            task.model,
            json.dumps(task.api_config),
            json.dumps(task.metadata),
            task.priority,
            task.created_at,
            TaskStatus.PENDING.value
        ))
        conn.commit()
        
        # Trigger rsync if sync_dir configured
        if self.sync_dir:
            self._sync_to_hpc()
        
        return task_id
    
    def _sync_to_hpc(self):
        """Sync SQLite file to HPC (called from queue monitor)."""
        # This will be called by the queue_monitor before submitting job
        pass
    
    def get_result(self, task_id: str, timeout: float) -> TaskResult:
        """Get task result with polling."""
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
        """Get task result without waiting."""
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        
        if not row:
            return None
        
        if row['status'] not in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value]:
            return None
        
        return TaskResult(
            task_id=task_id,
            status=TaskStatus(row['status']),
            content=json.loads(row['result'])['content'] if row['result'] else None,
            tool_calls=json.loads(row['result'])['tool_calls'] if row['result'] else None,
            reasoning=json.loads(row['result'])['reasoning'] if row['result'] else None,
            finish_reason=json.loads(row['result'])['finish_reason'] if row['result'] else None,
            usage=json.loads(row['result'])['usage'] if row['result'] else None,
            error=row['error'],
            started_at=row['started_at'],
            completed_at=row['completed_at']
        )
    
    def mark_running(self, task_id: str) -> bool:
        """Mark task as running."""
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE tasks SET status = ?, started_at = ? WHERE id = ? AND status = ?",
            (TaskStatus.RUNNING.value, time.time(), task_id, TaskStatus.PENDING.value)
        )
        conn.commit()
        return cursor.rowcount > 0
    
    def submit_result(self, result: TaskResult) -> bool:
        """Store task result."""
        conn = self._get_conn()
        
        result_data = {
            'content': result.content,
            'tool_calls': result.tool_calls,
            'reasoning': result.reasoning,
            'finish_reason': result.finish_reason,
            'usage': result.usage
        }
        
        conn.execute("""
            UPDATE tasks SET 
                status = ?,
                result = ?,
                error = ?,
                completed_at = ?
            WHERE id = ?
        """, (
            result.status.value,
            json.dumps(result_data) if result_data['content'] else None,
            result.error,
            result.completed_at or time.time(),
            result.task_id
        ))
        conn.commit()
        
        # Notify callbacks
        if result.task_id in self._callbacks:
            self._callbacks[result.task_id](result.task_id)
        
        return True
    
    def dequeue_batch(self, count: int, worker_id: str) -> list[LLMTask]:
        """Get batch of pending tasks."""
        conn = self._get_conn()
        
        # Atomic: get and mark as running in one transaction
        conn.execute("BEGIN IMMEDIATE")
        
        rows = conn.execute("""
            SELECT * FROM tasks 
            WHERE status = ? 
            ORDER BY priority DESC, created_at ASC
            LIMIT ?
        """, (TaskStatus.PENDING.value, count)).fetchall()
        
        if not rows:
            return []

        task_ids = [r['id'] for r in rows]
        conn.execute("""
            UPDATE tasks SET status = ?, started_at = ?
            WHERE id IN ({})
        """.format(','.join('?' * len(task_ids))),
            [TaskStatus.RUNNING.value, time.time()] + task_ids)
        
        conn.commit()
        
        tasks = []
        for row in rows:
            tasks.append(LLMTask(
                task_id=row['id'],
                agent_id=row['agent_id'],
                messages=json.loads(row['messages']),
                tools=json.loads(row['tools']),
                model=row['model'],
                api_config=json.loads(row['api_config']),
                metadata=json.loads(row['metadata']),
                priority=row['priority'],
                created_at=row['created_at']
            ))
        
        return tasks
    
    def get_queue_size(self) -> int:
        """Get number of pending tasks."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?",
            (TaskStatus.PENDING.value,)
        ).fetchone()
        return result[0] if result else 0
    
    def get_pending_tasks(self, limit: int = None) -> list[dict]:
        conn = self._get_conn()
        query = """SELECT * FROM tasks 
            WHERE status = ? AND slurm_job_id IS NULL 
            ORDER BY priority DESC, created_at ASC"""
        if limit:
            query += f" LIMIT {limit}"
        
        rows = conn.execute(query, (TaskStatus.PENDING.value,)).fetchall()
        return [dict(row) for row in rows]
    
    def update_slurm_job_id(self, task_ids: list[str], job_id: int):
        """Update slurm_job_id for processed tasks."""
        conn = self._get_conn()
        placeholders = ','.join('?' * len(task_ids))
        conn.execute(f"""
            UPDATE tasks SET slurm_job_id = ? 
            WHERE id IN ({placeholders})
        """, [job_id] + task_ids)
        conn.commit()
    
    def ping(self) -> bool:
        """Check if database is accessible."""
        try:
            conn = self._get_conn()
            conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False
    
    def get_metrics(self) -> dict:
        """Get queue metrics."""
        conn = self._get_conn()
        
        pending = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?",
            (TaskStatus.PENDING.value,)
        ).fetchone()[0]
        
        running = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?",
            (TaskStatus.RUNNING.value,)
        ).fetchone()[0]
        
        completed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?",
            (TaskStatus.COMPLETED.value,)
        ).fetchone()[0]
        
        failed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = ?",
            (TaskStatus.FAILED.value,)
        ).fetchone()[0]
        
        return {
            "pending": pending,
            "running": running,
            "completed": completed,
            "failed": failed,
            "total": pending + running + completed + failed
        }
    
    def on_task_completed(self, task_id: str, callback):
        """Register completion callback."""
        self._callbacks[task_id] = callback
    
    def subscribe_completed(self, callback):
        pass
    
    def log_metric(self, event: str, task_id: str = None, job_id: int = None, metadata: dict = None):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO metrics (event, task_id, job_id, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, (event, task_id, job_id, time.time(), json.dumps(metadata) if metadata else None))
        conn.commit()


def create_task(prompt: str, agent_id: str = "default",
               model: str = "anthropic/claude-sonnet-4-20250514",
               api_config: dict = None, **metadata) -> LLMTask:
    """Helper to create a task from simple prompt."""
    return LLMTask(
        task_id=str(uuid.uuid4()),
        agent_id=agent_id,
        messages=[{"role": "user", "content": prompt}],
        tools=[],  # Add tools as needed
        model=model,
        api_config=api_config or {},
        metadata=metadata
    )


if __name__ == "__main__":
    # Test the SQLite queue
    import sys
    
    db_path = sys.argv[1] if len(sys.argv) > 1 else "test_tasks.db"
    
    # Remove existing test db
    if os.path.exists(db_path):
        os.remove(db_path)
    
    queue = SQLiteTaskQueue(db_path=db_path)
    
    # Add test tasks
    for i in range(5):
        task = create_task(f"Task {i+1}: Process this request")
        task_id = queue.enqueue(task)
        print(f"Created task: {task_id}")
    
    print(f"\nQueue size: {queue.get_queue_size()}")
    print(f"Metrics: {queue.get_metrics()}")
    
    # Dequeue a batch
    tasks = queue.dequeue_batch(3, "test-worker")
    print(f"\nDequeued {len(tasks)} tasks:")
    for t in tasks:
        print(f"  - {t.task_id}: {t.messages[0]['content'][:50]}...")
    
    print(f"\nRemaining queue size: {queue.get_queue_size()}")