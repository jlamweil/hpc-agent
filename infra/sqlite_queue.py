"""SQLite task queue - local persistence layer for HPC architecture."""
import sqlite3
import json
import time
import uuid
import threading
from dataclasses import asdict
from typing import Optional, List
import os
import sys

HERO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASYNC_HERMES = os.path.join(os.path.dirname(HERO_DIR), "async-hermes-agent")
sys.path.insert(0, ASYNC_HERMES)

from runtime.task_queue import TaskQueue, LLMTask, TaskResult, TaskStatus


TASK_STATES = {
    "pending": "pending",
    "claimed": "claimed", 
    "running": "running",
    "done": "done",
    "failed": "failed"
}

VALID_TRANSITIONS = {
    "pending": ["claimed"],
    "claimed": ["running", "pending"],
    "running": ["done", "failed"],
    "done": [],
    "failed": ["pending"]
}

DEFAULT_MAX_ATTEMPTS = 3

DETERMINISTIC_ERRORS = frozenset([
    "invalid json",
    "parse error",
    "syntax error",
    "invalid prompt",
    "model not found",
    "invalid request",
    "authentication failed",
    "unauthorized",
    "rate limit exceeded",
    "quota exceeded",
])

def compute_backoff(attempt: int) -> float:
    return min(60 * (2 ** attempt), 3600)

def is_deterministic_error(error: str) -> bool:
    if not error:
        return False
    error_lower = error.lower()
    return any(known in error_lower for known in DETERMINISTIC_ERRORS)


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
                retry_count INTEGER DEFAULT 0,
                worker_id TEXT,
                claimed_at REAL,
                attempt_count INTEGER DEFAULT 0
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
        
        self._migrate_schema(conn)
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_created ON tasks(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_worker ON tasks(worker_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_claimed ON tasks(claimed_at)")
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
        pass
    
    def _migrate_schema(self, conn: sqlite3.Connection):
        cols = {r[1]: i for i, r in enumerate(conn.execute("PRAGMA table_info(tasks)").fetchall())}
        if "worker_id" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN worker_id TEXT")
        if "claimed_at" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN claimed_at REAL")
        if "attempt_count" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN attempt_count INTEGER DEFAULT 0")
        if "max_attempts" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN max_attempts INTEGER DEFAULT 3")
        if "last_error" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN last_error TEXT")
        if "retry_at" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN retry_at REAL")
        if "task_type" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN task_type TEXT DEFAULT 'llm'")
        conn.commit()
    
    def _validate_transition(self, from_state: str, to_state: str) -> bool:
        allowed = VALID_TRANSITIONS.get(from_state, [])
        return to_state in allowed
    
    def _transition_status(self, task_id: str, new_state: str) -> bool:
        conn = self._get_conn()
        row = conn.execute("SELECT status FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if not row:
            return False
        current = row[0]
        if not self._validate_transition(current, new_state):
            return False
        conn.execute("UPDATE tasks SET status = ? WHERE id = ?", (new_state, task_id))
        conn.commit()
        return True
    
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
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        
        if not row:
            return None
        
        if row['status'] not in ["done", "failed"]:
            return None
        
        return TaskResult(
            task_id=task_id,
            status=TaskStatus.COMPLETED if row['status'] == "done" else TaskStatus.FAILED,
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
        conn = self._get_conn()
        cursor = conn.execute(
            "UPDATE tasks SET status = 'running', started_at = ? WHERE id = ? AND status = 'claimed'",
            (time.time(), task_id)
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
        conn = self._get_conn()
        result = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending'"
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
        try:
            conn = self._get_conn()
            conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False
    
    def claim_tasks(self, worker_id: str, batch_size: int, fresh_ratio: float = 0.7) -> List[dict]:
        conn = self._get_conn()
        now = time.time()
        
        fresh_count = int(batch_size * fresh_ratio)
        retry_count = batch_size - fresh_count
        
        if retry_count > 0:
            conn.execute("""
                UPDATE tasks
                SET status = 'claimed',
                    worker_id = ?,
                    claimed_at = ?,
                    retry_at = NULL,
                    attempt_count = COALESCE(attempt_count, 0) + 1
                WHERE id IN (
                    SELECT id FROM tasks
                    WHERE status = 'pending'
                    AND (retry_at IS NULL OR retry_at <= ?)
                    AND attempt_count > 0
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                )
            """, (worker_id, now, now, retry_count))
        
        if fresh_count > 0:
            conn.execute("""
                UPDATE tasks
                SET status = 'claimed',
                    worker_id = ?,
                    claimed_at = ?,
                    retry_at = NULL,
                    attempt_count = COALESCE(attempt_count, 0) + 1
                WHERE id IN (
                    SELECT id FROM tasks
                    WHERE status = 'pending'
                    AND (retry_at IS NULL OR retry_at <= ?)
                    AND (attempt_count IS NULL OR attempt_count = 0)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                )
            """, (worker_id, now, now, fresh_count))
        
        conn.commit()
        
        rows = conn.execute("""
            SELECT * FROM tasks
            WHERE worker_id = ? AND claimed_at = ?
        """, (worker_id, now)).fetchall()
        
        return [dict(row) for row in rows]
    
    def release_task(self, task_id: str) -> bool:
        conn = self._get_conn()
        conn.execute("""
            UPDATE tasks
            SET status = 'pending',
                worker_id = NULL,
                claimed_at = NULL
            WHERE id = ? AND status = 'claimed'
        """, (task_id,))
        conn.commit()
        return True
    
    def handle_task_failure(self, task_id: str, error: str) -> dict:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT attempt_count, max_attempts, priority FROM tasks WHERE id = ?
        """, (task_id,)).fetchone()
        
        if not row:
            return {"action": "error", "message": "Task not found"}
        
        attempt = row[0] or 0
        max_att = row[1] or DEFAULT_MAX_ATTEMPTS
        current_priority = row[2] or 0
        
        if is_deterministic_error(error):
            conn.execute("""
                UPDATE tasks
                SET status = 'failed',
                    last_error = ?,
                    completed_at = ?
                WHERE id = ?
            """, (error, time.time(), task_id))
            conn.commit()
            return {
                "action": "failed_deterministic",
                "reason": "deterministic error - no retry"
            }
        
        if attempt < max_att:
            backoff = compute_backoff(attempt + 1)
            retry_at = time.time() + backoff
            new_priority = max(-5, current_priority - 1)
            conn.execute("""
                UPDATE tasks
                SET status = 'pending',
                    worker_id = NULL,
                    claimed_at = NULL,
                    retry_at = ?,
                    last_error = ?,
                    attempt_count = attempt_count + 1,
                    priority = ?
                WHERE id = ?
            """, (retry_at, error, new_priority, task_id))
            conn.commit()
            return {
                "action": "retry",
                "retry_at": retry_at,
                "attempt": attempt + 1,
                "max_attempts": max_att,
                "old_priority": current_priority,
                "new_priority": new_priority
            }
        else:
            conn.execute("""
                UPDATE tasks
                SET status = 'failed',
                    last_error = ?,
                    completed_at = ?
                WHERE id = ?
            """, (error, time.time(), task_id))
            conn.commit()
            return {
                "action": "failed",
                "attempt": attempt,
                "max_attempts": max_att
            }
    
    def mark_claimed_with_job(self, task_id: str, slurm_job_id: int):
        conn = self._get_conn()
        conn.execute("""
            UPDATE tasks SET slurm_job_id = ? WHERE id = ?
        """, (slurm_job_id, task_id))
        conn.commit()
    
    def update_task_status(self, task_id: str, status: str, 
                          started_at: float = None, completed_at: float = None,
                          result: str = None, error: str = None):
        conn = self._get_conn()
        set_clauses = ["status = ?"]
        params = [status]
        
        if started_at is not None:
            set_clauses.append("started_at = ?")
            params.append(started_at)
        if completed_at is not None:
            set_clauses.append("completed_at = ?")
            params.append(completed_at)
        if result is not None:
            set_clauses.append("result = ?")
            params.append(result)
        if error is not None:
            set_clauses.append("error = ?")
            params.append(error)
        
        params.append(task_id)
        conn.execute(f"UPDATE tasks SET {', '.join(set_clauses)} WHERE id = ?", params)
        conn.commit()
    
    def recover_stuck_claimed(self, timeout_seconds: int = 600):
        conn = self._get_conn()
        cutoff = time.time() - timeout_seconds
        conn.execute("""
            UPDATE tasks
            SET status = 'pending', worker_id = NULL, claimed_at = NULL
            WHERE status = 'claimed' AND claimed_at < ?
        """, (cutoff,))
        conn.commit()
    
    def recover_stuck_running(self, timeout_seconds: int = 3600):
        conn = self._get_conn()
        cutoff = time.time() - timeout_seconds
        
        rows = conn.execute("""
            SELECT id, attempt_count, max_attempts FROM tasks
            WHERE status = 'running' AND started_at < ?
        """, (cutoff,)).fetchall()
        
        for row in rows:
            task_id = row[0]
            attempt = row[1] or 0
            max_att = row[2] or DEFAULT_MAX_ATTEMPTS
            
            if attempt < max_att:
                backoff = compute_backoff(attempt + 1)
                retry_at = time.time() + backoff
                conn.execute("""
                    UPDATE tasks
                    SET status = 'pending',
                        worker_id = NULL,
                        claimed_at = NULL,
                        retry_at = ?,
                        last_error = 'Stuck running - auto recovered'
                    WHERE id = ?
                """, (retry_at, task_id))
            else:
                conn.execute("""
                    UPDATE tasks
                    SET status = 'failed',
                        last_error = 'Stuck running - max attempts reached',
                        completed_at = ?
                    WHERE id = ?
                """, (time.time(), task_id))
        
        conn.commit()
    
    def get_metrics(self) -> dict:
        conn = self._get_conn()
        
        pending = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending'"
        ).fetchone()[0]
        
        claimed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'claimed'"
        ).fetchone()[0]
        
        running = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'running'"
        ).fetchone()[0]
        
        completed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'done'"
        ).fetchone()[0]
        
        failed = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'failed'"
        ).fetchone()[0]
        
        fresh_pending = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND (attempt_count IS NULL OR attempt_count = 0)"
        ).fetchone()[0]
        
        retry_pending = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status = 'pending' AND attempt_count > 0"
        ).fetchone()[0]
        
        total_attempts = conn.execute(
            "SELECT SUM(COALESCE(attempt_count, 0)) FROM tasks"
        ).fetchone()[0] or 0
        
        avg_queue_delay = conn.execute("""
            SELECT AVG(claimed_at - created_at) FROM tasks 
            WHERE claimed_at IS NOT NULL
        """).fetchone()[0] or 0
        
        avg_exec_time = conn.execute("""
            SELECT AVG(completed_at - started_at) FROM tasks 
            WHERE started_at IS NOT NULL AND completed_at IS NOT NULL
        """).fetchone()[0] or 0
        
        return {
            "pending": pending,
            "claimed": claimed,
            "running": running,
            "completed": completed,
            "failed": failed,
            "total": pending + claimed + running + completed + failed,
            "fresh_pending": fresh_pending,
            "retry_pending": retry_pending,
            "total_attempts": total_attempts,
            "retry_rate": (total_attempts / max(1, completed + failed)),
            "avg_queue_delay_sec": avg_queue_delay,
            "avg_execution_time_sec": avg_exec_time
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