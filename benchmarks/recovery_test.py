#!/usr/bin/env python3
"""Recovery test for stuck task handling."""
import sqlite3
import json
import time
from datetime import datetime

def run_recovery_test(model="Qwen2.5-Coder-32B"):
    conn = sqlite3.connect("tasks.db")
    cursor = conn.cursor()
    
    task_ids = []
    for i in range(5):
        tid = f"recovery_{i}"
        cursor.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, 
                             metadata, priority, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tid, "bench-agent", json.dumps([{"role": "user", "content": f"Stuck task {i}"}]),
              json.dumps([]), model, json.dumps({}),
              json.dumps({"benchmark": True}), 0, datetime.now().timestamp(), "claimed"))
        task_ids.append(tid)
    
    conn.commit()
    
    stuck_time = time.time() - 600
    cursor.execute(f"""
        UPDATE tasks SET claimed_at = {stuck_time}, worker_id = 'dead-worker'
        WHERE id IN ({','.join('?' for _ in task_ids)})
    """, task_ids)
    conn.commit()
    
    from infra.sqlite_queue import SQLiteTaskQueue
    queue = SQLiteTaskQueue(db_path="tasks.db")
    queue.recover_stuck_claimed(timeout_seconds=300)
    
    cursor.execute("SELECT id, status FROM tasks WHERE id IN ({})".format(
        ','.join('?' for _ in task_ids)), task_ids)
    
    results = cursor.fetchall()
    recovered = sum(1 for r in results if r[1] == 'pending')
    
    task_ids2 = [f"recovery_running_{i}" for i in range(3)]
    for tid in task_ids2:
        cursor.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, 
                             metadata, priority, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tid, "bench-agent", json.dumps([{"role": "user", "content": f"Running {tid}"}]),
              json.dumps([]), model, json.dumps({}),
              json.dumps({"benchmark": True}), 0, datetime.now().timestamp(), "running"))
    
    conn.commit()
    
    start_time = time.time() - 3600
    cursor.execute(f"""
        UPDATE tasks SET started_at = {start_time}, worker_id = 'dead-worker'
        WHERE id IN ({','.join('?' for _ in task_ids2)})
    """, task_ids2)
    conn.commit()
    
    queue.recover_stuck_running(timeout_seconds=1800)
    
    cursor.execute("SELECT id, status FROM tasks WHERE id IN ({})".format(
        ','.join('?' for _ in task_ids2)), task_ids2)
    
    running_results = cursor.fetchall()
    recovered_running = sum(1 for r in running_results if r[1] in ['pending', 'failed'])
    
    conn.close()
    
    return {
        "model": model,
        "claimed_recovered": recovered,
        "claimed_total": len(task_ids),
        "running_recovered": recovered_running,
        "running_total": 3,
        "recovery_success": recovered == len(task_ids) and recovered_running == 3
    }

if __name__ == "__main__":
    import sys
    model = sys.argv[1] if len(sys.argv) > 1 else "Qwen2.5-Coder-32B"
    
    result = run_recovery_test(model)
    print(json.dumps(result, indent=2))
    
    with open(f"benchmarks/{model.replace('/', '_')}_recovery.json", "w") as f:
        json.dump(result, f, indent=2)
