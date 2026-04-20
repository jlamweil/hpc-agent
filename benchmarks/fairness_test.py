#!/usr/bin/env python3
"""Fairness test for priority handling."""
import sqlite3
import json
from datetime import datetime

def run_fairness_test(model="Qwen2.5-Coder-32B", high_count=10, low_count=30):
    conn = sqlite3.connect("tasks.db")
    cursor = conn.cursor()
    
    for i in range(high_count):
        tid = f"fair_high_{i}"
        cursor.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, 
                             metadata, priority, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tid, "bench-agent", json.dumps([{"role": "user", "content": f"High priority {i}"}]),
              json.dumps([]), model, json.dumps({}),
              json.dumps({"benchmark": True}), 10, datetime.now().timestamp(), "pending"))
    
    for i in range(low_count):
        tid = f"fair_low_{i}"
        cursor.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, 
                             metadata, priority, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tid, "bench-agent", json.dumps([{"role": "user", "content": f"Low priority {i}"}]),
              json.dumps([]), model, json.dumps({}),
              json.dumps({"benchmark": True}), 0, datetime.now().timestamp(), "pending"))
    
    conn.commit()
    
    cursor.execute("""
        SELECT id, priority FROM tasks 
        WHERE status = 'pending' ORDER BY priority DESC, created_at ASC
    """)
    all_tasks = cursor.fetchall()
    
    total_claim = min(20, len(all_tasks))
    fresh_claim = int(total_claim * 0.7)
    
    claim_ids = [t[0] for t in all_tasks[:total_claim]]
    
    for i, tid in enumerate(claim_ids):
        if i < fresh_claim:
            cursor.execute("""
                UPDATE tasks SET status = 'claimed', worker_id = 'fair-worker', 
                claimed_at = ? WHERE id = ?
            """, (datetime.now().timestamp(), tid))
        else:
            cursor.execute("""
                UPDATE tasks SET status = 'claimed', worker_id = 'fair-worker', 
                claimed_at = ?, retry_at = NULL WHERE id = ? AND attempt_count > 0
            """, (datetime.now().timestamp(), tid))
    
    conn.commit()
    
    cursor.execute("""
        SELECT id, priority, status FROM tasks WHERE id IN ({})
    """.format(','.join('?' for _ in claim_ids)), claim_ids)
    
    results = cursor.fetchall()
    conn.close()
    
    high_claimed = sum(1 for r in results if r[1] == 10 and r[2] == 'claimed')
    low_claimed = sum(1 for r in results if r[1] == 0 and r[2] == 'claimed')
    
    return {
        "model": model,
        "high_priority_tasks": high_count,
        "low_priority_tasks": low_count,
        "total_claimed": len(results),
        "high_claimed": high_claimed,
        "low_claimed": low_claimed,
        "fairness_ratio": high_claimed / low_claimed if low_claimed > 0 else 0,
        "expected_high_ratio": 0.7,
        "actual_high_ratio": high_claimed / total_claim if total_claim > 0 else 0
    }

if __name__ == "__main__":
    import sys
    model = sys.argv[1] if len(sys.argv) > 1 else "Qwen2.5-Coder-32B"
    high = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    low = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    
    result = run_fairness_test(model, high, low)
    print(json.dumps(result, indent=2))
    
    with open(f"benchmarks/{model.replace('/', '_')}_fairness.json", "w") as f:
        json.dump(result, f, indent=2)
