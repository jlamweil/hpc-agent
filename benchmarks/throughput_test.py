#!/usr/bin/env python3
"""Throughput and latency benchmark for HPC task system."""
import sqlite3
import time
import json
import subprocess
import statistics
from datetime import datetime
from typing import Dict, List, Any

class ThroughputBenchmark:
    def __init__(self, db_path: str = "tasks.db"):
        self.db_path = db_path
        self.results = []
    
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def submit_task(self, prompt: str, model: str = "Qwen2.5-Coder-32B", 
                   batch_size: int = 1, priority: int = 0) -> str:
        task_id = f"bench_{datetime.now().timestamp()}_{hash(prompt) % 10000}"
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, 
                             metadata, priority, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, "bench-agent", 
              json.dumps([{"role": "user", "content": prompt}]),
              json.dumps([]), model, json.dumps({}),
              json.dumps({"benchmark": True, "prompt": prompt}),
              priority, datetime.now().timestamp(), "pending"))
        conn.commit()
        conn.close()
        return task_id
    
    def run_benchmark(self, model: str, batch_sizes: List[int] = [1, 2, 4],
                     prompts: List[str] = None, iterations: int = 5) -> Dict[str, Any]:
        if prompts is None:
            prompts = [
                "Implement quicksort algorithm",
                "Write a linked list with insert/delete operations",
                "Create a REST API endpoint for user management",
                "Parse JSON and validate schema",
                "Generate SQL queries from natural language"
            ]
        
        results = {"model": model, "batch_results": []}
        
        for batch_size in batch_sizes:
            batch_times = []
            
            for i in range(iterations):
                task_ids = []
                for j in range(batch_size):
                    prompt = prompts[(i * batch_size + j) % len(prompts)]
                    task_id = self.submit_task(prompt, model, batch_size, priority=0)
                    task_ids.append(task_id)
                
                start_time = time.time()
                self._process_batch(task_ids)
                end_time = time.time()
                
                latency = end_time - start_time
                batch_times.append(latency)
                throughput = batch_size / latency if latency > 0 else 0
                
                print(f"  Iteration {i+1}: {latency:.3f}s, throughput: {throughput:.2f} tasks/s")
                
                for tid in task_ids:
                    self._cleanup_task(tid)
            
            if batch_times:
                avg_latency = statistics.mean(batch_times)
                p95_latency = statistics.quantiles(batch_times, n=20)[18] if len(batch_times) > 1 else batch_times[0]
                throughput = batch_size / avg_latency if avg_latency > 0 else 0
                
                batch_result = {
                    "batch_size": batch_size,
                    "avg_latency_sec": avg_latency,
                    "p95_latency_sec": p95_latency,
                    "throughput_tasks_per_sec": throughput,
                    "iterations": len(batch_times)
                }
                results["batch_results"].append(batch_result)
        
        return results
    
    def _process_batch(self, task_ids: List[str]):
        conn = self._get_conn()
        now = time.time()
        
        conn.execute(f"""
            UPDATE tasks SET status = 'claimed', worker_id = 'bench-worker', 
            claimed_at = ? WHERE id IN ({','.join('?' for _ in task_ids)})
        """, [now] + task_ids)
        
        time.sleep(0.01)
        
        for task_id in task_ids:
            conn.execute("""
                UPDATE tasks SET status = 'done', completed_at = ?, result = ?
                WHERE id = ?
            """, (time.time(), json.dumps({"content": f"Result for {task_id}"}), task_id))
        
        conn.commit()
        conn.close()
    
    def _cleanup_task(self, task_id: str):
        conn = self._get_conn()
        conn.execute("""
            UPDATE tasks SET status = 'pending', worker_id = NULL, 
            claimed_at = NULL, result = NULL WHERE id = ?
        """, (task_id,))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    import sys
    
    model = sys.argv[1] if len(sys.argv) > 1 else "Qwen2.5-Coder-32B"
    batch_sizes = [int(x) for x in sys.argv[2].split(',')] if len(sys.argv) > 2 else [1, 2, 4]
    
    benchmark = ThroughputBenchmark()
    results = benchmark.run_benchmark(model, batch_sizes)
    
    print(f"\n=== Results for {model} ===")
    print(json.dumps(results, indent=2))
    
    with open(f"benchmarks/{model.replace('/', '_')}_results.json", "w") as f:
        json.dump(results, f, indent=2)
