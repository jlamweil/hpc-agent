#!/usr/bin/env python3
"""Worker - pulls tasks from SQLite and processes with transformers."""
import argparse
import json
import os
import sqlite3
import sys
import time


DEFAULT_DB = "tasks.db"


def get_pending_tasks(db_path: str, limit: int) -> list:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    rows = conn.execute("""
        SELECT * FROM tasks 
        WHERE status = 'pending' 
        ORDER by priority DESC, created_at ASC
        LIMIT ?
    """, (limit,)).fetchall()
    
    tasks = []
    for row in rows:
        tasks.append({
            "id": row["id"],
            "messages": json.loads(row["messages"]),
            "metadata": json.loads(row["metadata"]),
            "model": row["model"]
        })
        
        conn.execute("""
            UPDATE tasks SET status = 'running', started_at = ?
            WHERE id = ?
        """, (time.time(), row["id"]))
    
    conn.commit()
    conn.close()
    return tasks


def update_task_status(db_path: str, task_id: str, status: str, result: dict = None, error: str = None):
    conn = sqlite3.connect(db_path)
    completed_at = time.time()
    
    conn.execute("""
        UPDATE tasks SET status = ?, completed_at = ?, result = ?, error = ?
        WHERE id = ?
    """, (status, completed_at, json.dumps(result) if result else None, error, task_id))
    conn.commit()
    conn.close()


def process_task_with_transformers(task: dict, model_name: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0") -> dict:
    from transformers import AutoTokenizer, AutoModelForCausalLM
    import torch
    
    prompt = task["messages"][0]["content"]
    
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype=torch.float16,
        device_map="auto"
    )
    
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(**inputs, max_new_tokens=100, do_sample=False)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    return {
        "response": response,
        "model": model_name,
        "status": "completed"
    }


def run_worker(db_path: str, batch_size: int = 5, model_name: str = None, dry_run: bool = False):
    print(f"Worker starting (batch_size={batch_size})")
    
    while True:
        tasks = get_pending_tasks(db_path, batch_size)
        
        if not tasks:
            print(f"No pending tasks, waiting...")
            time.sleep(10)
            continue
        
        print(f"Processing {len(tasks)} tasks")
        
        for task in tasks:
            try:
                print(f"Processing task: {task['id']}")
                
                if dry_run:
                    result = {"response": "DRY RUN - no actual inference", "status": "completed"}
                else:
                    model = task.get("model") or model_name
                    result = process_task_with_transformers(task, model)
                
                update_task_status(db_path, task["id"], "completed", result)
                print(f"Task {task['id']} completed")
                
            except Exception as e:
                print(f"Task {task['id']} failed: {e}")
                update_task_status(db_path, task["id"], "failed", error=str(e))
        
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Process tasks from SQLite")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database")
    parser.add_argument("--batch-size", type=int, default=5, help="Tasks per batch")
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0", help="Model name")
    parser.add_argument("--dry-run", action="store_true", help="Don't run actual inference")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        return
    
    run_worker(args.db, args.batch_size, args.model, args.dry_run)


if __name__ == "__main__":
    main()