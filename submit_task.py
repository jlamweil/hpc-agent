#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime

DEFAULT_DB = "tasks.db"


def init_db(db_path: str):
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


def submit_task(prompt: str, db_path: str, agents: str = "default", 
                model: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0",
                priority: int = 0, metadata: dict = None):
    task_id = str(uuid.uuid4())
    created_at = datetime.now().timestamp()
    
    agent_list = [a.strip() for a in agents.split(",")] if agents else ["default"]
    
    messages = [{"role": "user", "content": prompt}]
    tools = []
    task_metadata = metadata or {}
    task_metadata["agents"] = agent_list
    
    conn = sqlite3.connect(db_path)
    conn.execute("""
        INSERT INTO tasks (
            id, agent_id, messages, tools, model, api_config, metadata,
            priority, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        agents,
        json.dumps(messages),
        json.dumps(tools),
        model,
        json.dumps({}),
        json.dumps(task_metadata),
        priority,
        created_at,
        "pending"
    ))
    conn.commit()
    conn.close()
    
    print(f"Submitted task: {task_id}")
    print(f"  Prompt: {prompt[:60]}...")
    print(f"  Agents: {agent_list}")
    print(f"  Model: {model}")
    print(f"  Status: pending")
    
    return task_id


def submit_batch(prompts_file: str, db_path: str, agents: str = "default"):
    with open(prompts_file, 'r') as f:
        prompts = [line.strip() for line in f if line.strip()]
    
    task_ids = []
    for prompt in prompts:
        tid = submit_task(prompt, db_path, agents)
        task_ids.append(tid)
    
    print(f"\nSubmitted {len(task_ids)} tasks")


def main():
    parser = argparse.ArgumentParser(description="Submit tasks to HPC queue")
    parser.add_argument("--prompt", help="Task prompt")
    parser.add_argument("--file", help="File with prompts (one per line)")
    parser.add_argument("--agents", default="default", help="Comma-separated agent names")
    parser.add_argument("--model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0", help="Model to use")
    parser.add_argument("--priority", type=int, default=0, help="Task priority (higher = first)")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    
    args = parser.parse_args()
    
    init_db(args.db)
    
    if args.file:
        submit_batch(args.file, args.db, args.agents)
    elif args.prompt:
        submit_task(args.prompt, args.db, args.agents, args.model, args.priority)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()