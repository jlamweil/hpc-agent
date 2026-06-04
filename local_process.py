#!/usr/bin/env python3
"""Local task processor — replaces HPC SLURM job for testing.
Reads pending tasks from SQLite, processes with Ollama, writes results.
Usage: python3 local_process.py [--db tasks.db] [--model qwen2.5:0.5b] [--limit 5]
"""
import argparse
import json
import os
import sqlite3
import time
import urllib.request
import urllib.error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="tasks.db")
    parser.add_argument("--model", default="qwen2.5:0.5b")
    parser.add_argument("--ollama-url", default="http://localhost:11434")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--poll", action="store_true", help="Keep polling for new tasks")
    args = parser.parse_args()

    db_path = args.db
    if not os.path.dirname(db_path):
        db_path = os.path.join(os.path.dirname(__file__) or ".", db_path)

    api_base = args.ollama_url.rstrip("/")

    print(f"Local processor: {args.model} | DB: {db_path} | API: {api_base}")

    # Ensure Ollama is running
    try:
        r = urllib.request.urlopen(f"{api_base}/api/tags", timeout=5)
        models = json.loads(r.read())
        model_names = [m["name"] for m in models.get("models", [])]
        print(f"  Ollama models: {model_names}")
        if args.model not in model_names:
            print(f"  WARN: {args.model} not found, pulling...")
    except Exception as e:
        print(f"  ERROR: Ollama not reachable at {api_base}: {e}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    processed = 0
    while True:
        # Ensure task_deps table exists (migration for older DBs)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_deps (
                task_id TEXT NOT NULL,
                depends_on TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (task_id, depends_on)
            )
        """)

        # Dequeue pending tasks (respecting DAG dependencies)
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute("""
            SELECT id, messages, model, metadata FROM tasks
            WHERE status = 'pending'
              AND NOT EXISTS (
                  SELECT 1 FROM task_deps
                  WHERE task_deps.task_id = tasks.id
                    AND task_deps.depends_on NOT IN (
                        SELECT id FROM tasks WHERE status = 'completed'
                    )
              )
            ORDER BY created_at ASC
            LIMIT ?
        """, (args.limit,)).fetchall()

        if not rows:
            conn.execute("ROLLBACK")
            if processed > 0:
                print(f"  All done — {processed} tasks processed")
            if args.poll:
                print("  No pending tasks, waiting 10s...")
                time.sleep(10)
                continue
            break

        task_ids = [r["id"] for r in rows]
        conn.execute(
            "UPDATE tasks SET status = 'running', started_at = ? WHERE id IN ({})".format(
                ",".join("?" * len(task_ids))
            ),
            [time.time()] + task_ids,
        )
        conn.commit()
        print(f"\nDequeued {len(rows)} tasks")

        for row in rows:
            task_id = row["id"]
            task_model = row["model"] or args.model
            messages = json.loads(row["messages"])
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
            task_type = metadata.get("type", "generic")

            # Extract user prompt
            prompt_text = ""
            for msg in messages if isinstance(messages, list) else [{"role": "user", "content": str(messages)}]:
                if msg.get("role") in ("user", "system"):
                    prompt_text += msg.get("content", "") + "\n"

            print(f"  [{task_type}] {task_id[:12]}... ({len(prompt_text)} chars)")

            # Truncate very long prompts (Ollama has context limits)
            max_chars = 12000
            if len(prompt_text) > max_chars:
                prompt_text = prompt_text[:max_chars] + "\n\n[TRUNCATED]"

            # Call Ollama
            try:
                body = json.dumps({
                    "model": args.model,
                    "prompt": prompt_text,
                    "stream": False,
                    "options": {
                        "temperature": 0.7,
                        "num_predict": 2048,
                    }
                }).encode()

                req = urllib.request.Request(
                    f"{api_base}/api/generate",
                    data=body,
                    headers={"Content-Type": "application/json"},
                )
                resp = json.loads(urllib.request.urlopen(req, timeout=300).read())

                content = resp.get("response", "")
                usage = {
                    "prompt_tokens": resp.get("prompt_eval_count", 0),
                    "completion_tokens": resp.get("eval_count", 0),
                    "total_tokens": (resp.get("prompt_eval_count", 0) +
                                     resp.get("eval_count", 0)),
                }

                result_data = json.dumps({
                    "content": content,
                    "tool_calls": None,
                    "reasoning": None,
                    "finish_reason": "stop",
                    "usage": usage,
                })

                conn.execute(
                    "UPDATE tasks SET status = 'completed', completed_at = ?, result = ? WHERE id = ?",
                    (time.time(), result_data, task_id),
                )
                conn.commit()
                print(f"    OK ({usage['total_tokens']} tokens, {len(content)} chars)")
                processed += 1

            except Exception as e:
                error_msg = str(e)[:500]
                print(f"    FAIL: {error_msg[:60]}")
                conn.execute(
                    "UPDATE tasks SET status = 'failed', error = ?, completed_at = ? WHERE id = ?",
                    (error_msg, time.time(), task_id),
                )
                conn.commit()

    conn.close()
    print(f"\nDone. Processed {processed} tasks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
