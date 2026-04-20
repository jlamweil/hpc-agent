#!/usr/bin/env python3
"""Correctness test for generated code."""
import sqlite3
import subprocess
import json
from datetime import datetime

def test_code_correctness(task_id):
    conn = sqlite3.connect("tasks.db")
    cursor = conn.cursor()
    cursor.execute("SELECT result FROM tasks WHERE id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return False, "Task not found"
    
    result = json.loads(row[0])
    code = result.get("content", "")
    
    if not code:
        return False, "Empty code"
    
    with open("/tmp/bench_code.py", "w") as f:
        f.write(code)
    
    try:
        subprocess.run(["python", "/tmp/bench_code.py"], check=True, timeout=10, capture_output=True)
        return True, "OK"
    except subprocess.CalledProcessError as e:
        return False, f"Runtime error: {e.stderr.decode()[:200]}"
    except Exception as e:
        return False, f"Exception: {str(e)[:200]}"

def run_correctness_test(model="Qwen2.5-Coder-32B", count=10):
    conn = sqlite3.connect("tasks.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, result FROM tasks 
        WHERE model = ? AND status = 'done' 
        ORDER BY completed_at DESC LIMIT ?
    """, (model, count))
    
    rows = cursor.fetchall()
    conn.close()
    
    passed = 0
    failed = 0
    details = []
    
    for task_id, result_json in rows:
        result = json.loads(result_json)
        code = result.get("content", "")
        
        is_valid = True
        error = ""
        
        try:
            compile(code, "<string>", "exec")
        except SyntaxError as e:
            is_valid = False
            error = f"Syntax error: {str(e)[:100]}"
        
        if is_valid:
            try:
                subprocess.run(["python", "/tmp/bench_code.py"], 
                             check=True, timeout=10, capture_output=True)
            except Exception as e:
                is_valid = False
                error = f"Runtime error: {str(e)[:100]}"
        
        if is_valid:
            passed += 1
        else:
            failed += 1
        
        details.append({
            "task_id": task_id,
            "passed": is_valid,
            "error": error
        })
    
    return {
        "model": model,
        "total": len(rows),
        "passed": passed,
        "failed": failed,
        "success_rate": passed / len(rows) if rows else 0,
        "details": details
    }

if __name__ == "__main__":
    import sys
    model = sys.argv[1] if len(sys.argv) > 1 else "Qwen2.5-Coder-32B"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    result = run_correctness_test(model, count)
    print(json.dumps(result, indent=2))
    
    with open(f"benchmarks/{model.replace('/', '_')}_correctness.json", "w") as f:
        json.dump(result, f, indent=2)
