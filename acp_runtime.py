#!/usr/bin/env python3
"""Minimal Hermes-compatible ACP Runtime for HPC.

Multica → ACP → orchestrator → HPC → result → Multica

This is a DETERMINISTIC, BLOCKING, LLM-FREE protocol adapter.
"""
import json
import os
import sys
import time
import uuid
from pathlib import Path

# Add orchestrator path to sys.path
HERMES_PATH = os.path.expanduser("~/Documents/GAIA/async-hermes-agent")
HPC_AGENT_PATH = os.path.expanduser("~/Documents/GAIA/hpc-agent")
sys.path.insert(0, HPC_AGENT_PATH)

# Import HPC orchestrator
try:
    from orchestrator import submit_hpc_task, wait_for_task, get_task_result
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False
    # Fallback: submit directly to SQLite
    import sqlite3
    import json as _json


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_TIMEOUT = 600  # 10 minutes
HPC_DB_PATH = os.path.expanduser("~/Documents/GAIA/hpc-agent/tasks.db")


# ═══════════════════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════════

class ACPState:
    session_id: str = None
    model: str = "hpc"
    
state = ACPState()


# ═══════════════════════════════════════════════════════════════════════════════
# FALLBACK ORCHESTRATOR (if import fails)
# ═══════════════════════════════════════════════════════════════════════════════

def _fallback_submit(prompt: str) -> str:
    """Fallback submission if orchestrator module unavailable."""
    task_id = str(uuid.uuid4())
    created_at = time.time()
    
    conn = sqlite3.connect(HPC_DB_PATH)
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
        INSERT INTO tasks (id, agent_id, messages, tools, model, api_config, metadata, priority, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        task_id,
        "acp-runtime",
        json.dumps([{"role": "user", "content": prompt}]),
        json.dumps([]),
        "hpc",
        json.dumps({}),
        json.dumps({"source": "acp-runtime"}),
        0,
        created_at,
        "pending"
    ))
    conn.commit()
    conn.close()
    
    return task_id


def _fallback_wait(task_id: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Fallback wait if orchestrator module unavailable."""
    start = time.time()
    
    while time.time() - start < timeout:
        conn = sqlite3.connect(HPC_DB_PATH)
        row = conn.execute("""
            SELECT status, result, error FROM tasks WHERE id = ?
        """, (task_id,)).fetchone()
        conn.close()
        
        if row and row[0] in ("done", "completed", "failed"):
            return {"status": row[0], "result": row[1], "error": row[2]}
        
        time.sleep(0.5)
    
    return {"status": "timeout", "error": f"Task timed out after {timeout}s"}


# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def submit_to_hpc(prompt: str) -> str:
    """Submit task to HPC orchestrator."""
    if ORCHESTRATOR_AVAILABLE:
        return submit_hpc_task(prompt)
    return _fallback_submit(prompt)


def wait_for_hpc(task_id: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """Wait for HPC task completion."""
    if ORCHESTRATOR_AVAILABLE:
        return wait_for_task(task_id, timeout=timeout)
    return _fallback_wait(task_id, timeout)


# ═══════════════════════════════════════════════════════════════════════════════════════
# JSON-RPC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def read_request() -> dict:
    """Read a single JSON-RPC request from stdin."""
    line = sys.stdin.readline()
    if not line:
        return None
    
    line = line.strip()
    if not line:
        return None
    
    try:
        return json.loads(line)
    except json.JSONDecodeError as e:
        error_resp = {
            "jsonrpc": "2.0",
            "error": {"code": -32700, "message": f"Parse error: {e}"}
        }
        print(json.dumps(error_resp), flush=True)
        return None


def write_response(req_id, result: dict) -> None:
    """Write JSON-RPC response to stdout."""
    resp = {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": result
    }
    print(json.dumps(resp), flush=True)


def write_error(req_id, code: int, message: str) -> None:
    """Write JSON-RPC error to stdout."""
    resp = {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": code, "message": message}
    }
    print(json.dumps(resp), flush=True)


def write_notification(method: str, params: dict) -> None:
    """Write JSON-RPC notification to stdout."""
    resp = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params
    }
    print(json.dumps(resp), flush=True)


def stream_message(session_id: str, text: str) -> None:
    """Stream agent message to Multica."""
    write_notification("session/update", {
        "sessionId": session_id,
        "update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {
                "type": "text",
                "text": text
            }
        }
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ACP HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════════════

def handle_initialize(req_id: int, params: dict) -> dict:
    """Handle initialize method."""
    print(f"[ACP] initialize: protocolVersion={params.get('protocolVersion')}", file=sys.stderr)
    return {
        "protocolVersion": 1,
        "capabilities": {}
    }


def handle_session_new(req_id: int, params: dict) -> dict:
    """Handle session/new method."""
    state.session_id = f"hpc-{uuid.uuid4().hex[:8]}"
    state.model = params.get("model", "hpc")
    
    cwd = params.get("cwd", ".")
    print(f"[ACP] session/new: session_id={state.session_id}, cwd={cwd}, model={state.model}", file=sys.stderr)
    
    return {
        "sessionId": state.session_id,
        "models": {
            "availableModels": [
                {"modelId": "hpc", "name": "HPC Backend", "description": "Deterministic HPC task execution"}
            ],
            "currentModelId": "hpc"
        }
    }


def handle_session_prompt(req_id: int, params: dict) -> dict:
    """Handle session/prompt method - CORE LOGIC."""
    session_id = params.get("sessionId", state.session_id)
    
    # Step 1: Extract prompt
    prompt_list = params.get("prompt", [])
    if not prompt_list:
        error_msg = "Empty prompt received"
        stream_message(session_id, f"Error: {error_msg}")
        return {"stopReason": "end_turn", "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}
    
    prompt = prompt_list[0].get("text", "")
    if not prompt:
        error_msg = "Empty prompt text"
        stream_message(session_id, f"Error: {error_msg}")
        return {"stopReason": "end_turn", "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}
    
    print(f"[ACP] session/prompt: session_id={session_id}, prompt={prompt[:100]}...", file=sys.stderr)
    
    # Step 2: Submit to orchestrator
    task_id = submit_to_hpc(prompt)
    print(f"[ACP] task submitted: task_id={task_id}", file=sys.stderr)
    
    stream_message(session_id, f"Task submitted to HPC: {task_id}")
    
    # Step 3: Wait for result (blocking)
    start_time = time.time()
    result = wait_for_hpc(task_id, timeout=DEFAULT_TIMEOUT)
    duration = time.time() - start_time
    
    print(f"[ACP] task completed: task_id={task_id}, status={result.get('status')}, duration={duration:.1f}s", file=sys.stderr)
    
    # Step 4: Process result
    if result.get("status") == "timeout":
        error_msg = f"Task timed out after {DEFAULT_TIMEOUT}s"
        stream_message(session_id, error_msg)
        return {"stopReason": "end_turn", "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}
    
    if result.get("status") == "error":
        error_msg = result.get("error", "Unknown error")
        stream_message(session_id, f"Error: {error_msg}")
        return {"stopReason": "end_turn", "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}
    
    if result.get("status") == "failed":
        error_msg = result.get("error", "Task failed")
        stream_message(session_id, f"Task failed: {error_msg}")
        return {"stopReason": "end_turn", "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}}
    
    # Success - extract result
    result_data = result.get("result")
    if result_data:
        if isinstance(result_data, dict):
            output = result_data.get("content", str(result_data))
        else:
            output = str(result_data)
    else:
        output = "Task completed successfully"
    
    print(f"[ACP] result: {output[:200]}...", file=sys.stderr)
    
    # Step 5: Stream final result to Multica
    stream_message(session_id, output)
    
    return {
        "stopReason": "end_turn",
        "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
    }


def handle_unknown(req_id: int, method: str) -> None:
    """Handle unknown method."""
    print(f"[ACP] unknown method: {method}", file=sys.stderr)
    write_error(req_id, -32601, f"Method not found: {method}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Main ACP runtime loop."""
    print(f"[ACP] Runtime starting (timeout={DEFAULT_TIMEOUT}s)", file=sys.stderr)
    
    while True:
        req = read_request()
        if req is None:
            break
        
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        
        print(f"[ACP] Received: method={method}, id={req_id}", file=sys.stderr)
        
        # Handle each method
        if method == "initialize":
            result = handle_initialize(req_id, params)
            write_response(req_id, result)
        
        elif method == "session/new":
            result = handle_session_new(req_id, params)
            write_response(req_id, result)
        
        elif method == "session/prompt":
            result = handle_session_prompt(req_id, params)
            write_response(req_id, result)
        
        elif method in ("session/resume", "session/set_model"):
            # Unsupported - just acknowledge
            write_response(req_id, {"sessionId": state.session_id})
        
        else:
            handle_unknown(req_id, method)
    
    print(f"[ACP] Runtime exiting", file=sys.stderr)


if __name__ == "__main__":
    main()