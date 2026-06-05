#!/bin/bash
#SBATCH --job-name=dsv4-batch
#SBATCH --output=dsv4-batch-%j.out
#SBATCH --error=dsv4-batch-%j.err
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:2
#SBATCH --constraint='gpuh100'
#SBATCH --mem=64G
#SBATCH --partition=pri2021gpu
#SBATCH --account=qosmivgpu
#SBATCH --qos=preemptqos

# ============================================================================
# DeepSeek V4 Flash Batch Worker
# ============================================================================
# Starts vLLM with DS V4 Flash, processes all pending tasks from SQLite DB,
# writes results, shuts down cleanly.
#
# Config via env vars (defaults work for H100 80GB NVLink):
#   DB_PATH       — path to tasks.db (default: ~/hpc-agent/tasks.db)
#   BATCH_SIZE    — tasks per batch (default: 10, 0 = unlimited)
#   MODEL         — HF model name (default: deepseek-ai/DeepSeek-V4-Flash)
#   TP            — tensor parallel size (default: 4, for 4x H100 80GB)
#   GPU_MEM_UTIL  — vLLM gpu-memory-utilization (default: 0.92)
#   MAX_MODEL_LEN — vLLM max-model-len (default: 4096)
#   KVCACHE_DTYPE — KV cache dtype (default: fp8)
#   VLLM_PORT     — vLLM API port (default: 8000)
#   IDLE_TIMEOUT  — idle seconds before exit when queue empty (default: 600 = 10 min)
#   POLL_INTERVAL — seconds between idle queue checks (default: 60)
#   FETCH_DIR     — fetch proxy request directory (default: ~/hpc-agent/fetch-requests)
#
# Usage:
#   rsync tasks.db hpc-login.u-strasbg.fr:~/hpc-agent/
#   ssh hpc-login.u-strasbg.fr "sbatch slurm/dsv4_batch.slurm"
# ============================================================================

set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────────────
HERO_DIR="$HOME/hpc-agent"
LOG_DIR="$HERO_DIR/logs"
RESULTS_DIR="$HERO_DIR/results-$SLURM_JOB_ID"
SCRATCH="/tmp/scratch_${SLURM_JOB_ID}"

DB_PATH="${DB_PATH:-$HERO_DIR/tasks.db}"
BATCH_SIZE="${BATCH_SIZE:-10}"
MODEL="${MODEL:-deepseek-ai/DeepSeek-V4-Flash}"
TP="${TP:-2}"
GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.92}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-2048}"
KVCACHE_DTYPE="${KVCACHE_DTYPE:-fp8}"
VLLM_PORT="${VLLM_PORT:-8000}"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-600}"
POLL_INTERVAL="${POLL_INTERVAL:-60}"
FETCH_DIR="${FETCH_DIR:-$HERO_DIR/fetch-requests}"
FETCH_RESPONSE_DIR="${FETCH_RESPONSE_DIR:-$HERO_DIR/fetch-responses}"
PROXY_HEARTBEAT="${PROXY_HEARTBEAT:-$HERO_DIR/proxy-heartbeat.json}"
RESULTS_DIR="${RESULTS_DIR:-$HERO_DIR/results-$SLURM_JOB_ID}"

mkdir -p "$LOG_DIR" "$RESULTS_DIR" "$SCRATCH"
cd "$SCRATCH"

# ── Cleanup handler ─────────────────────────────────────────────────────────
cleanup() {
    local EXIT_CODE=$?
    echo "[$(date -Iseconds)] Cleaning up..."

    # ── Preemption guard: reset running fill tasks ─────────────────────
    # If the job is being preempted (exit code from signal), running fill
    # tasks should be retried rather than left as failed.
    if [ -f "$DB_PATH" ]; then
        python3 -u - "$DB_PATH" << 'PYEOF' 2>/dev/null || true
import json, sqlite3, time, os
db = os.sys.argv[1]
conn = sqlite3.connect(db)
# Reset fill tasks that were running (type='fill' in metadata)
rows = conn.execute("""
    SELECT id, COALESCE(attempt_count, 0) as attempt FROM tasks
    WHERE status = 'running'
    AND json_extract(metadata, '$.type') = 'fill'
""").fetchall()
for row in rows:
    tid, attempt = row
    if attempt < 5:
        conn.execute("""
            UPDATE tasks SET status = 'pending', worker_id = NULL,
            claimed_at = NULL, retry_at = NULL,
            attempt_count = ?, last_error = 'Preempted by SLURM'
            WHERE id = ?
        """, (attempt + 1, tid))
        print(f"  Preempted fill task {tid[:12]}... reset to pending (attempt {attempt+1}/5)")
    else:
        conn.execute("""
            UPDATE tasks SET status = 'failed', completed_at = ?,
            last_error = 'Fill task preempted 5 times, giving up'
            WHERE id = ?
        """, (time.time(), tid))
        print(f"  Fill task {tid[:12]}... failed after 5 preemptions")
conn.commit()
conn.close()
PYEOF
    fi

    # Kill vLLM if running
    [ -n "${VLLM_PID:-}" ] && kill "$VLLM_PID" 2>/dev/null || true
    # Copy results to persistent storage
    if [ -f "$DB_PATH" ]; then
        cp "$DB_PATH" "${RESULTS_DIR}/tasks-completed.db" 2>/dev/null || true
    fi
    # Clean scratch
    rm -rf "$SCRATCH" 2>/dev/null || true
    echo "[$(date -Iseconds)] Cleanup done (exit=$EXIT_CODE)"
}

trap cleanup EXIT

# ── Logging ─────────────────────────────────────────────────────────────────
echo "=== DS V4 Flash Batch Job ==="
echo "Job ID:    $SLURM_JOB_ID"
echo "Node:      $SLURM_JOB_NODELIST"
echo "Model:     $MODEL"
echo "DB:        $DB_PATH"
echo "Batch:     $BATCH_SIZE"
echo "TP:        $TP"
echo "GPU_MEM:   $GPU_MEM_UTIL"
echo "Max Len:   $MAX_MODEL_LEN"
echo "KV Cache:  $KVCACHE_DTYPE"
echo "Port:      $VLLM_PORT"
echo "Results:   $RESULTS_DIR"
echo "================================"

nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader | while IFS= read -r line; do echo "  GPU: $line"; done

# ── Environment ─────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Setting up CUDA libraries..."
# PyTorch 2.6.0+cu124 provides libcudart.so.12 (driver 12.3 compatible)
# vLLM _C extension needs libcudart.so.13 — available on shared BeeGFS
export LD_LIBRARY_PATH="/usr/local/cuda/13.0/lib64:$LD_LIBRARY_PATH"
echo "  Driver: $(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -1)"

echo "[$(date -Iseconds)] Activating conda env..."
CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
    source "$CONDA_SH" && conda activate autogluon-env && echo "  Conda: autogluon-env activated ($(python --version))" || echo "  WARN: conda activate failed, using default python"
else
    echo "  WARN: conda.sh not found at $CONDA_SH, using default python"
fi

# Critical env vars MUST be set before any vLLM import (including the version check)
# DeepGEMM JIT compiler needs nvcc in PATH
export PATH="/usr/local/cuda/13.0/bin:$PATH"
export CUDA_HOME="/usr/local/cuda/13.0"
# CUDA memory management
export PYTORCH_CUDA_ALLOC_CONF="expandable_segments:True"
export CUDA_CACHE_DISABLE=1
# NCCL_SOCKET_IFNAME: excludes loopback to avoid 'bind failed: Cannot assign requested address'
export NCCL_SOCKET_IFNAME='^lo,docker'
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export HF_HOME="$HOME/hf_cache"
echo "[$(date -Iseconds)] HF_HOME=$HF_HOME"
mkdir -p "$HF_HOME"

# Check vLLM (must be pre-installed — compute nodes have no internet)
echo "[$(date -Iseconds)] Checking vLLM..."
if python -c "import vllm; print(f'vLLM {vllm.__version__}', end='')" 2>/dev/null; then
    echo "[$(date -Iseconds)] vLLM: $(python -c 'import vllm; print(vllm.__version__)')"
else
    echo "[$(date -Iseconds)] ERROR: vLLM not installed. Install on login node first:"
    echo "  source /usr/local/python/Miniconda/etc/profile.d/conda.sh"
    echo "  conda activate autogluon-env"
    echo "  pip install --no-deps ~/vllm_wheels/vllm-0.22.0-cp38-abi3-manylinux_2_28_x86_64.whl"
    exit 1
fi
echo "[$(date -Iseconds)] HF_HOME=$HF_HOME (BeeGFS — model loads via network)"
mkdir -p "$HF_HOME"

# ── Check DB exists ─────────────────────────────────────────────────────────
if [ ! -f "$DB_PATH" ]; then
    echo "[$(date -Iseconds)] ERROR: DB not found at $DB_PATH"
    echo "[]" > "$RESULTS_DIR/errors.json"
    exit 1
fi

# Count pending tasks
PENDING_COUNT=$(python3 -c "
import json, sqlite3
conn = sqlite3.connect('$DB_PATH')
count = conn.execute(\"SELECT COUNT(*) FROM tasks WHERE status = 'pending'\").fetchone()[0]
conn.close()
print(count)
" 2>/dev/null || echo "0")

echo "[$(date -Iseconds)] Pending tasks in DB: $PENDING_COUNT"

if [ "$PENDING_COUNT" -eq 0 ] && [ "$BATCH_SIZE" -ne 0 ]; then
    echo "[$(date -Iseconds)] No pending tasks — nothing to do."
    echo "[]" > "$RESULTS_DIR/errors.json"
    exit 0
fi

# ── Register deepseek_v4 model type ──────────────────────────────────────────
python3 -c "
from transformers.models.auto.configuration_auto import CONFIG_MAPPING
import transformers.models.auto.configuration_auto as cfg_auto
if 'deepseek_v4' not in cfg_auto.CONFIG_MAPPING:
    cfg_auto.CONFIG_MAPPING['deepseek_v4'] = cfg_auto.AutoConfig
    print('Registered deepseek_v4')
" 2>&1 | sed 's/^/[register] /'

# ── Resolve local model path ────────────────────────────────────────────────
MODEL_PATH="$HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/$(ls $HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/ 2>/dev/null | tail -1)"
echo "[$(date -Iseconds)] Model path: $MODEL_PATH"
ls "$MODEL_PATH"/config.json 2>/dev/null && echo "  Config: OK" || echo "  Config: MISSING"

# ── Detect GPUs ──────────────────────────────────────────────────────────────
# TP must divide 64 (attention heads). Valid: 1, 2, 4, 8.
# Use env var (default 4). Auto-detect only as fallback.
if [ "$TP" = "auto" ] || [ -z "$TP" ]; then
    DETECTED=$(python3 -c "import torch; c=torch.cuda.device_count() if torch.cuda.is_available() else 2; print(c if 64%c==0 else 2)" 2>/dev/null || echo "2")
    TP="$DETECTED"
    echo "[$(date -Iseconds)] Auto-detected TP=$TP (must divide 64 heads)"
else
    echo "[$(date -Iseconds)] Using configured TP=$TP"
fi

# ── Start vLLM ──────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Starting vLLM server..."
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_PATH" \
    --host 0.0.0.0 --port "$VLLM_PORT" \
    --tensor-parallel-size "$TP" \
    --gpu-memory-utilization 0.96 \
    --enforce-eager \
    --max-model-len "$MAX_MODEL_LEN" \
    --kv-cache-dtype fp8 \
    --trust-remote-code \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --reasoning-parser deepseek_v4 \
    --no-disable-hybrid-kv-cache-manager \
    > "$LOG_DIR/vllm-${SLURM_JOB_ID}.log" 2>&1 &

VLLM_PID=$!
echo "[$(date -Iseconds)] vLLM PID: $VLLM_PID"

# Wait for vLLM to be ready
echo "[$(date -Iseconds)] Waiting for vLLM to be ready..."
for i in $(seq 1 270); do
    if curl -s --max-time 10 "http://127.0.0.1:${VLLM_PORT}/v1/models" > /dev/null 2>&1; then
        echo "[$(date -Iseconds)] vLLM ready! (${i}s)"
        break
    fi
    if [ "$i" -eq 270 ]; then
        echo "[$(date -Iseconds)] ERROR: vLLM failed to start after 270 attempts"
        tail -50 "$LOG_DIR/vllm-${SLURM_JOB_ID}.log"
    fi
    sleep 5
done

# Verify model loaded
curl -s --max-time 10 "http://127.0.0.1:${VLLM_PORT}/v1/models" | python3 -m json.tool 2>/dev/null || \
    echo "[WARN] Could not verify loaded models"

# ── Process tasks ───────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Starting task processing..."
TIMING_ALL_START=$(date +%s)

# Process in batches with idle-poll
TOTAL_DONE=0
TOTAL_FAILED=0
BATCH_INDEX=0
IDLE_SECONDS=0

while [ $IDLE_SECONDS -lt $IDLE_TIMEOUT ]; do
    # Calculate batch limit: 0 means unlimited
    if [ "$BATCH_SIZE" -eq 0 ]; then
        THIS_BATCH=100
    else
        THIS_BATCH=$BATCH_SIZE
    fi

    TIMING_BATCH_START=$(date +%s)
    BATCH_INDEX=$((BATCH_INDEX + 1))

    echo "[$(date -Iseconds)] Batch $BATCH_INDEX (up to $THIS_BATCH tasks)"

    BATCH_RESULT_FILE="$SCRATCH/batch_result_${BATCH_INDEX}.json"
    export BATCH_RESULT_FILE DB_PATH VLLM_PORT THIS_BATCH

    python3 -u - "$DB_PATH" "$VLLM_PORT" "$THIS_BATCH" << 'PYEOF'
import json, sqlite3, sys, time, os, urllib.request, urllib.error

db_path = sys.argv[1]
vllm_port = int(sys.argv[2])
batch_size = int(sys.argv[3])
result_file = os.environ.get("BATCH_RESULT_FILE", "/tmp/batch_result.json")

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

def write_result(completed=0, failed=0):
    with open(result_file, 'w') as f:
        json.dump({"completed": completed, "failed": failed}, f)

# Dequeue pending tasks (atomic: claim + update)
try:
    # Ensure task_deps table exists (migration for older DBs)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_deps (
            task_id TEXT NOT NULL,
            depends_on TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (task_id, depends_on)
        )
    """)

    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute("""
        SELECT id, messages, model FROM tasks
        WHERE status = 'pending'
          AND NOT EXISTS (
              SELECT 1 FROM task_deps
              WHERE task_deps.task_id = tasks.id
                AND task_deps.depends_on NOT IN (
                    SELECT id FROM tasks WHERE status = 'completed'
                )
          )
        ORDER BY priority DESC, created_at ASC
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not rows:
        conn.execute("ROLLBACK")
        print("  No pending tasks")
        write_result(0, 0)
        conn.close()
        sys.exit(0)

    task_ids = [r['id'] for r in rows]
    now = time.time()
    conn.execute("""
        UPDATE tasks SET status = 'running', started_at = ?
        WHERE id IN ({})
    """.format(','.join('?' * len(task_ids))),
        [now] + task_ids)
    conn.commit()
except Exception as e:
    conn.execute("ROLLBACK")
    print(f"  Error claiming tasks: {e}")
    write_result(0, 0)
    conn.close()
    sys.exit(1)

print(f"  Claimed {len(rows)} tasks")

api_base = f"http://127.0.0.1:{vllm_port}"

# ── Fetch proxy tool ─────────────────────────────────────────────────
FETCH_TOOL = {
    "type": "function",
    "function": {
        "name": "fetch_url",
        "description": "Fetch a URL or search the web. Use this to look up papers, code, or research data.",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL to fetch"},
                "reason": {"type": "string", "description": "Why this fetch is needed"}
            },
            "required": ["url"]
        }
    }
}

fetch_dir = os.environ.get("FETCH_DIR", "")
fetch_response_dir = os.environ.get("FETCH_RESPONSE_DIR", "")
proxy_heartbeat = os.environ.get("PROXY_HEARTBEAT", "")

def _proxy_available() -> bool:
    """Check if the fetch proxy heartbeat is recent."""
    if not proxy_heartbeat or not os.path.exists(proxy_heartbeat):
        return False
    try:
        hb = json.load(open(proxy_heartbeat))
        return (time.time() - hb.get("timestamp", 0)) < 60
    except Exception:
        return False

def _handle_fetch(task_id: str, func_args: dict) -> dict:
    """Write fetch request to BeeGFS, poll for response."""
    if not fetch_dir or not _proxy_available():
        print(f"    Fetch unavailable for {task_id[:12]} (no proxy)", end=" ")
        return {"error": "proxy not available", "url": func_args.get("url", "")}

    os.makedirs(fetch_dir, exist_ok=True)
    os.makedirs(fetch_response_dir, exist_ok=True)

    request = {
        "id": task_id,
        "type": "http",
        "url": func_args.get("url", ""),
        "method": "GET",
        "timeout": 30,
        "created_at": time.time(),
    }

    req_path = os.path.join(fetch_dir, f"{task_id}.json")
    with open(req_path, "w") as f:
        json.dump(request, f)

    # Poll for response with exponential backoff (max ~2 min)
    for attempt in range(8):
        resp_path = os.path.join(fetch_response_dir, f"{task_id}.json")
        if os.path.exists(resp_path):
            with open(resp_path) as f:
                resp = json.load(f)
            os.remove(resp_path)
            return resp
        time.sleep(4 * (2 ** attempt))

    return {"error": "fetch timeout", "url": func_args.get("url", "")}

def call_vllm(messages, model_name):
    """Call vLLM API with retries, including fetch_url tool definition."""
    body = json.dumps({
        "model": model_name,
        "messages": messages,
        "tools": [FETCH_TOOL],
        "temperature": 0.7,
        "max_tokens": 4096,
    }).encode()

    last_error = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                f"{api_base}/v1/chat/completions",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=300) as resp:
                return json.loads(resp.read())
        except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
            last_error = str(e)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    raise Exception(f"vLLM call failed after 3 retries: {last_error}")

completed = 0
failed = 0

for row in rows:
    task_id = row['id']
    model_name = row['model'] or 'deepseek-ai/DeepSeek-V4-Flash'
    messages = json.loads(row['messages'])

    print(f"  Processing: {task_id[:12]}...", end=" ")

    try:
        result = call_vllm(messages, model_name)
        response_message = result['choices'][0]['message']

        # ── Handle tool calls (fetch proxy) ────────────────────────
        max_tool_rounds = 5
        tool_round = 0
        while response_message.get('tool_calls') and tool_round < max_tool_rounds:
            tool_round += 1
            for tc in response_message['tool_calls']:
                fn = tc.get('function', {})
                fn_name = fn.get('name', '')
                fn_args_str = fn.get('arguments', '{}')

                if fn_name == 'fetch_url':
                    try:
                        fn_args = json.loads(fn_args_str)
                    except json.JSONDecodeError:
                        fn_args = {}
                    print(f"fetch", end=" ")
                    fetch_response = _handle_fetch(task_id, fn_args)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc['id'],
                        "content": json.dumps(fetch_response)
                    })
                else:
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc['id'],
                        "content": json.dumps({"error": f"Unknown tool: {fn_name}"})
                    })

            # Re-call vLLM with tool responses
            result = call_vllm(messages, model_name)
            response_message = result['choices'][0]['message']

        if tool_round >= max_tool_rounds:
            print(f"tool_limit", end=" ")

        result_data = json.dumps({
            'content': response_message.get('content', ''),
            'tool_calls': response_message.get('tool_calls'),
            'reasoning': response_message.get('reasoning'),
            'finish_reason': response_message.get('finish_reason'),
            'usage': result.get('usage'),
        })

        conn.execute("""
            UPDATE tasks SET status = 'completed', completed_at = ?, result = ?
            WHERE id = ?
        """, (time.time(), result_data, task_id))
        conn.commit()
        completed += 1
        print("OK")
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"FAIL: {error_msg[:60]}")
        conn.execute("""
            UPDATE tasks SET status = 'failed', error = ?, completed_at = ?
            WHERE id = ?
        """, (error_msg, time.time(), task_id))
        conn.commit()
        failed += 1

conn.close()

# Write result file for bash to read
write_result(completed, failed)
PYEOF

# Capture exit code and batch results from file
BATCH_RC=$?
if [ -f "$BATCH_RESULT_FILE" ]; then
    BATCH_DONE=$(python3 -c "import json;d=json.load(open('$BATCH_RESULT_FILE'));print(d['completed'])" 2>/dev/null || echo "0")
    BATCH_FAIL=$(python3 -c "import json;d=json.load(open('$BATCH_RESULT_FILE'));print(d['failed'])" 2>/dev/null || echo "0")
    rm -f "$BATCH_RESULT_FILE"
else
    BATCH_DONE=0
    BATCH_FAIL=0
fi

TOTAL_DONE=$((TOTAL_DONE + BATCH_DONE))
TOTAL_FAILED=$((TOTAL_FAILED + BATCH_FAIL))

TIMING_BATCH_ELAPSED=$(( $(date +%s) - TIMING_BATCH_START ))
echo "[$(date -Iseconds)] Batch $BATCH_INDEX done: +${BATCH_DONE} ok, +${BATCH_FAIL} fail, ${TIMING_BATCH_ELAPSED}s"

# Idle-poll: if no tasks found, sleep and check again
if [ "$BATCH_RC" -eq 0 ] && [ "$BATCH_DONE" -eq 0 ] && [ "$BATCH_FAIL" -eq 0 ]; then
    IDLE_SECONDS=$((IDLE_SECONDS + POLL_INTERVAL))
    if [ $IDLE_SECONDS -ge $IDLE_TIMEOUT ]; then
        echo "[$(date -Iseconds)] Idle timeout (${IDLE_TIMEOUT}s). Exiting."
        break
    fi
    echo "[$(date -Iseconds)] No tasks. Idle ${IDLE_SECONDS}s/${IDLE_TIMEOUT}s. Polling in ${POLL_INTERVAL}s..."
    sleep $POLL_INTERVAL
    continue
fi
IDLE_SECONDS=0  # reset idle on any activity

# If batch size = 0 (unlimited), process all in one shot
if [ "$BATCH_SIZE" -eq 0 ]; then
    echo "[$(date -Iseconds)] Unlimited batch finished all tasks."
    break
fi

done

TIMING_ALL_ELAPSED=$(( $(date +%s) - TIMING_ALL_START ))
echo "=== Summary ==="
echo "Total completed: $TOTAL_DONE"
echo "Total failed:    $TOTAL_FAILED"
echo "Total elapsed:   ${TIMING_ALL_ELAPSED}s"
echo "================"

# ── Stop vLLM ───────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Stopping vLLM..."
kill "$VLLM_PID" 2>/dev/null || true
wait "$VLLM_PID" 2>/dev/null || true
VLLM_PID=""

# ── Archive results ─────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Archiving results..."
cp "$DB_PATH" "${RESULTS_DIR}/tasks-completed.db" 2>/dev/null || true

# Write summary
cat > "$RESULTS_DIR/summary.json" << EOF
{
  "slurm_job_id": $SLURM_JOB_ID,
  "model": "$MODEL",
  "node": "$SLURM_JOB_NODELIST",
  "completed": $TOTAL_DONE,
  "failed": $TOTAL_FAILED,
  "elapsed_seconds": $TIMING_ALL_ELAPSED,
  "finished_at": "$(date -Iseconds)"
}
EOF

echo "[$(date -Iseconds)] Results in: $RESULTS_DIR"
echo "[$(date -Iseconds)] Job complete."
