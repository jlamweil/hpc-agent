#!/bin/bash
#SBATCH --job-name=dsv4-batch
#SBATCH --output=dsv4-batch-%j.out
#SBATCH --error=dsv4-batch-%j.err
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:2
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
TP="${TP:-4}"
GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.92}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-4096}"
KVCACHE_DTYPE="${KVCACHE_DTYPE:-fp8}"
VLLM_PORT="${VLLM_PORT:-8000}"
RESULTS_DIR="${RESULTS_DIR:-$HERO_DIR/results-$SLURM_JOB_ID}"

mkdir -p "$LOG_DIR" "$RESULTS_DIR" "$SCRATCH"
cd "$SCRATCH"

# ── Cleanup handler ─────────────────────────────────────────────────────────
cleanup() {
    local EXIT_CODE=$?
    echo "[$(date -Iseconds)] Cleaning up..."
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
echo "[$(date -Iseconds)] Loading CUDA module..."
if module load cuda 2>/dev/null; then
    echo "  CUDA: $(nvcc --version 2>/dev/null | grep release)"
else
    echo "  WARN: CUDA module load failed — trying cuda/cuda-12.3"
    module load cuda/cuda-12.3 2>/dev/null || echo "  WARN: No CUDA module available, vLLM may fail"
fi

echo "[$(date -Iseconds)] Activating conda env..."
CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
    source "$CONDA_SH" && conda activate autogluon-env && echo "  Conda: autogluon-env activated ($(python --version))" || echo "  WARN: conda activate failed, using default python"
else
    echo "  WARN: conda.sh not found at $CONDA_SH, using default python"
fi

# Check vLLM (must be pre-installed — compute nodes have no internet)
if python -c "import vllm; print(f'vLLM {vllm.__version__}', end='')" 2>/dev/null; then
    echo "[$(date -Iseconds)] vLLM: $(python -c 'import vllm; print(vllm.__version__)')"
else
    echo "[$(date -Iseconds)] ERROR: vLLM not installed. Install on login node first:"
    echo "  source /usr/local/python/Miniconda/etc/profile.d/conda.sh"
    echo "  conda activate autogluon-env"
    echo "  pip install --no-deps ~/vllm_wheels/vllm-0.22.0-cp38-abi3-manylinux_2_28_x86_64.whl"
    exit 1
fi

export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export NCCL_NET=Socket
export HF_HOME="$HOME/hf_cache"
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
TP=$(python3 -c "import torch; print(torch.cuda.device_count() if torch.cuda.is_available() else 2)" 2>/dev/null || echo "2")
echo "[$(date -Iseconds)] Available GPUs: $TP"

# ── Start vLLM ──────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Starting vLLM server..."
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_PATH" \
    --host 0.0.0.0 --port "$VLLM_PORT" \
    --tensor-parallel-size "$TP" \
    --gpu-memory-utilization 0.88 \
    --enforce-eager \
    --max-model-len "$MAX_MODEL_LEN" \
    --kv-cache-dtype "$KVCACHE_DTYPE" \
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

# Process in batches
TOTAL_DONE=0
TOTAL_FAILED=0
BATCH_INDEX=0

while true; do
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
    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute("""
        SELECT id, messages, model FROM tasks
        WHERE status = 'pending'
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

def call_vllm(messages, model_name):
    """Call vLLM API with retries."""
    body = json.dumps({
        "model": model_name,
        "messages": messages,
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

        result_data = json.dumps({
            'content': result['choices'][0]['message'].get('content', ''),
            'tool_calls': result['choices'][0]['message'].get('tool_calls'),
            'reasoning': result['choices'][0]['message'].get('reasoning'),
            'finish_reason': result['choices'][0]['message'].get('finish_reason'),
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

# Check if we should continue
if [ "$BATCH_RC" -eq 0 ] && [ "$BATCH_DONE" -eq 0 ] && [ "$BATCH_FAIL" -eq 0 ]; then
    echo "[$(date -Iseconds)] No more pending tasks — done."
    break
fi

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
