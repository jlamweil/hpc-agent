#!/bin/bash
#SBATCH --job-name=dsv4-small
#SBATCH --output=dsv4-small-%j.out
#SBATCH --error=dsv4-small-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=32G
#SBATCH --partition=pri2021gpu
#SBATCH --account=qosmivgpu
#SBATCH --qos=preemptqos

# Small model validation — tests vLLM + task processing pipeline
# Uses Qwen2.5-1.5B-Instruct (~3GB) to validate the full batch workflow
# before committing to DS V4 Flash on H100s.

set -euo pipefail

echo "=== Small Model Validation ==="
echo "Job: $SLURM_JOB_ID | Node: $SLURMD_NODENAME"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader | while IFS= read -r line; do echo "  GPU: $line"; done
echo "================================"

# ── Environment ──
module load cuda 2>/dev/null || module load cuda/cuda-12.3 2>/dev/null || echo "WARN: no CUDA module"
echo "CUDA: $(nvcc --version 2>/dev/null | grep release || echo 'n/a')"

CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
    source "$CONDA_SH" 2>/dev/null
    conda activate autogluon-env 2>/dev/null && echo "Conda: autogluon-env active" || echo "WARN: conda activate failed"
else
    echo "WARN: conda.sh not found"
fi

echo "=== Checking vLLM ==="
python -c "import vllm; print(f'vLLM {vllm.__version__}')" 2>/dev/null || { echo "ERROR: vLLM not installed"; exit 1; }

export HF_HOME="$HOME/hf_cache"
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
mkdir -p "$HF_HOME"
SMALL_MODEL="Qwen/Qwen2.5-1.5B-Instruct"
echo "Model: $SMALL_MODEL | HF_HOME: $HF_HOME"

echo "=== Starting vLLM server ==="
python -m vllm.entrypoints.openai.api_server \
    --model "$SMALL_MODEL" \
    --host 0.0.0.0 --port 8000 \
    --tensor-parallel-size 1 \
    --gpu-memory-utilization 0.9 \
    --max-model-len 4096 \
    --trust-remote-code \
    > vllm-server-$SLURM_JOB_ID.log 2>&1 &

VLLM_PID=$!
echo "vLLM PID: $VLLM_PID"

echo "Waiting for vLLM (up to 10 min)..."
for i in $(seq 1 120); do
    if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
        echo "vLLM ready! (${i}s)"
        break
    fi
    if [ "$i" -eq 120 ]; then
        echo "vLLM failed to start in time"
        tail -80 vllm-server-$SLURM_JOB_ID.log
        exit 1
    fi
    if [ "$i" -eq 1 ]; then sleep 15; else sleep 5; fi
done

# ── Test inference ──
echo "=== Test Inference ==="
RESPONSE=$(curl -s http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{"model":"'$SMALL_MODEL'","messages":[{"role":"user","content":"Say hello in one word"}],"max_tokens":10}')
echo "$RESPONSE" | python -m json.tool 2>/dev/null || echo "$RESPONSE"
echo ""

nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv,noheader
echo ""

# ── Process queued tasks ──
RESULTS_DIR="$HOME/hpc-agent/results-$SLURM_JOB_ID"
mkdir -p "$RESULTS_DIR"
DB_PATH="$HOME/hpc-agent/tasks.db"
echo "=== Processing tasks from $DB_PATH ==="

if [ ! -f "$DB_PATH" ]; then
    echo "No tasks.db found"
elif [ "$(python3 -c "
import sqlite3; conn=sqlite3.connect('$DB_PATH')
c=conn.execute(\"SELECT COUNT(*) FROM tasks WHERE status='pending'\").fetchone()[0]
conn.close(); print(c)" 2>/dev/null || echo 0)" -eq 0 ]; then
    echo "No pending tasks"
else
    python3 -u - "$DB_PATH" "$RESULTS_DIR" << 'PYEOF'
import json, sqlite3, sys, time, urllib.request
db_path = sys.argv[1]
results_dir = sys.argv[2]
api_base = "http://localhost:8000"

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

conn.execute("BEGIN IMMEDIATE")
rows = conn.execute("""
    SELECT id, messages, model, metadata FROM tasks
    WHERE status = 'pending' ORDER BY created_at ASC LIMIT 20
""").fetchall()

if not rows:
    conn.execute("ROLLBACK"); conn.close(); sys.exit(0)

task_ids = [r['id'] for r in rows]
conn.execute("UPDATE tasks SET status='running', started_at=? WHERE id IN ({})"
    .format(','.join('?' * len(task_ids))), [time.time()] + task_ids)
conn.commit()

print(f"  Processing {len(rows)} tasks...")
ok, fail = 0, 0

for row in rows:
    tid = row['id']
    msgs = json.loads(row['messages']) if row['messages'] else [{"role":"user","content":"hi"}]
    model_name = 'Qwen/Qwen2.5-1.5B-Instruct'  # Must match the model vLLM is serving
    try:
        body = json.dumps({"model": model_name, "messages": msgs, "temperature": 0.7, "max_tokens": 2048}).encode()
        req = urllib.request.Request(f"{api_base}/v1/chat/completions", data=body,
            headers={"Content-Type":"application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
        rd = json.dumps({'content': result['choices'][0]['message'].get('content',''),
            'tool_calls': None, 'reasoning': None, 'finish_reason': 'stop', 'usage': result.get('usage')})
        conn.execute("UPDATE tasks SET status='completed', completed_at=?, result=? WHERE id=?", (time.time(), rd, tid))
        conn.commit(); ok += 1
        u = result.get('usage', {}); print(f"    OK: {tid[:12]} ({u.get('total_tokens',0)} tokens)")
    except Exception as e:
        conn.execute("UPDATE tasks SET status='failed', error=?, completed_at=? WHERE id=?", (str(e)[:500], time.time(), tid))
        conn.commit(); fail += 1
        print(f"    FAIL: {tid[:12]} — {str(e)[:60]}")

conn.close()
print(f"  Done: {ok} ok, {fail} fail")
PYEOF
    cp "$DB_PATH" "$RESULTS_DIR/tasks-completed.db"
fi

cat > "$RESULTS_DIR/summary.json" << EOF
{"slurm_job_id": $SLURM_JOB_ID, "model": "$SMALL_MODEL",
 "node": "$SLURMD_NODENAME", "test_inference": true,
 "finished_at": "$(date -Iseconds)"}
EOF

echo "=== Results saved to $RESULTS_DIR ==="
echo "=== Job complete ==="
