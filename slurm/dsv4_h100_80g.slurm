#!/bin/bash
#SBATCH --job-name=dsv4-h100-80
#SBATCH --output=dsv4-h100-80-%j.out
#SBATCH --error=dsv4-h100-80-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:4
#SBATCH --constraint='gpuh100&gpu80g&nvlink'
#SBATCH --mem=128G
#SBATCH --partition=pri2021gpu
#SBATCH --account=qosmivgpu
#SBATCH --qos=preemptqos

# ============================================================================
# DS V4 Flash validation on H100 80GB NVLink
# Starts vLLM, runs test inference, processes queued tasks from tasks.db
# ============================================================================

SCRIPT_START=$(date +%s)

echo "=== dsv4-h100-80 Job $SLURM_JOB_ID ==="
echo "Node: $SLURMD_NODENAME"
nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader

# ── Environment ──
module load cuda 2>/dev/null || module load cuda/cuda-12.3 2>/dev/null || echo "WARN: no CUDA module"
echo "CUDA: $(nvcc --version 2>/dev/null | grep release || echo 'n/a')"

CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
    source "$CONDA_SH" && conda activate autogluon-env && echo "Conda: autogluon-env active"
else
    echo "WARN: conda.sh not found"
fi

export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export HF_HOME="$HOME/hf_cache"
export NCCL_NET=Socket                     # Avoid NCCL plugin segfault on this cluster

python3 -c "import vllm; print(f'vLLM {vllm.__version__}')" || { echo "vLLM not installed"; exit 1; }

# Resolve local cache path for the model (HF_HUB_OFFLINE breaks HF model name lookup)
MODEL_PATH="$HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/$(ls $HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/ 2>/dev/null | tail -1)"
echo "Model: deepseek-ai/DeepSeek-V4-Flash"
echo "Local path: $MODEL_PATH"
echo "Cache size: $(du -sh $HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash 2>/dev/null | cut -f1)"
ls -lh "$MODEL_PATH/config.json" 2>/dev/null && echo "Config: OK" || echo "Config: MISSING"
ls "$MODEL_PATH"/*.safetensors 2>/dev/null | wc -l | xargs echo "Safetensors:"
RESULTS_DIR="$HOME/hpc-agent/results-$SLURM_JOB_ID"
mkdir -p "$RESULTS_DIR"

# ── Detect available GPUs for TP ──
TP=$(python3 -c "import torch; print(torch.cuda.device_count() if torch.cuda.is_available() else 0)" 2>/dev/null || echo "0")
if [ "$TP" -lt 1 ]; then TP=4; fi
echo "Available GPUs: $TP"

# ── Start vLLM ──
# Pre-register deepseek_v4 model type with transformers to avoid Config error
python3 -c "
from transformers.models.auto.configuration_auto import CONFIG_MAPPING
import transformers.models.auto.configuration_auto as cfg_auto
if 'deepseek_v4' not in cfg_auto.CONFIG_MAPPING:
    cfg_auto.CONFIG_MAPPING['deepseek_v4'] = cfg_auto.AutoConfig
    print('Registered deepseek_v4')
else:
    print('deepseek_v4 already registered')
" 2>&1

echo "Starting vLLM (TP=$TP, FP8 KV cache)..."

python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_PATH" --host 0.0.0.0 --port 8000 \
    --tensor-parallel-size "$TP" --gpu-memory-utilization 0.88 \
    --max-model-len 4096 --kv-cache-dtype fp8 \
    --trust-remote-code --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 --reasoning-parser deepseek_v4 \
    --no-disable-hybrid-kv-cache-manager \
    --enforce-eager \                     # Fix: skip CUDA graphs to avoid EngineCore shm broadcast hang
    > "$RESULTS_DIR/vllm-server.log" 2>&1 &

VLLM_PID=$!
echo "vLLM PID: $VLLM_PID"

# ── Wait for vLLM (30 min max) ──
echo "Waiting for model to load (149GB from BeeGFS, ~30-60 min)..."
VLLM_READY=false
for i in $(seq 1 180); do
    if curl -s --max-time 5 http://127.0.0.1:8000/v1/models > /dev/null 2>&1; then
        VLLM_READY=true
        echo "vLLM ready after ~$(( (i * 10) / 60 )) min"
        break
    fi
    sleep 10
done

# ── Test Inference ──
if [ "$VLLM_READY" = true ]; then
    echo "=== Test Inference ==="
    curl -s --max-time 300 http://127.0.0.1:8000/v1/chat/completions \
        -H "Content-Type: application/json" \
        -d '{"model":"'"$MODEL_PATH"'","messages":[{"role":"user","content":"Say hello in 3 words"}],"max_tokens":20}' \
        | python3 -m json.tool
    
    echo ""
    nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv,noheader
    echo "=== vLLM working — processing queued tasks ==="
else
    echo "=== vLLM NOT ready after 30 min ==="
    tail -50 "$RESULTS_DIR/vllm-server.log" | head -20
fi

# ── Process queued tasks (only if vLLM is running) ──
DB_PATH="$HOME/hpc-agent/tasks.db"
if [ "$VLLM_READY" = false ] || ! curl -s --max-time 5 http://127.0.0.1:8000/v1/models > /dev/null 2>&1; then
    echo "vLLM unavailable — skipping task processing"
    echo '{"skipped":"vLLM not running"}' > "$RESULTS_DIR/errors.json"
else
    echo "=== Processing tasks from $DB_PATH ==="
    python3 -u - "$DB_PATH" "$RESULTS_DIR" "$MODEL_PATH" << 'PYEOF'
import json, sqlite3, sys, time, urllib.request
db_path, results_dir, model_name = sys.argv[1], sys.argv[2], sys.argv[3]
api_base = "http://127.0.0.1:8000"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

conn.execute("BEGIN IMMEDIATE")
rows = conn.execute("SELECT id, messages FROM tasks WHERE status = 'pending' ORDER BY created_at ASC LIMIT 50").fetchall()
if not rows:
    conn.execute("ROLLBACK"); conn.close(); sys.exit(0)
task_ids = [r['id'] for r in rows]
conn.execute("UPDATE tasks SET status='running', started_at=? WHERE id IN ({})".format(','.join('?'*len(task_ids))), [time.time()]+task_ids)
conn.commit()
print(f"  Processing {len(rows)} tasks...")
ok, fail = 0, 0
for row in rows:
    tid = row['id']
    msgs = json.loads(row['messages']) if row['messages'] else [{"role":"user","content":"hi"}]
    if not isinstance(msgs, list): msgs = [{"role":"user","content":str(msgs)}]
    try:
        body = json.dumps({"model":model_name,"messages":msgs,"temperature":0.7,"max_tokens":2048}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:8000/v1/chat/completions", data=body, headers={"Content-Type":"application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=300) as resp:
            result = json.loads(resp.read())
        rd = json.dumps({'content':result['choices'][0]['message'].get('content',''),'tool_calls':None,'reasoning':None,'finish_reason':'stop','usage':result.get('usage')})
        conn.execute("UPDATE tasks SET status='completed', completed_at=?, result=? WHERE id=?", (time.time(), rd, tid))
        conn.commit(); ok += 1
        u = result.get('usage',{}); print(f"    OK: {tid[:12]} ({u.get('total_tokens',0)} tokens)")
    except Exception as e:
        conn.execute("UPDATE tasks SET status='failed', error=?, completed_at=? WHERE id=?", (str(e)[:500], time.time(), tid))
        conn.commit(); fail += 1
        print(f"    FAIL: {tid[:12]} — {str(e)[:60]}")
conn.close()
print(f"  Done: {ok} ok, {fail} fail")
PYEOF
    cp "$DB_PATH" "$RESULTS_DIR/tasks-completed.db"
fi

# ── Summary ──
TASK_COUNT=$(python3 -c "import sqlite3; c=sqlite3.connect('$DB_PATH'); print(c.execute('SELECT COUNT(*) FROM tasks').fetchone()[0]); c.close()" 2>/dev/null || echo 0)
ELAPSED=$(( $(date +%s) - SCRIPT_START ))
cat > "$RESULTS_DIR/summary.json" << EOF
{"slurm_job_id":$SLURM_JOB_ID,"model":"$MODEL_PATH","node":"$SLURMD_NODENAME","vllm_ready":$VLLM_READY,"task_count":$TASK_COUNT,"elapsed_sec":$ELAPSED,"finished_at":"$(date -Iseconds)"}
EOF

echo "=== Results: $RESULTS_DIR ==="
echo "=== Job complete ($ELAPSED sec) ==="
