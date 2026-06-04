#!/bin/bash
#SBATCH --job-name=dsv4-h200
#SBATCH --output=dsv4-h200-%j.out
#SBATCH --error=dsv4-h200-%j.err
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:8
#SBATCH --constraint=gpuh200
#SBATCH --mem=512G
#SBATCH --partition=publicgpu

set -e

echo "=== DeepSeek-V4-Flash on H200 ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "GPUs: $(nvidia-smi --query-gpu=index,name,memory.total --format=csv,noheader 2>/dev/null | tr '\n' ';')"
echo "================================"

# Load modules
module load cuda/12.3
echo "CUDA: $(nvcc --version 2>/dev/null | grep release)"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Set HF cache to home (627TB free)
export HF_HOME=$HOME/hf_cache
mkdir -p $HF_HOME

# Install latest vLLM
echo "=== Installing vLLM ==="
pip install vllm==0.22.0 2>&1 | tail -5
echo "vLLM version: $(python -c 'import vllm; print(vllm.__version__)' 2>/dev/null)"

MODEL="deepseek-ai/DeepSeek-V4-Flash"
echo "Model: $MODEL"
echo "HF_HOME: $HF_HOME"
echo "Free disk: $(df -h $HOME | tail -1 | awk '{print $4}')"

# H200 can run native FP4+FP8 — use expert parallel across 8 GPUs
echo "=== Starting vLLM server ==="
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" \
    --host 0.0.0.0 \
    --port 8000 \
    --tensor-parallel-size 4 \
    --enable-expert-parallel \
    --gpu-memory-utilization 0.92 \
    --max-model-len 8192 \
    --kv-cache-dtype fp8 \
    --trust-remote-code \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --reasoning-parser deepseek_v4 \
    --no-disable-hybrid-kv-cache-manager \
    > vllm-server-$SLURM_JOB_ID.log 2>&1 &

VLLM_PID=$!
echo "vLLM PID: $VLLM_PID"

# Wait for server to be ready
echo "Waiting for vLLM server to be ready..."
for i in $(seq 1 60); do
    if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
        echo "vLLM server is ready! (after ${i} checks)"
        break
    fi
    echo "Waiting... ($i/60)"
    sleep 10
done

# Run test inference
if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
    echo "=== Running Test Inference ==="
    curl -s http://localhost:8000/v1/chat/completions \
        -H "Content-Type: application/json" \
        -d '{
            "model": "'"$MODEL"'",
            "messages": [{"role": "user", "content": "Write a Python function to compute fibonacci numbers."}],
            "max_tokens": 100
        }' 2>&1 | python -m json.tool 2>/dev/null || echo "Raw response: $(curl -s http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"deepseek-ai/DeepSeek-V4-Flash","messages":[{"role":"user","content":"Write a Python function to compute fibonacci numbers."}],"max_tokens":100}')"
    
    echo ""
    echo "=== GPU Memory After Test ==="
    nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv,noheader
    echo ""
    echo "=== DONE - DeepSeek V4 Flash is working on H200! ==="
else
    echo "=== Server failed to start ==="
    echo "Last 80 lines of log:"
    tail -80 vllm-server-$SLURM_JOB_ID.log
fi

# Keep alive for manual testing
echo ""
echo "=== Server running on $(hostname -s):8000 ==="
echo "Cancel with: scancel $SLURM_JOB_ID"

wait $VLLM_PID
