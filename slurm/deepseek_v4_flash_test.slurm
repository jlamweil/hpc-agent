#!/bin/bash
#SBATCH --job-name=dsv4-flash
#SBATCH --output=dsv4-flash-%j.out
#SBATCH --error=dsv4-flash-%j.err
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:4
#SBATCH --constraint='gpuh100&gpu80g'
#SBATCH --mem=256G
#SBATCH --partition=publicgpu

set -e

echo "=== DeepSeek-V4-Flash Test ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "GPUs: $(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null | tr '\n' ';')"
echo "================================"

# Load modules
module load cuda/12.3
echo "CUDA: $(nvcc --version 2>/dev/null | grep release)"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Set HF cache to home (plenty of space there)
export HF_HOME=$HOME/hf_cache
mkdir -p $HF_HOME

# Install latest vLLM with DeepSeek V4 support
echo "=== Installing vLLM ==="
pip install vllm==0.22.0 2>&1 | tail -5
echo "vLLM version: $(python -c 'import vllm; print(vllm.__version__)' 2>/dev/null || echo 'checking...')"

# Check what model to use
echo "=== Model Setup ==="
# Try native model first (vLLM 0.22 should handle FP4+FP8 on H100 with FP8 fallback)
MODEL="deepseek-ai/DeepSeek-V4-Flash"

echo "Model: $MODEL"
echo "HF_HOME: $HF_HOME"
echo "Free disk: $(df -h $HOME | tail -1 | awk '{print $4}')"

# Start vLLM server with DeepSeek V4 specific flags
echo "=== Starting vLLM server ==="
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL" \
    --host 0.0.0.0 \
    --port 8000 \
    --tensor-parallel-size 4 \
    --gpu-memory-utilization 0.92 \
    --max-model-len 4096 \
    --kv-cache-dtype fp8 \
    --trust-remote-code \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --reasoning-parser deepseek_v4 \
    --no-disable-hybrid-kv-cache-manager \
    > vllm-server-$SLURM_JOB_ID.log 2>&1 &

VLLM_PID=$!
echo "vLLM PID: $VLLM_PID"

# Wait for server to be ready (longer wait for big model)
echo "Waiting for vLLM server to be ready..."
for i in $(seq 1 60); do
    if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
        echo "vLLM server is ready! (after ${i} checks)"
        break
    fi
    echo "Waiting... ($i/60)"
    sleep 10
done

# Run a quick test if server is up
if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
    echo "=== Running Test Inference ==="
    curl -s http://localhost:8000/v1/chat/completions \
        -H "Content-Type: application/json" \
        -d '{
            "model": "'"$MODEL"'",
            "messages": [{"role": "user", "content": "Say hello in one word."}],
            "max_tokens": 20
        }' 2>&1 | python -m json.tool 2>/dev/null || echo "Raw response: $(curl -s http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"deepseek-ai/DeepSeek-V4-Flash","messages":[{"role":"user","content":"Say hello in one word."}],"max_tokens":20}')"
    
    echo ""
    echo "=== GPU Memory After Test ==="
    nvidia-smi --query-gpu=index,name,memory.used,memory.total --format=csv,noheader
else
    echo "=== Server failed to start ==="
    echo "Last 50 lines of log:"
    tail -50 vllm-server-$SLURM_JOB_ID.log
fi

# Keep running for manual testing if needed
echo ""
echo "=== Server Status ==="
echo "vLLM running on node $SLURM_JOB_NODELIST:8000"
echo "Job will run for 12h or until cancelled."
echo "To test manually from login node:"
echo "  curl http://$(hostname -s):8000/v1/chat/completions ..."

wait $VLLM_PID
