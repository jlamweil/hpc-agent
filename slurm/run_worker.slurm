#!/bin/bash
#SBATCH --job-name=hpc-worker
#SBATCH --output=hpc-worker-%j.out
#SBATCH --error=hpc-worker-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=8G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
ASYNC_HERMES="$HOME/async-hermes-agent"

QUEUE_HOST="${QUEUE_HOST:-localhost}"
QUEUE_PORT="${QUEUE_PORT:-6379}"
BATCH_SIZE="${BATCH_SIZE:-1}"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-}"
VLLM_ENDPOINT="${VLLM_ENDPOINT:-http://localhost:8000}"
VLLM_MODEL="${VLLM_MODEL:-meta-llama/Llama-3.2-1B-Instruct}"
WORKER_MODE="${WORKER_MODE:-transformers}"

echo "=== Slurm Worker Starting ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "Queue: $QUEUE_HOST:$QUEUE_PORT"
echo "Worker mode: $WORKER_MODE"
echo "================================"

cd "$HERO_DIR"

# Activate venv if exists
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -f "/usr/local/python/Miniconda/etc/profile.d/conda.sh" ]; then
    source /usr/local/python/Miniconda/etc/profile.d/conda.sh
    conda activate autogluon-env
fi

# Start vLLM server if mode is vllm and no endpoint provided
if [ "$WORKER_MODE" = "vllm" ] && [ "$VLLM_ENDPOINT" = "http://localhost:8000" ]; then
    echo "Starting local vLLM server..."
    pip install vllm --quiet 2>/dev/null || true
    
    python -m vllm.entrypoints.openai.api_server \
        --model "$VLLM_MODEL" \
        --host 0.0.0.0 \
        --port 8000 \
        --tensor-parallel-size 1 \
        --gpu-memory-utilization 0.85 \
        --max-model-len 2048 \
        > vllm-$SLURM_JOB_ID.log 2>&1 &
    
    # Wait for vLLM to start
    for i in {1..30}; do
        if curl -s http://localhost:8000/v1/models > /dev/null 2>&1; then
            echo "vLLM server ready"
            break
        fi
        sleep 5
    done
fi

# Build args based on mode
if [ "$WORKER_MODE" = "ollama" ]; then
    MODE_ARGS="--ollama --ollama-endpoint $VLLM_ENDPOINT"
elif [ "$WORKER_MODE" = "vllm" ]; then
    MODE_ARGS="--vllm --vllm-endpoint $VLLM_ENDPOINT --vllm-model $VLLM_MODEL"
elif [ "$WORKER_MODE" = "transformers" ]; then
    MODE_ARGS="--transformers --transformers-model $VLLM_MODEL"
else
    MODE_ARGS="--mock"
fi

exec python worker/worker.py \
    --queue-host "$QUEUE_HOST" \
    --queue-port "$QUEUE_PORT" \
    --batch-size "$BATCH_SIZE" \
    $MODE_ARGS \
    ${IDLE_TIMEOUT:+"--idle-timeout" "$IDLE_TIMEOUT"}