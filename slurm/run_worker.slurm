#!/bin/bash
#SBATCH --job-name=hpc-worker
#SBATCH --output=hpc-worker-%j.out
#SBATCH --error=hpc-worker-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=8G
#SBATCH --partition=publicgpu

#SBATCH --export=ALL

set -e

HERO_DIR="$HOME/hpc-agent"
ASYNC_HERMES="$HOME/async-hermes-agent"

QUEUE_HOST="${QUEUE_HOST:-localhost}"
QUEUE_PORT="${QUEUE_PORT:-6379}"
BATCH_SIZE="${BATCH_SIZE:-1}"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-}"
OLLAMA_ENDPOINT="${OLLAMA_ENDPOINT:-http://localhost:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-llama3:8b}"
WORKER_MODE="${WORKER_MODE:-mock}"

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
fi

# Build args based on mode
if [ "$WORKER_MODE" = "ollama" ]; then
    MODE_ARGS="--ollama --ollama-endpoint $OLLAMA_ENDPOINT --ollama-model $OLLAMA_MODEL"
elif [ "$WORKER_MODE" = "vllm" ]; then
    MODE_ARGS="--model-endpoint $OLLAMA_ENDPOINT"
else
    MODE_ARGS="--mock"
fi

exec python worker/worker.py \
    --queue-host "$QUEUE_HOST" \
    --queue-port "$QUEUE_PORT" \
    --batch-size "$BATCH_SIZE" \
    $MODE_ARGS \
    ${IDLE_TIMEOUT:+"--idle-timeout" "$IDLE_TIMEOUT"}