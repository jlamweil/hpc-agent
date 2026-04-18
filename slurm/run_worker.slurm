#!/bin/bash
#SBATCH --job-name=hpc-worker
#SBATCH --output=hpc-worker-%j.out
#SBATCH --error=hpc-worker-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=8G

#SBATCH --export=ALL

# Load required modules (customize for your HPC)
# module load python/3.11
# module load cuda/12.2

set -e

# Environment variables passed from runtime
QUEUE_HOST="${QUEUE_HOST:-localhost}"
QUEUE_PORT="${QUEUE_PORT:-6379}"
BATCH_SIZE="${BATCH_SIZE:-1}"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-}"
OLLAMA_ENDPOINT="${OLLAMA_ENDPOINT:-http://localhost:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-llama3:8b}"
WORKER_MODE="${WORKER_MODE:-ollama}"  # ollama, mock, vllm

echo "=== Slurm Worker Starting ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "Queue: $QUEUE_HOST:$QUEUE_PORT"
echo "Worker mode: $WORKER_MODE"
echo "================================"

cd /home/lam/Documents/GAIA/hpc-agent

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the worker
exec python worker/worker.py \
    --queue-host "$QUEUE_HOST" \
    --queue-port "$QUEUE_PORT" \
    --batch-size "$BATCH_SIZE" \
    --ollama \
    --ollama-endpoint "$OLLAMA_ENDPOINT" \
    --ollama-model "$OLLAMA_MODEL" \
    ${IDLE_TIMEOUT:+"--idle-timeout" "$IDLE_TIMEOUT"}