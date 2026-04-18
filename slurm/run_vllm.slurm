#!/bin/bash
#SBATCH --job-name=vllm-server
#SBATCH --output=vllm-server-%j.out
#SBATCH --error=vllm-server-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=32G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
VLLM_MODEL="${VLLM_MODEL:-meta-llama/Llama-3.2-1B-Instruct}"
VLLM_PORT="${VLLM_PORT:-8000}"

echo "=== vLLM Server Starting ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "Model: $VLLM_MODEL"
echo "Port: $VLLM_PORT"
echo "================================"

cd "$HERO_DIR"

# Activate conda env
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install vLLM (on compute node, more space)
pip install vllm --quiet 2>/dev/null || pip install vllm

# Get node hostname
NODE_HOSTNAME=$(hostname -s)

# Start vLLM server in background
echo "Starting vLLM server..."
python -m vllm.entrypoints.openai.api_server \
    --model "$VLLM_MODEL" \
    --host 0.0.0.0 \
    --port "$VLLM_PORT" \
    --tensor-parallel-size 1 \
    --gpu-memory-utilization 0.85 \
    --max-model-len 2048 \
    > vllm-server-$SLURM_JOB_ID.log 2>&1 &

VLLM_PID=$!
echo "vLLM started with PID $VLLM_PID"

# Wait for server to be ready
echo "Waiting for vLLM server to be ready..."
sleep 30

# Check if server is up
for i in {1..30}; do
    if curl -s http://localhost:$VLLM_PORT/v1/models > /dev/null 2>&1; then
        echo "vLLM server is ready!"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 5
done

# Keep job alive
echo "vLLM server running. Job will run until time limit or cancelled."
echo "To test: curl http://$NODE_HOSTNAME:$VLLM_PORT/v1/chat/completions"

# Keep alive
wait $VLLM_PID