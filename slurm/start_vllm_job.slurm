#!/bin/bash
#SBATCH --job-name=vllm_server
#SBATCH --time=24:00:00
#SBATCH --gres=gpu:1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

# Load modules
module load python/3.11
module load cuda/12.4

# Activate venv if exists
source ~/venv/bin/activate 2>/dev/null || true

# Start vLLM server
PORT=8000
python -m vllm.entrypoints.openai.api_server \
    --model ${MODEL:-meta-llama/Llama-2-7b-hf} \
    --tensor-parallel-size 1 \
    --host 0.0.0.0 \
    --port $PORT \
    &
VLLM_PID=$!

echo "vLLM started on port $PORT (PID: $VLLM_PID)"

# Wait for vLLM to be ready
for i in {1..60}; do
    curl -s http://localhost:$PORT/v1/models >/dev/null 2>&1 && break
    sleep 2
done

# Start reverse tunnel
echo "Starting reverse SSH tunnel..."
autossh -M 0 -N -R $PORT:localhost:$PORT ${LOCAL_USER}@${LOCAL_HOST} &
TUNNEL_PID=$!

echo "Tunnel started (PID: $TUNNEL_PID)"
echo "vLLM available at: ssh -L 8000:localhost:8000 ${LOCAL_USER}@${LOCAL_HOST}"
echo "vLLM URL: http://localhost:$PORT/v1"

# Wait for job end
wait