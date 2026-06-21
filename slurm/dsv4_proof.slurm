#!/bin/bash
#SBATCH --job-name=dsv4-proof
#SBATCH --output=dsv4-proof-%j.out
#SBATCH --error=dsv4-proof-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:4
#SBATCH --constraint='gpuh100'
#SBATCH --exclusive
#SBATCH --mem=128G
#SBATCH --partition=pri2021gpu
#SBATCH --account=qosmivgpu
#SBATCH --qos=preemptqos

set -euo pipefail
HERO_DIR="$HOME/hpc-agent"
LOG_DIR="$HERO_DIR/logs"

export HF_HOME="$HOME/hf_cache"
export LD_LIBRARY_PATH="/usr/local/cuda/13.0/lib64:$LD_LIBRARY_PATH"
export PATH="/usr/local/cuda/13.0/bin:$PATH"
export CUDA_HOME="/usr/local/cuda/13.0"
export NCCL_NET=Socket
export PYTORCH_CUDA_ALLOC_CONF="expandable_segments:True"

source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

SNAP=$(ls "$HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/" 2>/dev/null | tail -1)
MODEL_PATH="$HF_HOME/models--deepseek-ai--DeepSeek-V4-Flash/snapshots/$SNAP"

# Register model type
python3 -c "
from transformers.models.auto.configuration_auto import CONFIG_MAPPING
if 'deepseek_v4' not in CONFIG_MAPPING:
    CONFIG_MAPPING['deepseek_v4'] = __import__('transformers').models.auto.configuration_auto.AutoConfig
    print('registered')
"

TP=4
VLLM_LOG="$LOG_DIR/vllm-proof-${SLURM_JOB_ID}.log"

# Start vLLM with flags proven to work in job 17140161
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_PATH" \
    --served-model-name "deepseek-ai/DeepSeek-V4-Flash" \
    --host 0.0.0.0 --port 8000 \
    --tensor-parallel-size "$TP" \
    --gpu-memory-utilization 0.88 \
    --enforce-eager \
    --max-model-len 32768 \
    --kv-cache-dtype fp8 \
    --trust-remote-code \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --enable-auto-tool-choice \
    --reasoning-parser deepseek_v4 \
    --no-disable-hybrid-kv-cache-manager \
    > "$VLLM_LOG" 2>&1 &

VLLM_PID=$!

# Wait for ready
for i in $(seq 1 300); do
    if grep -q "Starting vLLM server on http" "$VLLM_LOG" 2>/dev/null; then
        echo "vLLM ready (${i}s)"
        sleep 5
        break
    fi
    sleep 5
done

# Keep alive — provider will submit via srun
echo "Session ready. Job ID: $SLURM_JOB_ID"
echo "Waiting 1.5h for provider requests..."
sleep 5400  # 1.5h

# Cleanup
kill "$VLLM_PID" 2>/dev/null || true
wait "$VLLM_PID" 2>/dev/null || true
echo "Done"
