#!/bin/bash
#SBATCH --job-name=dsv4-fast
#SBATCH --output=dsv4-fast-%j.out
#SBATCH --error=dsv4-fast-%j.err
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:4
#SBATCH --exclusive
#SBATCH --mem=128G
#SBATCH --partition=your-partition
#SBATCH --account=your-account
#SBATCH --qos=defaultqos

set -uo pipefail

HERO_DIR="$HOME/hpc-agent"
LOG_DIR="$HERO_DIR/logs"
DIAG_LOG="$LOG_DIR/vllm-diag-${SLURM_JOB_ID}.log"

echo "=== dsv4-fast job starting: $(date) ===" | tee -a "$DIAG_LOG"
echo "SLURM_JOB_ID=$SLURM_JOB_ID SLURM_NODELIST=$SLURM_NODELIST" | tee -a "$DIAG_LOG"

export HF_HOME="$HOME/hf_cache"
export LD_LIBRARY_PATH="/usr/local/cuda/13.0/lib64:$LD_LIBRARY_PATH"
export PATH="/usr/local/cuda/13.0/bin:$PATH"
export CUDA_HOME="/usr/local/cuda/13.0"
export NCCL_NET=Socket
export VLLM_ALLOW_LONG_MAX_MODEL_LEN=1

# Clean ONLY vLLM-specific SHM segments — NOT ipcrm -a (which removes ALL
# IPC resources and can break conda/Python processes within the job).
rm -rf /dev/shm/vllm* /dev/shm/*.vllm* 2>/dev/null || true
export VLLM_RPC_MODE=FILE

echo "conda: activating your-conda-env..." | tee -a "$DIAG_LOG"
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate your-conda-env
echo "conda: OK, python=$(which python)" | tee -a "$DIAG_LOG"

MODEL_PATH="$HF_HOME/models--Qwen--Qwen2.5-72B-Instruct"
VLLM_LOG="$LOG_DIR/vllm-fast-${SLURM_JOB_ID}.log"
IDLE_TIMEOUT=600  # 10 min idle before self-terminate

echo "vLLM: starting with model=$MODEL_PATH (log=$VLLM_LOG)" | tee -a "$DIAG_LOG"
echo "vLLM: nvidia-smi at start:" | tee -a "$DIAG_LOG"
nvidia-smi --query-gpu=index,name,memory.total,memory.free --format=csv,noheader 2>&1 | tee -a "$DIAG_LOG"

# Start vLLM
python -m vllm.entrypoints.openai.api_server \
    --model "$MODEL_PATH" \
    --served-model-name "Qwen/Qwen2.5-72B-Instruct" \
    --host 0.0.0.0 --port 8000 \
    --tensor-parallel-size 4 \
    --gpu-memory-utilization 0.88 \
    --max-model-len 131072 \
    --kv-cache-dtype fp8 \
    --trust-remote-code \
    --tool-call-parser hermes \
    --enable-auto-tool-choice \
    > "$VLLM_LOG" 2>&1 &

VLLM_PID=$!
echo "vLLM: PID=$VLLM_PID" | tee -a "$DIAG_LOG"

# Wait for vLLM to be ready
echo "vLLM: waiting for startup..." | tee -a "$DIAG_LOG"
STARTED_AT=$(date +%s)
for i in $(seq 1 300); do
    # Check vLLM process is still alive
    if ! kill -0 "$VLLM_PID" 2>/dev/null; then
        echo "vLLM: process died during startup (after $((i*5))s)" | tee -a "$DIAG_LOG"
        tail -20 "$VLLM_LOG" 2>/dev/null | tee -a "$DIAG_LOG" || echo "(no vllm log)" | tee -a "$DIAG_LOG"
        exit 1
    fi
    if grep -q "Starting vLLM server on http" "$VLLM_LOG" 2>/dev/null; then
        ELAPSED=$(( $(date +%s) - STARTED_AT ))
        echo "vLLM: ready after ${ELAPSED}s (${i} checks)" | tee -a "$DIAG_LOG"
        echo "vLLM: nvidia-smi at ready:" | tee -a "$DIAG_LOG"
        nvidia-smi --query-gpu=index,name,memory.used --format=csv,noheader 2>&1 | tee -a "$DIAG_LOG"
        break
    fi
    sleep 5
done

# If we got here without vLLM ready, check if it actually started
if ! grep -q "Starting vLLM server on http" "$VLLM_LOG" 2>/dev/null; then
    echo "vLLM: TIMEOUT after 1500s — server never started" | tee -a "$DIAG_LOG"
    tail -30 "$VLLM_LOG" 2>/dev/null | tee -a "$DIAG_LOG"
    kill "$VLLM_PID" 2>/dev/null || true
    exit 1
fi

echo "$SLURM_JOB_ID"

# The HermesWorker daemon (hpc-startup.sh --worker) runs on the orchestrator
# machine and handles all task processing via SSH tunnel to this compute node.
# This SLURM job only runs vLLM — no inline worker loop needed.

# Wait for vLLM to finish (job ends when vLLM exits or wall time expires)
echo "vLLM: entering long wait (PID=$VLLM_PID)" | tee -a "$DIAG_LOG"
wait "$VLLM_PID"
EXIT_CODE=$?
echo "vLLM: stopped (exit code $EXIT_CODE) at $(date)" | tee -a "$DIAG_LOG"
exit $EXIT_CODE
