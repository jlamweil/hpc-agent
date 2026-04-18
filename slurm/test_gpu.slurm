#!/bin/bash
#SBATCH --job-name=vllm-test
#SBATCH --output=vllm-test-%j.out
#SBATCH --error=vllm-test-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=16G
#SBATCH --partition=publicgpu

set -e

echo "=== vLLM Test Job ==="
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_JOB_NODELIST"
echo "================================"

# Check GPU
echo "GPU Info:"
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv

# Check disk
echo ""
echo "Disk Info:"
df -h /tmp /var

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Try to import vLLM
echo ""
echo "Python environment:"
python --version
pip show vllm 2>/dev/null || echo "vLLM not installed"

# Try smaller model with transformers directly (bypass vLLM install)
echo ""
echo "Testing transformers inference..."
python << 'PYEOF'
import os
os.environ['HF_HOME'] = '/tmp/hf_cache'
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

print("Loading small model...")
model_name = "microsoft/Phi-3-mini-4k-instruct"  # ~7B, fits in GPU

try:
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        model_name, 
        trust_remote_code=True,
        torch_dtype=torch.float16,
        device_map="auto"
    )
    print("Model loaded successfully!")
    
    # Test inference
    messages = [{"role": "user", "content": "Say 'Hello from GPU'"}]
    inputs = tokenizer.apply_chat_template(messages, return_tensors="pt").to(model.device)
    outputs = model.generate(inputs, max_new_tokens=50)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    print(f"Response: {response}")
except Exception as e:
    print(f"Error: {e}")
PYEOF

echo ""
echo "Test complete."