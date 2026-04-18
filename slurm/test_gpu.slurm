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

nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv
echo ""

# Compute node has more space - install here
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install transformers and accelerate
echo "Installing transformers..."
pip install transformers accelerate --quiet 2>/dev/null || pip install transformers accelerate

# Set cache to /tmp
export HF_HOME=/tmp/hf_cache
export TRANSFORMERS_CACHE=/tmp/transformers_cache

python << 'PYEOF'
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

print("Loading tiny model...")
model_name = "microsoft/Phi-3-mini-4k-instruct"

try:
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    print("Tokenizer loaded")
    
    model = AutoModelForCausalLM.from_pretrained(
        model_name, 
        trust_remote_code=True,
        torch_dtype=torch.float16,
        device_map="auto"
    )
    print("Model loaded on GPU!")
    
    # Quick inference test
    messages = [{"role": "user", "content": "Say 'GPU OK'"}]
    inputs = tokenizer.apply_chat_template(messages, return_tensors="pt").to(model.device)
    outputs = model.generate(inputs, max_new_tokens=20, do_sample=False)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    print(f"Response: {response}")
    print("SUCCESS!")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
PYEOF

echo "Test complete."