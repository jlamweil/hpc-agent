#!/bin/bash
#SBATCH --job-name=dsv4-canary
#SBATCH --output=dsv4-canary-%j.out
#SBATCH --error=dsv4-canary-%j.err
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
# no --gres=gpu — this runs on ANY node, no GPU needed

set -euo pipefail
echo "=== DS V4 Canary $(date -Iseconds) ==="
echo "Job: $SLURM_JOB_ID | Node: $SLURMD_NODENAME | CPUs: $SLURM_CPUS_ON_NODE | Mem: $(free -h | grep Mem | awk '{print $2}')"

echo ""
echo "--- 1. BASE ENV ---"
hostname
whoami
cat /etc/os-release 2>/dev/null | head -3
echo "Kernel: $(uname -r)"
echo "Python: $(python3 --version 2>/dev/null || which python3)"

echo ""
echo "--- 2. CUDA MODULE ---"
module load cuda 2>&1 && echo "CUDA LOADED: $(nvcc --version 2>/dev/null | grep release)" || echo "CUDA MODULE FAILED"
module list 2>&1 | grep -i cuda || echo "No CUDA module loaded"

echo ""
echo "--- 3. CONDA ---"
CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
    source "$CONDA_SH"
    echo "conda.sh sourced OK"
    conda activate autogluon-env 2>&1 && echo "autogluon-env activated" || echo "CONDA ACTIVATE FAILED"
    echo "Python: $(python --version 2>/dev/null)"
    echo "Which: $(which python 2>/dev/null)"
else
    echo "conda.sh NOT FOUND at $CONDA_SH"
fi

echo ""
echo "--- 4. USER CONDA ---"
ls ~/.conda/envs/ 2>/dev/null || echo "No user conda envs"
ls ~/.conda/envs/autogluon-env/bin/python 2>/dev/null && echo "User conda python exists" || echo "No user autogluon-env"

echo ""
echo "--- 5. TORCH CHECK ---"
python -c "
import sys
try:
    import torch
    print(f'torch {torch.__version__}')
    print(f'CUDA available: {torch.cuda.is_available()}')
    print(f'CUDA version: {torch.version.cuda}')
    if torch.cuda.is_available():
        print(f'Device count: {torch.cuda.device_count()}')
        print(f'Device name: {torch.cuda.get_device_name(0)}')
except Exception as e:
    print(f'torch failed: {e}')
" 2>&1 || echo "TORCH IMPORT FAILED"

echo ""
echo "--- 6. NETWORK (HF + PyPI) ---"
python -c "
import urllib.request, sys
for url, name in [('https://huggingface.co', 'HF'), ('https://pypi.org', 'PyPI'), ('https://google.com', 'Internet')]:
    try:
        r = urllib.request.urlopen(url, timeout=10)
        print(f'{name}: OK ({r.status})')
    except Exception as e:
        print(f'{name}: BLOCKED ({e})')
" 2>&1

echo ""
echo "--- 7. HF API (DS V4 Flash model info) ---"
python -c "
import json, urllib.request
try:
    r = urllib.request.urlopen('https://huggingface.co/api/models/deepseek-ai/DeepSeek-V4-Flash', timeout=30)
    data = json.loads(r.read())
    safetensors = [s for s in data.get('siblings',[]) if s.get('rfilename','').endswith('.safetensors')]
    total_gb = sum(s.get('size',0) for s in safetensors) / 1e9
    print(f'Safetensor files: {len(safetensors)}')
    print(f'Total model size: {total_gb:.1f} GB')
    for s in safetensors[:3]:
        print(f'  {s[\"rfilename\"]}: {s[\"size\"]/1e9:.1f} GB')
except Exception as e:
    print(f'HF API error: {e}')
" 2>&1

echo ""
echo "--- 8. PIP AVAILABILITY ---"
pip --version 2>/dev/null && echo "pip available" || echo "pip NOT available"
python -m pip index versions vllm 2>/dev/null | head -3 || echo "pip index not working"

echo ""
echo "--- 9. DISK ---"
df -h "$HOME" | tail -1
echo "HF_CACHE: $HOME/hf_cache"
mkdir -p "$HOME/hf_cache" 2>/dev/null && echo "  Created OK" || echo "  CREATE FAILED"
ls -la "$HOME/hf_cache/" 2>/dev/null | head -3

echo ""
echo "--- 10. FILESYSTEM CHECKS ---"
echo "HOME2020: $(df -h /home2020 2>/dev/null | tail -1)"
echo "TMP: $(df -h /tmp 2>/dev/null | tail -1)"
echo "Scratch: $(ls -la /scratch/ 2>/dev/null | head -5 || echo 'No /scratch')"
echo "TMPDIR: ${TMPDIR:-<unset>}"

echo ""
echo "=== CANARY COMPLETE $(date -Iseconds) ==="
