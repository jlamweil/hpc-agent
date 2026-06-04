#!/usr/bin/env python3
"""Fix dsv4 slurm scripts on HPC — correct CUDA module path and conda activation."""
import re
import os

scripts = [
    os.path.expanduser("~/hpc-agent/slurm/dsv4_a100_80g.slurm"),
    os.path.expanduser("~/hpc-agent/slurm/dsv4_h982.slurm"),
]

new_cuda = 'module load cuda 2>/dev/null || module load cuda/cuda-12.3 2>/dev/null || echo "WARN: no CUDA module"'

new_conda = (
    'CONDA_SH="/usr/local/python/Miniconda/etc/profile.d/conda.sh"\n'
    'if [ -f "$CONDA_SH" ]; then\n'
    '    source "$CONDA_SH" 2>/dev/null\n'
    '    conda activate autogluon-env 2>/dev/null && echo "Conda: autogluon-env active" || echo "WARN: conda activate failed"\n'
    'else\n'
    '    echo "WARN: conda.sh not found"\n'
    'fi'
)

for spath in scripts:
    with open(spath, "r") as f:
        content = f.read()

    # Fix CUDA module line
    content = content.replace("module load cuda/12.3", new_cuda)

    # Fix conda activation block — handle both original and already-corrupted
    old_conda = "source /usr/local/python/Miniconda/etc/profile.d/conda.sh\nconda activate autogluon-env"
    if old_conda in content:
        content = content.replace(old_conda, new_conda)
    else:
        # Already partially mangled – brute force replace
        content = re.sub(
            r'CONDA_SH="/usr/local/python/Miniconda/etc/profile\.d/conda\.sh".*?fi',
            new_conda,
            content,
            flags=re.DOTALL,
        )

    with open(spath, "w") as f:
        f.write(content)

    # Show result
    print(f"\n=== {os.path.basename(spath)} ===")
    for i, line in enumerate(open(spath).readlines(), 1):
        if "CUDA" in line or "CONDA" in line or "conda" in line:
            print(f"  {i}: {line.rstrip()}")

print("\nDone")
