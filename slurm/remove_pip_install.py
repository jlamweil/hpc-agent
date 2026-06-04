#!/usr/bin/env python3
"""Remove pip install vllm lines from slurm scripts (compute nodes have no internet)."""
import os

scripts = [
    os.path.expanduser("~/hpc-agent/slurm/dsv4_h100_80g.slurm"),
    os.path.expanduser("~/hpc-agent/slurm/dsv4_a100_80g.slurm"),
    os.path.expanduser("~/hpc-agent/slurm/dsv4_h982.slurm"),
]

# The block to replace — echo + pip install + verify
import_block = """echo "=== Installing vLLM ==="
pip install vllm==0.22.0 2>&1 | tail -5
python -c "import vllm; print(f'vLLM {vllm.__version__}')" 2>/dev/null"""

replacement = """echo "=== Checking vLLM ==="
python -c "import vllm; print(f'vLLM {vllm.__version__}')" 2>/dev/null || { echo "ERROR: vLLM not installed — pre-install on login node"; exit 1; }"""

for spath in scripts:
    with open(spath, "r") as f:
        content = f.read()

    if import_block in content:
        content = content.replace(import_block, replacement)
        with open(spath, "w") as f:
            f.write(content)
        print(f"Fixed: {os.path.basename(spath)}")
    else:
        print(f"Block not found in {os.path.basename(spath)} — checking for partial match")
        if "pip install vllm" in content:
            print(f"  Found 'pip install vllm' at line")
            for i, line in enumerate(content.splitlines(), 1):
                if "pip install" in line and "vllm" in line:
                    print(f"  Line {i}: {line}")

print("Done")
