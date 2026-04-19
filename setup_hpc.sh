#!/bin/bash
set -e

echo "=== HPC Agent Setup (Login Node) ==="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Checking conda environment..."
if [ -f "/usr/local/python/Miniconda/etc/profile.d/conda.sh" ]; then
    source /usr/local/python/Miniconda/etc/profile.d/conda.sh
    conda activate autogluon-env
    echo "Using conda environment: autogluon-env"
else
    echo "Conda not found, checking for venv..."
    if [ -d "venv" ]; then
        source venv/bin/activate
        echo "Using local venv"
    fi
fi

echo "Installing Python packages..."
pip install -q redis transformers accelerate fakeredis || pip install redis transformers accelerate fakeredis

echo "Creating logs directory..."
mkdir -p logs

echo "Creating results directory..."
mkdir -p results

echo ""
echo "=== HPC Setup Complete ==="
echo ""
echo "Slurm job submission:"
echo "  sbatch --export=TASK_IDS='a,b,c' slurm/hpc_batch_job.slurm"
echo ""
echo "Check running jobs:"
echo "  squeue -u jlam"
echo ""
echo "Check logs:"
echo "  tail -f logs/slurm_*.out"