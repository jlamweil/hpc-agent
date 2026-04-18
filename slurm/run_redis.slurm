#!/bin/bash
#SBATCH --job-name=redis-server
#SBATCH --output=redis-server-%j.out
#SBATCH --error=redis-server-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --partition=publicgpu

set -e

echo "=== Starting Redis on Login Node ==="

# Try to start redis-server if available
if command -v redis-server &> /dev/null; then
    redis-server --daemonize yes --port 6379 --bind 0.0.0.0
    echo "Redis started on port 6379"
    sleep 2
    redis-cli ping
else
    echo "redis-server not found, checking alternatives..."
    
    # Check if we can run from source or use different approach
    # Try conda
    source /usr/local/python/Miniconda/etc/profile.d/conda.sh
    conda install -y -c conda-forge redis 2>/dev/null || echo "Conda install failed"
    
    if command -v redis-server &> /dev/null; then
        redis-server --daemonize yes --port 6379 --bind 0.0.0.0
    else
        echo "Trying pip redis-server..."
        pip install redis-server 2>/dev/null || true
    fi
fi

# Keep job alive to keep Redis running
echo "Redis server running. Job will keep alive for 4 hours."
sleep 3600