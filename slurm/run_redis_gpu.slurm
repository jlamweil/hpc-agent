#!/bin/bash
#SBATCH --job-name=redis-gpu
#SBATCH --output=redis-gpu-%j.out
#SBATCH --error=redis-gpu-%j.err
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=2G
#SBATCH --partition=publicgpu

set -e

echo "=== Redis on GPU Node ==="
echo "Node: $SLURM_JOB_NODELIST"

# Download and run Redis
cd /tmp
curl -sL https://github.com/redis/redis/archive/7.4.1.tar.gz -o redis.tar.gz
tar -xzf redis.tar.gz
cd redis-7.4.1
make -j4

# Start Redis on all interfaces
./src/redis-server --bind 0.0.0.0 --port 6379 --daemonize yes

sleep 2

# Test
echo "Redis started, testing..."
./src/redis-cli -h $(hostname) ping

echo "Redis available at: $(hostname):6379"
echo "Keep job alive..."

# Keep alive
sleep 3600