#!/bin/bash
#SBATCH --job-name=redis-tunnel-test
#SBATCH --output=redis-tunnel-test-%j.out
#SBATCH --error=redis-tunnel-test-%j.err
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=2G
#SBATCH --partition=publicgpu

set -e

echo "=== Redis Tunnel Test ==="
echo "Testing: compute -> login (tunnel) -> local Redis"

cd "$HOME/hpc-agent"

# Activate conda env
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install redis client
echo "Installing redis client..."
pip install redis --quiet 2>/dev/null || pip install redis

echo "Attempting to connect to Redis via tunnel..."
echo "Trying: login node localhost:6379"

# Test Redis connection via the SSH tunnel
python3 -c "
import redis
import time

# Try to connect to login node via tunnel
# The tunnel maps login:6379 -> local:6379
# But we need to connect to the login node's localhost
# which should be forwarded to the local machine

hosts_to_try = [
    ('localhost', 6379),      # Local to compute node
    ('127.0.0.1', 6379),     # Local to compute node
]

for host, port in hosts_to_try:
    try:
        print(f'Trying {host}:{port}...')
        r = redis.Redis(host=host, port=port, socket_timeout=5)
        r.ping()
        print(f'SUCCESS! Connected to {host}:{port}')
        # Test basic operation
        r.set('test_key', 'test_value')
        val = r.get('test_key')
        print(f'Redis GET/SET test: {val}')
        print('=== TUNNEL WORKS! ===')
        exit(0)
    except Exception as e:
        print(f'Failed: {e}')

print('All connection attempts failed')
exit(1)
" 2>&1 || echo "FAILED: Could not connect to Redis via tunnel"

echo "Test complete."