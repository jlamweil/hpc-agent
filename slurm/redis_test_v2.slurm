#!/bin/bash
#SBATCH --job-name=redis-test-v2
#SBATCH --output=redis-test-v2-%j.out
#SBATCH --error=redis-test-v2-%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=2G
#SBATCH --partition=publicgpu

set -e

echo "=== Redis Tunnel Test v2 ==="

source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

pip install redis --quiet 2>/dev/null || pip install redis

# Try connecting to login node's internal IP
python3 -c "
import redis

# Try login node internal IP
hosts_to_try = [
    ('192.168.3.220', 6379),  # Login node internal IP
    ('192.168.3.220', 6380),  # Alternative port
]

for host, port in hosts_to_try:
    try:
        print(f'Trying {host}:{port}...')
        r = redis.Redis(host=host, port=port, socket_timeout=5)
        r.ping()
        print(f'SUCCESS! Connected to {host}:{port}')
        r.set('test', 'ok')
        print(f'GET test: {r.get(\"test\")}')
        print('=== TUNNEL WORKS! ===')
        exit(0)
    except Exception as e:
        print(f'Failed: {e}')

print('All attempts failed')
exit(1)
" 2>&1