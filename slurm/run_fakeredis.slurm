#!/bin/bash
#SBATCH --job-name=fakeredis-worker
#SBATCH --output=fakeredis-worker-%j.out
#SBATCH --error=fakeredis-worker-%j.err
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=4G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
NODE_HOSTNAME=$(hostname -s)

echo "=== fakeredis + Worker Test ==="
echo "Node: $NODE_HOSTNAME"

cd "$HERO_DIR"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install fakeredis
pip install fakeredis --quiet 2>/dev/null || pip install fakeredis

# Test fakeredis
echo "Testing fakeredis..."
python3 -c "
import fakeredis
r = fakeredis.FakeRedis()
r.set('test', 'hello')
print(f'fakeredis works: {r.get(\"test\")}')
"

# Create a simple TCP server that responds to Redis PING
echo "Starting minimal Redis-compatible server..."
python3 -c "
import socket
import threading

def handle_client(conn, addr):
    try:
        data = conn.recv(1024)
        if not data:
            return
        # Handle PING
        if b'PING' in data.upper():
            conn.send(b'+PONG\r\n')
        # Handle GET
        elif b'GET' in data.upper():
            parts = data.split()
            if len(parts) >= 2:
                key = parts[1].decode()
                if key == 'task':
                    conn.send(b'\$8\r\ntest_task\r\n')
                else:
                    conn.send(b'$-1\r\n')
        # Handle DEL
        elif b'DEL' in data.upper():
            conn.send(b':0\r\n')
        else:
            conn.send(b'+OK\r\n')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        conn.close()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', 6379))
sock.listen(5)
print('Server listening on 6379')

while True:
    conn, addr = sock.accept()
    threading.Thread(target=handle_client, args=(conn, addr)).start()
" > redis-server-$SLURM_JOB_ID.log 2>&1 &

sleep 2

echo "Starting worker..."
python worker/worker.py \
    --queue-host localhost \
    --queue-port 6379 \
    --mock \
    --mock-response "Worker on $NODE_HOSTNAME" \
    --latency-min 1 \
    --latency-max 2 \
    --idle-timeout 60 \
    > worker-$SLURM_JOB_ID.log 2>&1 &

echo "Worker started. Monitoring..."
sleep 60
echo "Done"