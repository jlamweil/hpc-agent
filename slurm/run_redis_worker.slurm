#!/bin/bash
#SBATCH --job-name=redis-worker
#SBATCH --output=redis-worker-%j.out
#SBATCH --error=redis-worker-%j.err
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=4G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
NODE_HOSTNAME=$(hostname -s)
REDIS_PORT=6379

echo "=== Redis + Worker Job ==="
echo "Node: $NODE_HOSTNAME"
echo "Redis will run locally, worker connects to localhost"

cd "$HERO_DIR"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install redis
pip install redis fakeredis --quiet 2>/dev/null || pip install redis fakeredis

# Start Redis in background (using fakeredis for testing, or real redis if available)
echo "Starting Redis server..."
python3 -c "
import socket
import threading
import time

# Simple Redis-compatible server for testing
class SimpleRedis:
    def __init__(self, port=6379):
        self.store = {}
        self.port = port
    
    def handle(self, conn, addr):
        try:
            data = conn.recv(1024)
            if not data:
                return
            # Simple PING/PONG
            if data.upper().startswith(b'PING'):
                conn.send(b'+PONG\r\n')
            elif data.upper().startswith(b'ECHO'):
                conn.send(b'+OK\r\n')
            elif data.upper().startswith(b'SET'):
                parts = data.split()
                if len(parts) >= 3:
                    key = parts[1].decode()
                    val = parts[2].decode()
                    self.store[key] = val
                    conn.send(b'+OK\r\n')
            elif data.upper().startswith(b'GET'):
                parts = data.split()
                if len(parts) >= 2:
                    key = parts[1].decode()
                    val = self.store.get(key, '')
                    if val:
                        conn.send(f'\${len(val)}\r\n{val}\r\n'.encode())
                    else:
                        conn.send(b'$-1\r\n')
            else:
                conn.send(b'+OK\r\n')
        except Exception as e:
            print(f'Error: {e}')
        finally:
            conn.close()
    
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', self.port))
        sock.listen(5)
        print(f'Redis server listening on port {self.port}')
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=self.handle, args=(conn, addr)).start()

redis = SimpleRedis(port=$REDIS_PORT)
redis.run()
" > redis-server-$SLURM_JOB_ID.log 2>&1 &

REDIS_PID=$!
echo "Redis started with PID $REDIS_PID"
sleep 2

# Now start worker connecting to localhost
echo "Starting worker (connecting to localhost:$REDIS_PORT)..."
python worker/worker.py \
    --queue-host localhost \
    --queue-port $REDIS_PORT \
    --mock \
    --mock-response "Worker on node $NODE_HOSTNAME" \
    --latency-min 1 \
    --latency-max 3 \
    --idle-timeout 120 \
    > worker-$SLURM_JOB_ID.log 2>&1 &

WORKER_PID=$!
echo "Worker started with PID $WORKER_PID"

# Keep job alive
echo "Redis and Worker running. Monitoring..."
wait $REDIS_PID