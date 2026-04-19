#!/bin/bash
#SBATCH --job-name=simple-worker-test
#SBATCH --output=simple-worker-test-%j.out
#SBATCH --error=simple-worker-test-%j.err
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=4G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
NODE_HOSTNAME=$(hostname -s)

echo "=== Simple Worker Test ==="
echo "Node: $NODE_HOSTNAME"

cd "$HERO_DIR"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Create a proper Redis-compatible mock server
echo "Starting mock Redis server..."
python3 << 'PYEOF' > redis-server-$SLURM_JOB_ID.log 2>&1 &
import socket
import threading
import json
import time

# Global task storage
tasks = []
results = []
task_id = 0

def handle_client(conn, addr):
    global task_id
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            
            # Parse Redis protocol (simplified)
            lines = data.decode().split('\r\n')
            
            # Handle PING
            if 'PING' in data.decode().upper():
                conn.send(b'+PONG\r\n')
            
            # Handle LPUSH (enqueue task)
            elif 'LPUSH' in data.decode().upper():
                parts = data.decode().split()
                if len(parts) >= 4:
                    # LPUSH queue_name task_json
                    task_data = ' '.join(parts[3:])
                    tasks.append(task_data)
                    conn.send(f':{len(tasks)}\r\n'.encode())
                else:
                    conn.send(b'+OK\r\n')
            
            # Handle RPOP (dequeue task)
            elif 'RPOP' in data.decode().upper():
                if tasks:
                    task = tasks.pop(0)
                    conn.send(f'+{task}\r\n'.encode())
                else:
                    conn.send(b'$-1\r\n')
            
            # Handle RPOPLPUSH (atomic dequeue)
            elif 'RPOPLPUSH' in data.decode().upper():
                parts = data.decode().split()
                if len(parts) >= 3 and tasks:
                    task = tasks.pop(0)
                    results.append(task)
                    conn.send(f'+{task}\r\n'.encode())
                else:
                    conn.send(b'$-1\r\n')
            
            # Handle LRANGE (get results)
            elif 'LRANGE' in data.decode().upper() and 'results' in data.decode():
                conn.send(f'+{json.dumps(results)}\r\n'.encode())
            
            # Handle LLEN
            elif 'LLEN' in data.decode().upper():
                parts = data.decode().split()
                if len(parts) >= 3:
                    if 'queue' in parts[2].lower():
                        conn.send(f':{len(tasks)}\r\n'.encode())
                    elif 'results' in parts[2].lower():
                        conn.send(f':{len(results)}\r\n'.encode())
                    else:
                        conn.send(b':0\r\n')
                else:
                    conn.send(b':0\r\n')
            
            # Handle SET/GET for simple values
            elif 'SET' in data.decode().upper():
                conn.send(b'+OK\r\n')
            elif 'GET' in data.decode().upper():
                conn.send(b'$-1\r\n')
            
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
print(f'Mock Redis listening on 6379')

while True:
    conn, addr = sock.accept()
    threading.Thread(target=handle_client, args=(conn, addr)).start()
PYEOF

REDIS_PID=$!
echo "Redis server started with PID $REDIS_PID"
sleep 3

# Test the server
echo "Testing Redis server..."
python3 -c "
import redis
r = redis.Redis(host='localhost', port=6379, socket_timeout=5)
r.ping()
print('Redis PING: OK')
r.lpush('queue', 'test_task_1')
print(f'LPUSH: OK, queue size: {r.llen(\"queue\")}')
task = r.rpop('queue')
print(f'RPOP: {task}')
print('Redis server working!')
"

echo "Starting worker..."
python worker/worker.py \
    --queue-host localhost \
    --queue-port 6379 \
    --mock \
    --mock-response "Worker on $NODE_HOSTNAME" \
    --latency-min 1 \
    --latency-max 2 \
    --idle-timeout 30 \
    > worker-$SLURM_JOB_ID.log 2>&1 &

WORKER_PID=$!
echo "Worker started with PID $WORKER_PID"
sleep 10

echo "Checking worker log..."
cat worker-$SLURM_JOB_ID.log