#!/bin/bash
#SBATCH --job-name=all-in-one-worker
#SBATCH --output=all-in-one-%j.out
#SBATCH --error=all-in-one-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=8G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"
NODE_HOSTNAME=$(hostname -s)

echo "=== All-in-One Worker (Redis + Transformers + Worker) ==="
echo "Node: $NODE_HOSTNAME"

cd "$HERO_DIR"

# Activate conda
source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

# Install dependencies
pip install redis transformers accelerate --quiet 2>/dev/null || pip install redis transformers accelerate

# Start a minimal Redis-compatible server
echo "Starting Redis server..."
python3 << 'REDIS' > redis-$SLURM_JOB_ID.log 2>&1 &
import socket
import threading

tasks = []

def handle(conn, addr):
    try:
        data = conn.recv(4096)
        if not data:
            return
        d = data.decode().upper()
        
        if 'PING' in d:
            conn.send(b'+PONG\r\n')
        elif 'LPUSH' in d:
            tasks.append('mock_task')
            conn.send(b':1\r\n')
        elif 'RPOP' in d:
            if tasks:
                conn.send(b'+task_data\r\n')
            else:
                conn.send(b'$-1\r\n')
        elif 'LLEN' in d:
            conn.send(b':0\r\n')
        else:
            conn.send(b'+OK\r\n')
    except:
        pass
    finally:
        conn.close()

sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', 6379))
sock.listen(5)
print('Redis on 6379')
while True:
    c, a = sock.accept()
    threading.Thread(target=handle, args=(c, a)).start()
REDIS

sleep 2

echo "Starting Transformers model..."
python3 << 'MODEL' > model-$SLURM_JOB_ID.log 2>&1 &
import os
os.environ['HF_HOME'] = '/tmp/hf'
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

print('Loading TinyLlama...')
tokenizer = AutoTokenizer.from_pretrained("TinyLlama/TinyLlama-1.1B-Chat-v1.0")
model = AutoModelForCausalLM.from_pretrained(
    "TinyLlama/TinyLlama-1.1B-Chat-v1.0",
    torch_dtype=torch.float16,
    device_map="auto"
)
print('Model loaded!')

# Simple inference loop
while True:
    try:
        with open('/tmp/inference_request', 'r') as f:
            prompt = f.read().strip()
        if prompt:
            inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
            outputs = model.generate(**inputs, max_new_tokens=50, do_sample=False)
            result = tokenizer.decode(outputs[0], skip_special_tokens=True)
            with open('/tmp/inference_result', 'w') as f:
                f.write(result)
            print(f'Inference done: {result[:50]}...')
            os.remove('/tmp/inference_request')
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f'Error: {e}')
    import time
    time.sleep(2)
MODEL

sleep 30  # Wait for model to load

echo "Starting worker..."
python worker/worker.py \
    --queue-host localhost \
    --queue-port 6379 \
    --transformers \
    --transformers-model TinyLlama/TinyLlama-1.1B-Chat-v1.0 \
    --batch-size 1 \
    --idle-timeout 60 \
    > worker-$SLURM_JOB_ID.log 2>&1 &

echo "All components started. Worker running..."
sleep 300  # Run for 5 minutes
echo "Done"