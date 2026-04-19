#!/bin/bash
#SBATCH --job-name=hpc-batch
#SBATCH --output=/home2020/home/icube/jlam/hpc-agent/logs/slurm_%j_%N.out
#SBATCH --error=/home2020/home/icube/jlam/hpc-agent/logs/slurm_%j_%N.err
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=8G
#SBATCH --partition=publicgpu
#SBATCH --requeue

HERO_DIR="$HOME/hpc-agent"
DB_PATH="$HERO_DIR/tasks.db"
NODE_HOSTNAME=$(hostname -s)
LOG_DIR="$HERO_DIR/logs"
RESULTS_DIR="$HERO_DIR/results-$SLURM_JOB_ID"
SCRATCH_DIR="/tmp/scratch_${SLURM_JOB_ID}"

trap 'cleanup' EXIT

cleanup() {
    exit_code=$?
    if [ -d "$SCRATCH_DIR" ]; then
        rm -rf "$SCRATCH_DIR"
    fi
    if [ $exit_code -ne 0 ]; then
        echo "Job failed with exit code $exit_code" >&2
    fi
}

mkdir -p "$LOG_DIR" "$RESULTS_DIR" "$SCRATCH_DIR"
cd "$SCRATCH_DIR"

echo "=== HPC Batch Job Started ===" | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"
echo "Job ID: $SLURM_JOB_ID" | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"
echo "Node: $NODE_HOSTNAME" | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"
echo "DB: $DB_PATH" | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"

source /usr/local/python/Miniconda/etc/profile.d/conda.sh
conda activate autogluon-env

pip install redis transformers accelerate --quiet 2>/dev/null || pip install redis transformers accelerate

export HF_HOME=/tmp/hf
export TRANSFORMERS_CACHE=/tmp/transformers_cache
export TF_CPP_MIN_LOG_LEVEL=3

echo "Loading Transformers model..." | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"

python3 << MODEL_EOF 2>&1 | tee -a "$LOG_DIR/model_${SLURM_JOB_ID}.log"
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
import os
import sys
import time
import sqlite3
import json

db_path = os.environ.get("DB_PATH", "$DB_PATH")
start_time = time.time()

print('Loading TinyLlama...')
tokenizer = AutoTokenizer.from_pretrained('TinyLlama/TinyLlama-1.1B-Chat-v1.0')
model = AutoModelForCausalLM.from_pretrained(
    'TinyLlama/TinyLlama-1.1B-Chat-v1.0',
    torch_dtype=torch.float16,
    device_map='auto'
)
load_time = time.time() - start_time
print(f'Model loaded on GPU in {load_time:.1f}s!')

print('Reading pending tasks from SQLite...')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

tasks = conn.execute("""
    SELECT id, messages FROM tasks 
    WHERE status = 'pending' 
    ORDER BY priority DESC, created_at ASC
    LIMIT 10
""").fetchall()

if not tasks:
    print('No pending tasks found!')
    conn.close()
    sys.exit(0)

print(f'Found {len(tasks)} pending tasks')

completed = 0
failed = 0

for task in tasks:
    task_id = task['id']
    messages = json.loads(task['messages'])
    prompt = messages[0]['content']
    
    try:
        print(f'Processing task: {task_id}')
        print(f'  Prompt: {prompt[:50]}...')
        
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        outputs = model.generate(**inputs, max_new_tokens=100, do_sample=False)
        result = tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        print(f'  Result: {result[:80]}...')
        
        conn.execute("""
            UPDATE tasks SET status = 'completed', completed_at = ?, result = ?
            WHERE id = ?
        """, (time.time(), json.dumps({"response": result}), task_id))
        conn.commit()
        
        with open('/tmp/result_{}.txt'.format(task_id), 'w') as f:
            f.write(result)
        
        completed += 1
    except Exception as e:
        print(f'  Task {task_id} failed: {e}', file=sys.stderr)
        conn.execute("""
            UPDATE tasks SET status = 'failed', error = ?
            WHERE id = ?
        """, (str(e), task_id))
        conn.commit()
        
        with open('/tmp/result_{}.txt'.format(task_id), 'w') as f:
            f.write(f'ERROR: {e}')
        failed += 1

conn.close()

total_time = time.time() - start_time
print(f'All tasks processed: {completed} completed, {failed} failed in {total_time:.1f}s')
MODEL_EOF

EXIT_CODE=$?

echo "Syncing results back to local..." | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"

mkdir -p "$RESULTS_DIR"
rsync -av /tmp/result_*.txt "$RESULTS_DIR/" 2>/dev/null || true
rsync -av "$DB_PATH" "${HERO_DIR}/test_tasks.db" 2>/dev/null || true

echo "=== Job Complete (exit code: $EXIT_CODE) ===" | tee -a "$LOG_DIR/slurm_${SLURM_JOB_ID}.log"
echo "Results saved in $RESULTS_DIR"

exit $EXIT_CODE