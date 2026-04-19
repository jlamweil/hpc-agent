#!/bin/bash
#SBATCH --job-name=load-test
#SBATCH --output=load-test-%j.out
#SBATCH --error=load-test-%j.err
#SBATCH --time=02:00:00
#SBATCH --nodes=2
#SBATCH --gres=gpu:1
#SBATCH --mem=4G
#SBATCH --partition=publicgpu

set -e

HERO_DIR="$HOME/hpc-agent"

QUEUE_HOST="${QUEUE_HOST:-localhost}"
QUEUE_PORT="${QUEUE_PORT:-6379}"
NUM_WORKERS="${NUM_WORKERS:-4}"
BATCH_SIZE="${BATCH_SIZE:-2}"

echo "=== Load Test - $NUM_WORKERS Workers ==="
echo "Queue: $QUEUE_HOST:$QUEUE_PORT"

cd "$HERO_DIR"

if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -f "/usr/local/python/Miniconda/etc/profile.d/conda.sh" ]; then
    source /usr/local/python/Miniconda/etc/profile.d/conda.sh
    conda activate autogluon-env
fi

echo "Starting $NUM_WORKERS workers..."

for i in $(seq 1 $NUM_WORKERS); do
    python worker/worker.py \
        --queue-host "$QUEUE_HOST" \
        --queue-port "$QUEUE_PORT" \
        --batch-size "$BATCH_SIZE" \
        --mock \
        --mock-response "Worker $i response" \
        --latency-min 0.5 \
        --latency-max 1.5 \
        --idle-timeout 60 \
        > worker-$i-$SLURM_JOB_ID.log 2>&1 &
    echo "Started worker $i (PID: $!)"
done

echo "All workers started. Monitoring..."
echo "Queue host reachable from compute nodes: testing..."

srun --nodes=1 python3 -c "
import redis
r = redis.Redis(host='$QUEUE_HOST', port=int('$QUEUE_PORT'), socket_timeout=5)
r.ping()
print('Redis reachable from compute node!')
" 2>/dev/null || echo "Redis NOT reachable from compute nodes"

echo "Workers running. Check worker logs for activity."
echo "Job will run for 2 hours or until idle timeout."

sleep 3600