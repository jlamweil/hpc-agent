# HPC Agent - Production Guide

## Quick Start

```bash
# Local setup
./setup_local.sh

# Submit task
python submit_task.py --prompt "Summarize this: The quick brown fox..."

# Check results
python get_results.py --all

# Start monitor
python queue_monitor.py --db tasks.db --batch-size 5
```

## Architecture

```
┌──────────────┐    rsync    ┌──────────────┐   sbatch   ┌──────────────┐
│  Local       │ ──────────► │  HPC Login   │ ─────────► │  GPU Node    │
│  SQLite      │             │  Slurm       │            │  Transformers│
│  + Monitor   │             │  + Logs      │            │  + Worker    │
└──────────────┘             └──────────────┘            └──────────────┘
```

## Key Design Decisions

### Synchronous Processing
- **No async complexity**: Worker runs synchronously (blocking calls)
- **Matches Slurm**: Slurm jobs are naturally synchronous
- **Simpler debugging**: Standard Python exceptions

### Redis Usage
Redis is ONLY used for:
- **Task batching**: Temporary queue between SQLite and worker
- **Coordination**: Passing data between agents (if needed)

### Hermes Integration
- Direct integration with Transformers (no hermes-agent CLI)
- Worker reads from SQLite, runs inference, writes results back

## CLI Commands

### Submit Tasks
```bash
python submit_task.py --prompt "Your task" --agents "worker"
python submit_task.py --file prompts.txt --agents "worker"
python submit_task.py --prompt "Task" --model "meta-llama/Llama-3.2-1B"
```

### Get Results
```bash
python get_results.py --all
python get_results.py --status completed
python get_results.py --task-id <UUID> --metrics
```

### Monitor
```bash
python queue_monitor.py --db tasks.db
python queue_monitor.py --batch-size 5 --max-parallel-jobs 4 --dry-run
```

## Files

| File | Purpose |
|------|---------|
| `submit_task.py` | CLI for task submission |
| `get_results.py` | CLI for result retrieval |
| `queue_monitor.py` | Poll SQLite, trigger HPC jobs |
| `worker.py` | Process tasks on HPC |
| `cleanup.py` | Delete old tasks/results |
| `backup_db.py` | Backup SQLite database |
| `config.yaml` | Central configuration |
| `requirements.txt` | Python dependencies |

## HPC Jobs

```bash
# Manual submission
ssh jlam@hpc-login.u-strasbg.fr
cd ~/hpc-agent
sbatch --export=TASK_IDS="a,b,c" slurm/hpc_batch_job.slurm
```

## Monitoring

```bash
tail -f monitor.log
ssh jlam@hpc-login.u-strasbg.fr 'tail -f ~/hpc-agent/logs/slurm_*.out'
sqlite3 tasks.db "SELECT * FROM metrics;"
```

## Maintenance

```bash
python backup_db.py
python cleanup.py --vacuum --hpc-cleanup
```