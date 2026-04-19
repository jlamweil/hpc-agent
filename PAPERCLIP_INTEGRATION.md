# Paperclip Integration Guide

## What is Paperclip?

**Paperclip** is an open source "human control plane for AI labor" - an agent orchestration platform with:
- Org charts for AI agents
- Task/issue management
- Goal alignment
- Budget and cost tracking
- Governance

## Hermes + Paperclip Integration

Nous Research provides `hermes-paperclip-adapter` - an NPM package that runs Hermes Agent as a managed employee in Paperclip.

### Architecture

```
Paperclip (web UI / API)
    │
    ├── Heartbeat scheduler wakes agents
    │
    └── hermes-paperclip-adapter
            │
            ├── Spawns: hermes chat -q "<task>"
            │
            └── Hermes writes to SQLite queue
                        │
                        ↓
                async_queue_monitor
                        │
                        ↓
                HPC (Slurm + Transformers)
```

## Installation

### 1. Install Node.js dependencies (if not already)

```bash
# Check if node is available
node --version
npm --version
```

### 2. Install Hermes Paperclip adapter

```bash
npm install -g hermes-paperclip-adapter
```

### 3. Configure Paperclip

In Paperclip UI:
1. Go to **Agents** → **Add Agent**
2. Select **Hermes** as the agent type
3. Configure:

```json
{
  "model": "anthropic/claude-sonnet-4-20250514",
  "provider": "auto",
  "paperclipApiUrl": "http://127.0.0.1:3100/api",
  "timeoutSec": 300,
  "toolsets": "terminal,file,web"
}
```

### 4. Environment variables

```bash
# In ~/.hermes/.env
ANTHROPIC_API_KEY=sk-...
OPENROUTER_API_KEY=...
```

## How It Works

1. **Task Assignment**: You create an issue in Paperclip
2. **Heartbeat**: Paperclip wakes the Hermes agent
3. **Execution**: Hermes adapter spawns `hermes chat -q "<issue>"`
4. **Queue Submission**: Hermes writes task to SQLite (deterministic)
5. **HPC Processing**: async_queue_monitor triggers Slurm batch
6. **Result Sync**: Results sync back, Hermes returns response to Paperclip

## Integration with HPC Agent

The existing `local_agent.py` already handles SQLite queue submission:

```python
# Paperclip → Hermes → SQLite → HPC → Results
```

No changes needed to the HPC agent side. The key is:

1. **Deterministic writes** - SQLite is synchronous, no race conditions
2. **Task tracking** - Each task has unique UUID
3. **Result retrieval** - Use `get_results.py` or direct SQLite queries

## Testing

### Test 1: Manual Submission

```bash
# Submit a task directly to SQLite
cd ~/Documents/GAIA/hpc-agent
python -c "
from local_agent import submit_batch
import asyncio
asyncio.run(submit_batch(['Write a factorial function']))
"
```

### Test 2: Via Hermes CLI

```bash
# If Hermes is installed
hermes -q "Write a function to calculate fibonacci"
```

### Test 3: Via Paperclip

1. Create issue in Paperclip
2. Agent wakes up
3. Check SQLite: `sqlite3 tasks.db "SELECT * FROM tasks"`

## Monitoring

```bash
# Watch queue
python async_queue_monitor.py --batch-size 5

# Check results
python get_results.py --all
```

## Files

| File | Purpose |
|------|---------|
| `local_agent.py` | Async submission to SQLite |
| `async_queue_monitor.py` | Batch trigger for HPC |
| `worker/hpc_worker.py` | Skill-based HPC worker |
| `tasks.db` | SQLite queue |