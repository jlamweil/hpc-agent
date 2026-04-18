#!/bin/bash
set -e

echo "=== HPC Agent Local Setup ==="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Installing Python dependencies..."
pip install --upgrade pip setuptools wheel

echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

pip install -q transformers accelerate redis sqlite3

echo "Initializing SQLite database..."
python3 -c "
import sqlite3
import os

db_path = 'tasks.db'
if os.path.exists(db_path):
    print(f'Database {db_path} already exists')
else:
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            messages TEXT NOT NULL,
            tools TEXT NOT NULL,
            model TEXT NOT NULL,
            api_config TEXT NOT NULL,
            metadata TEXT NOT NULL,
            priority INTEGER DEFAULT 0,
            created_at REAL NOT NULL,
            status TEXT DEFAULT \"pending\",
            result TEXT,
            started_at REAL,
            completed_at REAL,
            error TEXT,
            slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0
        )
    ''')
    conn.execute('''
        CREATE TABLE metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            task_id TEXT,
            job_id INTEGER,
            timestamp REAL NOT NULL,
            metadata TEXT
        )
    ''')
    conn.execute('CREATE INDEX idx_status ON tasks(status)')
    conn.execute('CREATE INDEX idx_created ON tasks(created_at)')
    conn.commit()
    print(f'Created {db_path}')
"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Usage:"
echo "  source venv/bin/activate"
echo ""
echo "Submit tasks:"
echo "  python submit_task.py --prompt 'Your task here'"
echo "  python submit_task.py --file prompts.txt --agents 'agent1,agent2'"
echo ""
echo "Get results:"
echo "  python get_results.py --all"
echo "  python get_results.py --task-id <ID>"
echo ""
echo "Start monitor:"
echo "  python queue_monitor.py --db tasks.db --batch-size 5"
echo ""
echo "For help:"
echo "  python submit_task.py --help"
echo "  python get_results.py --help"