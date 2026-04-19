#!/usr/bin/env python3
"""Cleanup script for SQLite and old HPC results."""
import argparse
import os
import sqlite3
import time
from datetime import datetime, timedelta


def cleanup_sqlite(db_path: str, completed_days: int = 30, failed_days: int = 7):
    """Delete old completed/failed tasks from SQLite."""
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    before_completed = (datetime.now() - timedelta(days=completed_days)).timestamp()
    before_failed = (datetime.now() - timedelta(days=failed_days)).timestamp()
    
    deleted_completed = conn.execute("""
        DELETE FROM tasks 
        WHERE status = 'completed' AND created_at < ?
    """, (before_completed,)).rowcount
    
    deleted_failed = conn.execute("""
        DELETE FROM tasks 
        WHERE status = 'failed' AND created_at < ?
    """, (before_failed,)).rowcount
    
    deleted_metrics = conn.execute("""
        DELETE FROM metrics 
        WHERE timestamp < ?
    """, (before_completed,)).rowcount
    
    conn.commit()
    
    print(f"Deleted: {deleted_completed} completed, {deleted_failed} failed, {deleted_metrics} metrics")
    conn.close()


def vacuum_sqlite(db_path: str):
    """Optimize SQLite database."""
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    conn.execute("VACUUM")
    conn.close()
    print(f"Vacuumed {db_path}")


def cleanup_hpc_results(hpc_user: str, hpc_host: str, hpc_dir: str, keep_days: int = 7):
    """Clean up old results directories on HPC."""
    cmd = [
        "ssh", f"{hpc_user}@{hpc_host}",
        f"find {hpc_dir}/results-* -maxdepth 0 -type d -mtime +{keep_days} -exec rm -rf {{}} + 2>/dev/null || true"
    ]
    
    import subprocess
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(f"HPC results cleanup: {result.stdout.strip() or 'No old results to clean'}")


def main():
    parser = argparse.ArgumentParser(description="Cleanup old tasks and results")
    parser.add_argument("--db", default="tasks.db", help="SQLite database")
    parser.add_argument("--completed-days", type=int, default=30, help="Keep completed tasks for N days")
    parser.add_argument("--failed-days", type=int, default=7, help="Keep failed tasks for N days")
    parser.add_argument("--hpc-user", default="jlam", help="HPC SSH user")
    parser.add_argument("--hpc-host", default="hpc-login.u-strasbg.fr", help="HPC hostname")
    parser.add_argument("--hpc-dir", default="~/hpc-agent", help="HPC directory")
    parser.add_argument("--hpc-cleanup", action="store_true", help="Also cleanup HPC results")
    parser.add_argument("--vacuum", action="store_true", help="Vacuum SQLite after cleanup")
    
    args = parser.parse_args()
    
    print(f"Cleaning up tasks older than {args.completed_days} days (completed) / {args.failed_days} days (failed)")
    
    cleanup_sqlite(args.db, args.completed_days, args.failed_days)
    
    if args.vacuum:
        vacuum_sqlite(args.db)
    
    if args.hpc_cleanup:
        cleanup_hpc_results(args.hpc_user, args.hpc_host, args.hpc_dir)


if __name__ == "__main__":
    main()