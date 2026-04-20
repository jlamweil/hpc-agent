#!/usr/bin/env python3
"""Async queue monitor - watches SQLite and triggers HPC batches."""
import asyncio
import os
import sqlite3
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.sqlite_queue import SQLiteTaskQueue

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
HPC_USER = os.environ.get("HPC_USER", "jlam")
HPC_HOST = os.environ.get("HPC_HOST", "hpc-login.u-strasbg.fr")
HPC_DIR = os.environ.get("HPC_DIR", "~/hpc-agent")


class AsyncQueueMonitor:
    def __init__(self, db_path: str = "tasks.db", batch_size: int = BATCH_SIZE,
                 hpc_user: str = HPC_USER, hpc_host: str = HPC_HOST,
                 hpc_dir: str = HPC_DIR, dry_run: bool = False):
        self.queue = SQLiteTaskQueue(db_path=db_path)
        self.batch_size = batch_size
        self.hpc_user = hpc_user
        self.hpc_host = hpc_host
        self.hpc_dir = hpc_dir
        self.dry_run = dry_run
        self.poll_interval = POLL_INTERVAL
    
    async def get_pending_count(self) -> int:
        return self.queue.get_queue_size()
    
    async def get_pending_tasks(self) -> list:
        tasks = self.queue.get_pending_tasks(limit=self.batch_size)
        # Handle both dict and object formats
        if tasks and isinstance(tasks[0], dict):
            return tasks
        return tasks
    
    async def sync_db_to_hpc(self) -> bool:
        cmd = ["rsync", "-av", self.queue.db_path, f"{self.hpc_user}@{self.hpc_host}:{self.hpc_dir}/"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    
    async def submit_hpc_job(self, task_ids: list[str]) -> str:
        task_ids_str = ",".join(task_ids)
        
        cmd = [
            "ssh", f"{self.hpc_user}@{self.hpc_host}",
            f"cd {self.hpc_dir} && sbatch --export=TASK_IDS=\"{task_ids_str}\" slurm/hpc_batch_job.slurm"
        ]
        
        if self.dry_run:
            print(f"[DRY RUN] Would execute: {' '.join(cmd)}")
            return "dry-run-job-id"
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"sbatch failed: {result.stderr}")
            return None
        
        output = result.stdout.strip()
        if "Submitted batch job" in output:
            job_id = output.split()[-1]
            for tid in task_ids:
                self.queue.mark_running(tid)
            return job_id
        return None
    
    async def run(self):
        print(f"Async Queue Monitor starting (batch_size={self.batch_size})")
        print(f"DB: {self.queue.db_path}, Poll interval: {self.poll_interval}s")
        
        while True:
            try:
                pending = await self.get_pending_count()
                print(f"Queue: {pending} pending, {self.queue.get_metrics()}")
                
                if pending >= self.batch_size:
                    tasks = await self.get_pending_tasks()
                    
                    if tasks:
                        # Handle both dict and object formats
                        # Note: orchestrator uses "id" not "task_id"
                        if isinstance(tasks[0], dict):
                            task_ids = [t["id"] for t in tasks]
                        else:
                            task_ids = [t.id for t in tasks]
                        
                        print(f"Batch ready ({len(task_ids)} tasks). Syncing to HPC...")
                        sync_ok = await self.sync_db_to_hpc()
                        
                        if sync_ok:
                            job_id = await self.submit_hpc_job(task_ids)
                            
                            if job_id and job_id != "dry-run-job-id":
                                self.queue.update_slurm_job_id(task_ids, int(job_id))
                                print(f"Submitted job {job_id} for {len(task_ids)} tasks")
                        else:
                            print("Sync failed, skipping job submission")
                else:
                    print(f"Waiting for {self.batch_size - pending} more tasks...")
                
            except KeyboardInterrupt:
                print("\nStopping monitor")
                break
            except Exception as e:
                print(f"Error: {e}")
            
            await asyncio.sleep(self.poll_interval)


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Async queue monitor")
    parser.add_argument("--db", default="tasks.db")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--hpc-user", default=HPC_USER)
    parser.add_argument("--hpc-host", default=HPC_HOST)
    parser.add_argument("--hpc-dir", default=HPC_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=POLL_INTERVAL)
    
    args = parser.parse_args()
    
    monitor = AsyncQueueMonitor(
        db_path=args.db,
        batch_size=args.batch_size,
        hpc_user=args.hpc_user,
        hpc_host=args.hpc_host,
        hpc_dir=args.hpc_dir,
        dry_run=args.dry_run
    )
    monitor.poll_interval = args.poll_interval
    
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())