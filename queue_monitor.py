#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.sqlite_queue import SQLiteTaskQueue


class QueueMonitor:
    def __init__(self, db_path: str, batch_size: int = 5,
                 hpc_user: str = "jlam", hpc_host: str = "hpc-login.u-strasbg.fr",
                 hpc_dir: str = "~/hpc-agent", dry_run: bool = False,
                 max_retries: int = 3, max_parallel_jobs: int = 4,
                 log_file: str = "monitor.log"):
        self.queue = SQLiteTaskQueue(db_path=db_path)
        self.batch_size = batch_size
        self.hpc_user = hpc_user
        self.hpc_host = hpc_host
        self.hpc_dir = hpc_dir
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.max_parallel_jobs = max_parallel_jobs
        self.poll_interval = 10
        self.log_file = log_file
        self._job_status_cache = {}
    
    def log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}"
        print(line)
        with open(self.log_file, "a") as f:
            f.write(line + "\n")
    
    def sync_db_to_hpc(self):
        self.log(f"Syncing {self.queue.db_path} to HPC...")
        
        cmd = [
            "rsync", "-av",
            self.queue.db_path,
            f"{self.hpc_user}@{self.hpc_host}:{self.hpc_dir}/"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            self.log(f"Rsync failed: {result.stderr}")
            return False
        
        self.log("Rsync complete")
        return True
    
    def check_job_status(self, job_id: str) -> str:
        if job_id in self._job_status_cache:
            return self._job_status_cache[job_id]
        
        cmd = [
            "ssh", f"{self.hpc_user}@{self.hpc_host}",
            f"sacct -j {job_id} --format=State --noheader"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            return "UNKNOWN"
        
        for line in result.stdout.strip().split("\n"):
            if job_id in line:
                if "FAILED" in line.upper():
                    self._job_status_cache[job_id] = "FAILED"
                    return "FAILED"
                elif "COMPLETED" in line.upper():
                    self._job_status_cache[job_id] = "COMPLETED"
                    return "COMPLETED"
                elif "RUNNING" in line.upper() or "PENDING" in line.upper():
                    self._job_status_cache[job_id] = "RUNNING"
                    return "RUNNING"
        
        return "UNKNOWN"
    
    def requeue_failed_tasks(self):
        conn = self.queue._get_conn()
        
        failed = conn.execute("""
            SELECT id, slurm_job_id FROM tasks 
            WHERE status = 'running' AND slurm_job_id IS NOT NULL
        """).fetchall()
        
        requeued = 0
        for row in failed:
            job_id = row['slurm_job_id']
            status = self.check_job_status(str(job_id))
            
            if status == "FAILED":
                retry_count = conn.execute(
                    "SELECT retry_count FROM tasks WHERE id = ?",
                    (row['id'],)
                ).fetchone()
                
                retry = (retry_count[0] or 0) + 1 if retry_count else 1
                
                if retry <= self.max_retries:
                    conn.execute("""
                        UPDATE tasks SET status = 'pending', retry_count = ?
                        WHERE id = ?
                    """, (retry, row['id']))
                    self.log(f"Requeued task {row['id']} (retry {retry}/{self.max_retries})")
                    requeued += 1
                else:
                    conn.execute("""
                        UPDATE tasks SET status = 'failed'
                        WHERE id = ?
                    """, (row['id'],))
                    self.log(f"Task {row['id']} failed after {self.max_retries} retries")
        
        conn.commit()
        return requeued
    
    def get_running_jobs_count(self) -> int:
        cmd = [
            "ssh", f"{self.hpc_user}@{self.hpc_host}",
            "squeue -u jlam -t RUNNING -h --format=%i"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return 0
        
        count = len([l for l in result.stdout.strip().split("\n") if l.strip()])
        return count
    
    def get_dynamic_batch_size(self, pending_count: int) -> int:
        if pending_count >= 50:
            return 20
        elif pending_count >= 20:
            return 15
        elif pending_count >= 10:
            return 10
        else:
            return self.batch_size
    
    def trigger_hpc_job(self, task_ids: list[str]):
        task_ids_str = ",".join(task_ids)
        
        self.log(f"Triggering HPC job for tasks: {task_ids_str}")
        
        if not self.sync_db_to_hpc():
            self.log("Failed to sync DB, skipping job submission")
            return None
        
        cmd = [
            "ssh", f"{self.hpc_user}@{self.hpc_host}",
            f"cd {self.hpc_dir} && sbatch --export=TASK_IDS=\"{task_ids_str}\" slurm/hpc_batch_job.slurm"
        ]
        
        if self.dry_run:
            self.log(f"[DRY RUN] Would execute: {' '.join(cmd)}")
            return "dry-run-job-id"
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            self.log(f"sbatch failed: {result.stderr}")
            return None
        
        output = result.stdout.strip()
        if "Submitted batch job" in output:
            job_id = output.split()[-1]
            self.log(f"Submitted job: {job_id}")
            return job_id
        
        self.log(f"Unexpected sbatch output: {output}")
        return None
    
    def mark_processing(self, task_ids: list[str], job_id: int):
        self.queue.update_slurm_job_id(task_ids, job_id)
        for tid in task_ids:
            self.log(f"Task {tid} → processing (Job {job_id})")
    
    def run(self):
        self.log(f"Starting queue monitor (batch_size={self.batch_size}, max_retries={self.max_retries})")
        self.log(f"DB: {self.queue.db_path}, Polling every {self.poll_interval}s")
        
        while True:
            try:
                running_jobs = self.get_running_jobs_count()
                pending_count = self.queue.get_queue_size()
                metrics = self.queue.get_metrics()
                
                self.log(f"Queue: {pending_count} pending, Running jobs: {running_jobs}, Metrics: {metrics}")
                
                requeued = self.requeue_failed_tasks()
                if requeued > 0:
                    self.log(f"Requeued {requeued} failed tasks")
                
                batch_size = self.get_dynamic_batch_size(pending_count)
                available_slots = self.max_parallel_jobs - running_jobs
                if pending_count >= batch_size and available_slots > 0:
                    tasks = self.queue.dequeue_batch(batch_size, "hpc-monitor")
                    
                    if tasks:
                        task_ids = [t.task_id for t in tasks]
                        job_id = self.trigger_hpc_job(task_ids)
                        
                        if job_id and job_id != "dry-run-job-id":
                            self.mark_processing(task_ids, int(job_id))
                
            except KeyboardInterrupt:
                self.log("\nStopping monitor")
                break
            except Exception as e:
                self.log(f"Error: {e}")
            
            time.sleep(self.poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Monitor SQLite queue and trigger HPC jobs")
    parser.add_argument("--db", default="tasks.db")
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--hpc-user", default="jlam")
    parser.add_argument("--hpc-host", default="hpc-login.u-strasbg.fr")
    parser.add_argument("--hpc-dir", default="~/hpc-agent")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--poll-interval", type=int, default=10)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-parallel-jobs", type=int, default=4)
    parser.add_argument("--log-file", default="monitor.log")
    
    args = parser.parse_args()
    
    monitor = QueueMonitor(
        db_path=args.db,
        batch_size=args.batch_size,
        hpc_user=args.hpc_user,
        hpc_host=args.hpc_host,
        hpc_dir=args.hpc_dir,
        dry_run=args.dry_run,
        max_retries=args.max_retries,
        max_parallel_jobs=args.max_parallel_jobs,
        log_file=args.log_file
    )
    monitor.poll_interval = args.poll_interval
    
    monitor.run()


if __name__ == "__main__":
    main()