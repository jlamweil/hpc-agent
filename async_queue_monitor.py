#!/usr/bin/env python3
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.sqlite_queue import SQLiteTaskQueue
from infra.routing import TaskRouter
from collectors.base import CollectorResult
from collectors.config import get_collector_config

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
HPC_USER = os.environ.get("HPC_USER", "jlam")
HPC_HOST = os.environ.get("HPC_HOST", "hpc-login.u-strasbg.fr")
HPC_DIR = os.environ.get("HPC_DIR", "~/hpc-agent")
CLAIM_TIMEOUT = int(os.environ.get("CLAIM_TIMEOUT", "600"))
HIGH_THRESHOLD = int(os.environ.get("HIGH_THRESHOLD", "20"))
LOW_THRESHOLD = int(os.environ.get("LOW_THRESHOLD", "5"))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "3"))


class AsyncQueueMonitor:
    def __init__(self, db_path: str = "tasks.db", batch_size: int = BATCH_SIZE,
                 hpc_user: str = HPC_USER, hpc_host: str = HPC_HOST,
                 hpc_dir: str = HPC_DIR, dry_run: bool = False,
                 worker_id: str = None, high_threshold: int = HIGH_THRESHOLD,
                 low_threshold: int = LOW_THRESHOLD, max_jobs: int = MAX_CONCURRENT_JOBS,
                 router: str = None, fill_tasks: bool = True):
        self.queue = SQLiteTaskQueue(db_path=db_path)
        self.batch_size = batch_size
        self.hpc_user = hpc_user
        self.hpc_host = hpc_host
        self.hpc_dir = hpc_dir
        self.dry_run = dry_run
        self.poll_interval = POLL_INTERVAL
        self.worker_id = worker_id or f"monitor-{uuid.uuid4().hex[:8]}"
        self.high_threshold = high_threshold
        self.low_threshold = low_threshold
        self.max_jobs = max_jobs
        self.active_jobs = 0
        self.router = router  # 'local', 'hpc', or None
        self.fill_tasks_enabled = fill_tasks
        self._fill_generated = False  # only generate once per monitor run

    def _generate_fill_tasks(self):
        """Generate low-priority fill tasks from config.yaml generators.
        
        Called once when the first SLURM job is submitted.
        Fill tasks sit at priority -1 and are only processed when nothing else is pending.
        """
        if not self.fill_tasks_enabled or self._fill_generated:
            return 0
        
        import yaml
        from pathlib import Path
        
        # Load config
        config_path = Path(__file__).resolve().parent / "config.yaml"
        if not config_path.exists():
            return 0
        
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
        
        fill_config = config.get("fill_tasks", {})
        if not fill_config.get("enabled", False):
            return 0
        
        generators = fill_config.get("generators", [])
        max_per_job = fill_config.get("max_per_job", 50)
        total_generated = 0
        
        # For each generator, run the collector and submit fill tasks
        from hpc_batch import submit_task
        
        for gen in generators:
            if total_generated >= max_per_job:
                break
            
            collector_name = gen.get("collector", "")
            if not collector_name:
                # Echo generator: re-run failed tasks at low priority
                if gen.get("echo", False):
                    re_priority = gen.get("priority", -2)
                    conn = self.queue._get_conn()
                    rows = conn.execute(
                        "SELECT id FROM tasks WHERE status = 'failed' AND COALESCE(attempt_count, 0) < 3"
                    ).fetchall()
                    for row in rows:
                        if total_generated >= max_per_job:
                            break
                        conn.execute(
                            "UPDATE tasks SET status = 'pending', attempt_count = COALESCE(attempt_count, 0) + 1, "
                            "priority = ?, last_error = NULL, completed_at = NULL WHERE id = ?",
                            (re_priority, row[0]),
                        )
                        total_generated += 1
                    conn.commit()
                    conn.close()
                continue
            
            # Run collector
            try:
                import collectors
                collector_cls = collectors.get_collector(collector_name)
                collector = collector_cls()
                params = gen.get("params", {})
                results = collector.collect(params)
            except Exception as e:
                print(f"  [fill] Collector {collector_name} error: {e}")
                continue
            
            if not results:
                continue
            
            # Submit each result as a fill task
            gen_priority = gen.get("priority", -1)
            gen_tasks_config = gen.get("tasks", [{"type": "generic"}])
            
            for r in results:
                if total_generated >= max_per_job:
                    break
                for task_def in gen_tasks_config:
                    if total_generated >= max_per_job:
                        break
                    task_type = task_def.get("type", "generic")
                    meta = {
                        "type": task_type,
                        "source": collector_name,
                        "source_id": r.source_id,
                        "source_url": r.url,
                        "title": r.title,
                        "tags": task_def.get("tags", []) + ["fill"],
                        "provenance": {
                            "collector": f"{collector_name}/1.0",
                            "collected_at": time.time(),
                            "source": collector_name,
                        },
                    }
                    tid = submit_task(
                        prompt=f"Analyze: {r.title}\n\n{r.text}",
                        db_path=self.queue.db_path,
                        model=task_def.get("model", "deepseek-ai/DeepSeek-V4-Flash"),
                        metadata=meta,
                    )
                    # Set priority to -1 so fill tasks never block real work
                    conn = self.queue._get_conn()
                    conn.execute("UPDATE tasks SET priority = ? WHERE id = ?", (gen_priority, tid))
                    conn.commit()
                    total_generated += 1
        
        self._fill_generated = True
        if total_generated > 0:
            print(f"[fill] Generated {total_generated} fill tasks at priority -1")
        return total_generated
    
    async def get_pending_count(self) -> int:
        return self.queue.get_queue_size()
    
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
            return output.split()[-1]
        return None
    
    async def run(self):
        print(f"Queue Monitor starting (worker_id={self.worker_id}, batch_size={self.batch_size})")
        print(f"DB: {self.queue.db_path}, Poll interval: {self.poll_interval}s")
        print(f"Thresholds: high={self.high_threshold}, low={self.low_threshold}, max_jobs={self.max_jobs}")
        
        while True:
            try:
                metrics = self.queue.get_metrics()
                pending = metrics.get('pending', 0)
                
                self.queue.recover_stuck_claimed(CLAIM_TIMEOUT)
                
                if pending < self.low_threshold:
                    print(f"Queue low ({pending}), waiting...")
                    await asyncio.sleep(self.poll_interval)
                    continue
                
                if self.active_jobs >= self.max_jobs:
                    print(f"Max jobs ({self.max_jobs}) reached, waiting...")
                    await asyncio.sleep(self.poll_interval)
                    continue
                
                jobs_to_spawn = 1
                if pending >= self.high_threshold:
                    jobs_to_spawn = min(self.max_jobs - self.active_jobs, pending // self.batch_size + 1)
                    print(f"Queue high ({pending}), spawning {jobs_to_spawn} jobs")
                
                for _ in range(jobs_to_spawn):
                    claimed = self.queue.claim_tasks(self.worker_id, self.batch_size)
                    
                    if claimed:
                        # Filter by routing rule if --router is set
                        if self.router:
                            _router = TaskRouter()
                            filtered = []
                            for t in claimed:
                                decision = _router.route({
                                    "model": t["model"],
                                    "messages": json.loads(t["messages"]),
                                    "metadata": json.loads(t["metadata"]),
                                })
                                if decision == self.router:
                                    filtered.append(t)
                                else:
                                    self.queue.release_task(t["id"])
                            if len(filtered) < len(claimed):
                                print(f"Routing filter ({self.router}): kept {len(filtered)}/{len(claimed)} tasks")
                            claimed = filtered
                        
                        # Log routing distribution
                        if claimed:
                            _router = TaskRouter()
                            local_count = 0
                            for t in claimed:
                                d = _router.route({
                                    "model": t["model"],
                                    "messages": json.loads(t["messages"]),
                                    "metadata": json.loads(t["metadata"]),
                                })
                                if d == "local":
                                    local_count += 1
                            hpc_count = len(claimed) - local_count
                            print(f"Routing distribution: {local_count} local, {hpc_count} hpc")
                        
                        task_ids = [t['id'] for t in claimed]
                        print(f"Claimed {len(task_ids)} tasks: {[tid[:8] for tid in task_ids]}")

                        # Generate fill tasks for this job (low-priority background work)
                        fill_count = self._generate_fill_tasks()
                        if fill_count > 0:
                            print(f"[fill] Added {fill_count} fill tasks for idle GPU cycles")
                        
                        sync_ok = await self.sync_db_to_hpc()
                        
                        if sync_ok:
                            job_id = await self.submit_hpc_job(task_ids)
                            
                            if job_id and job_id != "dry-run-job-id":
                                self.active_jobs += 1
                                for tid in task_ids:
                                    self.queue.mark_claimed_with_job(tid, int(job_id))
                                print(f"Submitted job {job_id} for {len(task_ids)} tasks (active: {self.active_jobs})")
                            elif job_id == "dry-run-job-id":
                                print(f"[DRY RUN] Would submit {len(task_ids)} tasks")
                            else:
                                print("HPC submission failed, releasing tasks")
                                for tid in task_ids:
                                    self.queue.release_task(tid)
                        else:
                            print("Sync failed, releasing tasks")
                            for tid in task_ids:
                                self.queue.release_task(tid)
                
                print(f"Metrics: {metrics}")
                
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
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--router", choices=["local", "hpc"], default=None,
                        help="Only claim tasks routed to this target (local|hpc)")
    parser.add_argument("--fill", action="store_true", default=True,
                        help="Generate fill tasks when submitting HPC jobs (default: on)")
    parser.add_argument("--no-fill", action="store_false", dest="fill",
                        help="Disable fill task generation")
    
    args = parser.parse_args()
    
    monitor = AsyncQueueMonitor(
        db_path=args.db,
        batch_size=args.batch_size,
        hpc_user=args.hpc_user,
        hpc_host=args.hpc_host,
        hpc_dir=args.hpc_dir,
        dry_run=args.dry_run,
        worker_id=args.worker_id,
        router=args.router,
        fill_tasks=args.fill,
    )
    monitor.poll_interval = args.poll_interval
    
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())