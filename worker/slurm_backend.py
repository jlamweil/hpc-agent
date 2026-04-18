"""Slurm backend - submit and manage worker jobs on HPC cluster."""
import os
import subprocess
import time
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SlurmJob:
    job_id: str
    worker_id: str
    state: str  # PENDING, RUNNING, COMPLETED, FAILED
    node: Optional[str] = None
    submitted_at: float = None


class SlurmBackend:
    """Manages Slurm worker jobs."""
    
    def __init__(self, 
                 script_path: str = None,
                 queue_host: str = "localhost",
                 queue_port: int = 6379,
                 batch_size: int = 1,
                 ollama_endpoint: str = "http://localhost:11434",
                 ollama_model: str = "llama3:8b",
                 idle_timeout: Optional[int] = None,
                 sbatch_args: str = ""):
        
        self.script_path = script_path or self._default_script()
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.batch_size = batch_size
        self.ollama_endpoint = ollama_endpoint
        self.ollama_model = ollama_model
        self.idle_timeout = idle_timeout
        self.sbatch_args = sbatch_args
        
        self._jobs: dict[str, SlurmJob] = {}
    
    def _default_script(self) -> str:
        """Get default Slurm script path."""
        hero = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(hero, "slurm", "run_worker.slurm")
    
    def _build_env_export(self) -> str:
        """Build environment exports for sbatch."""
        env_vars = [
            f"QUEUE_HOST={self.queue_host}",
            f"QUEUE_PORT={self.queue_port}",
            f"BATCH_SIZE={self.batch_size}",
            f"OLLAMA_ENDPOINT={self.ollama_endpoint}",
            f"OLLAMA_MODEL={self.ollama_model}",
            "WORKER_MODE=ollama",
        ]
        if self.idle_timeout:
            env_vars.append(f"IDLE_TIMEOUT={self.idle_timeout}")
        return ",".join(env_vars)
    
    def submit_worker(self, worker_id: str = None) -> SlurmJob:
        """Submit a new worker job to Slurm."""
        worker_id = worker_id or f"worker-{int(time.time())}"
        
        # Build sbatch command
        cmd = ["sbatch"]
        
        # Add custom sbatch arguments
        if self.sbatch_args:
            for arg in self.sbatch_args.split():
                cmd.append(arg)
        
        # Set job name
        cmd.extend(["--job-name", f"hpc-{worker_id}"])
        
        # Export environment
        env_export = self._build_env_export()
        cmd.extend(["--export", env_export])
        
        # Add script
        cmd.append(self.script_path)
        
        logger.info(f"Submitting Slurm job: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse job ID from output like "Submitted batch job 12345"
        output = result.stdout.strip()
        job_id = output.split()[-1]
        
        job = SlurmJob(
            job_id=job_id,
            worker_id=worker_id,
            state="PENDING",
            submitted_at=time.time()
        )
        
        self._jobs[job_id] = job
        logger.info(f"Submitted job {job_id} for worker {worker_id}")
        
        return job
    
    def scale_to(self, target_count: int) -> list[SlurmJob]:
        """Scale workers to target count."""
        current = len([j for j in self._jobs.values() if j.state in ("PENDING", "RUNNING")])
        
        if target_count > current:
            # Scale up
            new_jobs = []
            for i in range(target_count - current):
                job = self.submit_worker(f"worker-{int(time.time())}-{i}")
                new_jobs.append(job)
            return new_jobs
        elif target_count < current:
            # Scale down - cancel excess jobs
            self._cancel_excess(target_count)
        
        return []
    
    def _cancel_excess(self, target_count: int):
        """Cancel excess running jobs."""
        active = [j for j in self._jobs.values() if j.state in ("PENDING", "RUNNING")]
        
        to_cancel = active[target_count:]
        
        for job in to_cancel:
            self.cancel_job(job.job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a Slurm job."""
        try:
            subprocess.run(
                ["scancel", job_id],
                check=True,
                capture_output=True
            )
            
            if job_id in self._jobs:
                self._jobs[job_id].state = "COMPLETED"
            
            logger.info(f"Cancelled job {job_id}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False
    
    def get_job_status(self, job_id: str) -> Optional[SlurmJob]:
        """Query job status from Slurm."""
        try:
            result = subprocess.run(
                ["squeue", "-j", job_id, "-o", "%T,%N", "--noheader"],
                capture_output=True,
                text=True,
                check=True
            )
            
            output = result.stdout.strip()
            if not output:
                # Job no longer exists
                if job_id in self._jobs:
                    self._jobs[job_id].state = "COMPLETED"
                return None
            
            state, node = output.split(",")
            state = state.strip()
            
            # Map Slurm states
            if state in ("PENDING", "CONFIGURING", "LAUNCHED"):
                mapped = "PENDING"
            elif state in ("RUNNING", "COMPLETING"):
                mapped = "RUNNING"
            elif state in ("COMPLETED", "CANCELLED", "TIMEOUT"):
                mapped = "COMPLETED"
            else:
                mapped = "FAILED"
            
            if job_id in self._jobs:
                self._jobs[job_id].state = mapped
                self._jobs[job_id].node = node.strip()
            
            return self._jobs.get(job_id)
            
        except subprocess.CalledProcessError:
            return None
    
    def refresh_all_statuses(self) -> dict[str, SlurmJob]:
        """Refresh status of all tracked jobs."""
        for job_id in list(self._jobs.keys()):
            self.get_job_status(job_id)
        
        return self._jobs
    
    def get_active_count(self) -> int:
        """Get number of active (PENDING or RUNNING) jobs."""
        self.refresh_all_statuses()
        return len([j for j in self._jobs.values() if j.state in ("PENDING", "RUNNING")])
    
    def cleanup(self):
        """Cancel all pending/running jobs."""
        for job in list(self._jobs.values()):
            if job.state in ("PENDING", "RUNNING"):
                self.cancel_job(job.job_id)


def is_slurm_available() -> bool:
    """Check if Slurm is available on this system."""
    try:
        result = subprocess.run(
            ["sbatch", "--version"],
            capture_output=True,
            check=True
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


if __name__ == "__main__":
    # Simple test
    if is_slurm_available():
        print("Slurm is available")
        backend = SlurmBackend()
        job = backend.submit_worker("test-worker")
        print(f"Submitted job: {job.job_id}")
    else:
        print("Slurm not available (this is expected on non-HPC systems)")