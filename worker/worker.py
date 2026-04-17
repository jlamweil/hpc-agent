"""Worker - pulls tasks from queue and processes them."""
import time
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime import TaskQueue, TaskStatus


class Worker:
    def __init__(self, queue: TaskQueue, model_runner, worker_id: str = None,
                 batch_size: int = 1):
        self.queue = queue
        self.runner = model_runner
        self.worker_id = worker_id or f"worker-{int(time.time())}"
        self.batch_size = batch_size

    def run_once(self) -> int:
        tasks = self.queue.dequeue_batch(self.batch_size, self.worker_id)
        if not tasks:
            return 0
        
        processed = 0
        for task in tasks:
            print(f"[{self.worker_id}] Processing task: {task.task_id}")
            result = self.runner.run(task)
            self.queue.submit_result(result)
            processed += 1
            print(f"[{self.worker_id}] Completed task: {task.task_id}")
        
        return processed

    def run_loop(self, poll_interval: float = 1.0, idle_timeout: int = None):
        print(f"[{self.worker_id}] Starting worker loop (batch_size={self.batch_size})...")
        idle_count = 0
        
        while True:
            processed = self.run_once()
            
            if processed > 0:
                idle_count = 0
            else:
                idle_count += 1
                if idle_timeout and idle_count >= idle_timeout:
                    print(f"[{self.worker_id}] Idle timeout reached, exiting...")
                    break
            
            time.sleep(poll_interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-host", default="localhost")
    parser.add_argument("--queue-port", type=int, default=6379)
    parser.add_argument("--model-endpoint", default=None)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--mock", action="store_true", help="Use mock runner")
    parser.add_argument("--mock-response", default="Mock response from worker")
    parser.add_argument("--latency-min", type=float, default=0.0, help="Min latency (seconds)")
    parser.add_argument("--latency-max", type=float, default=0.0, help="Max latency (seconds)")
    parser.add_argument("--batch-size", type=int, default=1, help="Number of tasks to process at once")
    parser.add_argument("--idle-timeout", type=int, default=None, help="Exit after N idle cycles")
    args = parser.parse_args()

    from infra.redis_queue import RedisTaskQueue
    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    
    if args.mock:
        from worker.model_runner import MockRunner
        runner = MockRunner(
            response=args.mock_response,
            latency_min=args.latency_min,
            latency_max=args.latency_max,
        )
    else:
        from worker.model_runner import VLLMRunner
        runner = VLLMRunner(endpoint=args.model_endpoint, api_key=args.api_key)
    
    worker = Worker(queue, runner, batch_size=args.batch_size)
    worker.run_loop(idle_timeout=args.idle_timeout)