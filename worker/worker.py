"""Worker - pulls tasks from queue and processes them."""
import time
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime import TaskQueue, TaskStatus
from worker.model_runner import VLLMRunner


class Worker:
    def __init__(self, queue: TaskQueue, model_runner, worker_id: str = None):
        self.queue = queue
        self.runner = model_runner
        self.worker_id = worker_id or f"worker-{int(time.time())}"

    def run_once(self) -> bool:
        tasks = self.queue.dequeue_batch(1, self.worker_id)
        if not tasks:
            return False
        task = tasks[0]
        result = self.runner.run(task)
        self.queue.submit_result(result)
        return True

    def run_loop(self, poll_interval: float = 1.0):
        print(f"[{self.worker_id}] Starting worker loop...")
        while True:
            if self.run_once():
                print(f"[{self.worker_id}] Processed task")
            time.sleep(poll_interval)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-host", default="localhost")
    parser.add_argument("--queue-port", type=int, default=6379)
    parser.add_argument("--model-endpoint", required=True)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args()

    from infra.redis_queue import RedisTaskQueue
    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    runner = VLLMRunner(endpoint=args.model_endpoint, api_key=args.api_key)
    worker = Worker(queue, runner)
    worker.run_loop()