"""Worker - pulls tasks from queue and processes tasks."""
import time
import sys
import os
import importlib.util


def _setup_paths():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hero = os.path.dirname(script_dir)
    async_hermes = os.path.join(os.path.dirname(hero), "async-hermes-agent")
    
    task_queue_path = f"{async_hermes}/runtime/task_queue.py"
    spec = importlib.util.spec_from_file_location(
        "runtime_task_queue",
        task_queue_path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["runtime.task_queue"] = module
    spec.loader.exec_module(module)
    
    return module.TaskQueue, module.TaskStatus


TaskQueue, TaskStatus = _setup_paths()


class Worker:
    def __init__(self, queue, model_runner, worker_id: str = None,
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
    import importlib.util
    
    def load_module(path, name):
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hero = os.path.dirname(script_dir)
    async_hermes = os.path.join(os.path.dirname(hero), "async-hermes-agent")
    
    rtq = load_module(f"{async_hermes}/runtime/task_queue.py", "task_queue")
    irq = load_module(f"{hero}/infra/redis_queue.py", "redis_queue")
    RedisTaskQueue = irq.RedisTaskQueue
    
    # Load worker.model_runner
    wmr = load_module(f"{hero}/worker/model_runner.py", "model_runner")
    MockRunner = wmr.MockRunner
    OllamaRunner = wmr.OllamaRunner
    VLLMRunner = wmr.VLLMRunner
    
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
    parser.add_argument("--ollama", action="store_true", help="Use Ollama runner")
    parser.add_argument("--ollama-model", default="llama3:8b", help="Ollama model name")
    parser.add_argument("--ollama-endpoint", default="http://localhost:11434", help="Ollama endpoint")
    parser.add_argument("--vllm", action="store_true", help="Use vLLM runner")
    parser.add_argument("--vllm-endpoint", default="http://localhost:8000", help="vLLM endpoint")
    parser.add_argument("--vllm-model", default="meta-llama/Llama-3.2-1B-Instruct", help="vLLM model name")
    parser.add_argument("--transformers", action="store_true", help="Use HuggingFace transformers runner")
    parser.add_argument("--transformers-model", default="TinyLlama/TinyLlama-1.1B-Chat-v1.0", help="Transformers model name")
    args = parser.parse_args()

    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    
    if args.mock:
        runner = MockRunner(
            response=args.mock_response,
            latency_min=args.latency_min,
            latency_max=args.latency_max,
        )
    elif args.ollama:
        runner = OllamaRunner(endpoint=args.ollama_endpoint, model=args.ollama_model)
    elif args.vllm:
        runner = VLLMRunner(endpoint=args.vllm_endpoint, model=args.vllm_model)
    elif args.transformers:
        runner = TransformersRunner(model_name=args.transformers_model)
    else:
        runner = VLLMRunner(endpoint=args.model_endpoint, api_key=args.api_key)
    
    worker = Worker(queue, runner, batch_size=args.batch_size)
    worker.run_loop(idle_timeout=args.idle_timeout)