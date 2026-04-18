"""Runtime - manages agent lifecycle and resume loop."""
import time
import sys
import os
import logging

logger = logging.getLogger(__name__)

HERO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASYNC_HERMES = os.path.join(os.path.dirname(HERO), "async-hermes-agent")
sys.path.insert(0, ASYNC_HERMES)
sys.path.insert(0, os.path.join(HERO, "worker"))

from runtime import AsyncAIAgent, TaskQueue, StateStore, AgentStatus, TaskStatus


class Runtime:
    def __init__(self, queue: TaskQueue, state_store: StateStore, model_runner=None,
                 max_concurrent_agents: int = 10, max_running_tasks: int = 20,
                 use_slurm: bool = False, slurm_workers: int = 1, **slurm_kwargs):
        self.queue = queue
        self.state_store = state_store
        self.model_runner = model_runner
        self._processed_tasks = set()
        self._agents = {}
        self._active_agents = {}
        self.max_concurrent_agents = max_concurrent_agents
        self.max_running_tasks = max_running_tasks
        self._last_backpressure_check = 0
        self._backpressure_threshold = 0.9
        
        self.use_slurm = use_slurm
        self._slurm_backend = None
        self._target_workers = slurm_workers
        
        if use_slurm:
            self._init_slurm_backend(**slurm_kwargs)
    
    def _init_slurm_backend(self, **kwargs):
        from worker.slurm_backend import SlurmBackend, is_slurm_available
        
        if not is_slurm_available():
            raise RuntimeError("Slurm is not available on this system")
        
        self._slurm_backend = SlurmBackend(**kwargs)
        
        self._slurm_backend.scale_to(self._target_workers)
        logger.info(f"Started {self._target_workers} Slurm workers")
    
    def _scale_workers_if_needed(self):
        if not self._slurm_backend:
            return
        
        active_workers = self._slurm_backend.get_active_count()
        
        if self._active_agents > active_workers:
            self._slurm_backend.scale_to(self._active_agents)
            logger.info(f"Scaled to {self._active_agents} workers")
    
    def get_worker_count(self) -> int:
        if self._slurm_backend:
            return self._slurm_backend.get_active_count()
        return 0
    
    def shutdown(self):
        if self._slurm_backend:
            logger.info("Cleaning up Slurm workers...")
            self._slurm_backend.cleanup()
        
    def _check_backpressure(self) -> bool:
        queue_size = self.queue.get_queue_size()
        tasks_at_capacity = queue_size >= (self.max_running_tasks * self._backpressure_threshold)
        
        agents_at_capacity = len(self._active_agents) >= self.max_concurrent_agents
        
        return tasks_at_capacity or agents_at_capacity
    
    def _wait_for_capacity(self, timeout: float = 30.0) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            if not self._check_backpressure():
                return True
            time.sleep(0.5)
        return False

    def run_agent(self, user_message: str, agent_id: str = None, allow_wait: bool = True):
        if self._check_backpressure():
            if not self._wait_for_capacity():
                raise RuntimeError("Backpressure: no capacity available")
        
        agent = AsyncAIAgent(
            task_queue=self.queue,
            state_store=self.state_store,
            agent_id=agent_id,
            max_iterations=5,
        )
        self._agents[agent.agent_id] = agent
        result = agent.run_conversation(user_message)
        
        if result.status == AgentStatus.WAITING:
            self._active_agents[agent.agent_id] = result.wait_task_id
        elif allow_wait and result.status not in (AgentStatus.COMPLETED, AgentStatus.FAILED):
            self._active_agents[agent.agent_id] = None
        
        return agent, result
    
    def get_active_count(self) -> int:
        return len(self._active_agents)
    
    def cleanup_agent(self, agent_id: str) -> None:
        if agent_id in self._active_agents:
            del self._active_agents[agent_id]

    def resume_agent(self, task_id: str):
        result = self.queue.get_result_nowait(task_id)
        if result is None:
            return None, None
        
        if result.status != TaskStatus.COMPLETED:
            return None, None
        
        state = self.state_store.load_by_task(task_id)
        if state is None:
            return None, None
        
        agent_id = state.agent_id
        if agent_id not in self._agents:
            self._agents[agent_id] = AsyncAIAgent(
                task_queue=self.queue,
                state_store=self.state_store,
                agent_id=agent_id,
                max_iterations=5,
            )
        
        agent = self._agents[agent_id]
        resume_result = agent.run_conversation("", resume_task_id=task_id)
        return agent, resume_result

    def run_full_cycle(self, user_message: str, poll_interval: float = 1.0, timeout: float = 60.0):
        agent, result = self.run_agent(user_message)
        
        if result.status != AgentStatus.WAITING:
            return agent, result
        
        task_id = result.wait_task_id
        print(f"[Runtime] Agent {agent.agent_id} enqueued task: {task_id}")
        
        start = time.time()
        while time.time() - start < timeout:
            result = agent.poll_for_result(task_id)
            if result is not None:
                print(f"[Runtime] Task {task_id} completed, status: {result.status.value}")
                return agent, result
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Timeout waiting for task {task_id}")

    def run_event_driven(self, user_message: str, timeout: float = 60.0):
        agent, result = self.run_agent(user_message)
        
        if result.status != AgentStatus.WAITING:
            return agent, result
        
        task_id = result.wait_task_id
        print(f"[Runtime] Agent {agent.agent_id} enqueued task: {task_id}")
        
        completed = {"flag": False}
        
        def on_complete(tid):
            if tid == task_id:
                completed["flag"] = True
        
        self.queue.on_task_completed(task_id, on_complete)
        
        start = time.time()
        while time.time() - start < timeout:
            if completed["flag"]:
                result = agent.poll_for_result(task_id)
                if result:
                    print(f"[Runtime] Task {task_id} completed via event")
                    return agent, result
            time.sleep(0.1)
        
        raise TimeoutError(f"Timeout waiting for task {task_id}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-host", default="localhost")
    parser.add_argument("--queue-port", type=int, default=6379)
    parser.add_argument("--state-host", default="localhost")
    parser.add_argument("--state-port", type=int, default=6379)
    parser.add_argument("--message", default="Hello")
    parser.add_argument("--mock", action="store_true", help="Use mock runner")
    parser.add_argument("--mock-response", default="Mock response from worker")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--event-driven", action="store_true", help="Use event-driven mode")
    parser.add_argument("--ollama", action="store_true", help="Use Ollama runner")
    parser.add_argument("--ollama-model", default="llama3:8b", help="Ollama model name")
    parser.add_argument("--ollama-endpoint", default="http://localhost:11434", help="Ollama endpoint")
    parser.add_argument("--use-slurm", action="store_true", help="Use Slurm workers")
    parser.add_argument("--slurm-workers", type=int, default=1, help="Number of Slurm workers")
    parser.add_argument("--slurm-script", default=None, help="Path to Slurm script")
    args = parser.parse_args()

    from infra.redis_queue import RedisTaskQueue
    from infra.redis_state_store import RedisStateStore

    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    state_store = RedisStateStore(host=args.state_host, port=args.state_port)

    runner = None
    if args.mock:
        from worker.model_runner import MockRunner
        runner = MockRunner(response=args.mock_response)
    elif args.ollama:
        from worker.model_runner import OllamaRunner
        runner = OllamaRunner(endpoint=args.ollama_endpoint, model=args.ollama_model)

    slurm_kwargs = {
        "script_path": args.slurm_script,
        "queue_host": args.queue_host,
        "queue_port": args.queue_port,
    }
    
    runtime = Runtime(
        queue, state_store, runner,
        use_slurm=args.use_slurm,
        slurm_workers=args.slurm_workers,
        **slurm_kwargs
    )
    
    try:
        if args.event_driven:
            agent, result = runtime.run_event_driven(
                args.message,
                timeout=args.timeout
            )
        else:
            agent, result = runtime.run_full_cycle(
                args.message,
                poll_interval=args.poll_interval,
                timeout=args.timeout
            )

        print("\n=== RESULT ===")
        print(f"Status: {result.status.value}")
        print(f"Final response: {result.final_response}")
        print(f"Messages: {len(result.messages)}")
        print(f"API calls: {result.api_calls}")
    finally:
        runtime.shutdown()


if __name__ == "__main__":
    main()