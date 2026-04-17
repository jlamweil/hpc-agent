"""Runtime - manages agent lifecycle and resume loop."""
import time
import sys
import importlib.util

hero_dir = "/home/lam/Documents/GAIA/async-hermes-agent"

async_agent_spec = importlib.util.spec_from_file_location(
    "async_agent", f"{hero_dir}/runtime/async_agent.py"
)
async_agent = importlib.util.module_from_spec(async_agent_spec)
sys.modules["runtime.async_agent"] = async_agent
async_agent_spec.loader.exec_module(async_agent)
AsyncAIAgent = async_agent.AsyncAIAgent

task_queue_spec = importlib.util.spec_from_file_location(
    "task_queue", f"{hero_dir}/runtime/task_queue.py"
)
task_queue = importlib.util.module_from_spec(task_queue_spec)
sys.modules["runtime.task_queue"] = task_queue
task_queue_spec.loader.exec_module(task_queue)
TaskQueue = task_queue.TaskQueue
TaskStatus = task_queue.TaskStatus

state_store_spec = importlib.util.spec_from_file_location(
    "state_store", f"{hero_dir}/runtime/state_store.py"
)
state_store = importlib.util.module_from_spec(state_store_spec)
sys.modules["runtime.state_store"] = state_store
state_store_spec.loader.exec_module(state_store)
StateStore = state_store.StateStore
AgentStatus = state_store.AgentStatus


class Runtime:
    def __init__(self, queue: TaskQueue, state_store: StateStore, model_runner=None,
                 max_concurrent_agents: int = 10, max_running_tasks: int = 20):
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
        # Keep agent in _agents for history

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
        """Run with event-driven resume via Redis Pub/Sub."""
        agent, result = self.run_agent(user_message)
        agent, result = self.run_agent(user_message)
        
        if result.status != AgentStatus.WAITING:
            return agent, result
        
        task_id = result.wait_task_id
        print(f"[Runtime] Agent {agent.agent_id} enqueued task: {task_id}")
        
        # Set up callback for this specific task
        completed = {"flag": False}
        
        def on_complete(tid):
            if tid == task_id:
                completed["flag"] = True
        
        self.queue.on_task_completed(task_id, on_complete)
        
        # Wait for completion
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

    runtime = Runtime(queue, state_store, runner)
    
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


if __name__ == "__main__":
    main()