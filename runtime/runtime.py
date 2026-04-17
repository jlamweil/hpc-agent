"""Runtime - manages agent lifecycle and resume loop."""
import time
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime import AsyncAIAgent, TaskQueue, StateStore, AgentStatus, TaskStatus, AgentState


class Runtime:
    def __init__(self, queue: TaskQueue, state_store: StateStore, model_runner=None):
        self.queue = queue
        self.state_store = state_store
        self.model_runner = model_runner
        self._processed_tasks = set()

    def run_agent(self, user_message: str, agent_id: str = None):
        agent = AsyncAIAgent(
            task_queue=self.queue,
            state_store=self.state_store,
            agent_id=agent_id,
            max_iterations=5,
        )
        result = agent.run_conversation(user_message)
        return agent, result

    def resume_agent(self, task_id: str):
        result = self.queue.get_result_nowait(task_id)
        if result is None:
            return None, None
        
        if result.status != TaskStatus.COMPLETED:
            return None, None
        
        state = self.state_store.load_by_task(task_id)
        if state is None:
            return None, None
        
        agent = AsyncAIAgent(
            task_queue=self.queue,
            state_store=self.state_store,
            agent_id=state.agent_id,
            max_iterations=5,
        )
        
        resume_result = agent.run_conversation(resume_task_id=task_id)
        return agent, resume_result

    def run_full_cycle(self, user_message: str, poll_interval: float = 1.0, timeout: float = 60.0):
        agent, result = self.run_agent(user_message)
        
        if result.status != AgentStatus.WAITING:
            return agent, result
        
        task_id = result.wait_task_id
        print(f"[Runtime] Agent enqueued task: {task_id}")
        
        start = time.time()
        while time.time() - start < timeout:
            result = agent.poll_for_result(task_id)
            if result is not None:
                print(f"[Runtime] Task {task_id} completed, status: {result.status.value}")
                return agent, result
            
            time.sleep(poll_interval)
        
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
    args = parser.parse_args()

    from infra.redis_queue import RedisTaskQueue
    from infra.redis_state_store import RedisStateStore

    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    state_store = RedisStateStore(host=args.state_host, port=args.state_port)

    runner = None
    if args.mock:
        from worker.model_runner import MockRunner
        runner = MockRunner(response=args.mock_response)

    runtime = Runtime(queue, state_store, runner)
    agent, result = runtime.run_full_cycle(
        args.message,
        poll_interval=args.poll_interval,
        timeout=args.timeout
    )

    print(f"\n=== RESULT ===")
    print(f"Status: {result.status.value}")
    print(f"Final response: {result.final_response}")
    print(f"Messages: {len(result.messages)}")
    print(f"API calls: {result.api_calls}")


if __name__ == "__main__":
    main()