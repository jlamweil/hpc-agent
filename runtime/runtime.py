"""Runtime - manages agent lifecycle and resume loop."""
import time
import sys
sys.path.insert(0, "../async-hermes-agent")
from runtime import AsyncAIAgent, TaskQueue, StateStore, AgentStatus, TaskStatus


class Runtime:
    def __init__(self, queue: TaskQueue, state_store: StateStore, model_endpoint: str, api_key: str = None):
        self.queue = queue
        self.state_store = state_store
        self.model_endpoint = model_endpoint
        self.api_key = api_key

    def run_agent(self, user_message: str):
        agent = AsyncAIAgent(
            task_queue=self.queue,
            state_store=self.state_store,
            max_iterations=90,
            api_config={"base_url": self.model_endpoint, "api_key": self.api_key},
        )
        result = agent.run_conversation(user_message)
        return agent, result

    def poll_loop(self, agent, task_id: str, poll_interval: float = 1.0, timeout: float = 300.0):
        start = time.time()
        while time.time() - start < timeout:
            result = agent.poll_for_result(task_id)
            if result is not None:
                return result
            time.sleep(poll_interval)
        raise TimeoutError(f"Timeout waiting for task {task_id}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-host", default="localhost")
    parser.add_argument("--queue-port", type=int, default=6379)
    parser.add_argument("--state-host", default="localhost")
    parser.add_argument("--state-port", type=int, default=6379)
    parser.add_argument("--model-endpoint", required=True)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--message", required=True)
    args = parser.parse_args()

    from infra.redis_queue import RedisTaskQueue
    from infra.redis_state_store import RedisStateStore

    queue = RedisTaskQueue(host=args.queue_host, port=args.queue_port)
    state_store = RedisStateStore(host=args.state_host, port=args.state_port)

    runtime = Runtime(queue, state_store, args.model_endpoint, args.api_key)
    agent, result = runtime.run_agent(args.message)

    print(f"Agent enqueued task: {result.wait_task_id}")
    print(f"Status: {result.status.value}")

    final = runtime.poll_loop(agent, result.wait_task_id)
    print(f"Final status: {final.status.value}")
    print(f"Response: {final.final_response[:200] if final.final_response else 'None'}")


if __name__ == "__main__":
    main()