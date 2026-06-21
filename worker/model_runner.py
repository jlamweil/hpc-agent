"""Model runner - calls LLM (vLLM, OpenAI, etc.) or mocks for testing."""
import time
import random
import sys
import os
from pathlib import Path

# model_runner.py is at: <project_root>/hpc-agent/worker/model_runner.py
_WORKER_DIR = os.path.dirname(os.path.abspath(__file__))          # …/hpc-agent/worker/
_HPC_DIR = os.path.dirname(_WORKER_DIR)                           # …/hpc-agent/

from hermes_runtime.task_queue import LLMTask, TaskResult, TaskStatus


class ModelRunner:
    # True if this runner type needs a vLLM endpoint (and should
    # trigger the circuit breaker when vLLM is unavailable).
    needs_vllm = False

    def __init__(self, endpoint: str, api_key: str = None):
        self.endpoint = endpoint
        self.api_key = api_key

    def run(self, task: LLMTask) -> TaskResult:
        raise NotImplementedError


class MockRunner(ModelRunner):
    """Mock runner for testing - returns fixed response with optional latency."""
    
    def __init__(self, response: str = "Mock response from worker",
                 latency_min: float = 0.0, latency_max: float = 0.0):
        super().__init__(endpoint="mock://", api_key=None)
        self.response = response
        self.latency_min = latency_min
        self.latency_max = latency_max

    def run(self, task: LLMTask, **kwargs) -> TaskResult:
        # Simulate latency
        if self.latency_max > 0:
            delay = random.uniform(self.latency_min, self.latency_max)
            time.sleep(delay)
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            content=self.response,
            tool_calls=None,
            finish_reason="stop",
        )


class VLLMRunner(ModelRunner):
    needs_vllm = True
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0
    
    def __init__(self, endpoint: str, api_key: str = None, model: str = None):
        super().__init__(endpoint, api_key)
        self.model = model
    
    def run(self, task: LLMTask) -> TaskResult:
        import requests
        last_error = None
        
        model_name = self.model or task.model
        
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.endpoint}/v1/chat/completions",
                    json={
                        "model": model_name,
                        "messages": task.messages,
                        "tools": task.tools,
                    },
                    headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=data["choices"][0]["message"].get("content"),
                    tool_calls=data["choices"][0]["message"].get("tool_calls"),
                    usage=data.get("usage"),
                )
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            error=str(last_error),
        )


class OllamaRunner(VLLMRunner):
    def __init__(self, endpoint: str = "http://localhost:11434", model: str = "llama3:8b"):
        super().__init__(endpoint=endpoint, api_key=None, model=model)
        self._api_base = endpoint.rstrip("/")
    
    def run(self, task: LLMTask) -> TaskResult:
        import requests
        last_error = None
        
        model_name = self.model
        
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self._api_base}/v1/chat/completions",
                    json={
                        "model": model_name,
                        "messages": task.messages,
                        "temperature": 0.7,
                        "max_tokens": 2048,
                    },
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
                
                content = data["choices"][0]["message"].get("content", "")
                
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=content,
                    tool_calls=data["choices"][0]["message"].get("tool_calls"),
                    usage=data.get("usage"),
                )
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (attempt + 1))
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            error=str(last_error),
        )


class HermesRunner(ModelRunner):
    """Runs tasks through Hermes Agent's AIAgent Python API — no subprocess."""
    needs_vllm = True

    def __init__(
        self,
        base_url: str | None = None,
        model: str = "Qwen/Qwen2.5-72B-Instruct",
        timeout: int = 3600,  # 1h default — P6-A preemption handles scheduling
        tunnel_port: int = 8100,
    ):
        # None → use tunnel (default endpoint). Explicit → skip tunnel.
        if base_url is None:
            super().__init__(endpoint="http://localhost:8000/v1", api_key=None)
            self._tunnel_established = False
        else:
            super().__init__(endpoint=base_url, api_key=None)
            self._tunnel_established = True
        self.model = model
        self.timeout = timeout
        self.tunnel_port = tunnel_port

    def _ensure_vllm_endpoint(self) -> str:
        """Establish SSH tunnel to HPC vLLM. Returns tunneled base_url or raises."""
        import sys
        print("[HermesRunner] _ensure_vllm_endpoint: looking for HPC job...",
              file=sys.stderr, flush=True)
        from worker.tunnel import find_vllm_node, ensure_tunnel
        from hpc_batch import _get_hpc_config

        hpc = _get_hpc_config()
        ssh_host = f"{hpc['user']}@{hpc['host']}"
        print(f"[HermesRunner] HPC config: user={hpc['user']} host={hpc['host']}",
              file=sys.stderr, flush=True)

        node = find_vllm_node(ssh_host)
        if not node:
            print("[HermesRunner] No running SLURM job found",
                  file=sys.stderr, flush=True)
            raise RuntimeError(
                "No running SLURM job found. Auto-dispatch may be starting one."
            )
        print(f"[HermesRunner] Found compute node: {node}", file=sys.stderr, flush=True)

        port = ensure_tunnel(ssh_host, node, self.tunnel_port)
        if port is None:
            print(f"[HermesRunner] Tunnel creation FAILED to {node}:{self.tunnel_port}",
                  file=sys.stderr, flush=True)
            raise RuntimeError("Failed to create SSH tunnel to compute node.")

        url = f"http://localhost:{port}/v1"
        print(f"[HermesRunner] Tunnel UP: {url}", file=sys.stderr, flush=True)
        return url

    def _extract_prompt(self, task: LLMTask) -> str:
        """Extract a single string prompt from task.messages or task.prompt."""
        if task.messages:
            parts = [m.get("content", "") for m in task.messages if m.get("content")]
            return "\n".join(parts)
        return getattr(task, "prompt", "")

    def run(self, task: LLMTask, conversation_history: list | None = None,
            fill_task_id: str | None = None,
            preempt_check=None,
            db_path: str | None = None) -> TaskResult:
        # preempt_check is an optional callable: () -> bool.
        # Used by P6-A priority preemption. Not type-annotated to avoid
        # import issues across Python versions.
        # db_path is the SQLite DB path for session persistence on timeout.
        # If None, defaults to hpc_batch.DEFAULT_DB.
        import sys
        task_id_short = task.task_id[:16] if task.task_id else "?"
        print(f"[HermesRunner] run(task={task_id_short}...)",
              file=sys.stderr, flush=True)

        # Establish tunnel if no explicit base_url was given
        effective_base = self.endpoint
        if not self._tunnel_established:
            try:
                tunneled = self._ensure_vllm_endpoint()
            except RuntimeError as e:
                print(f"[HermesRunner] Tunnel setup failed for {task_id_short}: {e}",
                      file=sys.stderr, flush=True)
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    error=str(e),
                )
            if tunneled:
                effective_base = tunneled
                self.endpoint = tunneled  # Persist for subsequent calls
                self._tunnel_established = True

        prompt = self._extract_prompt(task)
        print(f"[HermesRunner] Task {task_id_short} prompt: {prompt[:80]}...",
              file=sys.stderr, flush=True)

        from run_agent import AIAgent
        agent = AIAgent(
            base_url=effective_base,
            model=self.model,
        )
        session_id = agent.session_id  # Capture before thread starts (for session recovery on timeout)
        # AIAgent's provider config (~/.hermes/config.yaml) silently overrides
        # the base_url we passed.  Force it back so it uses our tunnel endpoint.
        agent.base_url = effective_base
        if hasattr(agent, '_client_kwargs') and isinstance(agent._client_kwargs, dict):
            agent._client_kwargs['base_url'] = effective_base
        # Recreate the primary client with the correct endpoint
        if hasattr(agent, '_create_openai_client') and hasattr(agent, '_client_kwargs'):
            try:
                agent.client = agent._create_openai_client(agent._client_kwargs, reason="tunnel_override", shared=True)
            except Exception as e:
                print(f"[HermesRunner] Client recreate failed (non-fatal): {e}",
                      file=sys.stderr, flush=True)
        print(f"[HermesRunner] AIAgent forced to base_url={agent.base_url}",
              file=sys.stderr, flush=True)

        import threading, queue as _queue
        result_q = _queue.Queue(maxsize=1)

        def _run_agent():
            """Run agent in a daemon thread so we can enforce timeout.
            
            Daemon thread dies with the process — resources (HTTP connections,
            file handles) are released on process exit. This is a pragmatic
            workaround; the real fix belongs in the AIAgent's HTTP stack
            (cancellable requests / per-call read timeout).
            """
            # Set fill context in THIS thread (same thread as submit_task calls)
            if fill_task_id:
                import hpc_batch as _hb
                _hb._fill_context.task_id = fill_task_id
            try:
                # P7+P8: forward conversation_history so retries resume from saved state
                result = agent.run_conversation(
                    prompt,
                    conversation_history=conversation_history,
                )
                result_q.put(result)
            except Exception as e:
                result_q.put(e)
            finally:
                if fill_task_id:
                    import hpc_batch as _hb
                    _hb._fill_context.task_id = None
                # Best-effort cleanup for normal completion
                try:
                    agent.client.close()
                except Exception:
                    pass

        # Support per-task timeout override via metadata (P2 fix).
        # HermesWorker task metadata can carry 'runner_timeout' to allow
        # complex tasks (e.g., handoff, multi-project analysis) more time.
        effective_timeout = self.timeout
        if task and hasattr(task, 'metadata') and isinstance(task.metadata, dict):
            task_timeout = task.metadata.get('runner_timeout')
            if task_timeout is not None:
                effective_timeout = int(task_timeout)
                print(f"[HermesRunner] Using per-task timeout: {effective_timeout}s (default: {self.timeout}s)",
                      file=sys.stderr, flush=True)

        t = threading.Thread(target=_run_agent, daemon=True)
        t.start()

        # Preemption loop setup
        def _save_session() -> int:
            """Try to save session messages from the session file on disk.
            Returns number of messages saved (0 if none)."""
            try:
                import json, time as _time
                _sp = Path.home() / ".hermes" / "sessions" / f"session_{session_id}.json"
                for _ in range(5):
                    if _sp.exists():
                        try:
                            _sdata = json.loads(_sp.read_text())
                            _msgs = _sdata.get("messages", [])
                            if _msgs:
                                import hpc_batch as _hb
                                _conn = _hb.init_db(db_path or str(_hb.DEFAULT_DB))
                                _meta = dict(task.metadata)
                                _meta['saved_conversation'] = _msgs
                                _conn.execute(
                                    "UPDATE tasks SET metadata = ? WHERE id = ?",
                                    (json.dumps(_meta), task.task_id),
                                )
                                _conn.commit()
                                _conn.close()
                                print(f"[HermesRunner] Saved {len(_msgs)} messages from session {session_id} for retry",
                                      file=sys.stderr, flush=True)
                                return len(_msgs)
                        except (json.JSONDecodeError, OSError):
                            pass
                    _time.sleep(0.2)
            except Exception as e:
                print(f"[HermesRunner] Session save failed for {task_id_short}: {e}",
                      file=sys.stderr, flush=True)
            return 0

        # P6-A: Preemption support. Instead of a single join(timeout), poll
        # in 10s intervals so the daemon can check for higher-priority tasks.
        # If preempt_check signals, save session (P7+P8) and return FAILED.
        # The preempted task gets retried via _recover_recoverable_failed().
        preempted = False
        _PREEMPT_POLL = 10.0
        elapsed = 0.0
        while elapsed < effective_timeout:
            t.join(timeout=min(_PREEMPT_POLL, effective_timeout - elapsed))
            elapsed += _PREEMPT_POLL
            if not t.is_alive():
                break  # Thread finished normally
            # Check for preemption (only when caller provided a check)
            if preempt_check is not None:
                try:
                    if preempt_check():
                        print(f"[HermesRunner] PREEMPTING {task_id_short} for higher-priority task",
                              file=sys.stderr, flush=True)
                        preempted = True
                        break
                except Exception:
                    pass  # Non-critical — continue if check fails

        if t.is_alive():
            # Thread still running — abandon (timeout or preempted).
            if preempted:
                reason = f"Preempted by higher-priority task after {elapsed:.0f}s"
                print(f"[HermesRunner] {reason} for {task_id_short}",
                      file=sys.stderr, flush=True)
            else:
                reason = f"HermesRunner timeout after {effective_timeout}s"
                print(f"[HermesRunner] TIMEOUT after {effective_timeout}s for {task_id_short}",
                      file=sys.stderr, flush=True)

            # P7+P8: Try to save session messages from the session file on disk.
            saved_count = _save_session()
            if saved_count:
                print(f"[HermesRunner] Saved {saved_count} messages from session {session_id} for retry",
                      file=sys.stderr, flush=True)

            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                content=None,
                finish_reason="preempted" if preempted else "timeout",
                error=reason,
            )

        # Thread finished — get result
        try:
            response = result_q.get_nowait()
        except _queue.Empty:
            print(f"[HermesRunner] AIAgent error for {task_id_short}: empty result",
                  file=sys.stderr, flush=True)
            _save_session()
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error="AIAgent returned no result",
            )

        if isinstance(response, Exception):
            print(f"[HermesRunner] AIAgent error for {task_id_short}: {response}",
                  file=sys.stderr, flush=True)
            _save_session()
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=f"AIAgent error: {response}",
            )

        # run_conversation returns dict with "final_response" (not "content")
        if isinstance(response, dict):
            content = (response.get("final_response") or response.get("content") or "")
        else:
            content = str(response)
        print(f"[HermesRunner] Task {task_id_short} DONE ({len(content)} chars)",
              file=sys.stderr, flush=True)

        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.COMPLETED,
            content=content or "",
            finish_reason="stop",
        )


class TransformersRunner(ModelRunner):
    MAX_RETRIES = 2
    
    def __init__(self, model_name: str = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"):
        super().__init__(endpoint="transformers://", api_key=None)
        self.model_name = model_name
        self._model = None
        self._tokenizer = None
    
    def _load_model(self):
        if self._model is None:
            import torch
            from transformers import AutoTokenizer, AutoModelForCausalLM
            
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self._model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16,
                device_map="auto"
            )
    
    def run(self, task: LLMTask) -> TaskResult:
        import torch
        
        last_error = None
        
        for attempt in range(self.MAX_RETRIES):
            try:
                self._load_model()
                
                user_message = ""
                if task.messages:
                    for msg in task.messages:
                        if msg.get("role") == "user":
                            user_message = msg.get("content", "")
                            break
                
                if not user_message and task.messages:
                    user_message = str(task.messages[-1].get("content", ""))
                
                prompt = user_message or "Say OK"
                
                inputs = self._tokenizer(prompt, return_tensors="pt").to(self._model.device)
                outputs = self._model.generate(
                    **inputs,
                    max_new_tokens=256,
                    do_sample=True,
                    temperature=0.7,
                )
                response = self._tokenizer.decode(outputs[0], skip_special_tokens=True)
                
                if response.startswith(prompt):
                    response = response[len(prompt):].strip()
                
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.COMPLETED,
                    content=response,
                    tool_calls=None,
                    finish_reason="stop",
                )
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(1)
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            error=str(last_error),
        )