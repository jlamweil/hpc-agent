#!/usr/bin/env python3
"""
Opencode LLM provider for HPC batch system.
Routes LLM calls through the BeeGFS task queue to HPC vLLM.
All tool execution stays local — HPC is text generation only.

Usage in opencode.json:
{
  "providers": {
    "hpc": {
      "type": "hpc_agent",
      "config": {
        "db_path": "~/hpc-agent/tasks.db",
        "model": "deepseek-ai/DeepSeek-V4-Flash",
        "poll_interval": 30,
        "timeout": 86400,
        "fallback": {
          "type": "anthropic",
          "api_key_env": "ANTHROPIC_API_KEY",
          "model": "claude-sonnet-4"
        },
        "warmth_policy": "prefer_hpc"
      }
    }
  },
  "subagents": {
    "oracle": { "provider": "hpc" },
    "librarian": { "provider": "hpc", "fallback": "anthropic" }
  }
}
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any


HPC_AGENT_DIR = Path(__file__).resolve().parent


class HpcProvider:
    """Opencode LLM provider that routes calls through HPC task queue.
    
    HPC is text generation only — no tools, no agent loop on the HPC side.
    All tool execution stays local on the subagent's machine.
    """

    def __init__(self, config: dict | None = None):
        config = config or {}
        self.db_path = self._resolve_db_path(config.get("db_path", "tasks.db"))
        self.model = config.get("model", "deepseek-ai/DeepSeek-V4-Flash")
        self.poll_interval = config.get("poll_interval", 30)
        self.timeout = config.get("timeout", 86400)
        self.warmth_policy = config.get("warmth_policy", "prefer_hpc")
        self.fallback_config = config.get("fallback")
        self._hpc_config: dict | None = None

    # ── Public API ───────────────────────────────────────────────────

    def chat_complete(self, messages: list[dict], **kwargs) -> dict[str, Any]:
        """Send messages to HPC vLLM and return response.
        
        Args:
            messages: List of message dicts (same format as OpenAI API).
            **kwargs: Additional options (temperature, max_tokens, etc.)
                      passed through to the model if supported.
        
        Returns:
            Dict with 'content' key containing the model's response text.
            Never raises — errors returned as dict with 'error' key.
        """
        # Check warmth and decide path
        warmth = self._check_warmth()
        
        if warmth == "cold":
            if self.fallback_config:
                return self._fallback_chat(messages, **kwargs)
            if self.warmth_policy == "hpc_only":
                return {"error": "HPC queue is cold (no running job). Cannot submit — policy is hpc_only."}
            # prefer_hpc: submit anyway (will queue and wait)
        
        # hot/warm or prefer_hpc: submit to HPC
        return self._submit_and_wait(messages, **kwargs)

    # ── Warmth Detection ────────────────────────────────────────────

    def _check_warmth(self) -> str:
        """Check if a live vLLM session is available.
        
        Returns:
            'hot':  vLLM loaded and idle-polling (heartbeat < 60s old)
            'warm': SLURM job running but no heartbeat yet
            'cold': No active SLURM job
        """
        # Check BeeGFS heartbeat first (most reliable signal)
        hpc_dir = self._get_hpc_config().get("dir", "~/hpc-agent")
        heartbeat_path = Path(hpc_dir).expanduser() / "proxy-heartbeat.json"
        
        if heartbeat_path.exists():
            try:
                hb = json.loads(heartbeat_path.read_text())
                age = time.time() - hb.get("timestamp", 0)
                if age < 60:
                    return "hot"
                return "warm"
            except (json.JSONDecodeError, OSError):
                pass
        
        # Fallback: check squeue via SSH
        try:
            hpc = self._get_hpc_config()
            result = subprocess.run(
                ["ssh", f"{hpc['user']}@{hpc['host']}",
                 f"squeue -u {hpc['user']} --noheader -o '%T' | head -5"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip():
                states = result.stdout.strip().split("\n")
                if any(s in states for s in ["RUNNING", "CONFIGURING"]):
                    return "warm"
        except Exception:
            pass
        
        return "cold"

    # ── HPC Task Submission ──────────────────────────────────────────

    def _submit_and_wait(self, messages: list[dict], **kwargs) -> dict[str, Any]:
        """Submit messages to HPC and wait for completion."""
        from hpc_batch import submit_task

        task_id = str(uuid.uuid4())
        meta: dict[str, Any] = {
            "type": "agent_call",
            "source": "opencode-provider",
            "hpc_provider": True,
        }
        
        # Extract optional params from kwargs
        temperature = kwargs.get("temperature", 0.7)
        max_tokens = kwargs.get("max_tokens", 4096)
        
        try:
            submit_task(
                db_path=str(self.db_path),
                model=self.model,
                agent_id="opencode-provider",
                metadata=meta,
                task_id=task_id,
                messages=messages,
            )
        except Exception as e:
            return {"error": f"Failed to submit task to HPC queue: {e}"}

        # Poll for completion
        deadline = time.time() + self.timeout
        poll = self.poll_interval
        db_path = str(self.db_path)

        while time.time() < deadline:
            try:
                import sqlite3
                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT status, result, error FROM tasks WHERE id = ?",
                    (task_id,),
                ).fetchone()
                conn.close()

                if row is None:
                    time.sleep(poll)
                    continue

                status = row[0]
                if status == "completed":
                    result_json = row[1]
                    if result_json:
                        try:
                            parsed = json.loads(result_json)
                            content = parsed.get("content", "")
                            usage = parsed.get("usage", {})
                            return {
                                "content": content,
                                "usage": usage,
                            }
                        except json.JSONDecodeError:
                            return {"content": result_json}
                    return {"content": ""}

                elif status == "failed":
                    return {"error": f"HPC task failed: {row[2] or 'unknown error'}"}

            except Exception as e:
                # DB may be locked or unavailable — retry on next poll
                pass

            time.sleep(poll)

        return {"error": f"HPC task timed out after {self.timeout}s"}

    # ── Fallback to API Provider ─────────────────────────────────────

    def _fallback_chat(self, messages: list[dict], **kwargs) -> dict[str, Any]:
        """Fallback to a configured API provider when HPC is cold."""
        if not self.fallback_config:
            return {"error": "HPC is cold and no fallback provider configured"}

        api_key_env = self.fallback_config.get("api_key_env", "ANTHROPIC_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            return {"error": f"Fallback API key not found in env var {api_key_env}"}

        fallback_type = self.fallback_config.get("type", "anthropic")
        fallback_model = self.fallback_config.get("model", "claude-sonnet-4")

        if fallback_type == "anthropic":
            return self._call_anthropic(messages, api_key, fallback_model, **kwargs)
        elif fallback_type == "openai":
            return self._call_openai(messages, api_key, fallback_model, **kwargs)
        else:
            return {"error": f"Unsupported fallback provider type: {fallback_type}"}

    def _call_anthropic(self, messages: list[dict], api_key: str,
                        model: str, **kwargs) -> dict[str, Any]:
        """Call Anthropic API as fallback."""
        try:
            import httpx
        except ImportError:
            return {"error": "httpx required for Anthropic fallback. Install: pip install httpx"}

        try:
            # Extract system message if present (Anthropic handles it separately)
            system = None
            api_messages = []
            for m in messages:
                if m.get("role") == "system":
                    system = m.get("content", "")
                else:
                    api_messages.append(m)

            body: dict[str, Any] = {
                "model": model,
                "messages": api_messages,
                "max_tokens": kwargs.get("max_tokens", 4096),
            }
            if system:
                body["system"] = system
            temperature = kwargs.get("temperature", 0.7)
            if temperature is not None:
                body["temperature"] = temperature

            with httpx.Client(timeout=self.timeout) as client:
                resp = client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            content = ""
            usage = {}
            for block in data.get("content", []):
                if block.get("type") == "text":
                    content += block.get("text", "")
            if "usage" in data:
                usage = data["usage"]
            return {"content": content, "usage": usage, "provider": "anthropic"}

        except Exception as e:
            return {"error": f"Anthropic fallback failed: {e}"}

    def _call_openai(self, messages: list[dict], api_key: str,
                     model: str, **kwargs) -> dict[str, Any]:
        """Call OpenAI-compatible API as fallback."""
        try:
            import httpx
        except ImportError:
            return {"error": "httpx required for OpenAI fallback. Install: pip install httpx"}

        try:
            body: dict[str, Any] = {
                "model": model,
                "messages": messages,
                "max_tokens": kwargs.get("max_tokens", 4096),
            }
            temperature = kwargs.get("temperature", 0.7)
            if temperature is not None:
                body["temperature"] = temperature

            with httpx.Client(timeout=self.timeout) as client:
                resp = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "authorization": f"Bearer {api_key}",
                        "content-type": "application/json",
                    },
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            choice = data.get("choices", [{}])[0]
            content = choice.get("message", {}).get("content", "")
            usage = data.get("usage", {})
            return {"content": content, "usage": usage, "provider": "openai"}

        except Exception as e:
            return {"error": f"OpenAI fallback failed: {e}"}

    # ── Config Helpers ──────────────────────────────────────────────

    def _resolve_db_path(self, path: str) -> Path:
        p = Path(path)
        if not p.is_absolute():
            p = HPC_AGENT_DIR / p
        return p

    def _get_hpc_config(self) -> dict[str, str]:
        if self._hpc_config is None:
            from hpc_batch import cfg
            self._hpc_config = {
                "user": cfg("hpc.user", "jlam"),
                "host": cfg("hpc.host", "hpc-login.u-strasbg.fr"),
                "dir": cfg("hpc.dir", "~/hpc-agent"),
            }
        return self._hpc_config

    # ── Schema (for opencode provider discovery) ─────────────────────

    @classmethod
    def get_schema(cls) -> dict:
        """Return the provider schema for opencode."""
        return {
            "type": "hpc_agent",
            "description": "Route LLM calls through HPC vLLM (DeepSeek V4 Flash). "
                           "All tools stay local — HPC is text generation only.",
            "config": {
                "db_path": {
                    "type": "string",
                    "description": "Path to tasks.db",
                    "default": "tasks.db",
                },
                "model": {
                    "type": "string",
                    "description": "Model on HPC",
                    "default": "deepseek-ai/DeepSeek-V4-Flash",
                },
                "poll_interval": {
                    "type": "integer",
                    "description": "Seconds between queue polls",
                    "default": 30,
                },
                "timeout": {
                    "type": "integer",
                    "description": "Max seconds to wait for HPC response",
                    "default": 86400,
                },
                "warmth_policy": {
                    "type": "string",
                    "enum": ["prefer_hpc", "fallback_on_cold", "hpc_only"],
                    "description": "Behavior when HPC queue is cold",
                    "default": "prefer_hpc",
                },
                "fallback": {
                    "type": "object",
                    "description": "API provider to use when HPC is cold",
                    "properties": {
                        "type": {"type": "string", "enum": ["anthropic", "openai"]},
                        "api_key_env": {"type": "string"},
                        "model": {"type": "string"},
                    },
                },
            },
        }


# ── OpenAI-Compatible API Server ───────────────────────────────────────
# Exposes the HPC provider as an OpenAI-compatible /v1/chat/completions
# endpoint so opencode can use it via @ai-sdk/openai-compatible.

import http.server
import urllib.parse

class _APIHandler(http.server.BaseHTTPRequestHandler):
    provider: HpcProvider | None = None
    server_version = "HPCProvider/1.0"

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/v1/models":
            self._json_response({
                "object": "list",
                "data": [{
                    "id": self.provider.model if self.provider else "deepseek-ai/DeepSeek-V4-Flash",
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "hpc-agent",
                }]
            })
        else:
            self._json_response({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/v1/chat/completions":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            messages = body.get("messages", [])
            model = body.get("model", "")
            temperature = body.get("temperature", 0.7)
            max_tokens = body.get("max_tokens", 4096)

            if not self.provider:
                self._json_response({"error": "provider not initialized"}, 500)
                return

            result = self.provider.chat_complete(
                messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )

            if "error" in result:
                self._json_response({
                    "error": {"message": result["error"], "type": "hpc_error"},
                }, 502)
                return

            openai_response = {
                "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model or self.provider.model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": result.get("content", ""),
                    },
                    "finish_reason": "stop",
                }],
                "usage": result.get("usage", {}),
            }
            self._json_response(openai_response)
        else:
            self._json_response({"error": "not found"}, 404)

    def _json_response(self, data: dict, status: int = 200):
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def log_message(self, fmt: str, *args):
        sys.stderr.write(f"[hpc-provider] {fmt % args}\n")


def serve(port: int = 9876, provider: HpcProvider | None = None,
          db_path: str = "", model: str = "", poll_interval: int = 30):
    """Start the OpenAI-compatible API server.
    
    Usage:
        python3 opencode_provider.py --serve
        python3 opencode_provider.py --serve --port 9876 --model deepseek-ai/DeepSeek-V4-Flash
    """
    _APIHandler.provider = provider or HpcProvider({
        "db_path": db_path or "tasks.db",
        "model": model or "deepseek-ai/DeepSeek-V4-Flash",
        "poll_interval": poll_interval,
    })

    server = http.server.HTTPServer(("127.0.0.1", port), _APIHandler)
    print(f"[hpc-provider] OpenAI-compatible API running on http://127.0.0.1:{port}/v1")
    print(f"[hpc-provider] Model: {_APIHandler.provider.model}")
    print(f"[hpc-provider] DB: {_APIHandler.provider.db_path}")
    print(f"[hpc-provider] Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[hpc-provider] Stopping")
        server.server_close()


# ── CLI entry point (for testing) ───────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HPC opencode provider")
    parser.add_argument("--serve", action="store_true", help="Start OpenAI-compatible API server")
    parser.add_argument("--port", type=int, default=9876, help="API server port (default: 9876)")
    parser.add_argument("--chat", action="store_true", help="Chat completion mode (one-shot)")
    parser.add_argument("--messages", help="JSON messages array")
    parser.add_argument("--config", help="JSON provider config")
    parser.add_argument("--db", help="Path to tasks.db")
    parser.add_argument("--model", help="Model name")
    parser.add_argument("--poll", type=int, default=30, help="Poll interval seconds")
    parser.add_argument("--warmth", action="store_true", help="Check warmth only")
    args = parser.parse_args()

    if args.serve:
        serve(port=args.port, db_path=args.db or "", model=args.model or "",
              poll_interval=args.poll)
        sys.exit(0)

    if args.warmth:
        provider = HpcProvider({})
        print(f"Warmth: {provider._check_warmth()}")
        sys.exit(0)

    if args.chat:
        config = json.loads(args.config) if args.config else {}
        if args.db:
            config["db_path"] = args.db
        if args.model:
            config["model"] = args.model
        if args.poll:
            config["poll_interval"] = args.poll
        messages = json.loads(args.messages) if args.messages else [{"role": "user", "content": "hello"}]
        provider = HpcProvider(config)
        result = provider.chat_complete(messages)
        print(json.dumps(result, indent=2))
        sys.exit(0)

    # Show schema
    provider = HpcProvider({})
    print(json.dumps(provider.get_schema(), indent=2))
