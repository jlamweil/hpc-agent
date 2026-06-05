#!/usr/bin/env python3
"""
promptfoo custom provider for HPC batch system.
Wraps hpc_batch.py submit-and-wait — submits prompt to Slurm queue,
blocks until HPC processes it, returns result.

Usage in promptfooconfig.yaml:
  providers:
    - id: exec:python /path/to/hpc_provider.py
      label: hpc-dsv4-flash
      config:
        model: deepseek-ai/DeepSeek-V4-Flash   # model on HPC
        timeout: 86400                          # max wait in seconds
        poll_interval: 60                       # check interval

Interface (called by promptfoo):
  argv[1] = prompt text
  argv[2] = options JSON (config)
  argv[3] = context JSON

Output: JSON with {"output": "..."} to stdout
"""

import json
import os
import subprocess
import sys
import time

HPC_BATCH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hpc_batch.py")


def call_api(prompt: str, options: dict) -> dict:
    """Submit prompt to HPC queue, wait for result, return output."""
    model = options.get("model", "deepseek-ai/DeepSeek-V4-Flash")
    timeout = options.get("timeout", 86400)
    poll_interval = options.get("poll_interval", 60)

    # Create a temp file for the prompt to avoid shell escaping issues
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        f.write(prompt)
        prompt_file = f.name

    try:
        result = subprocess.run(
            [
                sys.executable,
                HPC_BATCH,
                "--model", model,
                "submit-and-wait",
                "--prompt-file", prompt_file,
                "--poll", str(poll_interval),
                "--timeout", str(timeout),
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 60,
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip() or "HPC batch failed"
            return {"output": f"[HPC ERROR] {error_msg}"}

        # Parse JSON output from submit-and-wait
        stdout = result.stdout.strip()
        try:
            data = json.loads(stdout)
            content = data.get("result", {}).get("content", "")
            return {"output": content}
        except json.JSONDecodeError:
            return {"output": stdout}

    except subprocess.TimeoutExpired:
        return {"output": "[HPC TIMEOUT] Job did not complete within the time limit"}
    except Exception as e:
        return {"output": f"[HPC ERROR] {str(e)}"}
    finally:
        try:
            os.unlink(prompt_file)
        except OSError:
            pass


if __name__ == "__main__":
    # Called by promptfoo's exec: provider
    # argv[1] = prompt, argv[2] = options JSON, argv[3] = context JSON
    prompt = sys.argv[1] if len(sys.argv) > 1 else ""
    options = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    # context = json.loads(sys.argv[3]) if len(sys.argv) > 3 else {}

    result = call_api(prompt, options)
    print(json.dumps(result))
