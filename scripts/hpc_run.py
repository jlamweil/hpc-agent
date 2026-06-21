#!/usr/bin/env python3
"""Run a prompt through Hermes Agent → HPC vLLM.

Goes through Hermes Agent CLI for full tool access. The --direct flag
was removed in a previous cleanup when hpc_gateway.py was deleted —
all prompts now route through Hermes.

Usage:
    python3 scripts/hpc_run.py "Say PROOF"                  # via Hermes
    python3 scripts/hpc_run.py --file /path/to/prompt.txt    # from file
"""
import json
import sys
import subprocess
import argparse
MODEL = "Qwen/Qwen2.5-72B-Instruct"


def via_hermes(prompt: str, system: str | None = None,
               max_tokens: int = 4096) -> dict:
    """Run prompt through Hermes Agent CLI."""
    cmd = [
        sys.executable, "hermes", "chat",
        "--query", prompt,
        "--model", MODEL,
    ]
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        return {"error": f"Hermes exited {result.returncode}: {result.stderr[:200]}"}

    # Parse Hermes output for the final response
    output = result.stdout or result.stderr or ""
    lines = output.split("\n")
    content_lines = []
    in_response = False
    for line in lines:
        if "╭─ ⚕ Hermes" in line:
            in_response = True
            continue
        if "╰─" in line and in_response:
            break
        if in_response and line.strip():
            content_lines.append(line.strip())

    content = "\n".join(content_lines)
    return {"content": content, "provider": "hermes+hpc"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a prompt through Hermes → HPC vLLM")
    parser.add_argument("prompt", nargs="?", help="Prompt text")
    parser.add_argument("--system", help="System prompt")
    parser.add_argument("--file", help="Read prompt from file")
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    if not args.prompt and not args.file:
        parser.print_help()
        sys.exit(1)

    if args.file:
        with open(args.file) as f:
            prompt = f.read()
    else:
        prompt = args.prompt

    result = via_hermes(
        prompt, system=args.system,
        max_tokens=args.max_tokens,
    )

    if args.json:
        print(json.dumps(result, indent=2))
    elif "error" in result:
        print(f"ERROR: {result['error']}", file=sys.stderr)
        sys.exit(1)
    else:
        if result.get("content"):
            print(result["content"].strip())
