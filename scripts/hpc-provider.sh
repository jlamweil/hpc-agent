#!/bin/bash
# Start the HPC opencode provider API server in a tmux session.
# This exposes the HPC vLLM as an OpenAI-compatible endpoint on port 9876.
# Configured in ~/.config/opencode/opencode.json as provider "hpc".
#
# Usage:
#   ./scripts/hpc-provider.sh start    — start server in tmux session
#   ./scripts/hpc-provider.sh stop     — stop server
#   ./scripts/hpc-provider.sh status   — check if running
#   ./scripts/hpc-provider.sh logs     — follow logs

SESSION="hpc-provider"
PORT="${HPC_PROVIDER_PORT:-9876}"
DB_PATH="${HPC_PROVIDER_DB:-$HOME/hpc-agent/tasks.db}"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-status}" in
  start)
    echo "Starting HPC provider API server on port $PORT..."
    echo "  DB: $DB_PATH"
    echo "  Session: $SESSION"
    tmux new-session -d -s "$SESSION" \
      "python3 \"$SCRIPT_DIR/opencode_provider.py\" --serve --port $PORT --db \"$DB_PATH\" 2>&1 | tee -a /tmp/hpc-provider.log"
    sleep 1
    if tmux has-session -t "$SESSION" 2>/dev/null; then
      echo "  Started. Use 'scripts/hpc-provider.sh logs' to see output."
      curl -s --max-time 2 "http://127.0.0.1:$PORT/v1/models" > /dev/null 2>&1 \
        && echo "  API responding at http://127.0.0.1:$PORT/v1" \
        || echo "  WARN: API not responding yet (check logs)."
    else
      echo "  FAILED to start."
    fi
    ;;
  stop)
    echo "Stopping HPC provider server..."
    tmux send-keys -t "$SESSION" C-c 2>/dev/null || true
    sleep 1
    tmux kill-session -t "$SESSION" 2>/dev/null || true
    echo "  Stopped."
    ;;
  status)
    if tmux has-session -t "$SESSION" 2>/dev/null; then
      echo "Running. Port $PORT."
      curl -s --max-time 2 "http://127.0.0.1:$PORT/v1/models" > /dev/null 2>&1 \
        && echo "API responding." \
        || echo "API not responding (model may be loading)."
    else
      echo "Not running."
    fi
    ;;
  logs)
    tmux attach -t "$SESSION" 2>/dev/null || tail -f /tmp/hpc-provider.log
    ;;
  *)
    echo "Usage: $0 {start|stop|status|logs}"
    exit 1
    ;;
esac
