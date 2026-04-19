#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh" 2>/dev/null || true

HPC_HOST="${HPC_HOST:-jlam@hpc-login.u-strasbg.fr}"
SSH_KEY="${SSH_KEY:-}"
LOCAL_PORT="8000"
HERMES_PATH="${HERMES_PATH:-/home/lam/Documents/GAIA/async-hermes-agent/hermes}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_tunnel() { lsof -i :${LOCAL_PORT} -sTCP:LISTEN >/dev/null 2>&1; }

do_stop() {
    log_info "Stopping..."
    pkill -f "ssh.*-L.*${LOCAL_PORT}:localhost:${LOCAL_PORT}" 2>/dev/null || true
    multica daemon stop 2>/dev/null || true
    log_info "Done."
}

do_status() {
    echo "=== SSH Tunnel ==="
    check_tunnel && echo "  ${GREEN}Active on :${LOCAL_PORT}${NC}" || echo "  ${RED}Not running${NC}"
    echo "=== Multica ==="
    multica daemon status 2>/dev/null || echo "Not running"
    echo "=== Runtimes ==="
    multica runtime list 2>/dev/null | grep -E "NAME|hermes|claude|codex" || true
}

do_start() {
    log_info "Starting Multica with Hermes..."
    
    log_info "SSH tunnel for queue results..."
    if ! check_tunnel; then
        ssh -fN -L ${LOCAL_PORT}:localhost:${LOCAL_PORT} ${SSH_KEY:+-i $SSH_KEY} ${HPC_HOST}
        sleep 2
    fi
    
    log_info "Starting Multica daemon..."
    multica daemon stop 2>/dev/null || true
    sleep 1
    
    export MULTICA_HERMES_PATH="${HERMES_PATH}"
    export OPENAI_BASE_URL="http://localhost:11434/v1"
    export OPENAI_API_KEY="sk-dummy"
    
    multica daemon start
    sleep 3
    
    log_info "Verify:"
    multica runtime list | grep -q "hermes" && log_info "Hermes runtime OK!" || log_warn "Hermes not detected"
    log_info "Done."
    log_info ""
    log_info "Flow: Multica → Hermes → SQLite queue → async_queue_monitor → Slurm → HPC → results"
}

case "${1:-}" in
    --stop|-s) do_stop ;;
    --status) do_status ;;
    *) do_start ;;
esac