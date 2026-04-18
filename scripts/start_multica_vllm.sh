#!/bin/bash
set -e

HPC_HOST="${HPC_HOST:-user@login.hpc.edu}"
SSH_KEY="${SSH_KEY:-}"
VLLM_PORT="${VLLM_PORT:-8000}"
LOCAL_PORT="${LOCAL_PORT:-8000}"
HERMES_PATH="${HERMES_PATH:-/home/lam/Documents/GAIA/async-hermes-agent/hermes}"
MODEL_NAME="${MODEL_NAME:-llama-7b}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_tunnel() { lsof -i :${LOCAL_PORT} -sTCP:LISTEN >/dev/null 2>&1; }
check_vllm() { curl -s http://localhost:${LOCAL_PORT}/v1/models >/dev/null 2>&1; }

do_stop() {
    log_info "Stopping..."
    pkill -f "autossh.*${LOCAL_PORT}:localhost:${LOCAL_PORT}" 2>/dev/null || true
    multica daemon stop 2>/dev/null || true
    log_info "Done."
}

do_status() {
    echo "=== Tunnel ==="
    check_tunnel && echo "  ${GREEN}Active${NC}" || echo "  ${RED}Not running${NC}"
    echo "=== vLLM ==="
    if check_vllm; then
        echo "  ${GREEN}OK${NC} at http://localhost:${LOCAL_PORT}/v1"
        curl -s http://localhost:${LOCAL_PORT}/v1/models | python3 -c "import sys,json; d=json.load(sys.stdin); print('  Models:', ', '.join(m['id'] for m in d.get('data',[])))"
    else
        echo "  ${RED}Not responding${NC}"
    fi
    echo "=== Multica ==="
    multica daemon status 2>/dev/null || echo "Not running"
}

do_test() {
    log_info "Testing vLLM via Hermes..."
    cat > /tmp/test_vllm.py << EOF
import sys
sys.path.insert(0, "${HERMES_PATH%/hermes}")
from run_agent import AIAgent

agent = AIAgent(
    base_url="http://localhost:${LOCAL_PORT}/v1",
    model="${MODEL_NAME}",
    provider="custom",
    api_key="sk-dummy"
)
print(f"Response: {agent.chat('Say test')}")
EOF
    python3 /tmp/test_vllm.py
}

do_start() {
    log_info "Starting Multica + vLLM..."
    
    log_info "SSH tunnel..."
    if ! check_tunnel; then
        autossh -M 0 -N -L ${LOCAL_PORT}:localhost:${LOCAL_PORT} ${SSH_KEY:+-i $SSH_KEY} ${HPC_HOST} &
        sleep 3
    fi
    
    log_info "Waiting for vLLM..."
    for i in {1..30}; do
        check_vllm && break
        sleep 1
    done
    check_vllM || { log_error "vLLM not responding. Run HPC job first."; exit 1; }
    
    log_info "Starting Multica..."
    multica daemon stop 2>/dev/null || true
    sleep 1
    
    export MULTICA_HERMES_PATH="${HERMES_PATH}"
    export MULTICA_HERMES_MODEL="${MODEL_NAME}"
    export OPENAI_BASE_URL="http://localhost:${LOCAL_PORT}/v1"
    export OPENAI_API_KEY="sk-dummy"
    
    multica daemon start
    sleep 3
    
    log_info "Verify:"
    multica runtime list | grep -q "hermes" && log_info "Hermes OK!" || log_warn "Hermes not found"
    log_info "Done. Run --status to check."
}

case "${1:-}" in
    --stop|-s) do_stop ;;
    --status) do_status ;;
    --test|-t) do_test ;;
    *) do_start ;;
esac