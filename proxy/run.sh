#!/bin/bash
# Supervisor script for Fetch Proxy Server.
# Restarts automatically on crash.

while true; do
    python3 -m proxy.server --poll 5 \
        --request-dir "$HOME/hpc-agent/fetch-requests" \
        --response-dir "$HOME/hpc-agent/fetch-responses"
    echo "Proxy crashed at $(date), restarting..." >> proxy.log
    sleep 2
done
