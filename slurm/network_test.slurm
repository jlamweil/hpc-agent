#!/bin/bash
#SBATCH --job-name=network-test
#SBATCH --output=network-test-%j.out
#SBATCH --error=network-test-%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=1G
#SBATCH --partition=publicgpu

set -e

echo "=== Network Connectivity Test ==="

# Test various targets
echo "--- Testing internal IP (login node) ---"
timeout 5 nc -zv 192.168.3.220 22 2>&1 && echo "SSH port OK" || echo "SSH port FAIL"

echo "--- Testing localhost ---"
timeout 5 nc -zv localhost 6379 2>&1 || echo "localhost:6379 FAIL"

echo "--- Testing 127.0.0.1 ---"
timeout 5 nc -zv 127.0.0.1 6379 2>&1 || echo "127.0.0.1:6379 FAIL"

echo "Test complete"