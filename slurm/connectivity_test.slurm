#!/bin/bash
#SBATCH --job-name=connect-test
#SBATCH --output=connect-test-%j.out
#SBATCH --error=connect-test-%j.err
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --mem=1G
#SBATCH --partition=publicgpu

set -e

echo "=== Testing connectivity FROM compute TO various targets ==="

# Test 1: Login node internal IP (should work - internal network)
echo "--- Test 1: Login node 192.168.3.220:22 ---"
timeout 5 nc -zv 192.168.3.220 22 2>&1 && echo "SUCCESS" || echo "FAIL"

# Test 2: Can compute node reach login node on a random high port?
# Let's test SSH first, then try a different approach
echo "--- Test 2: Can we SSH from compute to login? ---"
timeout 5 ssh -o StrictHostKeyChecking=no -o BatchMode=yes 192.168.3.220 "echo OK" 2>&1 || echo "SSH from compute to login not configured"

# Test 3: Try using the internal hostname
echo "--- Test 3: Login node hostname ---"
timeout 5 nc -zv hpc-login 22 2>&1 && echo "SUCCESS via hostname" || echo "FAIL"

echo "Test complete"