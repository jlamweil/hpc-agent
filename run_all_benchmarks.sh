#!/bin/bash
echo "=== HPC Agent Benchmark Suite ==="
echo ""

# Clear database
python -c "import sqlite3; conn = sqlite3.connect('tasks.db'); cursor = conn.cursor(); cursor.execute('DELETE FROM tasks'); conn.commit(); conn.close(); print('DB cleared')"
sleep 1

# Benchmark 1: Throughput test - Qwen2.5
echo "=== Benchmark 1: Throughput (Qwen2.5-Coder-32B) ==="
python benchmarks/throughput_test.py Qwen2.5-Coder-32B "1,2,4" 2>&1
echo ""

# Clear for next test
python -c "import sqlite3; conn = sqlite3.connect('tasks.db'); cursor = conn.cursor(); cursor.execute('DELETE FROM tasks'); conn.commit(); conn.close(); print('DB cleared')"
sleep 1

# Benchmark 2: Correctness
echo "=== Benchmark 2: Correctness (Qwen2.5-Coder-32B) ==="
python benchmarks/correctness_test.py Qwen2.5-Coder-32B 10 2>&1
echo ""

# Clear for next test
python -c "import sqlite3; conn = sqlite3.connect('tasks.db'); cursor = conn.cursor(); cursor.execute('DELETE FROM tasks'); conn.commit(); conn.close(); print('DB cleared')"
sleep 1

# Benchmark 3: Fairness
echo "=== Benchmark 3: Fairness (Qwen2.5-Coder-32B) ==="
python benchmarks/fairness_test.py Qwen2.5-Coder-32B 10 30 2>&1
echo ""

# Clear for next test
python -c "import sqlite3; conn = sqlite3.connect('tasks.db'); cursor = conn.cursor(); cursor.execute('DELETE FROM tasks'); conn.commit(); conn.close(); print('DB cleared')"
sleep 1

# Benchmark 4: Recovery
echo "=== Benchmark 4: Recovery (Qwen2.5-Coder-32B) ==="
python benchmarks/recovery_test.py Qwen2.5-Coder-32B 2>&1
echo ""

# Benchmark 5: Throughput - DeepSeek
echo "=== Benchmark 5: Throughput (DeepSeek-Coder-V2-16B) ==="
python -c "import sqlite3; conn = sqlite3.connect('tasks.db'); cursor = conn.cursor(); cursor.execute('DELETE FROM tasks'); conn.commit(); conn.close(); print('DB cleared')"
sleep 1
python benchmarks/throughput_test.py DeepSeek-Coder-V2-16B "1,2,4,8" 2>&1
echo ""

echo "=== All Benchmarks Complete ==="
