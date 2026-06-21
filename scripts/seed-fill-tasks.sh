#!/bin/bash
# Seed fill tasks into the queue — run once, they cycle forever.
# Fill tasks are permanent residents: the HermesWorker samples from them
# during idle cycles and never modifies the DB row (stays pending forever).
set -euo pipefail

HPC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DB="${1:-$HPC_DIR/tasks.db}"

FILL_TYPES=("handoff" "frontier-agent" "frontier-improve" "lesson-mining"
            "_stress-test" "_spec-gap" "_data-explore" "_cross-ref" "_warmup")

for type in "${FILL_TYPES[@]}"; do
    echo "Seeding $type fill task..."
    python3 "$HPC_DIR/hpc_batch.py" --db "$DB" submit-fill --type "$type"
done

echo ""
echo "Fill tasks seeded. Verify:"
echo "  python3 hpc_batch.py --db $DB count --status pending --type fill"
echo "  python3 hpc_batch.py --db $DB status"
