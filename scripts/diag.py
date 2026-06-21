#!/usr/bin/env python3
"""HPC Hermes system diagnostics — thin CLI wrapper.

Usage:
    python3 scripts/diag.py
    python3 scripts/diag.py --json       # Machine-readable output
    TUNNEL_DEBUG=1 python3 scripts/diag.py  # Verbose tunnel debug

This is a thin wrapper around hpc_batch.cmd_diag_default().
The check functions live in hpc_batch.py — see that file for the
full implementation.
"""
import json
import os
import sys

# Ensure hpc_batch is importable when running as script
_script_dir = os.path.dirname(os.path.abspath(__file__))
_parent = os.path.dirname(_script_dir)
if _parent not in sys.path:
    sys.path.insert(0, _parent)

from hpc_batch import cmd_diag_default


def main():
    use_json = "--json" in sys.argv
    tunnel_debug = os.environ.get("TUNNEL_DEBUG") == "1"
    db_path = os.environ.get("HPC_DB_PATH")

    result = cmd_diag_default(
        db_path=db_path,
        use_json=use_json,
        tunnel_debug=tunnel_debug,
    )

    if use_json:
        print(json.dumps(result, indent=2))
    else:
        print(result)


if __name__ == "__main__":
    main()
