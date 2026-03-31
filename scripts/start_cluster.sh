#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start_cluster.sh
# Prints the commands to start all 3 Raft nodes in separate terminals.
# ─────────────────────────────────────────────────────────────────────────────

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$REPO_ROOT/.venv/bin/python"

if [ ! -f "$PYTHON" ]; then
  echo "ERROR: virtualenv not found at $REPO_ROOT/.venv"
  echo "Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       Agent-Sentinel — Start 3-Node Raft Cluster         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Open 3 separate terminals and run one command in each:"
echo ""
echo "  Terminal 1 (Node 0 — localhost:4321):"
echo "    cd $REPO_ROOT && $PYTHON -m agent_sentinel.control_plane.server --node 0"
echo ""
echo "  Terminal 2 (Node 1 — localhost:4322):"
echo "    cd $REPO_ROOT && $PYTHON -m agent_sentinel.control_plane.server --node 1"
echo ""
echo "  Terminal 3 (Node 2 — localhost:4323):"
echo "    cd $REPO_ROOT && $PYTHON -m agent_sentinel.control_plane.server --node 2"
echo ""
echo "Wait ~3s for leader election, then run the fault tolerance test:"
echo "    cd $REPO_ROOT && $PYTHON scripts/fault_tolerance_test.py"
echo ""
