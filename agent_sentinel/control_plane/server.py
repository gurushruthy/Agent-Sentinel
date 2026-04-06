"""
Entry point for a single Raft control plane node.

Usage:
    python -m agent_sentinel.control_plane.server --node 0
    python -m agent_sentinel.control_plane.server --node 1
    python -m agent_sentinel.control_plane.server --node 2
"""

import argparse
import logging
import signal
import sys
import time

from agent_sentinel.config import NODES, GRPC_PORT_BASE
from agent_sentinel.control_plane.node import ControlPlaneNode
from agent_sentinel.grpc_layer.server import GrpcServerManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Status line printed every STATUS_INTERVAL_SECONDS
STATUS_INTERVAL_SECONDS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start an Agent-Sentinel Raft control plane node."
    )
    parser.add_argument(
        "--node",
        type=int,
        required=True,
        choices=[0, 1, 2],
        help="Node index (0, 1, or 2). Determines address from config.NODES.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    node_id = args.node
    self_address = NODES[node_id]

    logger.info("=" * 55)
    logger.info("  Agent-Sentinel Control Plane — Node %d", node_id)
    logger.info("  Address : %s", self_address)
    logger.info("  Partners: %s", [a for i, a in enumerate(NODES) if i != node_id])
    logger.info("=" * 55)

    node = ControlPlaneNode(node_id=node_id)
    grpc_manager = GrpcServerManager(node, node_id=node_id)

    # ── Graceful shutdown on SIGINT / SIGTERM ─────────────────────────────
    def _shutdown(sig, frame):
        logger.info("Node %d shutting down (signal %s)...", node_id, sig)
        grpc_manager.stop()
        node.destroy()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Status loop ───────────────────────────────────────────────────────
    logger.info("Node %d running. Waiting for cluster to form...", node_id)
    while True:
        time.sleep(STATUS_INTERVAL_SECONDS)

        # Start or stop gRPC based on current leadership
        grpc_manager.sync()

        leader = node.get_leader()
        is_me = node.is_leader()
        role = "LEADER" if is_me else "follower"
        task_counts = _task_summary(node)
        grpc_status = f"gRPC=port {GRPC_PORT_BASE + node_id}" if grpc_manager.is_running else "gRPC=off"

        logger.info(
            "[Node %d | %s] leader=%s | %s | tasks: %s",
            node_id,
            role,
            leader or "unknown",
            grpc_status,
            task_counts,
        )


def _task_summary(node: ControlPlaneNode) -> str:
    """Return a compact task count string, e.g. 'PENDING=2 RUNNING=1 COMPLETED=0 FAILED=0'"""
    try:
        all_tasks = node.registry.list_tasks()
        counts = {"PENDING": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0}
        for t in all_tasks:
            status = t.get("status", "UNKNOWN")
            counts[status] = counts.get(status, 0) + 1
        return " ".join(f"{k}={v}" for k, v in counts.items())
    except Exception:
        return "unavailable"


if __name__ == "__main__":
    main()
