"""
gRPC server lifecycle manager for Agent-Sentinel.

Starts and stops the gRPC server dynamically based on Raft leadership:
  - When this node becomes leader  → start gRPC server on GRPC_PORT_BASE + node_id
  - When this node loses leadership → stop gRPC server gracefully

The gRPC server is intentionally only active on the leader so that workers
always talk to the node that owns the replicated state writes.
"""

import logging
import time
from concurrent import futures

import grpc

from agent_sentinel.config import GRPC_PORT_BASE
from agent_sentinel.grpc_layer.sentinel_pb2_grpc import add_OrchestratorServicer_to_server
from agent_sentinel.grpc_layer.servicer import SentinelServicer

logger = logging.getLogger(__name__)

# How long to wait for in-flight RPCs to complete on graceful shutdown (seconds)
_GRACEFUL_SHUTDOWN_SECONDS = 5


class GrpcServerManager:
    """
    Manages the lifecycle of the gRPC server on a Raft node.

    Intended to be called from the server.py status loop:

        manager = GrpcServerManager(node, node_id=0)

        # in status loop:
        manager.sync()   ← starts or stops gRPC based on current leadership
    """

    def __init__(self, node, node_id: int):
        """
        Args:
            node:    ControlPlaneNode — the live Raft node
            node_id: int — used to compute the gRPC port (GRPC_PORT_BASE + node_id)
        """
        self._node = node
        self._node_id = node_id
        self._port = GRPC_PORT_BASE + node_id
        self._server: grpc.Server | None = None

    # ─── Public API ──────────────────────────────────────────────────────────

    def sync(self) -> None:
        """
        Call this periodically from the status loop.
        Starts gRPC if this node just became leader.
        Stops gRPC if this node lost leadership.
        """
        if self._node.is_leader() and not self._is_running():
            self._start()
        elif not self._node.is_leader() and self._is_running():
            self._stop()

    def stop(self) -> None:
        """Unconditionally stop the gRPC server (called on process shutdown)."""
        if self._is_running():
            self._stop()

    @property
    def port(self) -> int:
        return self._port

    @property
    def is_running(self) -> bool:
        return self._is_running()

    # ─── Internal ────────────────────────────────────────────────────────────

    def _is_running(self) -> bool:
        return self._server is not None

    def _start(self) -> None:
        """Start the gRPC server and bind to the node's gRPC port."""
        logger.info(
            "Node %d became LEADER — starting gRPC server on port %d",
            self._node_id, self._port,
        )

        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                # Max message size 10MB — enough for large checkpoints
                ("grpc.max_receive_message_length", 10 * 1024 * 1024),
                ("grpc.max_send_message_length", 10 * 1024 * 1024),
            ],
        )

        # Register our servicer implementation
        add_OrchestratorServicer_to_server(
            SentinelServicer(self._node),
            server,
        )

        # Bind to port — insecure for local dev (Phase 5 can add TLS)
        address = f"[::]:{self._port}"
        server.add_insecure_port(address)
        server.start()

        self._server = server
        logger.info("gRPC server listening on %s", address)

    def _stop(self) -> None:
        """Stop the gRPC server, waiting for in-flight RPCs to finish."""
        logger.info(
            "Node %d lost leadership — stopping gRPC server on port %d",
            self._node_id, self._port,
        )
        if self._server is not None:
            self._server.stop(grace=_GRACEFUL_SHUTDOWN_SECONDS)
            self._server = None
            logger.info("gRPC server stopped.")
