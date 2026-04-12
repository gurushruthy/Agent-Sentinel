import logging
import os
import threading
import time
import weakref

from pysyncobj import SyncObj, SyncObjConf

from agent_sentinel.config import (
    NODES,
    LEASE_SWEEP_INTERVAL_SECONDS,
)
from agent_sentinel.control_plane.registry import TaskRegistry

logger = logging.getLogger(__name__)

# Lease sweep threads are kept here, not on ControlPlaneNode.__dict__. Attributes
# added after SyncObj.__init__ are included in pysyncobj log-compaction pickle;
# pickling a Thread pulls in the bound method and the full SyncObj (journal
# file handles), which breaks serialization.
_lease_sweep_threads: dict[int, threading.Thread] = {}


def _lease_sweep_loop_main(node_ref: weakref.ReferenceType["ControlPlaneNode"]) -> None:
    while True:
        time.sleep(LEASE_SWEEP_INTERVAL_SECONDS)
        node = node_ref()
        if node is None:
            return
        if not node.is_leader():
            continue
        try:
            node._expire_orphaned_tasks()
        except Exception:
            logger.exception("Error during lease sweep")


class ControlPlaneNode(SyncObj):
    """
    A single node in the 3-node Raft control plane.

    Responsibilities:
    - Participates in Raft leader election via pysyncobj
    - Hosts the replicated TaskRegistry
    - Runs the LeaseManager sweep on the leader to expire orphaned tasks

    Usage:
        node = ControlPlaneNode(node_id=0)
        node.registry.add_task("task-1", {"description": "do something"})
    """

    def __init__(self, node_id: int):
        self._node_id = node_id
        self_address = NODES[node_id]
        partners = [addr for i, addr in enumerate(NODES) if i != node_id]

        # Keep persistent Raft state inside this repository's data/ directory.
        data_dir = os.path.join(os.path.dirname(__file__), "../../data")
        data_dir = os.path.normpath(data_dir)
        os.makedirs(data_dir, exist_ok=True)

        conf = SyncObjConf(
            # Persist the Raft journal so the node can recover after a crash
            journalFile=os.path.join(data_dir, f"raft_journal_node{node_id}.bin"),
            # Full snapshot file for faster recovery on restart
            fullDump=os.path.join(data_dir, f"raft_dump_node{node_id}.bin"),
            # Print pysyncobj internal logs only at WARNING+ to reduce noise
            logLevel=logging.WARNING,
        )

        self.registry = TaskRegistry()

        super().__init__(self_address, partners, conf=conf, consumers=[self.registry])

        t = threading.Thread(
            target=_lease_sweep_loop_main,
            args=(weakref.ref(self),),
            daemon=True,
            name=f"lease-sweep-node{node_id}",
        )
        _lease_sweep_threads[node_id] = t
        t.start()
        logger.info("ControlPlaneNode %d started at %s", node_id, self_address)

    def destroy(self):
        _lease_sweep_threads.pop(self._node_id, None)
        super().destroy()

    # ─── Convenience accessors ───────────────────────────────────────────────

    def is_leader(self) -> bool:
        """Return True if this node is the current Raft leader."""
        return self._isLeader()

    def get_leader(self) -> str | None:
        """Return the address of the current Raft leader, or None if unknown."""
        leader = self._getLeader()
        return leader.address if leader is not None else None

    # ─── LeaseManager sweep ──────────────────────────────────────────────────

    def _expire_orphaned_tasks(self) -> None:
        now = time.time()
        running_tasks = self.registry.list_tasks(status="RUNNING")
        for task in running_tasks:
            if task["lease_expires_at"] > 0 and now > task["lease_expires_at"]:
                logger.warning(
                    "Lease expired for task %s (worker=%s). Resetting.",
                    task["task_id"],
                    task["worker_id"],
                )
                self.registry.expire_lease(task["task_id"])
