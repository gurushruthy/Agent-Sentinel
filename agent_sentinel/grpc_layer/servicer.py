"""
OrchestratorServicer — implements the 3 gRPC RPCs defined in sentinel.proto.

All write RPCs enforce two invariants:
  1. Leader-only: if this node is not the Raft leader, return NOT_LEADER status
     with the current leader's address so the worker can redirect.
  2. Fencing: version_token in the request must match the stored token exactly.
     Stale workers are rejected immediately.
"""

import json
import logging

import grpc

from agent_sentinel.grpc_layer.sentinel_pb2 import (
    Acknowledgment,
    TaskLease,
)
from agent_sentinel.grpc_layer.sentinel_pb2_grpc import OrchestratorServicer
from agent_sentinel.control_plane.registry import StaleTokenError

logger = logging.getLogger(__name__)


class SentinelServicer(OrchestratorServicer):
    """
    Concrete implementation of the Orchestrator gRPC service.

    Receives a reference to the live ControlPlaneNode so it can call
    registry methods directly (in-process, no extra network hop).
    """

    def __init__(self, node):
        """
        Args:
            node: ControlPlaneNode — the live Raft node this servicer runs on.
        """
        self._node = node

    # ─── Helper ──────────────────────────────────────────────────────────────

    def _not_leader(self, context: grpc.ServicerContext) -> str:
        """
        Abort the RPC with NOT_LEADER status and return the leader address.
        Calling code should return immediately after calling this.
        """
        leader = self._node.get_leader() or "unknown"
        context.abort(
            grpc.StatusCode.FAILED_PRECONDITION,
            f"NOT_LEADER. Current leader: {leader}",
        )
        return leader

    # ─── RPC implementations ─────────────────────────────────────────────────

    def AddTask(self, request, context: grpc.ServicerContext) -> Acknowledgment:
        """
        Client submits a new task to the cluster.

        Flow:
          1. Check this node is the leader — abort with NOT_LEADER if not.
          2. Parse metadata_json (defaults to empty dict if blank).
          3. Call registry.add_task() — creates PENDING record, replicates via Raft.
          4. Return Acknowledgment(success=True) on success.
             Return Acknowledgment(success=False) if task_id already exists.
        """
        if not self._node.is_leader():
            self._not_leader(context)
            return Acknowledgment(success=False)

        task_id = request.task_id
        try:
            metadata = json.loads(request.metadata_json) if request.metadata_json else {}
        except json.JSONDecodeError as e:
            return Acknowledgment(success=False, message=f"invalid metadata_json: {e}")

        try:
            self._node.registry.add_task(task_id, metadata)
            logger.info("Task %s added by client", task_id)
            return Acknowledgment(success=True, message=f"task {task_id} added")
        except ValueError as e:
            return Acknowledgment(success=False, message=str(e))

    def AcquireTask(self, request, context: grpc.ServicerContext) -> TaskLease:
        """
        Worker asks for a new task and a Lease.

        Flow:
          1. Check this node is the leader — abort with NOT_LEADER if not.
          2. Find the oldest PENDING task.
          3. Call registry.acquire_task() — increments version_token, sets lease.
          4. Return TaskLease with task_id, version_token, checkpoint, expiry.
             If no PENDING task exists, return TaskLease(task_id="") — worker polls again.
        """
        if not self._node.is_leader():
            self._not_leader(context)
            return TaskLease()

        worker_id = request.worker_id
        logger.debug("AcquireTask from worker=%s", worker_id)

        pending = self._node.registry.get_pending_task()
        if pending is None:
            logger.debug("No PENDING tasks available for worker=%s", worker_id)
            return TaskLease(task_id="")

        try:
            updated = self._node.registry.acquire_task(
                task_id=pending["task_id"],
                worker_id=worker_id,
            )
        except (KeyError, ValueError) as e:
            # Task was acquired by another worker between get_pending and acquire
            logger.warning("acquire_task race for worker=%s: %s", worker_id, e)
            return TaskLease(task_id="")

        logger.info(
            "Task %s acquired by worker=%s (token=%d)",
            updated["task_id"], worker_id, updated["version_token"],
        )

        return TaskLease(
            task_id=updated["task_id"],
            version_token=updated["version_token"],
            json_state=updated.get("checkpoint_json", ""),
            expires_at=int(updated["lease_expires_at"]),
        )

    def SendHeartbeat(self, request, context: grpc.ServicerContext) -> Acknowledgment:
        """
        Worker sends a heartbeat to keep the Lease alive.

        Flow:
          1. Check this node is the leader.
          2. Call registry.renew_lease() — extends lease_expires_at.
          3. Return Acknowledgment(success=True) on success.
          4. Return Acknowledgment(success=False) on stale token or missing task.
        """
        if not self._node.is_leader():
            self._not_leader(context)
            return Acknowledgment(success=False)

        task_id = request.task_id
        worker_id = request.worker_id
        version_token = request.version_token
        logger.debug(
            "SendHeartbeat task=%s worker=%s token=%d",
            task_id, worker_id, version_token,
        )

        try:
            self._node.registry.renew_lease(
                task_id=task_id,
                worker_id=worker_id,
                version_token=version_token,
            )
            return Acknowledgment(success=True, message="lease renewed")

        except StaleTokenError as e:
            logger.warning("Stale heartbeat rejected: %s", e)
            return Acknowledgment(success=False, message=str(e))

        except KeyError as e:
            logger.warning("Heartbeat for unknown task: %s", e)
            return Acknowledgment(success=False, message=str(e))

    def CommitState(self, request, context: grpc.ServicerContext) -> Acknowledgment:
        """
        Worker commits a checkpoint (serialized AgentState JSON).

        Flow:
          1. Check this node is the leader.
          2. Call registry.commit_state() — fencing check, persists checkpoint,
             renews lease. Replicated to all Raft nodes via @replicated.
          3. Return Acknowledgment(success=True) on success.
          4. Return Acknowledgment(success=False) on stale token or missing task.
        """
        if not self._node.is_leader():
            self._not_leader(context)
            return Acknowledgment(success=False)

        task_id = request.task_id
        worker_id = request.worker_id
        version_token = request.version_token
        json_state = request.json_state

        logger.debug(
            "CommitState task=%s worker=%s token=%d state_len=%d",
            task_id, worker_id, version_token, len(json_state),
        )

        try:
            self._node.registry.commit_state(
                task_id=task_id,
                worker_id=worker_id,
                version_token=version_token,
                checkpoint_json=json_state,
            )

            # If the final checkpoint marks all steps completed, finalize task.
            try:
                state = json.loads(json_state) if json_state else {}
                tool_results = state.get("tool_results", {})
                all_done = all(
                    tool_results.get(step, {}).get("status") == "COMPLETED"
                    for step in ("SEARCH", "SUMMARIZE", "SAVE")
                )
            except Exception:
                all_done = False

            if all_done:
                self._node.registry.complete_task(
                    task_id=task_id,
                    worker_id=worker_id,
                    version_token=version_token,
                )
                logger.info(
                    "Task %s marked COMPLETED by worker=%s token=%d",
                    task_id, worker_id, version_token,
                )

            logger.info(
                "Checkpoint committed task=%s worker=%s token=%d",
                task_id, worker_id, version_token,
            )
            return Acknowledgment(success=True, message="checkpoint committed")

        except StaleTokenError as e:
            logger.warning("Stale CommitState rejected: %s", e)
            return Acknowledgment(success=False, message=str(e))

        except KeyError as e:
            logger.warning("CommitState for unknown task: %s", e)
            return Acknowledgment(success=False, message=str(e))

        except ValueError as e:
            logger.warning("CommitState validation failed: %s", e)
            return Acknowledgment(success=False, message=str(e))
