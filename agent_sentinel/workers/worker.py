"""
Stateless Worker for Agent-Sentinel.

Each worker instance:
  1. Discovers which Raft node is currently the gRPC leader.
  2. Polls the leader for a PENDING task via AcquireTask.
  3. Starts a background heartbeat thread to keep the lease alive.
  4. Executes the 3-step agent pipeline (SEARCH → SUMMARIZE → SAVE),
     committing a checkpoint after every step.
  5. On completion, marks the task COMPLETED and loops back to polling.

All agent state lives in the Raft cluster — the worker itself is stateless.
Running N copies of this file (with different --worker-id flags) scales
throughput linearly with no coordination between workers.

Usage:
    python -m agent_sentinel.workers.worker --worker-id w1
"""

import argparse
import logging
import threading
import time

import grpc

from agent_sentinel.config import (
    GRPC_PORT_BASE,
    HEARTBEAT_INTERVAL_SECONDS,
    NODES,
)
from agent_sentinel.grpc_layer import sentinel_pb2, sentinel_pb2_grpc
from agent_sentinel.workers.checkpoint import (
    AgentState,
    deserialize,
    mark_step_completed,
    mark_step_in_progress,
    serialize,
    should_skip_step,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Seconds to wait between polls when no task is available
POLL_INTERVAL_SECONDS = 3

# gRPC addresses for all nodes (one per node, same index as NODES)
GRPC_ADDRESSES = [f"localhost:{GRPC_PORT_BASE + i}" for i in range(len(NODES))]


class Worker:
    """
    Stateless task worker. All persistent state lives in the Raft cluster.
    """

    def __init__(self, worker_id: str):
        self._worker_id = worker_id
        self._channel: grpc.Channel | None = None
        self._stub: sentinel_pb2_grpc.OrchestratorStub | None = None

    # ─── Public entry point ──────────────────────────────────────────────────

    def run(self) -> None:
        """Main loop: find leader → poll → execute → repeat."""
        logger.info("Worker %s starting.", self._worker_id)
        self._find_leader()

        while True:
            lease = self._poll_for_task()
            if lease is None:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            self._execute_task(lease)

    # ─── Leader discovery ────────────────────────────────────────────────────

    def _find_leader(self) -> None:
        """
        Try each gRPC address in round-robin until one responds to AcquireTask
        without returning NOT_LEADER. Caches the working channel + stub.

        Called once at startup and again whenever any RPC returns NOT_LEADER
        or raises a network error.
        """
        logger.info("Worker %s scanning for gRPC leader...", self._worker_id)
        while True:
            for address in GRPC_ADDRESSES:
                try:
                    channel = grpc.insecure_channel(address)
                    stub = sentinel_pb2_grpc.OrchestratorStub(channel)
                    # A lightweight probe: ask for a task. If the node isn't
                    # the leader it will abort with FAILED_PRECONDITION.
                    stub.AcquireTask(
                        sentinel_pb2.WorkerInfo(worker_id=self._worker_id),
                        timeout=2,
                    )
                    # No exception → this node is the leader (or no tasks yet)
                    self._channel = channel
                    self._stub = stub
                    logger.info("Worker %s connected to leader at %s", self._worker_id, address)
                    return
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        # This node explicitly told us it's not the leader
                        logger.debug("Node %s is not leader, trying next.", address)
                    else:
                        # Network error or node is down
                        logger.debug("Node %s unreachable (%s), trying next.", address, e.code())
                    try:
                        channel.close()
                    except Exception:
                        pass

            logger.warning(
                "Worker %s: no leader found, retrying in 3s...", self._worker_id
            )
            time.sleep(3)

    # ─── Polling ─────────────────────────────────────────────────────────────

    def _poll_for_task(self) -> sentinel_pb2.TaskLease | None:
        """
        Call AcquireTask on the leader.
        Returns TaskLease if a task was assigned, None if no PENDING task exists.
        Re-discovers leader on NOT_LEADER or network error.
        """
        try:
            lease = self._stub.AcquireTask(
                sentinel_pb2.WorkerInfo(worker_id=self._worker_id),
                timeout=5,
            )
            if not lease.task_id:
                logger.debug("Worker %s: no PENDING tasks.", self._worker_id)
                return None
            logger.info(
                "Worker %s acquired task=%s token=%d",
                self._worker_id, lease.task_id, lease.version_token,
            )
            return lease

        except grpc.RpcError as e:
            logger.warning(
                "Worker %s: AcquireTask failed (%s), re-finding leader.",
                self._worker_id, e.code(),
            )
            self._find_leader()
            return None

    # ─── Task execution ──────────────────────────────────────────────────────

    def _execute_task(self, lease: sentinel_pb2.TaskLease) -> None:
        """
        Run the full agent pipeline for the acquired task.
        Starts a heartbeat thread first, then runs each step in order.
        """
        task_id = lease.task_id
        version_token = lease.version_token

        # Deserialize checkpoint — fresh state if this is the first attempt
        state = deserialize(lease.json_state, task_id)
        logger.info(
            "Worker %s starting task=%s from step=%s (index=%d)",
            self._worker_id, task_id, state.current_step, state.step_index,
        )

        # ── Heartbeat thread setup ────────────────────────────────────────
        stop_event = threading.Event()
        stale_event = threading.Event()  # set by heartbeat thread if lease lost

        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(task_id, version_token, stop_event, stale_event),
            daemon=True,
        )
        heartbeat_thread.start()

        # ── Execute each step ─────────────────────────────────────────────
        try:
            for step_name in ["SEARCH", "SUMMARIZE", "SAVE"]:
                if stale_event.is_set():
                    logger.warning(
                        "Worker %s: lease lost mid-task=%s, abandoning.",
                        self._worker_id, task_id,
                    )
                    return

                if should_skip_step(state, step_name):
                    logger.info(
                        "Worker %s: skipping %s (already COMPLETED)", self._worker_id, step_name
                    )
                    continue

                # Mark IN_PROGRESS and commit so a crash is visible
                mark_step_in_progress(state, step_name)
                if not self._commit(state, lease):
                    return  # commit failed → stale or network error, abandon

                # Run the step
                result_fields = self._run_step(state, step_name)
                if result_fields is None:
                    logger.warning(
                        "Worker %s: step %s failed, abandoning task=%s",
                        self._worker_id, step_name, task_id,
                    )
                    return

                # Mark COMPLETED and commit
                mark_step_completed(state, step_name, result_fields)
                state.last_checkpoint_at = time.time()
                if not self._commit(state, lease):
                    return

            logger.info(
                "Worker %s: task=%s COMPLETED successfully.", self._worker_id, task_id
            )

        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=2)

    # ─── Step dispatcher ─────────────────────────────────────────────────────

    def _run_step(self, state: AgentState, step_name: str) -> dict | None:
        """
        Dispatch to the appropriate step implementation.
        Returns a dict of result_fields to store in tool_results, or None on failure.
        Phase 4 will replace these stubs with real LangGraph nodes.
        """
        if step_name == "SEARCH":
            return self._step_search(state)
        elif step_name == "SUMMARIZE":
            return self._step_summarize(state)
        elif step_name == "SAVE":
            return self._step_save(state)
        else:
            logger.error("Worker %s: unknown step %s", self._worker_id, step_name)
            return None

    # ─── Step stubs (replaced by LangGraph in Phase 4) ───────────────────────

    def _step_search(self, state: AgentState) -> dict:
        logger.info("Worker %s: executing SEARCH (idempotency_key=%s)",
                    self._worker_id, state.idempotency_key)
        time.sleep(1)  # simulate network call
        return {
            "query": f"query for {state.task_id}",
            "raw_results": [{"title": "Result 1"}, {"title": "Result 2"}],
        }

    def _step_summarize(self, state: AgentState) -> dict:
        logger.info("Worker %s: executing SUMMARIZE (idempotency_key=%s)",
                    self._worker_id, state.idempotency_key)
        search_results = state.tool_results.get("SEARCH", {}).get("raw_results", [])
        time.sleep(1)  # simulate LLM call
        return {
            "input_text": str(search_results),
            "summary": f"Summary of {len(search_results)} results for task {state.task_id}",
        }

    def _step_save(self, state: AgentState) -> dict:
        logger.info("Worker %s: executing SAVE (idempotency_key=%s)",
                    self._worker_id, state.idempotency_key)
        time.sleep(0.5)  # simulate DB write
        return {
            "destination": "results_db",
            "idempotency_key": state.idempotency_key,
            "response": {"saved": True},
        }

    # ─── Heartbeat loop ───────────────────────────────────────────────────────

    def _heartbeat_loop(
        self,
        task_id: str,
        version_token: int,
        stop_event: threading.Event,
        stale_event: threading.Event,
    ) -> None:
        """
        Background thread: sends SendHeartbeat every HEARTBEAT_INTERVAL_SECONDS.
        Sets stale_event if the leader rejects the heartbeat (stale token)
        so the main thread knows to abandon the task.
        """
        while not stop_event.wait(timeout=HEARTBEAT_INTERVAL_SECONDS):
            try:
                ack = self._stub.SendHeartbeat(
                    sentinel_pb2.LeaseToken(
                        task_id=task_id,
                        worker_id=self._worker_id,
                        version_token=version_token,
                    ),
                    timeout=5,
                )
                if ack.success:
                    logger.debug(
                        "Worker %s: heartbeat ok task=%s", self._worker_id, task_id
                    )
                else:
                    logger.warning(
                        "Worker %s: heartbeat rejected task=%s (%s) — lease lost.",
                        self._worker_id, task_id, ack.message,
                    )
                    stale_event.set()
                    return

            except grpc.RpcError as e:
                logger.warning(
                    "Worker %s: heartbeat RPC error task=%s (%s)",
                    self._worker_id, task_id, e.code(),
                )
                # Network blip — keep trying; main thread will notice if task
                # gets reassigned when it next calls CommitState

    # ─── Commit ───────────────────────────────────────────────────────────────

    def _commit(self, state: AgentState, lease: sentinel_pb2.TaskLease) -> bool:
        """
        Commit the current AgentState to the Raft cluster via CommitState.
        Returns True on success, False if the commit was rejected (stale token
        or network error) — caller should abandon the task.
        """
        try:
            ack = self._stub.CommitState(
                sentinel_pb2.StateUpdate(
                    task_id=lease.task_id,
                    worker_id=self._worker_id,
                    version_token=lease.version_token,
                    json_state=serialize(state),
                ),
                timeout=5,
            )
            if ack.success:
                logger.debug(
                    "Worker %s: committed state task=%s", self._worker_id, lease.task_id
                )
                return True
            else:
                logger.warning(
                    "Worker %s: CommitState rejected task=%s (%s)",
                    self._worker_id, lease.task_id, ack.message,
                )
                return False

        except grpc.RpcError as e:
            logger.warning(
                "Worker %s: CommitState RPC error task=%s (%s), re-finding leader.",
                self._worker_id, lease.task_id, e.code(),
            )
            self._find_leader()
            return False


# ─── CLI entry point ─────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start an Agent-Sentinel worker.")
    parser.add_argument(
        "--worker-id",
        required=True,
        help="Unique identifier for this worker (e.g. w1, w2).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    worker = Worker(worker_id=args.worker_id)
    worker.run()


if __name__ == "__main__":
    main()
