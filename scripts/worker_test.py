"""
Phase 3 — Worker Crash & Resume Test

What this script does:
  1. Starts all 3 Raft nodes as subprocesses (any can become leader).
  2. Probes all 3 gRPC ports until a leader responds.
  3. Submits one task via AddTask RPC (like a real client would).
  4. Starts worker-1 — waits until it commits a SEARCH checkpoint.
  5. Kills worker-1 (simulates crash).
  6. Waits for the lease to expire (30s per config) → task resets to PENDING.
  7. Starts worker-2 — it picks up the task and resumes from SEARCH checkpoint.
  8. Asserts final status == COMPLETED, all 3 steps COMPLETED, completed by w2.

Run from the repo root:
    .venv/bin/python scripts/worker_test.py
"""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import grpc

from agent_sentinel.config import NODES, GRPC_PORT_BASE, LEASE_DURATION_SECONDS
from agent_sentinel.grpc_layer import sentinel_pb2, sentinel_pb2_grpc

ELECTION_TIMEOUT = 20       # seconds to wait for gRPC leader to appear
POLL_INTERVAL    = 0.5
PYTHON = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../.venv/bin/python")
)
GRPC_ADDRESSES = [f"localhost:{GRPC_PORT_BASE + i}" for i in range(len(NODES))]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def log_step(step: str) -> None:
    print(f"\n{'─'*60}", flush=True)
    print(f"  {step}", flush=True)
    print(f"{'─'*60}", flush=True)


def cleanup_raft_state() -> None:
    """Remove persisted Raft files so this test starts from a clean cluster."""
    data_dir = Path(__file__).resolve().parents[1] / "data"
    patterns = (
        "raft_journal_node*.bin",
        "raft_dump_node*.bin",
        "raft_journal_node*.bin.meta",
        "raft_dump_node*.bin.meta",
    )
    removed = 0
    for pattern in patterns:
        for path in data_dir.glob(pattern):
            try:
                path.unlink()
                removed += 1
            except FileNotFoundError:
                pass
    log(f"Cleaned Raft state files: {removed}")


def start_raft_node(node_id: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [PYTHON, "-m", "agent_sentinel.control_plane.server", "--node", str(node_id)],
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log(f"Started Raft node {node_id} (pid={proc.pid}) at {NODES[node_id]}")
    return proc


def start_worker(worker_id: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [PYTHON, "-m", "agent_sentinel.workers.worker", "--worker-id", worker_id],
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=None,   # inherit — prints directly so we can see worker logs
        stderr=None,
    )
    log(f"Started {worker_id} (pid={proc.pid})")
    return proc


def find_grpc_leader(timeout: float) -> sentinel_pb2_grpc.OrchestratorStub:
    """
    Probe all gRPC addresses until one responds as the leader.
    Returns a connected stub to the leader.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        for address in GRPC_ADDRESSES:
            try:
                channel = grpc.insecure_channel(address)
                stub = sentinel_pb2_grpc.OrchestratorStub(channel)
                # Probe with fake heartbeat:
                # - leader: returns ack(success=False, "task not found")
                # - follower: aborts with FAILED_PRECONDITION (NOT_LEADER)
                stub.SendHeartbeat(
                    sentinel_pb2.LeaseToken(
                        task_id="__leader_probe__",
                        worker_id="__probe__",
                        version_token=0,
                    ),
                    timeout=2,
                )
                log(f"gRPC leader found at {address}")
                return stub
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    pass  # not leader, try next
                # else: node down or not ready, try next
            finally:
                pass
        time.sleep(1)
    raise TimeoutError(f"No gRPC leader found within {timeout}s")


def add_task_via_grpc(stub: sentinel_pb2_grpc.OrchestratorStub, task_id: str) -> None:
    ack = stub.AddTask(
        sentinel_pb2.TaskRequest(
            task_id=task_id,
            metadata_json=json.dumps({"description": "crash resume test"}),
        ),
        timeout=10,
    )
    if not ack.success:
        raise RuntimeError(f"AddTask failed: {ack.message}")
    log(f"Task submitted: {task_id}")


def poll_task_status(
    reader,
    task_id: str,
    target_status: str,
    timeout: float,
    check_fn=None,
):
    """
    Poll the in-process reader node's registry until the task reaches
    target_status (optionally also satisfying check_fn).
    Reading from a Raft follower is fine — ReplDict stays in sync locally.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = reader.registry.get_task(task_id)
        if task and task["status"] == target_status:
            if check_fn is None or check_fn(task):
                return task
        time.sleep(POLL_INTERVAL)
    task = reader.registry.get_task(task_id)
    current = task["status"] if task else "NOT FOUND"
    raise TimeoutError(
        f"Task {task_id} did not reach {target_status} within {timeout}s "
        f"(current: {current})"
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═" * 60)
    print("  Agent-Sentinel — Phase 3 Worker Crash & Resume Test")
    print("═" * 60)

    procs: dict[str, subprocess.Popen] = {}
    reader = None
    run_tag = uuid.uuid4().hex[:8]
    task_id = f"task-crash-{run_tag}"

    try:
        # ── Step 1: Start Raft cluster ────────────────────────────────────
        # Nodes 1+2 as subprocesses. Node 0 runs in-process as a read-only
        # observer — we use its registry to poll task state without needing
        # an extra gRPC query RPC.
        log_step("Step 1: Starting Raft cluster (nodes 1+2 subprocess, node 0 in-process)")
        cleanup_raft_state()
        procs["node1"] = start_raft_node(1)
        procs["node2"] = start_raft_node(2)
        time.sleep(2)
        from agent_sentinel.control_plane.node import ControlPlaneNode
        reader = ControlPlaneNode(node_id=0)
        time.sleep(3)

        # ── Step 2: Find gRPC leader ──────────────────────────────────────
        log_step("Step 2: Probing gRPC ports for leader")
        # gRPC server starts after Raft leader elected + sync() fires (up to 5s)
        log(f"Waiting up to {ELECTION_TIMEOUT}s for gRPC leader...")
        stub = find_grpc_leader(timeout=ELECTION_TIMEOUT)

        # ── Step 3: Submit task via AddTask RPC ───────────────────────────
        log_step("Step 3: Submitting task via AddTask RPC")
        add_task_via_grpc(stub, task_id)

        # ── Step 4: Start worker-1, wait for SEARCH checkpoint ────────────
        log_step("Step 4: Starting worker-1")
        procs["w1"] = start_worker("w1")

        log("Waiting for worker-1 to commit SEARCH checkpoint...")

        def search_completed(task):
            cp_json = task.get("checkpoint_json", "")
            if not cp_json:
                return False
            cp = json.loads(cp_json)
            return cp.get("tool_results", {}).get("SEARCH", {}).get("status") == "COMPLETED"

        poll_task_status(reader, task_id, "RUNNING", timeout=25, check_fn=search_completed)
        log("SEARCH checkpoint committed by worker-1.")

        # ── Step 5: Kill worker-1 ─────────────────────────────────────────
        log_step("Step 5: Killing worker-1 (simulating crash)")
        os.kill(procs["w1"].pid, signal.SIGKILL)
        procs["w1"].wait(timeout=3)
        del procs["w1"]
        log("worker-1 killed.")

        # ── Step 6: Wait for lease to expire → PENDING ────────────────────
        log_step(f"Step 6: Waiting for lease to expire ({LEASE_DURATION_SECONDS}s)...")
        poll_task_status(reader, task_id, "PENDING", timeout=LEASE_DURATION_SECONDS + 15)
        log("Task reset to PENDING — lease expired.")

        # ── Step 7: Start worker-2 ────────────────────────────────────────
        log_step("Step 7: Starting worker-2")
        procs["w2"] = start_worker("w2")

        # ── Step 8: Wait for COMPLETED ────────────────────────────────────
        log_step("Step 8: Waiting for worker-2 to complete the task")
        final_task = poll_task_status(reader, task_id, "COMPLETED", timeout=40)

        # ── Step 9: Assertions ────────────────────────────────────────────
        log_step("Step 9: Verifying results")

        assert final_task["status"] == "COMPLETED", "Task not COMPLETED"
        assert final_task["worker_id"] == "w2", (
            f"Expected w2 to complete, got {final_task['worker_id']}"
        )

        cp = json.loads(final_task["checkpoint_json"])
        tool_results = cp.get("tool_results", {})
        for step in ["SEARCH", "SUMMARIZE", "SAVE"]:
            step_status = tool_results.get(step, {}).get("status")
            icon = "✓" if step_status == "COMPLETED" else "✗"
            log(f"  {icon} {step}: {step_status}")
            assert step_status == "COMPLETED", f"Step {step} not COMPLETED"

        print("\n" + "═" * 60)
        print("  RESULT: ALL CHECKS PASSED")
        print(f"  task_id:  {task_id}")
        print(f"  Crashed:  worker-1 (after SEARCH checkpoint)")
        print(f"  Resumed:  worker-2 (skipped SEARCH, ran SUMMARIZE + SAVE)")
        print(f"  Final:    COMPLETED by w2")
        print("═" * 60 + "\n")

    except (AssertionError, TimeoutError, RuntimeError) as e:
        print(f"\n  FAIL: {e}\n")
        sys.exit(1)
    finally:
        try:
            reader.destroy()
        except Exception:
            pass
        for name, proc in procs.items():
            log(f"Stopping {name} (pid={proc.pid})")
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    main()
