"""
Phase 1.3 — Fault Tolerance Test

What this script does:
  1. Runs node 0 in this process and nodes 1–2 as subprocesses (full 3-node cluster,
     no duplicate bind on 4321–4323).
  2. Waits for leader election.
  3. Adds 3 tasks via Raft (_apply_record(sync=True) forwards to the leader.)
  4. Stops the leader (SIGTERM subprocess, or destroy in-process node 0).
  5. Waits for the surviving pair to elect a new leader.
  6. Verifies all 3 tasks are still present.
  7. Adds a 4th task after failover.

Task IDs include a per-run tag so persisted raft_journal_*.bin data does not cause
\"already exists\" on repeat runs.

Run from the repo root:
    .venv/bin/python scripts/fault_tolerance_test.py
"""

import os
import signal
import subprocess
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent_sentinel.config import NODES
from agent_sentinel.control_plane.node import ControlPlaneNode

ELECTION_TIMEOUT = 15
FAILOVER_TIMEOUT = 20
POLL_INTERVAL = 0.5
PYTHON = os.path.join(os.path.dirname(__file__), "../.venv/bin/python")
SUBPROCESS_NODE_IDS = (1, 2)


def log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def log_step(step: str) -> None:
    print(f"\n{'─'*55}", flush=True)
    print(f"  {step}", flush=True)
    print(f"{'─'*55}", flush=True)


def start_node(node_id: int) -> subprocess.Popen:
    cmd = [
        os.path.abspath(PYTHON),
        "-m",
        "agent_sentinel.control_plane.server",
        "--node",
        str(node_id),
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log(f"Started Node {node_id} (pid={proc.pid}) at {NODES[node_id]}")
    return proc


def wait_for_leader(
    node: ControlPlaneNode,
    timeout: float,
    exclude: str | None = None,
) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        leader = node.get_leader()
        if leader and leader != exclude:
            return leader
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(
        f"No leader elected within {timeout}s (exclude={exclude})"
    )


def find_leader_node_id(leader_addr: str) -> int:
    return NODES.index(leader_addr)


def add_task_via_raft(
    node: ControlPlaneNode, task_id: str, metadata: dict | None
) -> None:
    reg = node.registry
    if reg.get(task_id) is not None:
        raise ValueError(f"Task '{task_id}' already exists.")
    now = time.time()
    record = {
        "task_id": task_id,
        "status": "PENDING",
        "worker_id": None,
        "version_token": 0,
        "lease_expires_at": 0.0,
        "checkpoint_json": "",
        "error_count": 0,
        "metadata": metadata or {},
        "created_at": now,
        "updated_at": now,
    }
    reg._apply_record(task_id, record, sync=True, timeout=30)


def main() -> None:
    print("\n" + "═" * 55)
    print("  Agent-Sentinel — Phase 1.3 Fault Tolerance Test")
    print("═" * 55)

    procs: dict[int, subprocess.Popen] = {}
    client: ControlPlaneNode | None = None
    run_tag = uuid.uuid4().hex[:8]

    try:
        log_step("Step 1: Starting Raft nodes (0 in-process, 1–2 subprocess)")
        for i in SUBPROCESS_NODE_IDS:
            procs[i] = start_node(i)
        time.sleep(2)

        client = ControlPlaneNode(node_id=0)
        time.sleep(2)

        log_step("Step 2: Waiting for initial leader election")
        leader_addr = wait_for_leader(client, timeout=ELECTION_TIMEOUT)
        leader_id = find_leader_node_id(leader_addr)
        log(f"Leader elected: Node {leader_id} at {leader_addr}")

        log_step("Step 3: Adding 3 tasks to the registry via the leader")
        tasks_to_add = [
            (f"task-alpha-{run_tag}", {"description": "First task"}),
            (f"task-beta-{run_tag}", {"description": "Second task"}),
            (f"task-gamma-{run_tag}", {"description": "Third task"}),
        ]
        for task_id, meta in tasks_to_add:
            add_task_via_raft(client, task_id, meta)
            log(f"Added task: {task_id}")

        time.sleep(1)
        all_tasks = client.registry.list_tasks()
        log(
            f"Registry has {len(all_tasks)} task(s): {[t['task_id'] for t in all_tasks]}"
        )

        log_step(f"Step 4: Killing leader Node {leader_id}")
        if leader_id == 0:
            client.destroy()
            client = None
            log("In-process Node 0 stopped.")
        else:
            os.kill(procs[leader_id].pid, signal.SIGTERM)
            procs[leader_id].wait(timeout=5)
            del procs[leader_id]
            log(f"Subprocess Node {leader_id} terminated.")

        log_step("Step 5: Waiting for new leader election")
        time.sleep(2)
        if client is None:
            client = ControlPlaneNode(node_id=0)
            time.sleep(2)

        new_leader_addr = wait_for_leader(
            client, timeout=FAILOVER_TIMEOUT, exclude=leader_addr
        )
        new_leader_id = find_leader_node_id(new_leader_addr)
        log(f"NEW LEADER ELECTED: Node {new_leader_id} at {new_leader_addr}")

        log_step("Step 6: Verifying registry data is intact on new leader")
        time.sleep(1)
        recovered_tasks = client.registry.list_tasks()
        recovered_ids = {t["task_id"] for t in recovered_tasks}
        expected_ids = {t[0] for t in tasks_to_add}
        for task_id in expected_ids:
            status = "✓ PRESENT" if task_id in recovered_ids else "✗ MISSING"
            log(f"  {status}: {task_id}")
        assert expected_ids <= recovered_ids, (
            f"Data loss detected! Missing: {expected_ids - recovered_ids}"
        )
        log("All tasks verified — no data loss.")

        log_step("Step 7: Adding a 4th task to confirm writes resume")
        delta_id = f"task-delta-{run_tag}"
        add_task_via_raft(
            client, delta_id, {"description": "Post-failover task"}
        )
        time.sleep(1)
        task = client.registry.get_task(delta_id)
        assert task is not None, "post-failover task not found after write"
        log(f"Added {delta_id} successfully (status={task['status']})")

        print("\n" + "═" * 55)
        print("  RESULT: ALL CHECKS PASSED")
        print(f"  Failover: Node {leader_id} → Node {new_leader_id}")
        print(f"  Data intact: {len(recovered_tasks)} tasks preserved")
        print(f"  Writes resumed on new leader: Node {new_leader_id}")
        print("═" * 55 + "\n")

    except AssertionError as e:
        print(f"\n  FAIL: {e}\n")
        sys.exit(1)
    except TimeoutError as e:
        print(f"\n  TIMEOUT: {e}\n")
        sys.exit(1)
    finally:
        if client is not None:
            client.destroy()
        for node_id, proc in procs.items():
            log(f"Stopping Node {node_id} (pid={proc.pid})")
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    main()
