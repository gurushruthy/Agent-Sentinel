from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

import grpc

from agent_sentinel.config import GRPC_PORT_BASE, NODES
from agent_sentinel.grpc_layer import sentinel_pb2, sentinel_pb2_grpc


@dataclass
class LeaderConnection:
    port: int
    address: str
    stub: sentinel_pb2_grpc.OrchestratorStub


class LeaderNotFoundError(RuntimeError):
    pass


def grpc_addresses() -> list[str]:
    return [f"localhost:{GRPC_PORT_BASE + i}" for i in range(len(NODES))]


def find_leader(timeout_seconds: float = 2.0) -> LeaderConnection:
    for i, address in enumerate(grpc_addresses()):
        port = GRPC_PORT_BASE + i
        try:
            channel = grpc.insecure_channel(address)
            stub = sentinel_pb2_grpc.OrchestratorStub(channel)
            # Probe with fake heartbeat: leader returns Ack(false) for unknown task.
            stub.SendHeartbeat(
                sentinel_pb2.LeaseToken(
                    task_id="__api_probe__",
                    worker_id="api",
                    version_token=0,
                ),
                timeout=timeout_seconds,
            )
            return LeaderConnection(port=port, address=address, stub=stub)
        except grpc.RpcError:
            continue
    raise LeaderNotFoundError("No gRPC leader found on configured ports.")


def add_task(query: str, metadata: dict[str, Any] | None = None, task_id: str | None = None):
    leader = find_leader()
    tid = task_id or f"task-{uuid.uuid4().hex[:8]}"
    merged = dict(metadata or {})
    merged["query"] = query
    ack = leader.stub.AddTask(
        sentinel_pb2.TaskRequest(
            task_id=tid,
            metadata_json=json.dumps(merged),
        ),
        timeout=10,
    )
    return leader, tid, ack


def get_task(task_id: str):
    leader = find_leader()
    resp = leader.stub.GetTask(sentinel_pb2.TaskId(task_id=task_id), timeout=5)
    return leader, resp


def list_tasks(status: str | None = None, limit: int = 100, offset: int = 0):
    leader = find_leader()
    resp = leader.stub.ListTasks(
        sentinel_pb2.TaskListRequest(
            status=(status or ""),
            limit=limit,
            offset=offset,
        ),
        timeout=10,
    )
    return leader, resp


def cluster_status():
    statuses: list[dict[str, Any]] = []
    leader_port = None
    leader_grpc_address = None
    addresses = grpc_addresses()
    for i, grpc_address in enumerate(addresses):
        role_hint = "unknown"
        reachable = False
        try:
            channel = grpc.insecure_channel(grpc_address)
            stub = sentinel_pb2_grpc.OrchestratorStub(channel)
            stub.SendHeartbeat(
                sentinel_pb2.LeaseToken(
                    task_id="__api_probe__",
                    worker_id="api",
                    version_token=0,
                ),
                timeout=2,
            )
            reachable = True
            role_hint = "leader"
            leader_port = GRPC_PORT_BASE + i
            leader_grpc_address = grpc_address
        except grpc.RpcError:
            reachable = False
            role_hint = "follower_or_down"

        statuses.append(
            {
                "node_id": i,
                "raft_address": NODES[i],
                "grpc_address": grpc_address,
                "grpc_reachable": reachable,
                "role_hint": role_hint,
            }
        )

    return leader_port, leader_grpc_address, statuses
