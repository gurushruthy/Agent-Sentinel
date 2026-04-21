from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CreateTaskRequest(BaseModel):
    query: str = Field(..., min_length=1, description="User search query")
    metadata: dict[str, Any] = Field(default_factory=dict)
    task_id: str | None = None


class CreateTaskResponse(BaseModel):
    accepted: bool
    task_id: str
    leader_port: int
    message: str


class TaskResponse(BaseModel):
    found: bool
    task_id: str | None = None
    status: str | None = None
    worker_id: str | None = None
    version_token: int | None = None
    checkpoint_json: str | None = None
    metadata: dict[str, Any] | None = None
    tool_results: dict[str, Any] | None = None
    message: str | None = None


class TaskListResponse(BaseModel):
    total: int
    tasks: list[dict[str, Any]]


class NodeStatus(BaseModel):
    node_id: int
    raft_address: str
    grpc_address: str
    grpc_reachable: bool
    role_hint: str


class ClusterStatusResponse(BaseModel):
    leader_port: int | None
    leader_grpc_address: str | None
    nodes: list[NodeStatus]
