from __future__ import annotations

from fastapi import APIRouter

from agent_sentinel.api.schemas import ClusterStatusResponse, NodeStatus
from agent_sentinel.api.services.grpc_client import cluster_status

router = APIRouter(prefix="/cluster", tags=["cluster"])


@router.get("/status", response_model=ClusterStatusResponse)
def get_cluster_status():
    leader_port, leader_addr, nodes = cluster_status()
    return ClusterStatusResponse(
        leader_port=leader_port,
        leader_grpc_address=leader_addr,
        nodes=[NodeStatus(**n) for n in nodes],
    )
