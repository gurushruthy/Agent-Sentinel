from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from agent_sentinel.api.schemas import (
    CreateTaskRequest,
    CreateTaskResponse,
    TaskListResponse,
    TaskResponse,
)
from agent_sentinel.api.services.grpc_client import LeaderNotFoundError, add_task, get_task, list_tasks

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("", response_model=CreateTaskResponse)
def create_task(payload: CreateTaskRequest):
    try:
        leader, task_id, ack = add_task(
            query=payload.query,
            metadata=payload.metadata,
            task_id=payload.task_id,
        )
    except LeaderNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to add task: {exc}") from exc

    return CreateTaskResponse(
        accepted=bool(ack.success),
        task_id=task_id,
        leader_port=leader.port,
        message=ack.message,
    )


@router.get("/{task_id}", response_model=TaskResponse)
def get_task_by_id(task_id: str):
    try:
        _, resp = get_task(task_id)
    except LeaderNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to fetch task: {exc}") from exc

    if not resp.found:
        return TaskResponse(found=False, message=resp.message)

    task: dict[str, Any] = json.loads(resp.task_json)
    checkpoint_json = task.get("checkpoint_json") or ""
    tool_results = None
    if checkpoint_json:
        try:
            checkpoint = json.loads(checkpoint_json)
            tool_results = checkpoint.get("tool_results")
        except Exception:
            tool_results = None

    return TaskResponse(
        found=True,
        task_id=task.get("task_id"),
        status=task.get("status"),
        worker_id=task.get("worker_id"),
        version_token=task.get("version_token"),
        checkpoint_json=checkpoint_json,
        metadata=task.get("metadata"),
        tool_results=tool_results,
    )


@router.get("", response_model=TaskListResponse)
def get_tasks(
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    try:
        _, resp = list_tasks(status=status, limit=limit, offset=offset)
    except LeaderNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to list tasks: {exc}") from exc

    tasks = [json.loads(t) for t in resp.tasks_json]
    return TaskListResponse(total=resp.total, tasks=tasks)
