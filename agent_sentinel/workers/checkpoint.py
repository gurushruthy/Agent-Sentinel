"""
AgentState — the checkpoint schema carried inside TaskRecord.checkpoint_json.

All agent progress is stored here and committed to the Raft cluster via
CommitState so that any worker can resume from the last saved state.

Schema overview
───────────────
  task_id          str        which task this state belongs to
  current_step     str        the step currently executing ("SEARCH" | "SUMMARIZE" | "SAVE")
  step_index       int        0-based; recovering worker starts from here
  history          list[dict] LangGraph message history  [{role, content}, ...]
  tool_results     dict       per-step result keyed by step name
  idempotency_key  str        f"{task_id}-{step_name}" — stable across retries,
                              used to deduplicate calls to external APIs
  last_checkpoint_at float   Unix timestamp of last successful CommitState
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any


# ─── Step status constants ────────────────────────────────────────────────────

STATUS_PENDING = "PENDING"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"

# Ordered list of steps that the agent executes
STEPS = ["SEARCH", "SUMMARIZE", "SAVE"]


# ─── AgentState ──────────────────────────────────────────────────────────────

@dataclass
class AgentState:
    task_id: str
    current_step: str = STEPS[0]
    step_index: int = 0
    history: list[dict[str, str]] = field(default_factory=list)
    tool_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    idempotency_key: str = ""
    last_checkpoint_at: float = 0.0

    def __post_init__(self):
        # idempotency_key defaults to task_id + first step if not provided
        if not self.idempotency_key:
            self.idempotency_key = f"{self.task_id}-{self.current_step}"


# ─── Serialization ───────────────────────────────────────────────────────────

def serialize(state: AgentState) -> str:
    """Convert AgentState to the JSON string stored in checkpoint_json."""
    return json.dumps({
        "task_id": state.task_id,
        "current_step": state.current_step,
        "step_index": state.step_index,
        "history": state.history,
        "tool_results": state.tool_results,
        "idempotency_key": state.idempotency_key,
        "last_checkpoint_at": state.last_checkpoint_at,
    })


def deserialize(json_str: str, task_id: str) -> AgentState:
    """
    Parse a checkpoint_json string back into AgentState.
    If json_str is empty or None (new task, no checkpoint yet), returns a
    fresh AgentState starting from the first step.
    """
    if not json_str:
        return AgentState(task_id=task_id)

    data = json.loads(json_str)
    return AgentState(
        task_id=data["task_id"],
        current_step=data["current_step"],
        step_index=data["step_index"],
        history=data["history"],
        tool_results=data["tool_results"],
        idempotency_key=data["idempotency_key"],
        last_checkpoint_at=data["last_checkpoint_at"],
    )


# ─── Step helpers ────────────────────────────────────────────────────────────

def should_skip_step(state: AgentState, step_name: str) -> bool:
    """
    Return True if the step was already fully completed.
    IN_PROGRESS entries are NOT skipped — the step crashed mid-execution
    and must be re-run (idempotency_key ensures the external API call is safe).
    """
    result = state.tool_results.get(step_name, {})
    return result.get("status") == STATUS_COMPLETED


def mark_step_in_progress(state: AgentState, step_name: str) -> None:
    """
    Record that we are about to execute step_name.
    Written to state and then committed to the cluster immediately so that
    a crash mid-step is visible as IN_PROGRESS (not silently lost).
    """
    state.current_step = step_name
    state.idempotency_key = f"{state.task_id}-{step_name}"
    if step_name not in state.tool_results:
        state.tool_results[step_name] = {}
    state.tool_results[step_name]["status"] = STATUS_IN_PROGRESS


def mark_step_completed(
    state: AgentState,
    step_name: str,
    result_fields: dict[str, Any],
) -> None:
    """
    Record that step_name finished successfully.
    result_fields are merged into tool_results[step_name] alongside status=COMPLETED.

    Example:
        mark_step_completed(state, "SEARCH", {"query": q, "raw_results": results})
    """
    entry = state.tool_results.get(step_name, {})
    entry.update(result_fields)
    entry["status"] = STATUS_COMPLETED
    entry["completed_at"] = time.time()
    state.tool_results[step_name] = entry

    # Advance to the next step
    if step_name in STEPS:
        idx = STEPS.index(step_name)
        state.step_index = idx + 1
        if idx + 1 < len(STEPS):
            state.current_step = STEPS[idx + 1]
            state.idempotency_key = f"{state.task_id}-{state.current_step}"
