"""
LangGraph-backed step runner for worker execution.

This first Phase 4 cut keeps behavior deterministic and equivalent to the
existing stub step implementations while moving orchestration into LangGraph.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from agent_sentinel.config import (
    LANGGRAPH_SAVE_MODE,
    LANGGRAPH_SEARCH_FALLBACK,
    LANGGRAPH_SEARCH_MAX_RESULTS,
    LANGGRAPH_SEARCH_MODE,
    LANGGRAPH_SEARCH_TIMEOUT_SECONDS,
    LANGGRAPH_SUMMARIZER_FALLBACK,
    LANGGRAPH_SUMMARIZER_MODE,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_MODEL,
    OPENAI_TIMEOUT_SECONDS,
    RESULTS_DB_PATH,
)
from agent_sentinel.workers.checkpoint import AgentState

logger = logging.getLogger(__name__)


class StepGraphState(TypedDict):
    agent_state: AgentState
    step_name: str
    result_fields: dict[str, Any] | None


class LangGraphStepRunner:
    """Executes one worker step through a small LangGraph state machine."""

    def __init__(self) -> None:
        self._search_mode = LANGGRAPH_SEARCH_MODE
        self._save_mode = LANGGRAPH_SAVE_MODE
        self._summarizer_mode = LANGGRAPH_SUMMARIZER_MODE
        if self._save_mode not in {"sqlite", "deterministic"}:
            raise ValueError(
                f"Invalid LANGGRAPH_SAVE_MODE={self._save_mode!r}. "
                "Expected 'sqlite' or 'deterministic'."
            )
        if self._summarizer_mode not in {"llm", "deterministic"}:
            raise ValueError(
                f"Invalid LANGGRAPH_SUMMARIZER_MODE={self._summarizer_mode!r}. "
                "Expected 'llm' or 'deterministic'."
            )
        builder = StateGraph(StepGraphState)
        builder.add_node("SEARCH", self._search_node)
        builder.add_node("SUMMARIZE", self._summarize_node)
        builder.add_node("SAVE", self._save_node)

        builder.add_conditional_edges(
            START,
            self._route_to_step,
            {
                "SEARCH": "SEARCH",
                "SUMMARIZE": "SUMMARIZE",
                "SAVE": "SAVE",
            },
        )
        builder.add_edge("SEARCH", END)
        builder.add_edge("SUMMARIZE", END)
        builder.add_edge("SAVE", END)

        self._graph = builder.compile()

    def run_step(self, state: AgentState, step_name: str) -> dict[str, Any] | None:
        """Run exactly one named step and return its result payload."""
        if step_name not in {"SEARCH", "SUMMARIZE", "SAVE"}:
            raise ValueError(f"Unknown step for LangGraph runner: {step_name}")

        out = self._graph.invoke(
            {
                "agent_state": state,
                "step_name": step_name,
                "result_fields": None,
            }
        )
        return out.get("result_fields")

    @staticmethod
    def _route_to_step(graph_state: StepGraphState) -> str:
        return graph_state["step_name"]

    def _search_node(self, graph_state: StepGraphState) -> StepGraphState:
        state = graph_state["agent_state"]
        time.sleep(1)
        user_query = state.metadata.get("query") if isinstance(state.metadata, dict) else None
        query = user_query.strip() if isinstance(user_query, str) and user_query.strip() else f"query for {state.task_id}"
        if self._search_mode == "web":
            try:
                raw_results = self._search_with_duckduckgo(query=query)
                result_fields = {
                    "query": query,
                    "raw_results": raw_results,
                    "source": "duckduckgo",
                }
                return {**graph_state, "result_fields": result_fields}
            except Exception as exc:
                if not LANGGRAPH_SEARCH_FALLBACK:
                    raise
                logger.warning(
                    "LangGraph SEARCH web call failed for query=%r; using deterministic fallback. error=%s",
                    query,
                    exc,
                )

        # Deterministic path, or fallback when enabled.
        result_fields = self._deterministic_search(query=query)
        return {**graph_state, "result_fields": result_fields}

    def _summarize_node(self, graph_state: StepGraphState) -> StepGraphState:
        state = graph_state["agent_state"]
        search_results = state.tool_results.get("SEARCH", {}).get("raw_results", [])
        time.sleep(1)
        input_text = json.dumps(search_results, ensure_ascii=False)
        summary = self._summarize_text(input_text=input_text, task_id=state.task_id)
        result_fields = {
            "input_text": input_text,
            "summary": summary,
        }
        return {**graph_state, "result_fields": result_fields}

    def _save_node(self, graph_state: StepGraphState) -> StepGraphState:
        state = graph_state["agent_state"]
        time.sleep(0.5)
        if self._save_mode == "sqlite":
            result_fields = self._save_to_sqlite(state)
        else:
            result_fields = self._deterministic_save(state)
        return {**graph_state, "result_fields": result_fields}

    def _summarize_text(self, input_text: str, task_id: str) -> str:
        if self._summarizer_mode == "deterministic":
            return self._deterministic_summary(input_text=input_text, task_id=task_id)

        if self._summarizer_mode == "llm":
            try:
                return self._summarize_with_openai(input_text=input_text, task_id=task_id)
            except Exception as exc:
                if not LANGGRAPH_SUMMARIZER_FALLBACK:
                    raise
                logger.warning(
                    "LangGraph summarize LLM call failed for task=%s; using deterministic fallback. error=%s",
                    task_id,
                    exc,
                )

        # Fallback deterministic summary path (only if fallback is enabled).
        return self._deterministic_summary(input_text=input_text, task_id=task_id)

    @staticmethod
    def _deterministic_search(query: str) -> dict[str, Any]:
        return {
            "query": query,
            "raw_results": [{"title": "Result 1"}, {"title": "Result 2"}],
            "source": "deterministic",
        }

    @staticmethod
    def _search_with_duckduckgo(query: str) -> list[dict[str, str]]:
        endpoint = "https://api.duckduckgo.com/"
        params = urllib.parse.urlencode(
            {
                "q": query,
                "format": "json",
                "no_redirect": "1",
                "no_html": "1",
                "skip_disambig": "1",
            }
        )
        req = urllib.request.Request(f"{endpoint}?{params}", method="GET")

        try:
            with urllib.request.urlopen(req, timeout=LANGGRAPH_SEARCH_TIMEOUT_SECONDS) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"SEARCH HTTP error {err.code}: {detail}") from err

        data = json.loads(body)
        results: list[dict[str, str]] = []

        def _append(title: str, url: str, snippet: str) -> None:
            if not (title or snippet):
                return
            results.append(
                {
                    "title": (title or "").strip(),
                    "url": (url or "").strip(),
                    "snippet": " ".join((snippet or "").strip().split()),
                }
            )

        # Abstract answer (if present)
        _append(
            title=data.get("Heading", ""),
            url=data.get("AbstractURL", ""),
            snippet=data.get("AbstractText", ""),
        )

        # Related topics (primary list in Instant Answer API)
        for item in data.get("RelatedTopics", []) or []:
            if "Topics" in item:
                for sub in item.get("Topics", []) or []:
                    _append(
                        title=sub.get("Text", ""),
                        url=sub.get("FirstURL", ""),
                        snippet=sub.get("Text", ""),
                    )
            else:
                _append(
                    title=item.get("Text", ""),
                    url=item.get("FirstURL", ""),
                    snippet=item.get("Text", ""),
                )

        # Optional additional results field
        for item in data.get("Results", []) or []:
            _append(
                title=item.get("Text", ""),
                url=item.get("FirstURL", ""),
                snippet=item.get("Text", ""),
            )

        # Keep bounded payload size for checkpoint replication.
        return results[:LANGGRAPH_SEARCH_MAX_RESULTS]

    @staticmethod
    def _deterministic_summary(input_text: str, task_id: str) -> str:
        try:
            parsed = json.loads(input_text)
            n_results = len(parsed) if isinstance(parsed, list) else 0
        except Exception:
            n_results = 0
        return f"Summary of {n_results} results for task {task_id}"

    @staticmethod
    def _deterministic_save(state: AgentState) -> dict[str, Any]:
        return {
            "destination": "results_db",
            "idempotency_key": state.idempotency_key,
            "response": {"saved": True},
        }

    @staticmethod
    def _save_to_sqlite(state: AgentState) -> dict[str, Any]:
        db_path = RESULTS_DB_PATH
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        summarize_result = state.tool_results.get("SUMMARIZE", {})
        search_result = state.tool_results.get("SEARCH", {})
        payload = {
            "task_id": state.task_id,
            "step_index": state.step_index,
            "summary": summarize_result.get("summary", ""),
            "input_text": summarize_result.get("input_text", ""),
            "search_raw_results": search_result.get("raw_results", []),
            "tool_results": state.tool_results,
        }
        payload_json = json.dumps(payload, ensure_ascii=False)

        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_results (
                    task_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    summary TEXT,
                    payload_json TEXT NOT NULL,
                    saved_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            cur.execute(
                """
                INSERT INTO task_results (task_id, idempotency_key, summary, payload_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    idempotency_key=excluded.idempotency_key,
                    summary=excluded.summary,
                    payload_json=excluded.payload_json,
                    updated_at=datetime('now')
                """,
                (
                    state.task_id,
                    state.idempotency_key,
                    summarize_result.get("summary", ""),
                    payload_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        return {
            "destination": db_path,
            "idempotency_key": state.idempotency_key,
            "response": {"saved": True, "storage": "sqlite", "task_id": state.task_id},
        }

    @staticmethod
    def _summarize_with_openai(input_text: str, task_id: str) -> str:
        if not OPENAI_API_KEY:
            raise RuntimeError(
                "OPENAI_API_KEY is required when LANGGRAPH_SUMMARIZER_MODE=llm."
            )

        url = f"{OPENAI_BASE_URL}/chat/completions"
        payload = {
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a concise summarizer. Summarize search results for "
                        "a task in 2-4 short sentences."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Task ID: {task_id}\n"
                        f"Search results payload:\n{input_text}\n\n"
                        "Return only the summary text."
                    ),
                },
            ],
            "temperature": 0.2,
        }
        req = urllib.request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPENAI_API_KEY}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT_SECONDS) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"LLM HTTP error {err.code}: {detail}") from err

        data = json.loads(body)
        content = data["choices"][0]["message"]["content"]
        summary = (content or "").strip()
        if not summary:
            raise RuntimeError("LLM returned an empty summary.")
        return summary
