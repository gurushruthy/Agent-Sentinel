import json

import agent_sentinel.workers.langgraph_runner as lg_runner_module
from agent_sentinel.workers.checkpoint import (
    AgentState,
    mark_step_completed,
    mark_step_in_progress,
    serialize,
    deserialize,
    should_skip_step,
)
from agent_sentinel.workers.langgraph_runner import LangGraphStepRunner


def test_langgraph_runner_fresh_state_search_then_summarize() -> None:
    runner = LangGraphStepRunner()
    runner._search_mode = "deterministic"
    runner._summarizer_mode = "deterministic"
    state = AgentState(task_id="task-1")

    search_result = runner.run_step(state, "SEARCH")
    assert search_result is not None
    assert "query" in search_result
    assert "raw_results" in search_result

    mark_step_completed(state, "SEARCH", search_result)
    summarize_result = runner.run_step(state, "SUMMARIZE")
    assert summarize_result is not None
    assert "summary" in summarize_result
    assert "input_text" in summarize_result


def test_langgraph_runner_resumed_state_save() -> None:
    runner = LangGraphStepRunner()
    runner._save_mode = "deterministic"
    state = AgentState(task_id="task-2")

    mark_step_in_progress(state, "SEARCH")
    mark_step_completed(
        state,
        "SEARCH",
        {"query": "q", "raw_results": [{"title": "Result 1"}]},
    )
    mark_step_in_progress(state, "SUMMARIZE")
    mark_step_completed(
        state,
        "SUMMARIZE",
        {"input_text": "[]", "summary": "ok"},
    )

    # Simulate persisted+resumed checkpoint flow.
    resumed = deserialize(serialize(state), task_id="task-2")
    save_result = runner.run_step(resumed, "SAVE")
    assert save_result is not None
    assert save_result["response"]["saved"] is True
    assert save_result["idempotency_key"] == resumed.idempotency_key


def test_skip_behavior_completed_vs_pending() -> None:
    state = AgentState(task_id="task-3")
    assert should_skip_step(state, "SEARCH") is False

    mark_step_in_progress(state, "SEARCH")
    assert should_skip_step(state, "SEARCH") is False

    mark_step_completed(
        state,
        "SEARCH",
        {"query": "q", "raw_results": [{"title": "Result 1"}]},
    )
    assert should_skip_step(state, "SEARCH") is True


def test_langgraph_runner_llm_mode_calls_openai_path(monkeypatch) -> None:
    runner = LangGraphStepRunner()
    runner._summarizer_mode = "llm"

    state = AgentState(task_id="task-llm")
    mark_step_in_progress(state, "SEARCH")
    mark_step_completed(
        state,
        "SEARCH",
        {"query": "q", "raw_results": [{"title": "Result 1"}]},
    )

    monkeypatch.setattr(
        runner,
        "_summarize_with_openai",
        lambda input_text, task_id: f"LLM summary for {task_id}",
    )
    out = runner.run_step(state, "SUMMARIZE")
    assert out is not None
    assert out["summary"] == "LLM summary for task-llm"


def test_langgraph_runner_llm_fallback_to_deterministic(monkeypatch) -> None:
    runner = LangGraphStepRunner()
    runner._summarizer_mode = "llm"

    state = AgentState(task_id="task-fallback")
    mark_step_in_progress(state, "SEARCH")
    mark_step_completed(
        state,
        "SEARCH",
        {"query": "q", "raw_results": [{"title": "Result 1"}, {"title": "Result 2"}]},
    )

    monkeypatch.setattr(lg_runner_module, "LANGGRAPH_SUMMARIZER_FALLBACK", True)

    def _boom(input_text, task_id):
        raise RuntimeError("boom")

    monkeypatch.setattr(runner, "_summarize_with_openai", _boom)
    out = runner.run_step(state, "SUMMARIZE")
    assert out is not None
    assert out["summary"] == "Summary of 2 results for task task-fallback"


def test_langgraph_runner_search_web_mode_calls_duckduckgo(monkeypatch) -> None:
    runner = LangGraphStepRunner()
    runner._search_mode = "web"
    state = AgentState(task_id="task-web")

    monkeypatch.setattr(
        runner,
        "_search_with_duckduckgo",
        lambda query: [{"title": "Live", "url": "https://example.com", "snippet": "ok"}],
    )
    out = runner.run_step(state, "SEARCH")
    assert out is not None
    assert out["source"] == "duckduckgo"
    assert out["raw_results"][0]["title"] == "Live"


def test_langgraph_runner_search_web_fallback_to_deterministic(monkeypatch) -> None:
    runner = LangGraphStepRunner()
    runner._search_mode = "web"
    state = AgentState(task_id="task-web-fallback")

    monkeypatch.setattr(lg_runner_module, "LANGGRAPH_SEARCH_FALLBACK", True)

    def _boom(query):
        raise RuntimeError("boom")

    monkeypatch.setattr(runner, "_search_with_duckduckgo", _boom)
    out = runner.run_step(state, "SEARCH")
    assert out is not None
    assert out["source"] == "deterministic"


def test_langgraph_runner_save_sqlite_persists_row(monkeypatch, tmp_path) -> None:
    runner = LangGraphStepRunner()
    runner._save_mode = "sqlite"
    db_path = tmp_path / "results.db"
    monkeypatch.setattr(lg_runner_module, "RESULTS_DB_PATH", str(db_path))

    state = AgentState(task_id="task-sqlite")
    state.tool_results["SEARCH"] = {
        "status": "COMPLETED",
        "raw_results": [{"title": "R1"}],
    }
    state.tool_results["SUMMARIZE"] = {
        "status": "COMPLETED",
        "summary": "S1",
        "input_text": '[{"title":"R1"}]',
    }
    state.step_index = 2

    out = runner.run_step(state, "SAVE")
    assert out is not None
    assert out["response"]["saved"] is True
    assert out["response"]["storage"] == "sqlite"
    assert out["destination"] == str(db_path)

    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT task_id, idempotency_key, summary, payload_json FROM task_results WHERE task_id=?",
            ("task-sqlite",),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "task-sqlite"
    assert row[1] == state.idempotency_key
    assert row[2] == "S1"
    payload = json.loads(row[3])
    assert payload["task_id"] == "task-sqlite"
    assert payload["summary"] == "S1"
    assert payload["search_raw_results"] == [{"title": "R1"}]
