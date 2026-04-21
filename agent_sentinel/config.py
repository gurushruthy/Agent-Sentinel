import os

# ─── Raft Cluster ────────────────────────────────────────────────────────────
# Addresses for all 3 nodes. Each node is identified by its index (0, 1, 2).
NODES = [
    "localhost:4321",
    "localhost:4322",
    "localhost:4323",
]

# ─── Lease / Heartbeat ───────────────────────────────────────────────────────
# How long a worker's lease is valid after AcquireTask or SendHeartbeat.
LEASE_DURATION_SECONDS: int = 30

# How often a worker must call SendHeartbeat to keep the lease alive.
HEARTBEAT_INTERVAL_SECONDS: int = 10

# How often the LeaseManager background thread sweeps for expired leases
# and resets orphaned tasks back to PENDING.
LEASE_SWEEP_INTERVAL_SECONDS: int = 5

# ─── Poison Pill Guard ───────────────────────────────────────────────────────
# A task whose error_count reaches this threshold is permanently marked FAILED.
MAX_RETRIES: int = 3

# ─── gRPC (Phase 2) ──────────────────────────────────────────────────────────
# Node i listens on GRPC_PORT_BASE + i  →  50050, 50051, 50052
GRPC_PORT_BASE: int = 50050

# ─── Worker Execution Engine (Phase 4) ───────────────────────────────────────
# Selects the worker task execution engine:
#   - "stub"      : existing in-process deterministic step functions
#   - "langgraph" : LangGraph-backed step runner
WORKER_EXECUTION_MODE: str = os.getenv("WORKER_EXECUTION_MODE", "stub").strip().lower()

# ─── LangGraph Summarization Mode (Phase 4.1) ───────────────────────────────
# Controls SUMMARIZE behavior when WORKER_EXECUTION_MODE=langgraph:
#   - "deterministic" : local deterministic summary (no network)
#   - "llm"           : call OpenAI-compatible chat endpoint for summaries
LANGGRAPH_SUMMARIZER_MODE: str = os.getenv(
    "LANGGRAPH_SUMMARIZER_MODE", "llm"
).strip().lower()

# OpenAI-compatible endpoint settings used when LANGGRAPH_SUMMARIZER_MODE=llm.
OPENAI_BASE_URL: str = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
OPENAI_TIMEOUT_SECONDS: int = int(os.getenv("OPENAI_TIMEOUT_SECONDS", "20"))

# If true, LLM summarize failures fall back to deterministic summaries.
LANGGRAPH_SUMMARIZER_FALLBACK: bool = os.getenv(
    "LANGGRAPH_SUMMARIZER_FALLBACK", "false"
).strip().lower() in {"1", "true", "yes", "on"}

# ─── LangGraph Search Mode (Phase 4.2) ──────────────────────────────────────
# Controls SEARCH behavior when WORKER_EXECUTION_MODE=langgraph:
#   - "web"           : live web search via DuckDuckGo API
#   - "deterministic" : local deterministic payload (testing/fallback)
LANGGRAPH_SEARCH_MODE: str = os.getenv("LANGGRAPH_SEARCH_MODE", "web").strip().lower()
LANGGRAPH_SEARCH_TIMEOUT_SECONDS: int = int(os.getenv("LANGGRAPH_SEARCH_TIMEOUT_SECONDS", "15"))
LANGGRAPH_SEARCH_MAX_RESULTS: int = int(os.getenv("LANGGRAPH_SEARCH_MAX_RESULTS", "5"))
LANGGRAPH_SEARCH_FALLBACK: bool = os.getenv(
    "LANGGRAPH_SEARCH_FALLBACK", "false"
).strip().lower() in {"1", "true", "yes", "on"}

# ─── LangGraph Save Mode (Phase 4.3) ────────────────────────────────────────
# Controls SAVE behavior when WORKER_EXECUTION_MODE=langgraph:
#   - "sqlite"        : persists results in local sqlite DB
#   - "deterministic" : local mock response (testing/fallback)
LANGGRAPH_SAVE_MODE: str = os.getenv("LANGGRAPH_SAVE_MODE", "sqlite").strip().lower()
RESULTS_DB_PATH: str = os.getenv("RESULTS_DB_PATH", "data/results.db").strip()
