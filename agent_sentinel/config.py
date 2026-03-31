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
