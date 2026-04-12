import time
from typing import Optional

from pysyncobj.batteries import ReplDict

from agent_sentinel.config import LEASE_DURATION_SECONDS, MAX_RETRIES


class NotLeaderError(Exception):
    """Raised when a write is attempted on a non-leader node."""
    pass


class StaleTokenError(Exception):
    """Raised when version_token does not match the current lease (fencing)."""
    pass


class TaskRegistry(ReplDict):
    """
    Replicated task registry backed by Raft via pysyncobj.

    Pattern used throughout:
      - Public method  : validates inputs, checks leader, computes the full
                         updated record (including timestamps), then calls the
                         matching @replicated method.
      - @replicated method: receives the already-computed record as an argument
                         and writes it directly to the dict. Runs on ALL nodes
                         in Raft log order — making every mutation atomic.

    Why pass the full record as an argument?
      @replicated methods execute on every node. If we called time.time() or
      read self[task_id] inside them, each node would get different values
      (clock skew, timing). By computing everything on the leader first and
      passing it in, all nodes apply exactly the same bytes.
    """

    # ─── Internal helper ─────────────────────────────────────────────────────

    def _require_leader(self) -> None:
        """Raise NotLeaderError if this node is not the current Raft leader."""
        if self._syncObj is None:
            raise NotLeaderError("Registry is not attached to a SyncObj node.")
        if not self._syncObj._isLeader():
            leader = self._syncObj._getLeader()
            leader_addr = leader.address if leader is not None else "unknown"
            self_addr = self._syncObj.selfNode.address
            raise NotLeaderError(
                f"This node ({self_addr}) is not the leader. "
                f"Current leader: {leader_addr}"
            )

    def _require_running_lease(
        self, task_id: str, record: dict, worker_id: str, version_token: int
    ) -> None:
        """Ensure task is RUNNING, leased to worker_id, and token matches (strict fencing)."""
        if record["status"] != "RUNNING":
            raise ValueError(
                f"Task '{task_id}' is not RUNNING (status={record['status']})."
            )
        if record["worker_id"] != worker_id:
            raise ValueError(
                f"Task '{task_id}' is not leased to worker {worker_id!r}."
            )
        if version_token != record["version_token"]:
            raise StaleTokenError(
                f"Invalid token for '{task_id}': got {version_token}, "
                f"expected {record['version_token']}."
            )

    # ─── Replication primitive (applied on ALL nodes via Raft log) ────────────

    def _apply_record(self, task_id: str, record: dict) -> None:
        """
        Write a fully-computed TaskRecord via ReplDict's built-in replicated set.

        Note: ReplDict.__setitem__/set are already @replicated. Wrapping another
        @replicated method around them prevents writes from being applied.
        """
        # Block until the command is committed so RPC handlers don't return
        # success before the replicated write is actually durable/visible.
        self.set(task_id, record, sync=True, timeout=10)

    # ─── Public write API (leader-only entry points) ─────────────────────────

    def add_task(self, task_id: str, metadata: Optional[dict] = None) -> None:
        """Add a new PENDING task. Must be called on the leader."""
        self._require_leader()
        if self.get(task_id) is not None:
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
        self._apply_record(task_id, record)

    def acquire_task(self, task_id: str, worker_id: str) -> dict:
        """
        Assign a PENDING task to a worker. Increments version_token (fencing)
        and stamps the lease. Returns the updated TaskRecord.
        Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None:
            raise KeyError(f"Task '{task_id}' not found.")
        if record["status"] != "PENDING":
            raise ValueError(
                f"Task '{task_id}' is not PENDING (status={record['status']})."
            )
        now = time.time()
        updated = {
            **record,
            "status": "RUNNING",
            "worker_id": worker_id,
            "version_token": record["version_token"] + 1,
            "lease_expires_at": now + LEASE_DURATION_SECONDS,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)
        return updated

    def renew_lease(self, task_id: str, worker_id: str, version_token: int) -> None:
        """
        Extend lease on SendHeartbeat. Requires RUNNING, matching worker_id,
        and exact version_token (fencing). Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None:
            raise KeyError(f"Task '{task_id}' not found.")
        self._require_running_lease(task_id, record, worker_id, version_token)
        now = time.time()
        updated = {
            **record,
            "lease_expires_at": now + LEASE_DURATION_SECONDS,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)

    def commit_state(
        self,
        task_id: str,
        worker_id: str,
        version_token: int,
        checkpoint_json: str,
    ) -> None:
        """
        Persist an agent checkpoint. Requires RUNNING, matching worker_id,
        and exact version_token (fencing). Renews lease on success.
        Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None:
            raise KeyError(f"Task '{task_id}' not found.")
        self._require_running_lease(task_id, record, worker_id, version_token)
        now = time.time()
        updated = {
            **record,
            "checkpoint_json": checkpoint_json,
            "lease_expires_at": now + LEASE_DURATION_SECONDS,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)

    def complete_task(self, task_id: str, worker_id: str, version_token: int) -> None:
        """
        Mark a task COMPLETED. Requires RUNNING, matching worker_id,
        and exact version_token. Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None:
            raise KeyError(f"Task '{task_id}' not found.")
        self._require_running_lease(task_id, record, worker_id, version_token)
        now = time.time()
        updated = {
            **record,
            "status": "COMPLETED",
            "lease_expires_at": 0.0,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)

    def fail_task(self, task_id: str) -> None:
        """
        Increment error_count. If >= MAX_RETRIES mark FAILED permanently
        (poison pill guard), otherwise reset to PENDING.
        Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None:
            raise KeyError(f"Task '{task_id}' not found.")
        now = time.time()
        new_error_count = record["error_count"] + 1
        new_status = "FAILED" if new_error_count >= MAX_RETRIES else "PENDING"
        updated = {
            **record,
            "status": new_status,
            "worker_id": None,
            "lease_expires_at": 0.0,
            "error_count": new_error_count,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)

    def expire_lease(self, task_id: str) -> None:
        """
        Called by LeaseManager when a lease expires. Resets task to PENDING
        so another worker can pick it up. Must be called on the leader.
        """
        self._require_leader()
        record = self.get(task_id)
        if record is None or record["status"] != "RUNNING":
            return
        now = time.time()
        new_error_count = record["error_count"] + 1
        new_status = "FAILED" if new_error_count >= MAX_RETRIES else "PENDING"
        updated = {
            **record,
            "status": new_status,
            "worker_id": None,
            "lease_expires_at": 0.0,
            "error_count": new_error_count,
            "updated_at": now,
        }
        self._apply_record(task_id, updated)

    # ─── Read operations (local, no consensus needed) ─────────────────────────

    def get_task(self, task_id: str) -> Optional[dict]:
        """Return a TaskRecord by ID, or None if not found."""
        return self.get(task_id)

    def list_tasks(self, status: Optional[str] = None) -> list[dict]:
        """Return all tasks, optionally filtered by status."""
        tasks = list(self.rawData().values())
        if status is not None:
            tasks = [t for t in tasks if t["status"] == status]
        return tasks

    def get_pending_task(self) -> Optional[dict]:
        """Return the oldest PENDING task, or None if the queue is empty."""
        pending = sorted(
            [t for t in self.rawData().values() if t["status"] == "PENDING"],
            key=lambda t: t["created_at"],
        )
        return pending[0] if pending else None
