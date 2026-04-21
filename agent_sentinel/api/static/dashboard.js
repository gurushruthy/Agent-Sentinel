const el = {
  leaderPill: document.getElementById("leader-pill"),
  nodeGrid: document.getElementById("node-grid"),
  taskForm: document.getElementById("task-form"),
  queryInput: document.getElementById("query-input"),
  metaInput: document.getElementById("meta-input"),
  submitBtn: document.getElementById("submit-btn"),
  submitStatus: document.getElementById("submit-status"),
  statusFilter: document.getElementById("status-filter"),
  refreshBtn: document.getElementById("refresh-btn"),
  taskList: document.getElementById("task-list"),
  detail: document.getElementById("task-detail"),
  detailEmpty: document.getElementById("task-detail-empty"),
  taskMeta: document.getElementById("task-meta"),
  toolResults: document.getElementById("tool-results"),
  checkpointJson: document.getElementById("checkpoint-json"),
};

let selectedTaskId = null;

function fmt(obj) {
  return JSON.stringify(obj, null, 2);
}

function statusBadge(status) {
  if (status === "COMPLETED") return "ok";
  if (status === "RUNNING") return "warn";
  if (status === "FAILED") return "bad";
  return "warn";
}

async function api(path, opts = {}) {
  const res = await fetch(path, opts);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

async function loadCluster() {
  try {
    const data = await api("/cluster/status");
    el.leaderPill.textContent = data.leader_port
      ? `Leader: ${data.leader_grpc_address}`
      : "Leader: none";
    el.nodeGrid.innerHTML = "";
    data.nodes.forEach((n) => {
      const card = document.createElement("article");
      card.className = "node-card";
      card.innerHTML = `
        <h3>Node ${n.node_id}</h3>
        <div class="node-kv"><strong>Raft</strong>: ${n.raft_address}</div>
        <div class="node-kv"><strong>gRPC</strong>: ${n.grpc_address}</div>
        <div class="node-kv"><span class="badge ${n.grpc_reachable ? "ok" : "warn"}">${n.role_hint}</span></div>
      `;
      el.nodeGrid.appendChild(card);
    });
  } catch (err) {
    el.leaderPill.textContent = `Leader: unavailable (${err.message})`;
  }
}

function normalizeTask(t) {
  const cp = t.checkpoint_json ? JSON.parse(t.checkpoint_json) : {};
  return { ...t, checkpoint: cp };
}

async function loadTasks() {
  const status = el.statusFilter.value;
  const q = status ? `?status=${encodeURIComponent(status)}&limit=50&offset=0` : "?limit=50&offset=0";
  try {
    const data = await api(`/tasks${q}`);
    el.taskList.innerHTML = "";
    if (!data.tasks.length) {
      el.taskList.innerHTML = `<div class="empty-state">No tasks found.</div>`;
      return;
    }
    data.tasks.forEach((t) => {
      const row = document.createElement("div");
      row.className = "task-row";
      row.innerHTML = `
        <div>
          <div class="task-id">${t.task_id}</div>
          <div class="task-sub">status=${t.status} worker=${t.worker_id ?? "-"}</div>
        </div>
        <div>
          <span class="badge ${statusBadge(t.status)}">${t.status}</span>
          <button data-task="${t.task_id}" type="button">Inspect</button>
        </div>
      `;
      row.querySelector("button").addEventListener("click", () => {
        selectedTaskId = t.task_id;
        loadTaskDetail();
      });
      el.taskList.appendChild(row);
    });
  } catch (err) {
    el.taskList.innerHTML = `<div class="empty-state">Failed to load tasks: ${err.message}</div>`;
  }
}

async function loadTaskDetail() {
  if (!selectedTaskId) return;
  try {
    const data = await api(`/tasks/${encodeURIComponent(selectedTaskId)}`);
    if (!data.found) {
      el.detail.classList.add("hidden");
      el.detailEmpty.classList.remove("hidden");
      el.detailEmpty.textContent = data.message || "Task not found.";
      return;
    }
    const task = normalizeTask(data);
    el.detailEmpty.classList.add("hidden");
    el.detail.classList.remove("hidden");
    el.taskMeta.innerHTML = `
      <span><strong>task_id:</strong> ${task.task_id}</span>
      <span><strong>status:</strong> ${task.status}</span>
      <span><strong>worker:</strong> ${task.worker_id ?? "-"}</span>
      <span><strong>token:</strong> ${task.version_token ?? "-"}</span>
    `;
    el.toolResults.textContent = fmt(task.tool_results || task.checkpoint.tool_results || {});
    el.checkpointJson.textContent = task.checkpoint_json || "{}";
  } catch (err) {
    el.detailEmpty.classList.remove("hidden");
    el.detail.classList.add("hidden");
    el.detailEmpty.textContent = `Failed to load task: ${err.message}`;
  }
}

async function submitTask(e) {
  e.preventDefault();
  const query = el.queryInput.value.trim();
  if (!query) return;

  let metadata = {};
  const rawMeta = el.metaInput.value.trim();
  if (rawMeta) {
    try {
      metadata = JSON.parse(rawMeta);
    } catch {
      el.submitStatus.textContent = "Metadata JSON is invalid.";
      return;
    }
  }

  el.submitBtn.disabled = true;
  el.submitStatus.textContent = "Submitting...";

  try {
    const resp = await api("/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, metadata }),
    });
    el.submitStatus.textContent = `Accepted: ${resp.task_id}`;
    selectedTaskId = resp.task_id;
    await loadTasks();
    await loadTaskDetail();
    el.queryInput.value = "";
  } catch (err) {
    el.submitStatus.textContent = `Submit failed: ${err.message}`;
  } finally {
    el.submitBtn.disabled = false;
  }
}

function startRefreshLoops() {
  loadCluster();
  loadTasks();
  setInterval(loadCluster, 3000);
  setInterval(loadTasks, 4000);
  setInterval(loadTaskDetail, 2500);
}

el.taskForm.addEventListener("submit", submitTask);
el.statusFilter.addEventListener("change", loadTasks);
el.refreshBtn.addEventListener("click", () => {
  loadCluster();
  loadTasks();
  loadTaskDetail();
});

startRefreshLoops();
