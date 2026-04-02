const state = {
  eventSource: null,
};

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value ?? "-";
  }
}

function statusClass(status) {
  return `status-${status || "pending"}`;
}

function renderStages(pipeline) {
  const flow = document.getElementById("stage-flow");
  const stages = Object.values(pipeline.stages || {});
  flow.innerHTML = stages.map((stage) => `
    <article class="stage-card">
      <span class="stage-status ${statusClass(stage.status)}">${stage.status}</span>
      <h3>${stage.label}</h3>
      <p>${stage.message || "waiting"}</p>
    </article>
  `).join("");
}

function renderTickers(pipeline) {
  const grid = document.getElementById("ticker-grid");
  const tickers = pipeline.tickers || [];
  if (!tickers.length) {
    grid.innerHTML = '<p class="subtle">No tickers yet.</p>';
    return;
  }

  grid.innerHTML = tickers.map((ticker) => `
    <article class="ticker-card">
      <span class="ticker-status ${statusClass(ticker.status)}">${ticker.status}</span>
      <h3>${ticker.symbol}</h3>
      <p class="ticker-meta">phase: ${ticker.phase || "pending"}</p>
      <p class="ticker-meta">signal: ${ticker.signal || "-"}</p>
      <p class="ticker-meta">action: ${ticker.action || "-"}</p>
    </article>
  `).join("");
}

function renderEvents(pipeline) {
  const list = document.getElementById("event-list");
  const events = [...(pipeline.recent_events || [])].reverse();
  if (!events.length) {
    list.innerHTML = '<p class="subtle">No events yet.</p>';
    return;
  }

  list.innerHTML = events.map((event) => `
    <article class="event-card">
      <strong>${event.type}</strong>
      <p>${event.message || "-"}</p>
      <p>${event.timestamp || "-"}</p>
    </article>
  `).join("");
}

function renderPipeline(payload) {
  const pipeline = payload.pipeline || {};
  const status = pipeline.status || "idle";

  setText("run-status", status);
  setText("run-session", pipeline.session_name || "-");
  setText("run-id", pipeline.run_id || "-");
  setText("run-current", pipeline.current_ticker || pipeline.current_stage || "-");
  setText("model-quick", pipeline.llm_settings?.quick_model || "-");
  setText("model-selection", pipeline.llm_settings?.selection_model || "-");
  setText("model-fundamentals", pipeline.llm_settings?.fundamentals_model || "-");
  setText("model-deep", pipeline.llm_settings?.deep_model || "-");
  setText("discovery-mode", pipeline.discovery_context?.universe_mode || "-");
  setText("discovery-selected", (pipeline.discovery_context?.selected_symbols || []).join(", ") || "-");
  setText("discovery-held", (pipeline.discovery_context?.held_symbols || []).join(", ") || "-");
  setText("discovery-reason", pipeline.discovery_context?.selection_reason || "-");
  setText("pipeline-totals", `${pipeline.tickers_completed || 0} / ${pipeline.tickers_total || 0} tickers completed`);

  const badge = document.getElementById("connection-badge");
  badge.textContent = status;
  badge.className = `badge badge-${status === "running" ? "running" : status === "completed" ? "completed" : status === "failed" ? "failed" : "idle"}`;

  const triggerButton = document.getElementById("trigger-button");
  triggerButton.disabled = status === "running";

  renderStages(pipeline);
  renderTickers(pipeline);
  renderEvents(pipeline);
}

async function fetchPipeline() {
  const response = await fetch("/pipeline-state", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`pipeline-state ${response.status}`);
  }
  const payload = await response.json();
  renderPipeline(payload);
}

async function triggerRun() {
  const response = await fetch("/trigger?session=dashboard", { method: "POST" });
  const payload = await response.json();
  if (!response.ok && response.status !== 202 && response.status !== 409) {
    throw new Error(payload.reason || `trigger ${response.status}`);
  }
  await fetchPipeline();
}

function connectEvents() {
  if (!("EventSource" in window)) {
    setInterval(() => {
      fetchPipeline().catch(() => {});
    }, 3000);
    return;
  }

  state.eventSource = new EventSource("/events/pipeline");
  state.eventSource.addEventListener("pipeline", (event) => {
    renderPipeline(JSON.parse(event.data));
  });
  state.eventSource.onerror = () => {
    state.eventSource.close();
    setTimeout(connectEvents, 3000);
  };
}

document.getElementById("trigger-button").addEventListener("click", () => {
  triggerRun().catch((error) => {
    console.error(error);
  });
});

fetchPipeline().catch((error) => {
  console.error(error);
});
connectEvents();