const state = {
  eventSource: null,
};

function isTerminalStatus(status) {
  return ["completed", "failed", "stopped", "skipped"].includes(status || "");
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value ?? "-";
  }
}

function statusClass(status) {
  return `status-${status || "pending"}`;
}

function formatDuration(seconds) {
  if (seconds == null || Number.isNaN(seconds)) {
    return "-";
  }
  if (seconds < 60) {
    return `${Math.round(seconds)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

function formatUsd(value) {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  return `$${Number(value).toFixed(value < 0.01 ? 4 : 2)}`;
}

function renderStages(pipeline) {
  const flow = document.getElementById("stage-flow");
  const stages = Object.values(pipeline.stages || {});
  const durations = pipeline.telemetry?.stage_durations || {};
  flow.innerHTML = stages.map((stage) => `
    <article class="stage-card">
      <span class="stage-status ${statusClass(stage.status)}">${stage.status}</span>
      <h3>${stage.label}</h3>
      <p>${stage.message || "waiting"}</p>
      <p class="stage-meta">duration: ${formatDuration(durations[stage.key]?.duration_seconds)}</p>
    </article>
  `).join("");
}

function renderTickers(pipeline) {
  const grid = document.getElementById("ticker-grid");
  const tickers = pipeline.tickers || [];
  const telemetry = pipeline.telemetry?.ticker_durations || {};
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
      <p class="ticker-meta">duration: ${formatDuration(telemetry[ticker.symbol]?.duration_seconds)}</p>
      <p class="ticker-meta">cost: ${formatUsd(telemetry[ticker.symbol]?.actual_cost_usd)}</p>
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
  const stopRequested = Boolean(pipeline.stop_requested);

  setText("run-status", status);
  setText("run-session", pipeline.session_name || "-");
  setText("run-id", pipeline.run_id || "-");
  setText("run-current", pipeline.current_ticker || pipeline.current_stage || "-");
  setText("run-stop", stopRequested ? `requested at ${pipeline.stop_requested_at || "-"}` : "not requested");
  setText("model-quick", pipeline.llm_settings?.quick_model || "-");
  setText("model-selection", pipeline.llm_settings?.selection_model || "-");
  setText("model-analyst", pipeline.llm_settings?.analyst_model || "-");
  setText("model-fundamentals", pipeline.llm_settings?.fundamentals_model || "-");
  setText("model-research", pipeline.llm_settings?.research_model || "-");
  setText("model-trader", pipeline.llm_settings?.trader_model || "-");
  setText("model-risk", pipeline.llm_settings?.risk_model || "-");
  setText("model-manager", pipeline.llm_settings?.manager_model || "-");
  setText("model-deep", pipeline.llm_settings?.deep_model || "-");
  setText("discovery-mode", pipeline.discovery_context?.universe_mode || "-");
  setText("discovery-selected", (pipeline.discovery_context?.selected_symbols || []).join(", ") || "-");
  setText("discovery-held", (pipeline.discovery_context?.held_symbols || []).join(", ") || "-");
  setText("discovery-reason", pipeline.discovery_context?.selection_reason || "-");
  setText("pipeline-totals", `${pipeline.tickers_completed || 0} / ${pipeline.tickers_total || 0} tickers completed`);
  setText("telemetry-run-duration", formatDuration(pipeline.telemetry?.run_duration_seconds));
  setText("telemetry-completed-cost", formatUsd(pipeline.telemetry?.completed_actual_cost_usd));
  setText("telemetry-remaining-cost", pipeline.telemetry?.total_tokens ?? "-");
  setText("telemetry-total-cost", formatUsd(pipeline.telemetry?.final_actual_cost_usd));

  const badge = document.getElementById("connection-badge");
  badge.textContent = status;
  badge.className = `badge badge-${status === "running" ? "running" : status === "completed" ? "completed" : status === "failed" ? "failed" : status === "stopped" ? "stopped" : "idle"}`;

  const triggerButton = document.getElementById("trigger-button");
  const stopButton = document.getElementById("stop-button");
  triggerButton.disabled = status === "running";
  stopButton.disabled = status !== "running" || stopRequested || isTerminalStatus(status);

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

async function stopRun() {
  const response = await fetch("/stop?reason=dashboard_safe_stop", { method: "POST" });
  const payload = await response.json();
  if (!response.ok && response.status !== 202 && response.status !== 409) {
    throw new Error(payload.reason || `stop ${response.status}`);
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

document.getElementById("stop-button").addEventListener("click", () => {
  stopRun().catch((error) => {
    console.error(error);
  });
});

fetchPipeline().catch((error) => {
  console.error(error);
});
connectEvents();