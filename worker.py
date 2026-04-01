"""
TradingAgents scheduled worker.

Runs daily after NYSE close (16:05 ET = 21:05 UTC) and:
  1. Generates trading signals via TradingAgentsGraph (OpenRouter / z-ai models)
  2. Sends executable orders to Alpaca paper account via AlpacaExecutor

Configuration via environment variables:
    LLM_PROVIDER                optional (openai/google/anthropic/xai/openrouter/ollama)
    BACKEND_URL                 optional custom provider base URL
    OPENAI_API_KEY              optional when using OpenAI
    GOOGLE_API_KEY              optional when using Google
    ANTHROPIC_API_KEY           optional when using Anthropic
    XAI_API_KEY                 optional when using xAI
    OPENROUTER_API_KEY          optional when using OpenRouter
  ALPACA_API_KEY              required
  ALPACA_SECRET_KEY           required
  ALPACA_BASE_URL             optional (default: paper endpoint)
  TICKERS                     comma-separated list (default: SPY)
    UNIVERSE_MODE               fixed/sp500 (default: fixed)
    SCREEN_TOP_N                ranked candidate count for sp500 mode (default: 8)
    SCREEN_LOOKBACK_DAYS        lookback window for screener metrics (default: 180)
    SCREEN_BATCH_SIZE           batch size for yfinance downloads (default: 100)
    MIN_AVG_DOLLAR_VOLUME_USD   liquidity floor for screener (default: 50000000)
    PORTFOLIO_INCLUDE_OPEN_POSITIONS  true/false include current positions in analysis set
    MAX_NEW_BUYS_PER_RUN        cap new entries per session (default: 3)
  TRADING_ENABLED             true/false (default: true)
  MAX_ORDER_VALUE_USD         per-order notional cap (default: 500)
    QUICK_MODEL                 provider model id for quick tasks
    DEEP_MODEL                  provider model id for complex reasoning
        FUNDAMENTALS_MODEL          optional model override for fundamentals analyst
    RUN_ON_START                true/false to execute one cloud-side scan on service boot
        SCHEDULE_TIMES_UTC          optional CSV schedule like 14:00,17:00,19:45
  SCHEDULE_HOUR_UTC           UTC hour to run (default: 21)
  SCHEDULE_MINUTE_UTC         UTC minute to run (default: 5)
  LOG_LEVEL                   DEBUG/INFO/WARNING (default: INFO)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlparse

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
import requests


def _normalize_env_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    raw = value.strip()
    normalized_chars: list[str] = []
    in_single_quote = False
    in_double_quote = False
    for index, char in enumerate(raw):
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif char == "#" and not in_single_quote and not in_double_quote:
            if index == 0 or raw[index - 1].isspace():
                break
        normalized_chars.append(char)

    normalized = "".join(normalized_chars).strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
        normalized = normalized[1:-1].strip()
    return normalized


def _get_env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    value = _normalize_env_value(os.environ.get(name))
    if value in (None, ""):
        return default
    return value


def _get_env_bool(name: str, default: bool = False) -> bool:
    value = _get_env_str(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _get_env_int(name: str, default: int) -> int:
    value = _get_env_str(name)
    if value is None:
        return default
    return int(value)


def _get_env_float(name: str, default: float) -> float:
    value = _get_env_str(name)
    if value is None:
        return default
    return float(value)


load_dotenv(dotenv_path=".env")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = (_get_env_str("LOG_LEVEL", "INFO") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("worker")

_RUN_LOCK = threading.Lock()
_RUN_STATE_LOCK = threading.Lock()
_RUN_STATE = {
    "active": False,
    "current": None,
    "last": None,
}


@dataclass(frozen=True)
class SessionSchedule:
    name: str
    hour: int
    minute: int


class TelegramNotifier:
    """Send compact post-run summaries to configured Telegram chat IDs."""

    def __init__(self) -> None:
        self.enabled = _get_env_bool("TELEGRAM_ENABLED", False)
        self.token = _get_env_str("TELEGRAM_BOT_TOKEN", "") or ""
        raw_ids = _get_env_str("TELEGRAM_CHAT_IDS", "") or ""
        self.chat_ids = [x.strip() for x in raw_ids.split(",") if x.strip()]

    def is_configured(self) -> bool:
        return self.enabled and bool(self.token) and bool(self.chat_ids)

    def send(self, message: str) -> None:
        if not self.is_configured():
            logger.info("Telegram notifier is disabled or not configured.")
            return

        # Telegram hard limit is 4096 chars per message.
        text = message[:3900]
        logger.info(
            "Telegram: starting send to %d chat_id(s), message_size=%d bytes",
            len(self.chat_ids),
            len(text),
        )
        logger.debug("Telegram: message preview (first 200 chars):\n%s", text[:200])
        
        for idx, chat_id in enumerate(self.chat_ids, 1):
            try:
                logger.info(
                    "Telegram: sending to chat_id=%s (%d/%d)...",
                    chat_id,
                    idx,
                    len(self.chat_ids),
                )
                resp = requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "disable_web_page_preview": True,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                result = resp.json()
                msg_id = result.get("result", {}).get("message_id", "?")
                logger.info(
                    "Telegram: SUCCESS sent to chat_id=%s message_id=%s",
                    chat_id,
                    msg_id,
                )
            except Exception as exc:
                logger.error(
                    "Telegram: FAILED to send to chat_id=%s: %s",
                    chat_id,
                    exc,
                    exc_info=True,
                )


def _fmt_money(value: Optional[str]) -> str:
    if value is None:
        return "n/a"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _build_telegram_summary(
    run_date: str,
    session_name: str,
    run_id: str,
    tickers: list[str],
    discovery_context: dict,
    run_results: list[dict],
    account: dict,
    positions: list[dict],
    llm_settings: dict,
) -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "TradingAgents Gunluk Ozet",
        "-------------------------",
        f"Tetik tarihi: {run_date}",
        f"Seans: {session_name}",
        f"Calisma kimligi: {run_id}",
        f"Rapor zamani: {now_utc}",
        f"Evren modu: {discovery_context.get('universe_mode', 'fixed')}",
        f"Tickerlar: {', '.join(tickers)}",
        "",
        "Agentlar / Modeller / Gorevler",
        "- market/social/news/bull/bear/trader/risk -> QUICK_MODEL",
        "- fundamentals -> FUNDAMENTALS_MODEL",
        "- research_manager/portfolio_manager -> DEEP_MODEL",
        f"LLM_PROVIDER: {llm_settings['provider']}",
        f"BACKEND_URL: {llm_settings['backend_url']}",
        f"QUICK_MODEL: {llm_settings['quick_model']}",
        f"FUNDAMENTALS_MODEL: {llm_settings['fundamentals_model']}",
        f"DEEP_MODEL: {llm_settings['deep_model']}",
        "",
        "Aday Secimi",
    ]

    ranked_candidates = discovery_context.get("ranked_candidates", [])
    held_symbols = discovery_context.get("held_symbols", [])
    if ranked_candidates:
        lines.append(
            "- screened: "
            + ", ".join(
                f"{item['symbol']}({item['score']:.3f})"
                for item in ranked_candidates[:8]
            )
        )
    if held_symbols:
        lines.append(f"- held: {', '.join(held_symbols)}")
    if discovery_context.get("used_explicit_fallback"):
        lines.append("- fallback: explicit TICKERS env used after screener failure")

    lines.extend(
        [
        "",
        "Onemli kararlar / Sonuclar",
        ]
    )

    for item in run_results:
        action = item.get("action", "NO_ACTION")
        reason = item.get("reason")
        reason_suffix = f" reason={reason}" if reason else ""
        lines.append(
            f"- {item.get('ticker')}: signal={item.get('signal')} action={action}{reason_suffix} log={item.get('log_path', 'n/a')}"
        )

    lines.extend(
        [
            "",
            "Alpaca Hesap Ozeti",
            f"- status: {account.get('status', 'n/a')}",
            f"- equity: {_fmt_money(account.get('equity'))}",
            f"- cash: {_fmt_money(account.get('cash'))}",
            f"- buying_power: {_fmt_money(account.get('buying_power'))}",
            f"- portfolio_value: {_fmt_money(account.get('portfolio_value'))}",
            "",
            "Acilik Pozisyonlar",
        ]
    )

    if not positions:
        lines.append("- pozisyon yok")
    else:
        for pos in positions[:8]:
            lines.append(
                f"- {pos.get('symbol')}: qty={pos.get('qty')} mv={_fmt_money(pos.get('market_value'))}"
            )
        if len(positions) > 8:
            lines.append(f"- ... +{len(positions) - 8} pozisyon")

    return "\n".join(lines)


def _parse_schedule_time(value: str) -> tuple[int, int]:
    raw = value.strip()
    parts = raw.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid UTC schedule time: {value}")

    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid UTC schedule time: {value}")
    return hour, minute


def _default_session_name(index: int, total: int) -> str:
    if total == 3:
        return ("open", "midday", "close")[index]
    return f"session_{index + 1}"


def _get_schedule_sessions() -> list[SessionSchedule]:
    schedule_csv = _get_env_str("SCHEDULE_TIMES_UTC", "") or ""
    if schedule_csv:
        raw_times = [item.strip() for item in schedule_csv.split(",") if item.strip()]
        sessions = []
        for index, raw_time in enumerate(raw_times):
            hour, minute = _parse_schedule_time(raw_time)
            sessions.append(
                SessionSchedule(
                    name=_default_session_name(index, len(raw_times)),
                    hour=hour,
                    minute=minute,
                )
            )
        return sessions

    hour = _get_env_int("SCHEDULE_HOUR_UTC", 21)
    minute = _get_env_int("SCHEDULE_MINUTE_UTC", 5)
    return [SessionSchedule(name="daily", hour=hour, minute=minute)]


def _build_run_id(run_date: str, session_name: str, started_at: datetime) -> str:
    ts = started_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    return f"{run_date}_{session_name}_{ts}"


def _parse_tickers(raw_value: str) -> list[str]:
    return [ticker.strip().upper() for ticker in raw_value.split(",") if ticker.strip()]


def _get_explicit_tickers() -> list[str]:
    return _parse_tickers(_get_env_str("TICKERS", "SPY") or "SPY")


def _get_open_position_tickers(executor) -> list[str]:
    if not _get_env_bool("PORTFOLIO_INCLUDE_OPEN_POSITIONS", True):
        return []

    try:
        positions = executor.get_positions()
    except Exception as exc:
        logger.warning("Could not read open positions for portfolio inclusion: %s", exc)
        return []

    tickers = []
    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        qty = position.get("qty")
        if symbol and qty not in (None, "0", "0.0"):
            tickers.append(symbol)
    return tickers


def _discover_market_candidates(trade_date: str) -> list[dict]:
    from tradingagents.screeners.ranker import rank_sp500_candidates

    top_n = _get_env_int("SCREEN_TOP_N", 8)
    lookback_days = _get_env_int("SCREEN_LOOKBACK_DAYS", 180)
    batch_size = _get_env_int("SCREEN_BATCH_SIZE", 100)
    min_avg_dollar_volume = _get_env_float("MIN_AVG_DOLLAR_VOLUME_USD", 50000000)
    return rank_sp500_candidates(
        as_of_date=trade_date,
        top_n=top_n,
        lookback_days=lookback_days,
        batch_size=batch_size,
        min_avg_dollar_volume=min_avg_dollar_volume,
    )


def _merge_ticker_lists(primary: list[str], secondary: list[str]) -> list[str]:
    merged = []
    seen = set()
    for ticker in primary + secondary:
        if ticker and ticker not in seen:
            seen.add(ticker)
            merged.append(ticker)
    return merged


def _resolve_analysis_tickers(executor, trade_date: str) -> tuple[list[str], dict]:
    universe_mode = (_get_env_str("UNIVERSE_MODE", "fixed") or "fixed").lower()
    explicit_tickers = _get_explicit_tickers()

    if universe_mode != "sp500":
        return explicit_tickers, {
            "universe_mode": "fixed",
            "ranked_candidates": [],
            "held_symbols": _get_open_position_tickers(executor),
            "used_explicit_fallback": False,
        }

    held_symbols = _get_open_position_tickers(executor)
    try:
        ranked_candidates = _discover_market_candidates(trade_date)
        candidate_symbols = [item["symbol"] for item in ranked_candidates]
        tickers = _merge_ticker_lists(held_symbols, candidate_symbols)
        return tickers, {
            "universe_mode": "sp500",
            "ranked_candidates": ranked_candidates,
            "held_symbols": held_symbols,
            "used_explicit_fallback": False,
        }
    except Exception as exc:
        logger.warning("Screener failed, falling back to explicit tickers: %s", exc)
        tickers = _merge_ticker_lists(held_symbols, explicit_tickers)
        return tickers, {
            "universe_mode": "sp500",
            "ranked_candidates": [],
            "held_symbols": held_symbols,
            "used_explicit_fallback": True,
        }

# ---------------------------------------------------------------------------
# Lazy-import heavy deps (LangGraph, yfinance, etc.) only when job runs
# ---------------------------------------------------------------------------

def _resolve_llm_settings() -> dict:
    from tradingagents.default_config import DEFAULT_CONFIG

    env_provider = (_get_env_str("LLM_PROVIDER", "") or "").lower()
    env_backend_url = _get_env_str("BACKEND_URL", "") or ""

    if env_provider:
        provider = env_provider
    elif _get_env_str("OPENROUTER_API_KEY", ""):
        provider = "openrouter"
    else:
        provider = DEFAULT_CONFIG["llm_provider"]

    if env_backend_url:
        backend_url = env_backend_url
    elif provider == "openrouter":
        backend_url = "https://openrouter.ai/api/v1"
    else:
        backend_url = DEFAULT_CONFIG.get("backend_url")

    default_quick_model = DEFAULT_CONFIG["quick_think_llm"]
    default_deep_model = DEFAULT_CONFIG["deep_think_llm"]
    default_fundamentals_model = default_deep_model

    if provider == "openrouter":
        default_quick_model = "anthropic/claude-sonnet-4.6"
        default_deep_model = "openai/gpt-5.4"
        default_fundamentals_model = "google/gemini-3.1-pro-preview"

    return {
        "provider": provider,
        "backend_url": backend_url,
        "quick_model": _get_env_str("QUICK_MODEL", default_quick_model),
        "fundamentals_model": _get_env_str(
            "FUNDAMENTALS_MODEL", default_fundamentals_model
        ),
        "deep_model": _get_env_str("DEEP_MODEL", default_deep_model),
    }

def _build_ta_config() -> dict:
    from tradingagents.default_config import DEFAULT_CONFIG

    llm_settings = _resolve_llm_settings()

    cfg = DEFAULT_CONFIG.copy()
    cfg["llm_provider"] = llm_settings["provider"]
    cfg["backend_url"] = llm_settings["backend_url"]
    cfg["quick_think_llm"] = llm_settings["quick_model"]
    cfg["deep_think_llm"] = llm_settings["deep_model"]
    cfg["role_llm_models"] = {
        "fundamentals": llm_settings["fundamentals_model"],
    }
    cfg["max_debate_rounds"] = 1
    cfg["max_risk_discuss_rounds"] = 1
    cfg["data_vendors"] = {
        "core_stock_apis": "yfinance",
        "technical_indicators": "yfinance",
        "fundamental_data": "yfinance",
        "news_data": "yfinance",
    }
    return cfg


def _run_analysis(ticker: str, trade_date: str, run_id: str) -> str:
    """Run full TradingAgentsGraph and return processed signal string."""
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    cfg = _build_ta_config()
    cfg["log_file_suffix"] = run_id
    ta = TradingAgentsGraph(
        debug=False,
        config=cfg,
        selected_analysts=["market", "social", "news", "fundamentals"],
    )
    _, signal = ta.propagate(ticker, trade_date)
    return (signal or "HOLD").strip().upper()


def _snapshot_run_state() -> dict:
    with _RUN_STATE_LOCK:
        current = dict(_RUN_STATE["current"]) if _RUN_STATE["current"] else None
        last = dict(_RUN_STATE["last"]) if _RUN_STATE["last"] else None
        return {
            "active": _RUN_STATE["active"],
            "current": current,
            "last": last,
        }


def _mark_run_started(session_name: str, trigger_source: str) -> None:
    with _RUN_STATE_LOCK:
        _RUN_STATE["active"] = True
        _RUN_STATE["current"] = {
            "session_name": session_name,
            "trigger_source": trigger_source,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }


def _mark_run_finished(status: str, reason: Optional[str]) -> None:
    finished_at = datetime.now(timezone.utc).isoformat()
    with _RUN_STATE_LOCK:
        current = dict(_RUN_STATE["current"]) if _RUN_STATE["current"] else None
        _RUN_STATE["last"] = {
            "status": status,
            "reason": reason,
            "finished_at": finished_at,
            "session_name": current.get("session_name") if current else None,
            "trigger_source": current.get("trigger_source") if current else None,
            "started_at": current.get("started_at") if current else None,
        }
        _RUN_STATE["active"] = False
        _RUN_STATE["current"] = None


def _is_http_trigger_enabled() -> bool:
    if _get_env_bool("HTTP_ENABLED", False):
        return True
    return _get_env_str("PORT") is not None


def _get_http_host() -> str:
    return _get_env_str("HTTP_HOST", "0.0.0.0") or "0.0.0.0"


def _get_http_port() -> int:
    port_value = _get_env_str("HTTP_PORT") or _get_env_str("PORT")
    if port_value is None:
        return 8000
    return int(port_value)


def _build_health_payload() -> dict:
    return {
        "status": "ok",
        "scheduler_enabled": _get_env_bool("SCHEDULER_ENABLED", True),
        "http_trigger_enabled": _is_http_trigger_enabled(),
        "run_state": _snapshot_run_state(),
    }


def _run_reserved_job(session_name: str, trigger_source: str) -> dict:
    try:
        result = _execute_trading_job(session_name=session_name)
        _mark_run_finished(result["status"], result.get("reason"))
        return result
    except Exception as exc:
        logger.error(
            "Trading job failed: session=%s source=%s error=%s",
            session_name,
            trigger_source,
            exc,
            exc_info=True,
        )
        _mark_run_finished("failed", str(exc))
        return {"status": "failed", "reason": str(exc)}
    finally:
        _RUN_LOCK.release()


def _start_background_job(session_name: str, trigger_source: str = "http") -> tuple[bool, dict]:
    if not _RUN_LOCK.acquire(blocking=False):
        return False, {
            "status": "rejected",
            "reason": "job_already_running",
            "run_state": _snapshot_run_state(),
        }

    _mark_run_started(session_name=session_name, trigger_source=trigger_source)

    thread = threading.Thread(
        target=_run_reserved_job,
        kwargs={"session_name": session_name, "trigger_source": trigger_source},
        name=f"trading-job-{session_name}",
        daemon=True,
    )
    thread.start()
    return True, {
        "status": "accepted",
        "session_name": session_name,
        "trigger_source": trigger_source,
        "run_state": _snapshot_run_state(),
    }


class WorkerHttpHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/healthz", "/readyz"}:
            self._write_json(200, _build_health_payload())
            return
        if parsed.path == "/trigger":
            self._handle_trigger(parsed)
            return
        self._write_json(404, {"status": "not_found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/trigger":
            self._handle_trigger(parsed)
            return
        self._write_json(404, {"status": "not_found"})

    def log_message(self, fmt: str, *args) -> None:
        logger.info("HTTP %s - %s", self.command, fmt % args)

    def _handle_trigger(self, parsed) -> None:
        params = parse_qs(parsed.query)
        session_name = params.get("session", ["http"])[0].strip() or "http"
        accepted, payload = _start_background_job(
            session_name=session_name,
            trigger_source="http",
        )
        self._write_json(202 if accepted else 409, payload)

    def _write_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _build_http_server(host: str, port: int) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), WorkerHttpHandler)


def _start_http_server() -> Optional[ThreadingHTTPServer]:
    if not _is_http_trigger_enabled():
        return None

    host = _get_http_host()
    port = _get_http_port()
    server = _build_http_server(host, port)
    thread = threading.Thread(
        target=server.serve_forever,
        name="worker-http-server",
        daemon=True,
    )
    thread.start()
    logger.info("HTTP trigger server started on %s:%s", host, server.server_address[1])
    return server


def _wait_forever() -> None:
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker loop stopped.")


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------

def _execute_trading_job(session_name: str = "manual") -> dict:
    """Analyse each ticker and route signal to Alpaca executor."""
    from alpaca_trade import AlpacaExecutor

    started_at = datetime.now(timezone.utc)
    today = started_at.date().isoformat()
    run_id = _build_run_id(today, session_name, started_at)
    llm_settings = _resolve_llm_settings()

    executor = AlpacaExecutor()
    notifier = TelegramNotifier()
    executor.reset_session_guard()
    tickers, discovery_context = _resolve_analysis_tickers(executor, today)
    if not tickers:
        logger.warning("No tickers resolved for this run; aborting.")
        return {"status": "skipped", "reason": "no_tickers"}

    logger.info(
        "=== Trading job started: date=%s session=%s run_id=%s tickers=%s ===",
        today,
        session_name,
        run_id,
        tickers,
    )
    logger.info(
        "LLM routing: provider=%s quick=%s fundamentals=%s deep=%s",
        llm_settings["provider"],
        llm_settings["quick_model"],
        llm_settings["fundamentals_model"],
        llm_settings["deep_model"],
    )

    # Sanity check: verify Alpaca connection
    try:
        account = executor.get_account()
        logger.info(
            "Alpaca account: status=%s buying_power=%s",
            account.get("status"),
            account.get("buying_power"),
        )
    except Exception as exc:
        logger.error("Alpaca connection failed: %s — aborting job.", exc)
        return {"status": "failed", "reason": f"alpaca_connection_failed: {exc}"}

    logger.info(
        "Resolved analysis universe: mode=%s tickers=%s",
        discovery_context.get("universe_mode"),
        tickers,
    )

    run_results: list[dict] = []
    max_new_buys = _get_env_int("MAX_NEW_BUYS_PER_RUN", 3)
    new_buys_placed = 0

    for ticker in tickers:
        logger.info("Analysing %s ...", ticker)
        retries = 3
        signal = "HOLD"
        analysis_error = ""
        log_path = (
            f"eval_results/{ticker}/TradingAgentsStrategy_logs/"
            f"full_states_log_{run_id}.json"
        )
        for attempt in range(retries):
            try:
                signal = _run_analysis(ticker, today, run_id)
                logger.info("Signal for %s: %s", ticker, signal)
                break
            except Exception as exc:
                err = str(exc)
                analysis_error = err[:300]
                logger.warning(
                    "Analysis attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    retries,
                    ticker,
                    err[:200],
                )
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))

        if signal in executor.BUY_SIGNALS and new_buys_placed >= max_new_buys:
            logger.info(
                "Skipping buy for %s because run buy budget is exhausted (%d).",
                ticker,
                max_new_buys,
            )
            run_results.append(
                {
                    "ticker": ticker,
                    "signal": signal,
                    "action": "SKIPPED",
                    "reason": "buy_budget_reached",
                    "log_path": log_path,
                }
            )
            continue

        execution = executor.execute_with_details(ticker, signal)
        order = execution.get("order")
        if order:
            logger.info("Order placed: %s", order)
            if order.get("side") == "buy":
                new_buys_placed += 1
            run_results.append(
                {
                    "ticker": ticker,
                    "signal": signal,
                    "action": f"ORDER_{order.get('side', '').upper()}",
                    "reason": execution.get("reason"),
                    "log_path": log_path,
                }
            )
        else:
            reason = execution.get("reason")
            if analysis_error and signal == "HOLD":
                reason = f"analysis_failed_then_default_hold: {analysis_error}"
            logger.info(
                "No order placed for %s (signal=%s, reason=%s)",
                ticker,
                signal,
                reason,
            )
            run_results.append(
                {
                    "ticker": ticker,
                    "signal": signal,
                    "action": execution.get("status", "HOLD_OR_SKIPPED").upper(),
                    "reason": reason,
                    "log_path": log_path,
                }
            )

    # Send compact run summary to Telegram after each scan.
    try:
        logger.info("Preparing Telegram summary...")
        account_end = executor.get_account()
        logger.debug("Got account info: %s", account_end)
        positions_end = executor.get_positions()
        logger.debug("Got positions: count=%d", len(positions_end) if positions_end else 0)
        
        summary = _build_telegram_summary(
            run_date=today,
            session_name=session_name,
            run_id=run_id,
            tickers=tickers,
            discovery_context=discovery_context,
            run_results=run_results,
            account=account_end,
            positions=positions_end,
            llm_settings=llm_settings,
        )
        logger.info(
            "Telegram summary prepared: size=%d bytes, tickers=%s, results=%d",
            len(summary),
            len(tickers),
            len(run_results),
        )
        logger.info("Calling TelegramNotifier.send()...")
        notifier.send(summary)
        logger.info("Telegram notification flow completed.")
    except Exception as exc:
        logger.error(
            "Telegram summary step failed: %s",
            exc,
            exc_info=True,
        )

    logger.info("=== Trading job finished: run_id=%s ===", run_id)
    return {"status": "completed", "reason": None}


def trading_job(session_name: str = "manual", trigger_source: str = "manual") -> bool:
    if not _RUN_LOCK.acquire(blocking=False):
        logger.warning(
            "Trading job skipped because another run is already active: session=%s source=%s",
            session_name,
            trigger_source,
        )
        return False

    _mark_run_started(session_name=session_name, trigger_source=trigger_source)
    result = _run_reserved_job(session_name=session_name, trigger_source=trigger_source)
    return result["status"] == "completed"


def _maybe_run_startup_trigger() -> None:
    if not _get_env_bool("RUN_ON_START", False):
        return

    logger.info("Startup one-shot execution triggered via RUN_ON_START=true.")
    trading_job(session_name="startup", trigger_source="startup")


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def main() -> None:
    server = _start_http_server()
    _maybe_run_startup_trigger()

    if not _get_env_bool("SCHEDULER_ENABLED", True):
        if server is None:
            logger.info("Scheduler disabled and HTTP trigger disabled; exiting idle worker.")
            return
        logger.info("Scheduler disabled. Waiting for external HTTP triggers.")
        _wait_forever()
        return

    sessions = _get_schedule_sessions()

    scheduler = BlockingScheduler(timezone="UTC")
    for session in sessions:
        scheduler.add_job(
            trading_job,
            trigger=CronTrigger(
                hour=session.hour,
                minute=session.minute,
                day_of_week="mon-fri",
            ),
            kwargs={"session_name": session.name, "trigger_source": "scheduler"},
            id=f"trading_job_{session.name}",
            name=f"Trading analysis ({session.name})",
            max_instances=1,
            coalesce=True,
        )

    schedule_summary = ", ".join(
        f"{session.name}={session.hour:02d}:{session.minute:02d} UTC"
        for session in sessions
    )
    logger.info("Scheduler started. Trading jobs run Mon-Fri at %s.", schedule_summary)

    for session in sessions:
        job = scheduler.get_job(f"trading_job_{session.name}")
        next_run = getattr(job, "next_run_time", None) if job else None
        if next_run:
            logger.info("Next run for %s: %s", session.name, next_run)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    # Allow manual one-shot execution: python worker.py --now
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        logger.info("Manual one-shot execution triggered.")
        trading_job(session_name="manual", trigger_source="cli")
    else:
        main()
