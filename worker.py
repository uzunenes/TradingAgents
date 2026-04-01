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
  TRADING_ENABLED             true/false (default: true)
  MAX_ORDER_VALUE_USD         per-order notional cap (default: 500)
    QUICK_MODEL                 provider model id for quick tasks
    DEEP_MODEL                  provider model id for complex reasoning
        FUNDAMENTALS_MODEL          optional model override for fundamentals analyst
    RUN_ON_START                true/false to execute one cloud-side scan on service boot
  SCHEDULE_HOUR_UTC           UTC hour to run (default: 21)
  SCHEDULE_MINUTE_UTC         UTC minute to run (default: 5)
  LOG_LEVEL                   DEBUG/INFO/WARNING (default: INFO)
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
import requests

load_dotenv(dotenv_path=".env")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("worker")


class TelegramNotifier:
    """Send compact post-run summaries to configured Telegram chat IDs."""

    def __init__(self) -> None:
        self.enabled = os.environ.get("TELEGRAM_ENABLED", "false").lower() == "true"
        self.token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        raw_ids = os.environ.get("TELEGRAM_CHAT_IDS", "")
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
    tickers: list[str],
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
        f"Rapor zamani: {now_utc}",
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
        "Onemli kararlar / Sonuclar",
    ]

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

# ---------------------------------------------------------------------------
# Lazy-import heavy deps (LangGraph, yfinance, etc.) only when job runs
# ---------------------------------------------------------------------------

def _resolve_llm_settings() -> dict:
    from tradingagents.default_config import DEFAULT_CONFIG

    env_provider = os.environ.get("LLM_PROVIDER", "").strip().lower()
    env_backend_url = os.environ.get("BACKEND_URL", "").strip()

    if env_provider:
        provider = env_provider
    elif os.environ.get("OPENROUTER_API_KEY", "").strip():
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
        "quick_model": os.environ.get("QUICK_MODEL", default_quick_model),
        "fundamentals_model": os.environ.get(
            "FUNDAMENTALS_MODEL", default_fundamentals_model
        ),
        "deep_model": os.environ.get("DEEP_MODEL", default_deep_model),
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


def _run_analysis(ticker: str, trade_date: str) -> str:
    """Run full TradingAgentsGraph and return processed signal string."""
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    cfg = _build_ta_config()
    ta = TradingAgentsGraph(
        debug=False,
        config=cfg,
        selected_analysts=["market", "social", "news", "fundamentals"],
    )
    _, signal = ta.propagate(ticker, trade_date)
    return (signal or "HOLD").strip().upper()


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------

def trading_job() -> None:
    """Analyse each ticker and route signal to Alpaca executor."""
    from alpaca_trade import AlpacaExecutor

    tickers_raw = os.environ.get("TICKERS", "SPY")
    tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]
    today = date.today().isoformat()
    llm_settings = _resolve_llm_settings()

    logger.info("=== Trading job started: date=%s tickers=%s ===", today, tickers)
    logger.info(
        "LLM routing: provider=%s quick=%s fundamentals=%s deep=%s",
        llm_settings["provider"],
        llm_settings["quick_model"],
        llm_settings["fundamentals_model"],
        llm_settings["deep_model"],
    )

    executor = AlpacaExecutor()
    notifier = TelegramNotifier()
    executor.reset_daily_guard()

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
        return

    run_results: list[dict] = []

    for ticker in tickers:
        logger.info("Analysing %s ...", ticker)
        retries = 3
        signal = "HOLD"
        analysis_error = ""
        log_path = (
            f"eval_results/{ticker}/TradingAgentsStrategy_logs/"
            f"full_states_log_{today}.json"
        )
        for attempt in range(retries):
            try:
                signal = _run_analysis(ticker, today)
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

        execution = executor.execute_with_details(ticker, signal)
        order = execution.get("order")
        if order:
            logger.info("Order placed: %s", order)
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
            tickers=tickers,
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

    logger.info("=== Trading job finished ===")


def _maybe_run_startup_trigger() -> None:
    if os.environ.get("RUN_ON_START", "false").lower() != "true":
        return

    logger.info("Startup one-shot execution triggered via RUN_ON_START=true.")
    try:
        trading_job()
    except Exception as exc:
        logger.error("Startup one-shot execution failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def main() -> None:
    _maybe_run_startup_trigger()

    hour = int(os.environ.get("SCHEDULE_HOUR_UTC", "21"))
    minute = int(os.environ.get("SCHEDULE_MINUTE_UTC", "5"))

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        trading_job,
        trigger=CronTrigger(hour=hour, minute=minute, day_of_week="mon-fri"),
        id="trading_job",
        name="Daily trading analysis",
        max_instances=1,
        coalesce=True,
    )

    logger.info(
        "Scheduler started. Trading job runs Mon-Fri at %02d:%02d UTC.", hour, minute
    )

    # Show next run time if available across APScheduler versions
    job = scheduler.get_job("trading_job")
    next_run = getattr(job, "next_run_time", None) if job else None
    if next_run:
        logger.info("Next run: %s", next_run)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    # Allow manual one-shot execution: python worker.py --now
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        logger.info("Manual one-shot execution triggered.")
        trading_job()
    else:
        main()
