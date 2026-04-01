"""
TradingAgents scheduled worker.

Runs daily after NYSE close (16:05 ET = 21:05 UTC) and:
  1. Generates trading signals via TradingAgentsGraph (OpenRouter / z-ai models)
  2. Sends executable orders to Alpaca paper account via AlpacaExecutor

Configuration via environment variables:
  OPENROUTER_API_KEY          required
  ALPACA_API_KEY              required
  ALPACA_SECRET_KEY           required
  ALPACA_BASE_URL             optional (default: paper endpoint)
  TICKERS                     comma-separated list (default: SPY)
  TRADING_ENABLED             true/false (default: true)
  MAX_ORDER_VALUE_USD         per-order notional cap (default: 500)
  QUICK_MODEL                 OpenRouter model id (default: z-ai/glm-5-turbo)
  DEEP_MODEL                  OpenRouter model id (default: z-ai/glm-5)
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
        "- market -> QUICK_MODEL (hizli piyasa yorumu)",
        "- social -> QUICK_MODEL (sosyal duyarlilik)",
        "- news -> QUICK_MODEL (haber etkisi)",
        "- fundamentals -> DEEP_MODEL (derin finansal analiz)",
        f"QUICK_MODEL: {os.environ.get('QUICK_MODEL', 'z-ai/glm-5-turbo')}",
        f"DEEP_MODEL: {os.environ.get('DEEP_MODEL', 'z-ai/glm-5')}",
        "",
        "Onemli kararlar / Sonuclar",
    ]

    for item in run_results:
        action = item.get("action", "NO_ACTION")
        lines.append(
            f"- {item.get('ticker')}: signal={item.get('signal')} action={action}"
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

def _build_ta_config() -> dict:
    from tradingagents.default_config import DEFAULT_CONFIG

    cfg = DEFAULT_CONFIG.copy()
    cfg["llm_provider"] = "openrouter"
    cfg["backend_url"] = "https://openrouter.ai/api/v1"
    cfg["quick_think_llm"] = os.environ.get("QUICK_MODEL", "z-ai/glm-5-turbo")
    cfg["deep_think_llm"] = os.environ.get("DEEP_MODEL", "z-ai/glm-5")
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

    logger.info("=== Trading job started: date=%s tickers=%s ===", today, tickers)

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
        for attempt in range(retries):
            try:
                signal = _run_analysis(ticker, today)
                logger.info("Signal for %s: %s", ticker, signal)
                break
            except Exception as exc:
                err = str(exc)
                logger.warning(
                    "Analysis attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    retries,
                    ticker,
                    err[:200],
                )
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))

        order = executor.execute(ticker, signal)
        if order:
            logger.info("Order placed: %s", order)
            run_results.append(
                {
                    "ticker": ticker,
                    "signal": signal,
                    "action": f"ORDER_{order.get('side', '').upper()}",
                }
            )
        else:
            logger.info("No order placed for %s (signal=%s)", ticker, signal)
            run_results.append(
                {
                    "ticker": ticker,
                    "signal": signal,
                    "action": "HOLD_OR_SKIPPED",
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


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def main() -> None:
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
