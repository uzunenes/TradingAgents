"""
Alpaca paper-trading execution layer.

Maps TradingAgents signals to Alpaca market orders.

Signal mapping:
  BUY / OVERWEIGHT  -> buy (long)
  SELL / UNDERWEIGHT -> close any existing position + sell short (or just flat)
  HOLD              -> no action

Safety controls:
  - TRADING_ENABLED=false -> no orders sent, only logs
  - MAX_ORDER_VALUE_USD    -> notional cap per order (default $500)
  - Daily 1-trade-per-ticker guard via in-memory set (resets on process restart)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class AlpacaExecutor:
    """Execute orders on Alpaca paper endpoint."""

    BUY_SIGNALS = {"BUY", "OVERWEIGHT"}
    SELL_SIGNALS = {"SELL", "UNDERWEIGHT"}

    def __init__(self) -> None:
        self.api_key: str = os.environ["ALPACA_API_KEY"].strip().strip('"')
        self.secret_key: str = os.environ["ALPACA_SECRET_KEY"].strip().strip('"')
        self.base_url: str = (
            os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
            .strip()
            .strip('"')
            .rstrip("/")
        )
        self.trading_enabled: bool = (
            os.environ.get("TRADING_ENABLED", "true").lower().strip() == "true"
        )
        self.max_order_value: float = float(
            os.environ.get("MAX_ORDER_VALUE_USD", "500")
        )
        self._traded_today: set[str] = set()
        self._headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, ticker: str, signal: str) -> Optional[dict]:
        """
        Execute a trade based on signal.

        Returns order dict on success, None if skipped/disabled.
        """
        result = self.execute_with_details(ticker, signal)
        return result.get("order")

    def execute_with_details(self, ticker: str, signal: str) -> dict:
        """Execute a trade and return structured status for logging/UI."""
        signal = signal.strip().upper()
        ticker = ticker.strip().upper()

        if not self.trading_enabled:
            logger.info("[DISABLED] Trading disabled. Signal=%s ticker=%s", signal, ticker)
            return {
                "status": "skipped",
                "reason": "trading_disabled",
                "ticker": ticker,
                "signal": signal,
                "order": None,
            }

        if ticker in self._traded_today:
            logger.info("[SKIP] Already traded %s today.", ticker)
            return {
                "status": "skipped",
                "reason": "already_traded_today",
                "ticker": ticker,
                "signal": signal,
                "order": None,
            }

        if signal in self.BUY_SIGNALS:
            side = "buy"
        elif signal in self.SELL_SIGNALS:
            # Close any existing long position first, then open short
            self._close_position(ticker)
            side = "sell"
        else:
            logger.info("[HOLD] No action for signal=%s ticker=%s", signal, ticker)
            return {
                "status": "skipped",
                "reason": "hold_signal",
                "ticker": ticker,
                "signal": signal,
                "order": None,
            }

        result, error = self._submit_order_with_status(ticker, side=side)

        if result:
            self._traded_today.add(ticker)
            logger.info(
                "[ORDER] ticker=%s side=%s signal=%s order_id=%s status=%s",
                ticker,
                result.get("side"),
                signal,
                result.get("id"),
                result.get("status"),
            )
            return {
                "status": "ordered",
                "reason": result.get("status", "submitted"),
                "ticker": ticker,
                "signal": signal,
                "side": side,
                "order": result,
            }

        return {
            "status": "rejected",
            "reason": error or "order_submission_failed",
            "ticker": ticker,
            "signal": signal,
            "side": side,
            "order": None,
        }

    def reset_daily_guard(self) -> None:
        """Call at the start of each trading day to allow fresh trades."""
        self._traded_today.clear()
        logger.info("[RESET] Daily trade guard cleared.")

    def get_account(self) -> dict:
        """Return account info (useful for health checks)."""
        return self._get(f"{self.base_url}/account")

    def get_positions(self) -> list[dict]:
        """Return all open positions."""
        return self._get(f"{self.base_url}/positions")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _submit_order(self, ticker: str, side: str) -> Optional[dict]:
        result, _ = self._submit_order_with_status(ticker, side)
        return result

    def _submit_order_with_status(self, ticker: str, side: str) -> tuple[Optional[dict], Optional[str]]:
        """Submit a notional market order."""
        payload = {
            "symbol": ticker,
            "notional": str(self.max_order_value),
            "side": side,
            "type": "market",
            "time_in_force": "day",
        }
        try:
            resp = requests.post(
                f"{self.base_url}/orders",
                json=payload,
                headers=self._headers,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json(), None
        except requests.HTTPError as exc:
            message = exc.response.text[:300]
            logger.error("[ORDER FAILED] %s %s: %s", side, ticker, message)
            return None, message
        except Exception as exc:
            logger.error("[ORDER ERROR] %s %s: %s", side, ticker, exc)
            return None, str(exc)

    def _close_position(self, ticker: str) -> None:
        """Close an open position if it exists (ignore 404)."""
        try:
            resp = requests.delete(
                f"{self.base_url}/positions/{ticker}",
                headers=self._headers,
                timeout=15,
            )
            if resp.status_code == 200:
                logger.info("[CLOSE] Closed position for %s", ticker)
            elif resp.status_code == 404:
                pass  # no open position, that's fine
            else:
                logger.warning("[CLOSE WARN] %s: %s", ticker, resp.text[:200])
        except Exception as exc:
            logger.error("[CLOSE ERROR] %s: %s", ticker, exc)

    def _get(self, url: str) -> dict:
        resp = requests.get(url, headers=self._headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
