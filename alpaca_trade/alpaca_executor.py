"""
Alpaca paper-trading execution layer.

Maps TradingAgents signals to Alpaca market orders.

Signal mapping:
  BUY / OVERWEIGHT  -> buy (long)
    SELL / UNDERWEIGHT -> reduce or close an existing long position only
  HOLD              -> no action

Safety controls:
  - TRADING_ENABLED=false -> no orders sent, only logs
  - MAX_ORDER_VALUE_USD    -> notional cap per order (default $500)
    - Run-scoped 1-trade-per-ticker guard via in-memory set (resets on process restart)
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal, InvalidOperation
from typing import Optional

import requests

logger = logging.getLogger(__name__)


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


class AlpacaExecutor:
    """Execute orders on Alpaca paper endpoint."""

    BUY_SIGNALS = {"BUY", "OVERWEIGHT"}
    SELL_SIGNALS = {"SELL", "UNDERWEIGHT"}

    def __init__(self) -> None:
        self.api_key: str = _get_env_str("ALPACA_API_KEY") or ""
        self.secret_key: str = _get_env_str("ALPACA_SECRET_KEY") or ""
        if not self.api_key or not self.secret_key:
            raise KeyError("ALPACA_API_KEY and ALPACA_SECRET_KEY must be configured")
        self.base_url: str = (
            (_get_env_str("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2") or "https://paper-api.alpaca.markets/v2")
            .rstrip("/")
        )
        self.trading_enabled: bool = _get_env_bool("TRADING_ENABLED", True)
        self.max_order_value: float = float(
            _get_env_str("MAX_ORDER_VALUE_USD", "500") or "500"
        )
        self._traded_in_run: set[str] = set()
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

        if ticker in self._traded_in_run:
            logger.info("[SKIP] Already traded %s in this run.", ticker)
            return {
                "status": "skipped",
                "reason": "already_traded_in_run",
                "ticker": ticker,
                "signal": signal,
                "order": None,
            }

        if signal in self.BUY_SIGNALS:
            current_position = self.get_position(ticker)
            current_qty = self._safe_decimal(current_position.get("qty")) if current_position else None
            if current_qty is not None and current_qty > 0:
                logger.info("[SKIP] Already holding long position in %s; no add-on buys allowed.", ticker)
                return {
                    "status": "skipped",
                    "reason": "already_holding_long",
                    "ticker": ticker,
                    "signal": signal,
                    "order": None,
                }
            result, error = self._submit_order_with_status(ticker, side="buy")
        elif signal in self.SELL_SIGNALS:
            result, error = self._reduce_long_position(ticker)
        else:
            logger.info("[HOLD] No action for signal=%s ticker=%s", signal, ticker)
            return {
                "status": "skipped",
                "reason": "hold_signal",
                "ticker": ticker,
                "signal": signal,
                "order": None,
            }

        if result:
            self._traded_in_run.add(ticker)
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
                "side": result.get("side"),
                "order": result,
            }

        return {
            "status": "rejected",
            "reason": error or "order_submission_failed",
            "ticker": ticker,
            "signal": signal,
            "side": "sell" if signal in self.SELL_SIGNALS else "buy",
            "order": None,
        }

    def reset_session_guard(self) -> None:
        """Call at the start of each run to avoid duplicate same-run orders."""
        self._traded_in_run.clear()
        logger.info("[RESET] Run trade guard cleared.")

    def reset_daily_guard(self) -> None:
        """Backward compatible alias for older callers."""
        self.reset_session_guard()

    def get_account(self) -> dict:
        """Return account info (useful for health checks)."""
        return self._get(f"{self.base_url}/account")

    def get_positions(self) -> list[dict]:
        """Return all open positions."""
        return self._get(f"{self.base_url}/positions")

    def get_position(self, ticker: str) -> Optional[dict]:
        """Return a single open position or None when flat."""
        try:
            return self._get(f"{self.base_url}/positions/{ticker}")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _submit_order(self, ticker: str, side: str) -> Optional[dict]:
        result, _ = self._submit_order_with_status(ticker, side)
        return result

    def _submit_order_with_status(self, ticker: str, side: str) -> tuple[Optional[dict], Optional[str]]:
        """Submit a market order using either notional or quantity."""
        payload = {
            "symbol": ticker,
            "side": side,
            "type": "market",
            "time_in_force": "day",
        }
        if side == "buy":
            payload["notional"] = str(self.max_order_value)
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

    def _submit_quantity_order_with_status(
        self, ticker: str, side: str, quantity: Decimal
    ) -> tuple[Optional[dict], Optional[str]]:
        payload = {
            "symbol": ticker,
            "qty": self._format_decimal(quantity),
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
            logger.error("[ORDER FAILED] %s %s qty=%s: %s", side, ticker, payload["qty"], message)
            return None, message
        except Exception as exc:
            logger.error("[ORDER ERROR] %s %s qty=%s: %s", side, ticker, payload["qty"], exc)
            return None, str(exc)

    def _reduce_long_position(self, ticker: str) -> tuple[Optional[dict], Optional[str]]:
        position = self.get_position(ticker)
        if not position:
            logger.info("[SKIP] No existing long position to reduce for %s.", ticker)
            return None, "no_long_position_to_reduce"

        qty = self._safe_decimal(position.get("qty"))
        market_value = self._safe_decimal(position.get("market_value"))

        if qty is None or qty <= 0:
            logger.info("[SKIP] Position for %s is not a reducible long: qty=%s", ticker, position.get("qty"))
            return None, "no_long_position_to_reduce"

        if market_value is not None and market_value <= 0:
            logger.info("[SKIP] Position for %s has non-positive market value: %s", ticker, position.get("market_value"))
            return None, "no_long_position_to_reduce"

        current_price = self._safe_decimal(position.get("current_price"))
        if current_price is None or current_price <= 0:
            logger.info("[REDUCE] Price unavailable for %s; closing full long quantity=%s", ticker, qty)
            return self._submit_quantity_order_with_status(ticker, side="sell", quantity=qty)

        max_qty_by_notional = Decimal(str(self.max_order_value)) / current_price
        reduce_qty = qty if max_qty_by_notional >= qty else max_qty_by_notional
        reduce_qty = reduce_qty.quantize(Decimal("0.000001"))
        if reduce_qty <= 0:
            logger.info("[SKIP] Computed reduce quantity is zero for %s.", ticker)
            return None, "no_long_position_to_reduce"

        logger.info("[REDUCE] Reducing long position for %s by qty=%s", ticker, self._format_decimal(reduce_qty))
        return self._submit_quantity_order_with_status(ticker, side="sell", quantity=reduce_qty)

    @staticmethod
    def _safe_decimal(value: object) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _format_decimal(value: Decimal) -> str:
        normalized = value.normalize()
        text = format(normalized, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text

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
