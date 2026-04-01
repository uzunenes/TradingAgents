import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import worker


class TestWorkerPortfolioManagement(unittest.TestCase):
    def test_buy_budget_skips_additional_entries(self):
        fake_executor = type(
            "Executor",
            (),
            {
                "BUY_SIGNALS": {"BUY", "OVERWEIGHT"},
                "reset_session_guard": lambda self: None,
                "get_account": lambda self: {"status": "ACTIVE", "buying_power": "1000"},
                "get_positions": lambda self: [],
                "execute_with_details": lambda self, ticker, signal: {
                    "order": {"side": "buy", "id": ticker, "status": "accepted"},
                    "reason": "accepted",
                },
            },
        )()

        fake_notifier = type("Notifier", (), {"send": lambda self, message: None})()
        summaries = []

        with patch.dict(
            os.environ,
            {
                "MAX_NEW_BUYS_PER_RUN": "1",
                "UNIVERSE_MODE": "fixed",
                "TICKERS": "AAA,BBB",
            },
            clear=False,
        ), patch("alpaca_trade.AlpacaExecutor", return_value=fake_executor), patch.object(
            worker,
            "TelegramNotifier",
            return_value=fake_notifier,
        ), patch.object(
            worker,
            "_run_analysis",
            side_effect=["BUY", "BUY"],
        ), patch.object(
            worker,
            "_build_telegram_summary",
            side_effect=lambda **kwargs: summaries.append(kwargs) or "summary",
        ):
            worker.trading_job(session_name="test")

        run_results = summaries[0]["run_results"]
        self.assertEqual(run_results[0]["action"], "ORDER_BUY")
        self.assertEqual(run_results[1]["reason"], "buy_budget_reached")