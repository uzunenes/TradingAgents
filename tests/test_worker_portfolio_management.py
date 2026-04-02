import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

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

    def test_telegram_summary_is_explicit_about_actions_and_portfolio(self):
        summary = worker._build_telegram_summary(
            run_date="2026-04-01",
            session_name="midday",
            run_id="run-123",
            tickers=["SPY", "MSFT"],
            discovery_context={
                "universe_mode": "sp500",
                "ranked_candidates": [{"symbol": "MSFT", "score": 0.73}],
                "held_symbols": ["AAPL"],
                "used_explicit_fallback": False,
            },
            run_results=[
                {
                    "ticker": "SPY",
                    "signal": "SELL",
                    "action": "REJECTED",
                    "reason": "no_long_position_to_reduce",
                    "log_path": "eval_results/SPY/log.json",
                },
                {
                    "ticker": "MSFT",
                    "signal": "BUY",
                    "action": "ORDER_BUY",
                    "reason": "accepted",
                    "log_path": "eval_results/MSFT/log.json",
                },
            ],
            account={
                "status": "ACTIVE",
                "equity": "10000",
                "cash": "5000",
                "buying_power": "15000",
                "portfolio_value": "10000",
            },
            positions=[{"symbol": "MSFT", "qty": "2", "market_value": "800"}],
            llm_settings={
                "provider": "openrouter",
                "backend_url": "https://openrouter.ai/api/v1",
                "quick_model": "q",
                "fundamentals_model": "f",
                "deep_model": "d",
            },
        )

        self.assertIn("Run Ozeti", summary)
        self.assertIn("portfoyden dahil edilenler: AAPL", summary)
        self.assertIn("aksiyon=alim emri gonderildi", summary)
        self.assertIn("neden=azaltilacak long pozisyon yok, short acilmadi", summary)
        self.assertNotIn("BACKEND_URL:", summary)
        self.assertNotIn("log=eval_results", summary)

    def test_telegram_notifier_splits_long_messages_without_truncation(self):
        notifier = worker.TelegramNotifier()
        notifier.enabled = True
        notifier.token = "token"
        notifier.chat_ids = ["chat-1"]

        long_message = "\n".join(f"satir {index}: " + ("x" * 120) for index in range(60))
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {"result": {"message_id": 1}}

        with patch("worker.requests.post", return_value=fake_response) as post_mock:
            notifier.send(long_message)

        self.assertGreater(post_mock.call_count, 1)
        sent_parts = [call.kwargs["json"]["text"] for call in post_mock.call_args_list]
        self.assertEqual("".join(part.replace("\n", "") for part in sent_parts).count("satir"), 60)
        self.assertTrue(all(len(part) <= notifier.MAX_MESSAGE_LEN for part in sent_parts))