import os
from pathlib import Path
import sys
import unittest
from decimal import Decimal
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpaca_trade.alpaca_executor import AlpacaExecutor


class TestLongOnlyExecution(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(
            os.environ,
            {
                "ALPACA_API_KEY": "key",
                "ALPACA_SECRET_KEY": "secret",
                "TRADING_ENABLED": "true",
                "MAX_ORDER_VALUE_USD": "500",
            },
            clear=False,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_sell_signal_without_long_position_is_skipped(self):
        executor = AlpacaExecutor()

        with patch.object(executor, "get_position", return_value=None):
            result = executor.execute_with_details("SPY", "SELL")

        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["reason"], "no_long_position_to_reduce")
        self.assertIsNone(result["order"])

    def test_sell_signal_reduces_existing_long_without_shorting(self):
        executor = AlpacaExecutor()
        position = {
            "qty": "10",
            "market_value": "1000",
            "current_price": "100",
        }

        with patch.object(executor, "get_position", return_value=position), patch.object(
            executor,
            "_submit_quantity_order_with_status",
            return_value=({"id": "1", "side": "sell", "status": "accepted"}, None),
        ) as submit_mock:
            result = executor.execute_with_details("SPY", "UNDERWEIGHT")

        submit_mock.assert_called_once_with("SPY", side="sell", quantity=Decimal("5.000000"))
        self.assertEqual(result["status"], "ordered")
        self.assertEqual(result["side"], "sell")

    def test_buy_signal_skips_when_already_holding_long(self):
        executor = AlpacaExecutor()

        with patch.object(executor, "get_position", return_value={"qty": "3"}):
            result = executor.execute_with_details("SPY", "BUY")

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "already_holding_long")
        self.assertIsNone(result["order"])