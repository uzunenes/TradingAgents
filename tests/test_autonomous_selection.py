import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import worker
from tradingagents.screeners.ranker import rank_candidates_from_histories
from tradingagents.screeners.universe_builder import get_sp500_symbols


def _history(close_values: list[float], volume: int = 2_000_000) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=len(close_values), freq="B")
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": close_values,
            "High": [value * 1.01 for value in close_values],
            "Low": [value * 0.99 for value in close_values],
            "Close": close_values,
            "Volume": [volume] * len(close_values),
        }
    )


class TestAutonomousSelection(unittest.TestCase):
    def test_sp500_symbols_env_override(self):
        with patch.dict(os.environ, {"SP500_SYMBOLS": "BRK.B,MSFT,GOOG"}, clear=False):
            symbols = get_sp500_symbols()

        self.assertEqual(symbols, ["BRK-B", "MSFT", "GOOG"])

    def test_rank_candidates_prefers_stronger_liquid_momentum(self):
        histories = {
            "AAA": _history([100 + idx for idx in range(80)], volume=3_000_000),
            "BBB": _history([100 - (idx * 0.1) for idx in range(80)], volume=3_000_000),
            "CCC": _history([20 + (idx * 0.2) for idx in range(80)], volume=10_000),
        }

        ranked = rank_candidates_from_histories(histories, top_n=2, min_avg_dollar_volume=50_000_000)

        self.assertEqual([item["symbol"] for item in ranked], ["AAA", "BBB"])
        self.assertGreater(ranked[0]["score"], ranked[1]["score"])

    def test_worker_merges_held_positions_with_ranked_candidates(self):
        executor = type("Executor", (), {"get_positions": lambda self: [{"symbol": "AAPL", "qty": "5"}]})()

        with patch.dict(os.environ, {"UNIVERSE_MODE": "sp500", "PORTFOLIO_INCLUDE_OPEN_POSITIONS": "true"}, clear=False), patch.object(
            worker,
            "_discover_market_candidates",
            return_value=[{"symbol": "MSFT", "score": 0.5}, {"symbol": "AAPL", "score": 0.4}, {"symbol": "NVDA", "score": 0.3}],
        ):
            tickers, context = worker._resolve_analysis_tickers(executor, "2026-04-01")

        self.assertEqual(tickers, ["AAPL", "MSFT", "NVDA"])
        self.assertEqual(context["held_symbols"], ["AAPL"])
        self.assertFalse(context["used_explicit_fallback"])