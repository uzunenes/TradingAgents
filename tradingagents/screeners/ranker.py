from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
import yfinance as yf

from tradingagents.dataflows.stockstats_utils import _clean_dataframe, yf_retry

from .universe_builder import get_sp500_symbols


def _normalize_history_frame(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history

    frame = history.copy()
    if "Date" not in frame.columns:
        frame = frame.reset_index()
    if "Date" not in frame.columns:
        frame = frame.rename(columns={frame.columns[0]: "Date"})
    frame = _clean_dataframe(frame)
    frame = frame.sort_values("Date")
    return frame


def summarize_symbol_history(
    symbol: str,
    history: pd.DataFrame,
    min_avg_dollar_volume: float = 50_000_000,
    min_price: float = 5.0,
) -> dict | None:
    frame = _normalize_history_frame(history)
    if frame.empty or len(frame) < 65:
        return None

    close = frame["Close"]
    volume = frame["Volume"]
    latest_close = float(close.iloc[-1])
    avg_dollar_volume = float((close.tail(20) * volume.tail(20)).mean())
    if latest_close < min_price or avg_dollar_volume < min_avg_dollar_volume:
        return None

    ret_20 = float(close.iloc[-1] / close.iloc[-21] - 1)
    ret_60 = float(close.iloc[-1] / close.iloc[-61] - 1)
    volatility_20 = float(close.pct_change().tail(20).std() * (20 ** 0.5))
    score = (0.65 * ret_60) + (0.35 * ret_20) - (0.15 * volatility_20)

    return {
        "symbol": symbol,
        "score": score,
        "ret_20": ret_20,
        "ret_60": ret_60,
        "volatility_20": volatility_20,
        "avg_dollar_volume": avg_dollar_volume,
        "latest_close": latest_close,
    }


def rank_candidates_from_histories(
    histories: dict[str, pd.DataFrame],
    top_n: int = 8,
    min_avg_dollar_volume: float = 50_000_000,
) -> list[dict]:
    ranked = []
    for symbol, history in histories.items():
        summary = summarize_symbol_history(
            symbol=symbol,
            history=history,
            min_avg_dollar_volume=min_avg_dollar_volume,
        )
        if summary:
            ranked.append(summary)

    ranked.sort(
        key=lambda item: (item["score"], item["avg_dollar_volume"]),
        reverse=True,
    )
    return ranked[:top_n]


def _download_histories(
    symbols: Iterable[str],
    as_of_date: str,
    lookback_days: int = 180,
    batch_size: int = 100,
) -> dict[str, pd.DataFrame]:
    end_date = datetime.strptime(as_of_date, "%Y-%m-%d") + timedelta(days=1)
    start_date = end_date - timedelta(days=lookback_days)
    symbols_list = list(symbols)
    results: dict[str, pd.DataFrame] = {}

    for offset in range(0, len(symbols_list), batch_size):
        batch = symbols_list[offset : offset + batch_size]
        raw = yf_retry(
            lambda: yf.download(
                batch,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True,
            )
        )

        if len(batch) == 1:
            results[batch[0]] = raw.dropna(how="all")
            continue

        if isinstance(raw.columns, pd.MultiIndex):
            available = set(raw.columns.get_level_values(0))
            for symbol in batch:
                if symbol in available:
                    results[symbol] = raw[symbol].dropna(how="all")

    return results


def rank_sp500_candidates(
    as_of_date: str,
    top_n: int = 8,
    lookback_days: int = 180,
    batch_size: int = 100,
    min_avg_dollar_volume: float = 50_000_000,
    symbols: list[str] | None = None,
) -> list[dict]:
    universe = symbols or get_sp500_symbols()
    histories = _download_histories(
        symbols=universe,
        as_of_date=as_of_date,
        lookback_days=lookback_days,
        batch_size=batch_size,
    )
    return rank_candidates_from_histories(
        histories=histories,
        top_n=top_n,
        min_avg_dollar_volume=min_avg_dollar_volume,
    )