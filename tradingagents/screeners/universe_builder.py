from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from tradingagents.default_config import DEFAULT_CONFIG

logger = logging.getLogger(__name__)

SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _default_cache_path() -> Path:
    return Path(DEFAULT_CONFIG["data_cache_dir"]) / "sp500_symbols.csv"


def _read_cache(path: Path) -> list[str]:
    data = pd.read_csv(path)
    column = data.columns[0]
    return [_normalize_symbol(symbol) for symbol in data[column].dropna().tolist()]


def get_sp500_symbols(refresh: bool = False, cache_path: str | None = None) -> list[str]:
    env_override = os.environ.get("SP500_SYMBOLS", "").strip()
    if env_override:
        return [_normalize_symbol(symbol) for symbol in env_override.split(",") if symbol.strip()]

    path = Path(cache_path) if cache_path else _default_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not refresh:
        return _read_cache(path)

    try:
        tables = pd.read_html(SP500_WIKIPEDIA_URL)
        table = next(table for table in tables if "Symbol" in table.columns)
        symbols = [_normalize_symbol(symbol) for symbol in table["Symbol"].dropna().tolist()]
        pd.DataFrame({"symbol": symbols}).to_csv(path, index=False)
        return symbols
    except Exception:
        if path.exists():
            logger.warning("Falling back to cached S&P 500 constituents from %s", path)
            return _read_cache(path)
        raise