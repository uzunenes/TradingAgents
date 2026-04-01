from __future__ import annotations

import logging
import os
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from tradingagents.default_config import DEFAULT_CONFIG

logger = logging.getLogger(__name__)

SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKIPEDIA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _default_cache_path() -> Path:
    return Path(DEFAULT_CONFIG["data_cache_dir"]) / "sp500_symbols.csv"


def _read_cache(path: Path) -> list[str]:
    data = pd.read_csv(path)
    column = data.columns[0]
    return [_normalize_symbol(symbol) for symbol in data[column].dropna().tolist()]


def _fetch_sp500_table() -> pd.DataFrame:
    response = requests.get(SP500_WIKIPEDIA_URL, headers=WIKIPEDIA_HEADERS, timeout=20)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))
    return next(table for table in tables if "Symbol" in table.columns)


def get_sp500_symbols(refresh: bool = False, cache_path: str | None = None) -> list[str]:
    env_override = os.environ.get("SP500_SYMBOLS", "").strip()
    if env_override:
        return [_normalize_symbol(symbol) for symbol in env_override.split(",") if symbol.strip()]

    path = Path(cache_path) if cache_path else _default_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not refresh:
        return _read_cache(path)

    try:
        table = _fetch_sp500_table()
        symbols = [_normalize_symbol(symbol) for symbol in table["Symbol"].dropna().tolist()]
        pd.DataFrame({"symbol": symbols}).to_csv(path, index=False)
        return symbols
    except Exception:
        if path.exists():
            logger.warning("Falling back to cached S&P 500 constituents from %s", path)
            return _read_cache(path)
        raise