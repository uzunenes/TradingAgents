from __future__ import annotations

import json
from typing import Any

from tradingagents.llm_clients.factory import create_llm_client


def _candidate_line(item: dict[str, Any]) -> str:
    return (
        f"- {item['symbol']}: "
        f"score={item.get('score', 0.0):.4f}, "
        f"ret_60={item.get('ret_60', 0.0):.4f}, "
        f"ret_20={item.get('ret_20', 0.0):.4f}, "
        f"volatility_20={item.get('volatility_20', 0.0):.4f}, "
        f"avg_dollar_volume={item.get('avg_dollar_volume', 0.0):.2f}, "
        f"latest_close={item.get('latest_close', 0.0):.2f}"
    )


def _extract_json_payload(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("selector returned empty response")

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("selector did not return JSON")
        return json.loads(raw[start : end + 1])


def _normalize_symbols(symbols: list[Any], allowed_symbols: set[str], selection_count: int) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = str(symbol or "").strip().upper()
        if not value or value in seen or value not in allowed_symbols:
            continue
        normalized.append(value)
        seen.add(value)
        if len(normalized) >= selection_count:
            break
    return normalized


def select_candidates_with_llm(
    ranked_candidates: list[dict[str, Any]],
    held_symbols: list[str],
    trade_date: str,
    llm_settings: dict[str, str],
    selection_count: int,
) -> dict[str, Any]:
    if selection_count <= 0 or not ranked_candidates:
        return {
            "selected_candidates": [],
            "selected_symbols": [],
            "selection_reason": "selection_disabled_or_empty_pool",
            "raw_response": "",
        }

    pool = ranked_candidates[:]
    if len(pool) <= selection_count:
        selected_symbols = [item["symbol"] for item in pool]
        return {
            "selected_candidates": pool,
            "selected_symbols": selected_symbols,
            "selection_reason": "pool_not_larger_than_target",
            "raw_response": "",
        }

    llm = create_llm_client(
        provider=llm_settings["provider"],
        model=llm_settings["quick_model"],
        base_url=llm_settings.get("backend_url"),
        timeout=45,
        max_retries=2,
    ).get_llm()

    prompt_lines = [
        "You are a market selection agent.",
        f"Trade date: {trade_date}",
        f"Current held symbols: {', '.join(held_symbols) if held_symbols else 'none'}",
        f"Choose exactly {selection_count} symbols from the ranked candidate pool below for full multi-agent analysis.",
        "Optimize for a balanced shortlist using score quality, momentum, liquidity, volatility control, and diversification vs current holdings.",
        "Do not invent symbols and do not include symbols outside the list.",
        "Return strict JSON only in the form:",
        '{"selected_symbols": ["SYM1", "SYM2"], "selection_reason": "short explanation"}',
        "Candidate pool:",
    ]
    prompt_lines.extend(_candidate_line(item) for item in pool)
    prompt = "\n".join(prompt_lines)

    response = llm.invoke(prompt)
    content = getattr(response, "content", response)
    payload = _extract_json_payload(str(content))

    allowed_symbols = {item["symbol"] for item in pool}
    selected_symbols = _normalize_symbols(
        payload.get("selected_symbols", []),
        allowed_symbols=allowed_symbols,
        selection_count=selection_count,
    )
    if not selected_symbols:
        raise ValueError("selector returned no valid symbols")

    ranked_by_symbol = {item["symbol"]: item for item in pool}
    selected_candidates = [ranked_by_symbol[symbol] for symbol in selected_symbols]

    for item in pool:
        if len(selected_candidates) >= selection_count:
            break
        symbol = item["symbol"]
        if symbol in selected_symbols:
            continue
        selected_symbols.append(symbol)
        selected_candidates.append(item)

    return {
        "selected_candidates": selected_candidates,
        "selected_symbols": selected_symbols,
        "selection_reason": str(payload.get("selection_reason", "")).strip(),
        "raw_response": str(content).strip(),
    }