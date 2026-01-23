from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from analysis.data_preparation import fetch_stock_data
from database.ticker_repository import (
    get_symbols_for_price_movement_update,
    upsert_price_movement_data,
)
from utils.serialization import convert_to_python_types

DEFAULT_BATCH_SIZE = int(os.getenv("WATCHLIST_CACHE_BATCH_SIZE", "12"))


def _filter_latest_trading_session(df: pd.DataFrame | None):
    if df is None or df.empty or "date" not in df.columns:
        return df, None

    date_series = pd.to_datetime(df["date"], errors="coerce")
    if date_series.isna().all():
        return df, None

    working_df = df.copy()
    working_df["__date_key"] = date_series.dt.date
    working_df.dropna(subset=["__date_key"], inplace=True)
    if working_df.empty:
        working_df.drop(columns=["__date_key"], inplace=True, errors="ignore")
        return working_df, None

    working_df.sort_values("date", inplace=True)
    working_df.reset_index(drop=True, inplace=True)
    latest_day = working_df["__date_key"].iloc[-1]

    prev_close = None
    prev_df = working_df[working_df["__date_key"] != latest_day]
    if not prev_df.empty and "close" in prev_df.columns:
        prev_close_series = pd.to_numeric(prev_df["close"], errors="coerce").dropna()
        if not prev_close_series.empty:
            prev_close = float(prev_close_series.iloc[-1])

    latest_df = working_df[working_df["__date_key"] == latest_day].copy()
    latest_df.drop(columns=["__date_key"], inplace=True, errors="ignore")
    return latest_df, prev_close


def _get_prev_close_from_daily(daily_df: pd.DataFrame | None):
    if daily_df is None or daily_df.empty or "close" not in daily_df.columns:
        return None
    close_series = pd.to_numeric(daily_df["close"], errors="coerce").dropna()
    if close_series.empty:
        return None
    if len(close_series) >= 2:
        return float(close_series.iloc[-2])
    return float(close_series.iloc[-1])


def _build_payload(filtered_df: pd.DataFrame | None, prev_close):
    if filtered_df is None or filtered_df.empty:
        return None

    if "date" in filtered_df.columns:
        date_series = pd.to_datetime(filtered_df["date"], errors="coerce")
        if not date_series.isna().all():
            latest_day = date_series.dt.date.max()
            filtered_df = filtered_df[date_series.dt.date == latest_day].copy()

    payload = {
        "candles": filtered_df.to_dict(orient="records"),
        "summary": {"previousClose": prev_close},
    }
    return convert_to_python_types(payload)


def _normalize_symbols(symbols: Iterable[str]):
    if not symbols:
        return []
    normalized = []
    seen = set()
    for symbol in symbols:
        value = str(symbol).strip().upper()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def refresh_watchlist_cache(batch_size: int | None = None):
    batch_size = batch_size or DEFAULT_BATCH_SIZE
    symbols = _normalize_symbols(get_symbols_for_price_movement_update(batch_size))
    if not symbols:
        return {"updated": 0, "symbols": []}

    intraday_data = fetch_stock_data(symbols, period="5d", interval="5m", threads=False)

    interim = {}
    needs_daily = []
    updated_symbols = []
    for symbol in symbols:
        intraday_df = intraday_data.get(symbol)
        filtered_df, prev_close = _filter_latest_trading_session(intraday_df)
        if filtered_df is None or filtered_df.empty:
            continue
        interim[symbol] = {"filtered_df": filtered_df, "prev_close": prev_close}
        if prev_close is None:
            needs_daily.append(symbol)

    daily_data = {}
    if needs_daily:
        daily_data = fetch_stock_data(needs_daily, period="7d", interval="1d", threads=False)

    for symbol, info in interim.items():
        prev_close = info["prev_close"]
        if prev_close is None:
            prev_close = _get_prev_close_from_daily(daily_data.get(symbol))
        payload = _build_payload(info["filtered_df"], prev_close)
        if payload is None:
            continue
        upsert_price_movement_data(symbol, payload)
        updated_symbols.append(symbol)

    return {"updated": len(updated_symbols), "symbols": updated_symbols}
