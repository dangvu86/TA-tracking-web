"""
Price history snapshots for in-browser intraday calculation.

- history/base.json        : written at 08:45 for today, without today's bar. The browser
                             appends today's live bar and recomputes indicators.
- history/final/<date>.json: written by the 15:00 run with exactly the bars it used,
                             for the daily parity check.
"""
from datetime import datetime
from typing import Optional

import pandas as pd

from .data_fetcher import fetch_stock_data

WINDOW_DAYS = 365


def _num_list(series: pd.Series) -> list:
    return [None if pd.isna(v) else float(v) for v in series]


def build_history_payload(stock_df: pd.DataFrame, as_of: datetime, frames: Optional[dict] = None,
                          include_as_of: bool = True) -> dict:
    """frames: Ticker -> DataFrame actually used by the analysis run (preferred).
    Without it, fetch with the raw CSV Exchange value (NaN for index rows) so the
    fetch cache key is identical to the one analyze_stocks_parallel used.
    include_as_of=False drops bars dated as_of (the 08:45 base, before the open)."""
    tickers = []
    for _, row in stock_df.iterrows():
        raw_exchange = row["Exchange"]
        if frames is not None:
            df = frames.get(row["Ticker"])
        else:
            df = fetch_stock_data(row["Ticker"], as_of, exchange=raw_exchange)
        exchange = raw_exchange if pd.notna(raw_exchange) else ""
        entry = {"Ticker": row["Ticker"], "Sector": row["Sector"], "Exchange": exchange}
        if df is not None and not include_as_of:
            df = df[df["Date"].dt.date < as_of.date()]
        if df is None or df.empty:
            entry.update(d=[], o=[], h=[], l=[], c=[], v=[])
        else:
            df = df.sort_values("Date")
            entry.update(
                d=[d.strftime("%Y-%m-%d") for d in df["Date"].dt.date],
                o=_num_list(df["Open"]), h=_num_list(df["High"]), l=_num_list(df["Low"]),
                c=_num_list(df["Close"]), v=_num_list(df["Volume"]),
            )
        tickers.append(entry)
    return {"as_of": as_of.strftime("%Y-%m-%d"), "window_days": WINDOW_DAYS, "tickers": tickers}
