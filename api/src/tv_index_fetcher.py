"""
Index data fetcher for indices FireAnt does not carry (e.g. VNMID).

Uses the public TradingView-style daily history endpoints of VNDirect (primary)
and SSI iBoard (fallback); both return {t, o, h, l, c, v} arrays, no auth required.
Output matches fireant_fetcher format:
    Date (datetime), Open, High, Low, Close, Volume, Dividends, Stock Splits
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SOURCES = [
    ("VNDirect", "https://dchart-api.vndirect.com.vn/dchart/history",
     {"resolution": "D"}),
    ("SSI", "https://iboard-api.ssi.com.vn/statistics/charts/history",
     {"resolution": "1D"}),
]
REQUEST_TIMEOUT = (5, 20)  # (connect, read) seconds
CACHE_TTL = 1800

_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0 (ta-tracking)"})

# In-memory cache
_cache = {}
_cache_ts = {}


def _to_frame(payload: dict) -> Optional[pd.DataFrame]:
    # SSI wraps the arrays in "data"
    if isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not payload.get("t"):
        return None
    df = pd.DataFrame({
        # Timestamps are 00:00 UTC of the VN trading day
        "Date": pd.to_datetime(payload["t"], unit="s").normalize(),
        "Open": pd.to_numeric(payload.get("o"), errors="coerce"),
        "High": pd.to_numeric(payload.get("h"), errors="coerce"),
        "Low": pd.to_numeric(payload.get("l"), errors="coerce"),
        "Close": pd.to_numeric(payload.get("c"), errors="coerce"),
        "Volume": pd.to_numeric(payload.get("v"), errors="coerce"),
    })
    df["Dividends"] = 0
    df["Stock Splits"] = 0
    df = (
        df.dropna(subset=["Close"])
          .drop_duplicates(subset=["Date"], keep="last")
          .sort_values("Date")
          .reset_index(drop=True)
    )
    return df if not df.empty else None


def fetch_tv_index_data(symbol: str, period_days: int = 365, use_cache: bool = True) -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV for an index from VNDirect, falling back to SSI."""
    cache_key = f"tv_{symbol}_{period_days}"
    if use_cache and cache_key in _cache and time.time() - _cache_ts.get(cache_key, 0) < CACHE_TTL:
        return _cache[cache_key]

    end = datetime.now() + timedelta(days=1)
    start = end - timedelta(days=period_days + 1)
    params = {"symbol": symbol.upper(), "from": int(start.timestamp()), "to": int(end.timestamp())}

    for name, url, extra in SOURCES:
        try:
            resp = _session.get(url, params={**params, **extra}, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"[{symbol}/{name}] HTTP {resp.status_code}")
                continue
            df = _to_frame(resp.json())
            if df is None:
                logger.warning(f"[{symbol}/{name}] empty payload")
                continue
            logger.info(f"[{symbol}/{name}] {len(df)} rows, last {df['Date'].iloc[-1].date()}")
            _cache[cache_key] = df
            _cache_ts[cache_key] = time.time()
            return df
        except Exception as e:
            logger.warning(f"[{symbol}/{name}] request error: {e}")

    return None
