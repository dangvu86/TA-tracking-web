"""
FireAnt-based stock data fetcher (primary source).

Uses FireAnt's public historical-quotes endpoint (no auth required).
Prices are already in full VND (adjusted for corporate actions, incl. cash dividends);
indices (VNINDEX) are in points. Output matches google_drive_fetcher / vnstock_fetcher format:
    Date (datetime), Open, High, Low, Close, Volume, Dividends, Stock Splits
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

FIREANT_URL = "https://www.fireant.vn/api/Data/Markets/HistoricalQuotes"
REQUEST_TIMEOUT = (5, 20)  # (connect, read) seconds
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

_session = requests.Session()
_session.headers.update({"User-Agent": "Mozilla/5.0 (ta-tracking)"})

# In-memory cache
_cache = {}
_cache_ts = {}


def _get_cached(key, ttl=1800):
    if key in _cache and time.time() - _cache_ts.get(key, 0) < ttl:
        return _cache[key]
    return None


def _set_cache(key, value):
    _cache[key] = value
    _cache_ts[key] = time.time()


def _raw_fetch(symbol: str, start_date: str, end_date: str) -> Optional[list]:
    params = {"symbol": symbol, "startDate": start_date, "endDate": end_date}
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = _session.get(FIREANT_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                logger.warning(f"[{symbol}/FireAnt] unexpected payload: {str(data)[:200]}")
                return None
            logger.warning(f"[{symbol}/FireAnt] HTTP {resp.status_code} (attempt {attempt + 1})")
        except Exception as e:
            logger.warning(f"[{symbol}/FireAnt] request error (attempt {attempt + 1}): {e}")
        if attempt < MAX_ATTEMPTS - 1:
            time.sleep(RETRY_DELAY_SECONDS)
    return None


def fetch_fireant_data(ticker: str, period_days: int = 365) -> Optional[pd.DataFrame]:
    """Fetch daily OHLCV for a stock or index (e.g. VNINDEX) from FireAnt."""
    cache_key = f"fireant_{ticker}_{period_days}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    end_date = datetime.now() + timedelta(days=1)
    start_date = end_date - timedelta(days=period_days + 1)
    raw = _raw_fetch(ticker.upper(), start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if not raw:
        return None

    try:
        df = pd.DataFrame(raw)
        df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None).dt.normalize()
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["Dividends"] = 0
        df["Stock Splits"] = 0
        df = df[["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]]
        df = (
            df.dropna(subset=["Close"])
              .drop_duplicates(subset=["Date"], keep="last")
              .sort_values("Date")
              .reset_index(drop=True)
        )
    except Exception as e:
        logger.warning(f"Error normalizing FireAnt data for {ticker}: {e}")
        return None

    if df.empty:
        return None

    _set_cache(cache_key, df)
    return df


def get_last_trading_date() -> Optional[datetime]:
    """Return most recent trading date from VNINDEX history on FireAnt."""
    df = fetch_fireant_data("VNINDEX", period_days=10)
    if df is None or df.empty:
        return None
    return df["Date"].max().to_pydatetime()
