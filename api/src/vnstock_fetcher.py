"""
vnstock-based stock data fetcher.

Ported from Giaheo_Commo/scheduler/tasks/update_hose_stocks.py.
Replaces Google Drive CSV dependency with direct vnstock API calls.

Data source priority: KBS → VCI fallback (community tier, 60 req/min).
Prices are returned in VND (multiplied by 1000 to match google_drive_fetcher output format).
"""

import os
import re
import sys
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# Force UTF-8 on stdout/stderr so vnai's ✓/✗ prints don't crash on Windows (cp1252).
# No-op on Linux/Cloud Run where UTF-8 is the default.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Setup vnstock at import time so vnai telemetry has stable stdout handle
_VNSTOCK_READY = False
_VNSTOCK_ERROR = None

try:
    import vnai
    _api_key = os.getenv("VNSTOCK_API_KEY", "vnstock_afcd90123c73f72797add778cf608d44")
    try:
        vnai.setup_api_key(_api_key)
    except Exception as e:
        # Telemetry failure should not block data fetching
        logger.warning(f"vnai.setup_api_key warning (non-fatal): {e}")
    from vnstock import Vnstock
    _VNSTOCK_READY = True
except ImportError as e:
    _VNSTOCK_ERROR = f"vnstock/vnai not installed: {e}"
    logger.warning(_VNSTOCK_ERROR)
except Exception as e:
    _VNSTOCK_ERROR = f"vnstock init failed: {e}"
    logger.warning(_VNSTOCK_ERROR)

# Rate-limit tuning (Community tier = 60 req/min)
BATCH_SIZE = 5
BATCH_DELAY_SECONDS = 15
SINGLE_RETRY_DELAY = 3

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


def _is_rate_limit_error(exc) -> bool:
    msg = str(exc)
    return (
        "rate limit" in msg.lower()
        or "ratelimit" in msg.lower()
        or "60/60" in msg
        or "giới hạn" in msg.lower()
        or ("chờ" in msg.lower() and "giây" in msg.lower())
    )


def _parse_wait_seconds(exc) -> int:
    msg = str(exc)
    match = re.search(r"chờ\s+(\d+)\s+giây", msg, re.IGNORECASE)
    if match:
        return int(match.group(1)) + 5
    match = re.search(r"wait\s+(\d+)\s+sec", msg, re.IGNORECASE)
    if match:
        return int(match.group(1)) + 5
    return 65


def _raw_fetch(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """Raw vnstock call with KBS→VCI fallback and rate-limit aware retries."""
    if not _VNSTOCK_READY:
        logger.error(f"vnstock not ready: {_VNSTOCK_ERROR}")
        return None

    sources = ["KBS", "VCI"]
    for source in sources:
        for attempt in range(3):
            try:
                stock_obj = Vnstock().stock(symbol=symbol, source=source)
                df = stock_obj.quote.history(start=start_date, end=end_date)
                if df is not None and not df.empty:
                    return df
            except BaseException as e:
                if _is_rate_limit_error(e):
                    wait_sec = _parse_wait_seconds(e)
                    logger.warning(f"[{symbol}/{source}] rate limit, waiting {wait_sec}s")
                    time.sleep(wait_sec)
                elif attempt == 0:
                    time.sleep(SINGLE_RETRY_DELAY)
                continue
    return None


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Convert vnstock output to match google_drive_fetcher format.

    google_drive_fetcher returns:
        Date (datetime), Open, High, Low, Close (× 1000), Volume, Dividends, Stock Splits
    vnstock quote.history returns:
        time, open, high, low, close, volume (prices in thousands VND)
    """
    df = df.copy()
    if "time" in df.columns:
        df = df.rename(columns={"time": "Date"})
    df = df.rename(
        columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    )
    df["Date"] = pd.to_datetime(df["Date"])

    # vnstock returns prices in thousand VND; multiply to match Google Drive path (full VND)
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce") * 1000

    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    df["Dividends"] = 0
    df["Stock Splits"] = 0

    df = df[["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]]
    df = (
        df.dropna(subset=["Close"])
          .drop_duplicates(subset=["Date"], keep="last")
          .sort_values("Date")
          .reset_index(drop=True)
    )
    return df


def fetch_vnstock_stock_data(ticker: str, period_days: int = 365) -> Optional[pd.DataFrame]:
    """Fetch OHLCV for a single ticker via vnstock. Drop-in replacement for fetch_gdrive_stock_data."""
    cache_key = f"vnstock_{ticker}_{period_days}"
    cached = _get_cached(cache_key, ttl=300)
    if cached is not None:
        return cached

    end_date = datetime.now()
    start_date = end_date - timedelta(days=period_days)
    raw = _raw_fetch(ticker.upper(), start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    if raw is None or raw.empty:
        return None

    try:
        df = _normalize_ohlcv(raw)
    except Exception as e:
        logger.warning(f"Error normalizing {ticker}: {e}")
        return None

    _set_cache(cache_key, df)
    return df


def fetch_vnstock_index_data(ticker: str, period_days: int = 365) -> Optional[pd.DataFrame]:
    """Fetch index data (VNINDEX, HNX, etc.) via vnstock."""
    return fetch_vnstock_stock_data(ticker, period_days)


def prewarm_tickers(tickers: list, period_days: int = 365) -> dict:
    """Pre-fetch data for a list of tickers respecting rate limit (batch 5, delay 15s).

    Populates module cache so subsequent per-ticker calls hit cache.
    Returns a dict {ticker: success_bool} for logging/diagnostics.
    """
    if not _VNSTOCK_READY:
        logger.error(f"vnstock not ready: {_VNSTOCK_ERROR}")
        return {t: False for t in tickers}

    results = {}
    total = len(tickers)
    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info(
        f"Pre-warming {total} tickers via vnstock "
        f"(batch={BATCH_SIZE}, delay={BATCH_DELAY_SECONDS}s, ~{num_batches} batches)"
    )

    for batch_idx in range(num_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, total)
        batch = tickers[start:end]
        logger.info(f"Batch {batch_idx + 1}/{num_batches}: {batch}")

        t0 = time.time()
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            futures = {
                executor.submit(fetch_vnstock_stock_data, t, period_days): t for t in batch
            }
            for future in as_completed(futures):
                t = futures[future]
                try:
                    df = future.result()
                    results[t] = df is not None and not df.empty
                except Exception as e:
                    logger.warning(f"[{t}] prewarm error: {e}")
                    results[t] = False

        elapsed = time.time() - t0
        ok = sum(1 for v in results.values() if v)
        logger.info(f"Batch {batch_idx + 1}: done in {elapsed:.1f}s (ok so far: {ok}/{len(results)})")

        if batch_idx < num_batches - 1:
            time.sleep(BATCH_DELAY_SECONDS)

    return results


def get_last_trading_date() -> Optional[datetime]:
    """Return most recent trading date by querying a liquid ticker (VNM)."""
    try:
        df = fetch_vnstock_stock_data("VNM", period_days=10)
        if df is not None and not df.empty:
            latest = df["Date"].max()
            if hasattr(latest, "to_pydatetime"):
                return latest.to_pydatetime()
            return latest
    except Exception as e:
        logger.warning(f"get_last_trading_date via vnstock failed: {e}")
    return None


def is_vnstock_ready() -> bool:
    return _VNSTOCK_READY
