import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging
import time

from .google_sheets_simple import fetch_vnmidcap_from_sheets
from .tv_index_fetcher import fetch_tv_index_data
from .google_drive_fetcher import fetch_gdrive_stock_data, is_ticker_in_gdrive
from .fireant_fetcher import (
    fetch_fireant_data,
    get_last_trading_date as fireant_last_trading_date,
)
from .vnstock_fetcher import (
    fetch_vnstock_stock_data,
    fetch_vnstock_index_data,
    is_vnstock_ready,
    get_last_trading_date as vnstock_last_trading_date,
)

logger = logging.getLogger(__name__)

# Simple in-memory cache
_cache = {}
_cache_ts = {}

def _get_cached(key, ttl=300):
    if key in _cache and time.time() - _cache_ts.get(key, 0) < ttl:
        return _cache[key]
    return None

def _set_cache(key, value):
    _cache[key] = value
    _cache_ts[key] = time.time()

# Extra days fetched beyond the window, so trimming by calendar date never runs short
WINDOW_PAD_DAYS = 7


def _trim_window(df: pd.DataFrame, end_date: datetime, period_days: int) -> pd.DataFrame:
    """Keep bars in [end_date - period_days, end_date] by calendar date, so results
    depend only on the analysis date, never on when the job runs."""
    start = (end_date - timedelta(days=period_days)).date()
    dates = df['Date'].dt.date
    return df[(dates >= start) & (dates <= end_date.date())]


def _vn_today():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()


def _patch_today_index_bar(df: pd.DataFrame, symbol: str, end_date: datetime) -> pd.DataFrame:
    """FireAnt's daily index bar lags after the close (2026-11-04 15:00: close 1732.51 and
    volume 609M vs the real 1733.02 / 785M), while its stock bars are final. For a run on
    today's session only, take today's index bar from VNDirect (same source the browser
    uses intraday). Older bars stay on FireAnt: OHLV of past days differ between the two."""
    day = end_date.date()
    if day != _vn_today():
        return df
    try:
        live = fetch_tv_index_data(symbol, 10, use_cache=False)
    except Exception as e:
        logger.warning(f"[{symbol}] VNDirect today bar unavailable: {e}")
        return df
    if live is None or live.empty:
        return df
    row = live[live['Date'].dt.date == day]
    if row.empty:
        return df
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    out = df[df['Date'].dt.date != day]
    new = row.iloc[[-1]].copy()
    for c in df.columns:
        if c not in new.columns:
            new[c] = 0
    out = pd.concat([out, new[df.columns]], ignore_index=True).sort_values('Date').reset_index(drop=True)
    logger.info(f"[{symbol}] today's bar from VNDirect: {row.iloc[-1][cols].to_dict()}")
    return out


def fetch_stock_data(ticker: str, end_date: datetime, period_days: int = 365, exchange: str = '') -> Optional[pd.DataFrame]:
    """
    Fetch stock data from appropriate source.

    Data Source Priority:
    1. VNMIDCAP -> VNDirect -> SSI -> Google Sheets (FireAnt does not provide this index)
    2. VNINDEX -> FireAnt (primary; today's bar from VNDirect) -> vnstock -> Google Drive (fallback)
    3. Vietnamese stocks (HOSE/HNX/UPCOM) -> FireAnt (primary) -> vnstock -> Google Drive (fallback)
    4. US indices -> Yahoo Finance
    5. Fallback -> Yahoo Finance
    """
    # Fetchers count back from now, so cover the gap between the analysis date and today
    fetch_days = period_days + max(0, (datetime.now() - end_date).days) + WINDOW_PAD_DAYS
    cache_key = f"{ticker}_{end_date.date()}_{period_days}_{fetch_days}_{exchange}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        # VNMIDCAP: VNDirect -> SSI -> Google Sheets fallback
        if ticker in ['VNMID', 'VNMIDCAP'] or (ticker == 'VNMID' and exchange == ''):
            df = fetch_tv_index_data('VNMID', fetch_days)
            if df is None or df.empty:
                logger.warning(f"VNDirect/SSI unavailable for {ticker}, falling back to Google Sheets")
                df = fetch_vnmidcap_from_sheets()

            if df is not None and not df.empty:
                df = _trim_window(df, end_date, period_days)
                _set_cache(cache_key, df)
                return df
            else:
                logger.error(f"Failed to fetch VNMIDCAP data from Google Sheets for {ticker}")
                return None

        # VNINDEX: FireAnt primary -> vnstock -> Google Drive fallback
        elif ticker == 'VNINDEX':
            df = fetch_fireant_data(ticker, fetch_days)
            if df is not None and not df.empty:
                df = _patch_today_index_bar(df, ticker, end_date)
                df = _trim_window(df, end_date, period_days)
                _set_cache(cache_key, df)
                return df
            logger.info(f"FireAnt returned no data for {ticker}, falling back to vnstock")

            if is_vnstock_ready():
                df = fetch_vnstock_index_data(ticker, fetch_days)
                if df is not None and not df.empty:
                    df = _trim_window(df, end_date, period_days)
                    _set_cache(cache_key, df)
                    return df
                logger.info(f"vnstock returned no data for {ticker}, falling back to Google Drive")

            from .google_drive_fetcher import fetch_gdrive_index_data, is_index_in_gdrive
            if is_index_in_gdrive(ticker):
                df = fetch_gdrive_index_data(ticker, fetch_days)
                if df is not None and not df.empty:
                    df = _trim_window(df, end_date, period_days)
                    _set_cache(cache_key, df)
                    return df

            logger.warning(f"Could not fetch VNINDEX data from vnstock or Google Drive")
            return None

        # Vietnamese stocks: FireAnt primary -> vnstock -> Google Drive fallback -> Yahoo
        elif exchange in ['HOSE', 'HNX', 'UPCOM'] or is_ticker_in_gdrive(ticker):
            df = fetch_fireant_data(ticker, fetch_days)
            if df is not None and not df.empty:
                df = _trim_window(df, end_date, period_days)
                _set_cache(cache_key, df)
                return df
            logger.info(f"FireAnt returned no data for {ticker}, falling back to vnstock")

            if is_vnstock_ready():
                df = fetch_vnstock_stock_data(ticker, fetch_days)
                if df is not None and not df.empty:
                    df = _trim_window(df, end_date, period_days)
                    _set_cache(cache_key, df)
                    return df
                logger.info(f"vnstock returned no data for {ticker}, falling back to Google Drive")

            df = fetch_gdrive_stock_data(ticker, fetch_days)

            if df is not None and not df.empty:
                df = _trim_window(df, end_date, period_days)
                _set_cache(cache_key, df)
                return df
            else:
                logger.info(f"Ticker {ticker} not found in Google Drive, trying Yahoo Finance...")

        # Use Yahoo Finance (for US indices or fallback)
        start_date = end_date - timedelta(days=period_days + WINDOW_PAD_DAYS)

        if exchange in ['HOSE', 'HNX', 'UPCOM']:
            yahoo_ticker = f"{ticker}.VN"
        else:
            yahoo_ticker = ticker

        stock = yf.Ticker(yahoo_ticker)
        df = stock.history(start=start_date, end=end_date + timedelta(days=1))

        if df.empty:
            logger.warning(f"No data found for {ticker}")
            return None

        df.reset_index(inplace=True)
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
        df = _trim_window(df, end_date, period_days)
        if ticker.startswith('^'):
            # US indices: only sessions before the VN analysis date. On VN day D the last
            # finished US session is D-1; ^VIX is also quoted overnight (from ~3:00 ET, i.e.
            # ~15:15 VN in winter, ~14:15 in summer), so a later run would pick up a
            # provisional D bar (2026-11-04 rerun at 15:32: 19.06 vs the 15:00 run's 20.83).
            df = df[df['Date'].dt.date < end_date.date()]
        # Drop rows where Close is NaN (Yahoo sometimes returns in-progress rows with no price yet)
        df = df.dropna(subset=['Close'])

        _set_cache(cache_key, df)
        return df

    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        return None


def get_last_trading_date() -> datetime:
    """Get the last trading date based on available data.

    Priority: FireAnt -> vnstock -> Google Drive -> weekend heuristic.
    """
    fa_latest = fireant_last_trading_date()
    if fa_latest is not None:
        return fa_latest

    if is_vnstock_ready():
        vn_latest = vnstock_last_trading_date()
        if vn_latest is not None:
            return vn_latest

    from .google_drive_fetcher import get_latest_data_date

    gdrive_latest = get_latest_data_date()
    if gdrive_latest is not None:
        return gdrive_latest

    today = datetime.now()
    if today.weekday() == 5:
        return today - timedelta(days=1)
    elif today.weekday() == 6:
        return today - timedelta(days=2)
    else:
        return today


def validate_trading_date(selected_date: datetime) -> datetime:
    """Validate and adjust trading date if needed"""
    last_trading_date = get_last_trading_date()
    if selected_date > last_trading_date:
        return last_trading_date

    if selected_date.weekday() == 5:
        return selected_date - timedelta(days=1)
    elif selected_date.weekday() == 6:
        return selected_date - timedelta(days=2)

    return selected_date
