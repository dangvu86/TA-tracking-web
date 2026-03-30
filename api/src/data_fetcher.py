import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import logging
import time

from .google_sheets_simple import fetch_vnmidcap_from_sheets
from .google_drive_fetcher import fetch_gdrive_stock_data, is_ticker_in_gdrive

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


def fetch_stock_data(ticker: str, end_date: datetime, period_days: int = 365, exchange: str = '') -> Optional[pd.DataFrame]:
    """
    Fetch stock data from appropriate source.

    Data Source Priority:
    1. VNMIDCAP -> Google Sheets (exclusive)
    2. VNINDEX -> Google Drive (exclusive)
    3. Vietnamese stocks (HOSE/HNX/UPCOM) -> Google Drive CSV files (primary)
    4. US indices -> Yahoo Finance
    5. Fallback -> Yahoo Finance
    """
    cache_key = f"{ticker}_{end_date.date()}_{period_days}_{exchange}"
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    try:
        # Special handling for VNMIDCAP using Google Sheets ONLY
        if ticker in ['VNMID', 'VNMIDCAP'] or (ticker == 'VNMID' and exchange == ''):
            df = fetch_vnmidcap_from_sheets()

            if df is not None and not df.empty:
                df = df[df['Date'].dt.date <= end_date.date()]
                _set_cache(cache_key, df)
                return df
            else:
                logger.error(f"Failed to fetch VNMIDCAP data from Google Sheets for {ticker}")
                return None

        # Special handling for VNINDEX - use Google Drive only
        elif ticker == 'VNINDEX':
            from .google_drive_fetcher import fetch_gdrive_index_data, is_index_in_gdrive
            if is_index_in_gdrive(ticker):
                df = fetch_gdrive_index_data(ticker, period_days)
                if df is not None and not df.empty:
                    df = df[df['Date'].dt.date <= end_date.date()]
                    _set_cache(cache_key, df)
                    return df

            logger.warning(f"Could not fetch VNINDEX data from Google Drive")
            return None

        # Check if ticker is available in Google Drive data (for Vietnamese stocks)
        elif exchange in ['HOSE', 'HNX', 'UPCOM'] or is_ticker_in_gdrive(ticker):
            df = fetch_gdrive_stock_data(ticker, period_days)

            if df is not None and not df.empty:
                df = df[df['Date'].dt.date <= end_date.date()]
                _set_cache(cache_key, df)
                return df
            else:
                logger.info(f"Ticker {ticker} not found in Google Drive, trying Yahoo Finance...")

        # Use Yahoo Finance (for US indices or fallback)
        start_date = end_date - timedelta(days=period_days)

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
        df = df[df['Date'].dt.date <= end_date.date()]

        _set_cache(cache_key, df)
        return df

    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {str(e)}")
        return None


def get_last_trading_date() -> datetime:
    """Get the last trading date based on available data."""
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
