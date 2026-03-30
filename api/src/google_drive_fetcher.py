"""
Google Drive Stock Data Fetcher (no Streamlit dependency)
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional
import requests
from io import StringIO
import time

logger = logging.getLogger(__name__)

# Google Drive file IDs containing stock data
GDRIVE_FILES = [
    "1E0BDythcdIdGrIYdbJCNB0DxPHJ-njzc",  # 100 HOSE stocks (Group 1)
    "1cb9Ef1IDyArlmguRZ5u63tCcxR57KEfA",  # 100 HOSE stocks (Group 2)
    "1XPZKnRDklQ1DOdVgncn71SLg1pfisQtV",  # 100 HOSE stocks (Group 3)
    "1op_GzDUtbcXOJOMkI2K-0AU9cF4m8J1S",  # 94 HOSE stocks (Group 4)
    "1JE7XKfdM4QnI6nYNFriFShU3mSWG5_Rj",  # 8 HNX/UPCOM stocks
]

# Google Drive file IDs for indices
GDRIVE_INDEX_FILES = {
    "VNINDEX": "10TXB0G2HuCbMEC1nbB-Kj2eA33mGjQFn",
}

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


def _load_gdrive_file(file_id: str) -> Optional[pd.DataFrame]:
    cached = _get_cached(f"gdrive_{file_id}")
    if cached is not None:
        return cached

    try:
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))

        required_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            return None

        _set_cache(f"gdrive_{file_id}", df)
        return df

    except Exception as e:
        logger.warning(f"Error loading Google Drive file {file_id[:8]}...: {str(e)}")
        return None


def _get_all_gdrive_data() -> Optional[pd.DataFrame]:
    cached = _get_cached("gdrive_all")
    if cached is not None:
        return cached

    all_dfs = []

    for file_id in GDRIVE_FILES:
        df = _load_gdrive_file(file_id)
        if df is not None and not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return None

    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df['date'] = pd.to_datetime(combined_df['date'])

    _set_cache("gdrive_all", combined_df)
    return combined_df


def fetch_gdrive_stock_data(ticker: str, days: int = 365) -> Optional[pd.DataFrame]:
    cache_key = f"gdrive_stock_{ticker}_{days}"
    cached = _get_cached(cache_key, ttl=300)
    if cached is not None:
        return cached

    try:
        all_data = _get_all_gdrive_data()

        if all_data is None or all_data.empty:
            return None

        ticker_data = all_data[all_data['symbol'] == ticker.upper()].copy()

        if ticker_data.empty:
            return None

        ticker_data = ticker_data.sort_values('date')

        end_date = ticker_data['date'].max()
        start_date = end_date - timedelta(days=days)
        ticker_data = ticker_data[ticker_data['date'] >= start_date]

        ticker_data = ticker_data.rename(columns={
            'date': 'Date', 'open': 'Open', 'high': 'High',
            'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        })

        ticker_data = ticker_data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()

        price_cols = ['Open', 'High', 'Low', 'Close']
        for col in price_cols:
            ticker_data[col] = ticker_data[col] * 1000

        ticker_data['Dividends'] = 0
        ticker_data['Stock Splits'] = 0
        ticker_data = ticker_data.reset_index(drop=True)

        _set_cache(cache_key, ticker_data)
        return ticker_data

    except Exception as e:
        logger.warning(f"Error fetching Google Drive data for {ticker}: {str(e)}")
        return None


def fetch_gdrive_index_data(ticker: str, days: int = 365) -> Optional[pd.DataFrame]:
    cache_key = f"gdrive_index_{ticker}_{days}"
    cached = _get_cached(cache_key, ttl=300)
    if cached is not None:
        return cached

    try:
        if ticker.upper() not in GDRIVE_INDEX_FILES:
            return None

        file_id = GDRIVE_INDEX_FILES[ticker.upper()]
        url = f"https://drive.google.com/uc?export=download&id={file_id}"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time')

        end_date = df['time'].max()
        start_date = end_date - timedelta(days=days)
        df = df[df['time'] >= start_date]

        df = df.rename(columns={
            'time': 'Date', 'open': 'Open', 'high': 'High',
            'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        })

        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df['Dividends'] = 0
        df['Stock Splits'] = 0
        df = df.reset_index(drop=True)

        _set_cache(cache_key, df)
        return df

    except Exception as e:
        logger.warning(f"Error fetching Google Drive index data for {ticker}: {str(e)}")
        return None


def is_index_in_gdrive(ticker: str) -> bool:
    return ticker.upper() in GDRIVE_INDEX_FILES


def is_ticker_in_gdrive(ticker: str) -> bool:
    try:
        all_data = _get_all_gdrive_data()
        if all_data is not None and not all_data.empty:
            return ticker.upper() in all_data['symbol'].values
        return False
    except:
        return False


def get_latest_data_date() -> Optional[datetime]:
    try:
        all_data = _get_all_gdrive_data()

        if all_data is not None and not all_data.empty:
            latest_date = all_data['date'].max()
            if hasattr(latest_date, 'to_pydatetime'):
                return latest_date.to_pydatetime()
            return latest_date
        return None
    except Exception as e:
        logger.warning(f"Error getting latest data date: {str(e)}")
        return None
