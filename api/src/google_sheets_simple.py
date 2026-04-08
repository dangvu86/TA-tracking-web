import pandas as pd
import logging
from datetime import datetime
from typing import Optional
import requests
from io import StringIO
import re
import time

logger = logging.getLogger(__name__)

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


def fetch_vnmidcap_from_sheets() -> Optional[pd.DataFrame]:
    """Fetch VNMIDCAP data from Google Sheets"""
    cached = _get_cached("vnmidcap_sheets")
    if cached is not None:
        return cached

    try:
        sheet_id = "1-aoYbQDjBeOuzqT8LuURuOuEF4K0qsSA"
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

        response = requests.get(csv_url, timeout=10)
        response.raise_for_status()

        content = response.content.decode('utf-8', errors='ignore')
        csv_data = StringIO(content)
        df_raw = pd.read_csv(csv_data, header=None)

        # Find the first row that looks like actual data
        data_row = None
        for i in range(min(50, len(df_raw))):
            row = df_raw.iloc[i]
            if row.isna().all():
                continue
            first_val = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', first_val) or re.match(r'\d{4}-\d{1,2}-\d{1,2}', first_val):
                data_row = i
                break

        if data_row is None:
            logger.error("Could not find VNMIDCAP data starting row")
            return None

        df = df_raw.iloc[data_row:].copy()
        df = df.reset_index(drop=True)
        df = df.dropna(how='all')

        if df.empty:
            logger.error("No data found")
            return None

        # Column mapping: Date, %change(skip), Open, High, Low, Close, Volume
        if len(df.columns) >= 7:
            df = pd.DataFrame({
                'Date': df.iloc[:, 0].copy(),
                'Open': df.iloc[:, 2].copy(),
                'High': df.iloc[:, 3].copy(),
                'Low': df.iloc[:, 4].copy(),
                'Close': df.iloc[:, 5].copy(),
                'Volume': df.iloc[:, 6].copy()
            })
        else:
            logger.error("Not enough columns in Google Sheets data")
            return None

        # Convert Date (US format: mm/dd/yyyy)
        try:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=False)
        except:
            logger.error("Could not parse dates")
            return None

        df = df.dropna(subset=['Date'])

        # Convert price columns (Vietnamese number format: 2.492,45 -> 2492.45)
        price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.replace(r'\.(?=\d{3})', '', regex=True)
                df[col] = df[col].str.replace(',', '.', regex=False)
                df[col] = df[col].str.replace(' ', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill missing OHLC with Close if needed
        if 'Close' in df.columns and not df['Close'].isna().all():
            for col in ['Open', 'High', 'Low']:
                if col not in df.columns or df[col].isna().all():
                    df[col] = df['Close']

        if 'Volume' not in df.columns:
            df['Volume'] = 0

        df['Dividends'] = 0
        df['Stock Splits'] = 0

        df = df.dropna(subset=['Close'])
        df = df.sort_values('Date').reset_index(drop=True)

        if df.empty:
            logger.error("No valid data after processing")
            return None

        latest_date = df['Date'].max()
        days_old = (datetime.now() - latest_date.to_pydatetime()).days
        if days_old > 30:
            logger.warning(f"VNMIDCAP data may be outdated. Latest: {latest_date.strftime('%Y-%m-%d')}")

        _set_cache("vnmidcap_sheets", df)
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error accessing Google Sheets: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error processing VNMIDCAP data: {str(e)}")
        return None
