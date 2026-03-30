import pandas as pd
from typing import List


def load_stock_list(csv_path: str = "TA_Tracking_List.csv") -> pd.DataFrame:
    """Load stock list from CSV file"""
    try:
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"Stock list file not found: {csv_path}")
    except Exception as e:
        raise Exception(f"Error loading stock list: {str(e)}")


def get_sectors(df: pd.DataFrame) -> List[str]:
    """Get unique sectors from stock dataframe"""
    return sorted(df['Sector'].unique().tolist())


def get_stocks_by_sector(df: pd.DataFrame, sector: str = None) -> List[str]:
    """Get stocks filtered by sector"""
    if sector and sector != "All":
        filtered_df = df[df['Sector'] == sector]
    else:
        filtered_df = df
    
    return sorted(filtered_df['Ticker'].unique().tolist())


def format_ticker_for_yahoo(ticker: str, exchange: str) -> str:
    """Format ticker for Yahoo Finance API based on exchange"""
    if exchange in ['HOSE', 'HNX']:
        return f"{ticker}.VN"
    return ticker