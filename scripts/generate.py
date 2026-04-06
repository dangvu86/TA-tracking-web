"""
Pre-compute TA analysis and save as static JSON.
Reuses api/src/ logic with reduced memory footprint.
"""
import os
import sys
import json
import gc
import math
import logging
import time
from datetime import datetime

import pandas as pd
import numpy as np

# Add api directory to path
api_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api")
sys.path.insert(0, api_dir)

from src.utils.stock_loader import load_stock_list
from src.utils.parallel_processor import analyze_stocks_parallel
from src.utils.sector_analysis import analyze_sectors_new
from src.data_fetcher import get_last_trading_date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_value(val):
    """Clean a value for JSON serialization (handle NaN, Inf, Timestamp)"""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    return val


def clean_row(row: dict) -> dict:
    return {k: clean_value(v) for k, v in row.items()}


def main():
    start_time = time.time()

    # Get latest trading date
    selected_date = get_last_trading_date()
    date_str = selected_date.strftime("%Y-%m-%d")
    logger.info(f"Generating analysis for {date_str}")

    # Load stock list
    csv_path = os.path.join(api_dir, "TA_Tracking_List.csv")
    stock_df = load_stock_list(csv_path)
    logger.info(f"Loaded {len(stock_df)} stocks")

    # Run analysis with reduced workers to save memory
    results, errors = analyze_stocks_parallel(stock_df, selected_date, max_workers=5)
    gc.collect()

    elapsed = time.time() - start_time
    logger.info(f"Analysis done in {elapsed:.1f}s. {len(results)} results, {len(errors)} errors")

    # Build response (same structure as /api/analyze)
    df_results = pd.DataFrame(results)

    totals = {}
    numeric_cols = ['STRENGTH_ST', 'STRENGTH_LT', 'Rating_1_Current', 'Rating_1_Prev1',
                    'Rating_1_Prev2', 'Rating_2_Current', 'Rating_2_Prev1', 'Rating_2_Prev2']
    for col in numeric_cols:
        if col in df_results.columns:
            vals = pd.to_numeric(df_results[col], errors='coerce')
            totals[col] = clean_value(vals.sum())

    sector_analysis = {}
    if not df_results.empty:
        try:
            sector_analysis = analyze_sectors_new(df_results)
        except Exception as e:
            logger.error(f"Sector analysis error: {e}")

    clean_results = [clean_row(r) for r in results]

    response = {
        "results": clean_results,
        "sectors": sector_analysis,
        "totals": totals,
        "errors": errors,
        "meta": {
            "date": date_str,
            "total_stocks": len(results),
            "elapsed_seconds": round(elapsed, 1)
        }
    }

    # Write to web/public/data/latest.json
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web", "public", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "latest.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(response, f, ensure_ascii=False)

    file_size = os.path.getsize(output_path)
    logger.info(f"Written {output_path} ({file_size:,} bytes)")
    logger.info(f"Total time: {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    main()
