"""
FastAPI Backend for TA Tracking Web App
"""
import os
import sys
import logging
import time
import math
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import numpy as np

# Add api directory to path for src imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.stock_loader import load_stock_list
from src.utils.parallel_processor import analyze_stocks_parallel
from src.utils.sector_analysis import analyze_sectors_new
from src.data_fetcher import get_last_trading_date

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TA Tracking API", version="1.0.0")

# CORS - allow all origins for now, restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache for analysis results (keyed by date string)
_analysis_cache = {}
_analysis_cache_ts = {}
ANALYSIS_CACHE_TTL = 3600  # 1 hour


def _clean_value(val):
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


def _clean_row(row: dict) -> dict:
    """Clean a row dict for JSON serialization"""
    return {k: _clean_value(v) for k, v in row.items()}


class AnalyzeRequest(BaseModel):
    date: str  # YYYY-MM-DD format


@app.get("/api/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/last-trading-date")
def last_trading_date():
    try:
        dt = get_last_trading_date()
        return {"date": dt.strftime("%Y-%m-%d")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stocks")
def get_stocks():
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "TA_Tracking_List.csv")
        stock_df = load_stock_list(csv_path)
        return stock_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/analyze")
def analyze(request: AnalyzeRequest):
    """Run full analysis for all stocks on a given date"""
    try:
        # Check cache
        cache_key = request.date
        if cache_key in _analysis_cache:
            if time.time() - _analysis_cache_ts.get(cache_key, 0) < ANALYSIS_CACHE_TTL:
                logger.info(f"Returning cached results for {cache_key}")
                return _analysis_cache[cache_key]

        # Parse date
        selected_date = datetime.strptime(request.date, "%Y-%m-%d")

        # Load stock list
        csv_path = os.path.join(os.path.dirname(__file__), "TA_Tracking_List.csv")
        stock_df = load_stock_list(csv_path)

        logger.info(f"Starting analysis for {len(stock_df)} stocks on {request.date}")
        start_time = time.time()

        # Run parallel analysis
        results, errors = analyze_stocks_parallel(stock_df, selected_date, max_workers=5)

        elapsed = time.time() - start_time
        logger.info(f"Analysis completed in {elapsed:.1f}s. {len(results)} results, {len(errors)} errors")

        # Build results DataFrame for sector analysis
        df_results = pd.DataFrame(results)

        # Calculate totals
        totals = {}
        numeric_cols = ['STRENGTH_ST', 'STRENGTH_LT', 'Rating_1_Current', 'Rating_1_Prev1',
                        'Rating_1_Prev2', 'Rating_2_Current', 'Rating_2_Prev1', 'Rating_2_Prev2']
        for col in numeric_cols:
            if col in df_results.columns:
                vals = pd.to_numeric(df_results[col], errors='coerce')
                totals[col] = _clean_value(vals.sum())

        # Sector analysis
        sector_analysis = {}
        if not df_results.empty:
            try:
                sector_analysis = analyze_sectors_new(df_results)
            except Exception as e:
                logger.error(f"Sector analysis error: {e}")

        # Clean results for JSON
        clean_results = [_clean_row(r) for r in results]

        response = {
            "results": clean_results,
            "sectors": sector_analysis,
            "totals": totals,
            "errors": errors,
            "meta": {
                "date": request.date,
                "total_stocks": len(results),
                "elapsed_seconds": round(elapsed, 1)
            }
        }

        # Cache the response
        _analysis_cache[cache_key] = response
        _analysis_cache_ts[cache_key] = time.time()

        return response

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
