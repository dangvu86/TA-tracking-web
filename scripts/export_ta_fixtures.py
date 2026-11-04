"""
Export golden fixtures for the TypeScript port of the indicator pipeline.

indicators/<T>.json : bars + every indicator column (full history), per sample ticker
pipeline/<D>.json   : history payload for date D + the server response computed
                      in-process from exactly those bars (results, totals, sectors)

Usage: python scripts/export_ta_fixtures.py 2026-10-30 2026-11-02 2026-11-03
"""
import json
import math
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "api"))

import app as api_app  # noqa: E402
from src.history_export import build_history_payload  # noqa: E402
from src.indicators.calculator import calculate_all_indicators  # noqa: E402
from src.utils import parallel_processor  # noqa: E402
from src.utils.stock_loader import load_stock_list  # noqa: E402

OUT = os.path.join(ROOT, "web", "lib", "ta", "__fixtures__")
# Liquid, flat/illiquid UPCOM, and index cases (zero ranges, equal prices)
SAMPLE = ["HPG", "VCB", "QNS", "DRI", "VGI", "VNINDEX", "^GSPC"]


def clean(v):
    """NaN -> null, ±inf -> "Infinity"/"-Infinity" so the TS test can tell them apart."""
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (np.floating, np.integer)):
        v = v.item()
    if isinstance(v, float) and math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v


def frame_from_entry(e):
    nan = lambda xs: [np.nan if x is None else x for x in xs]  # noqa: E731
    return pd.DataFrame({
        "Date": pd.to_datetime(e["d"]), "Open": nan(e["o"]), "High": nan(e["h"]), "Low": nan(e["l"]),
        "Close": nan(e["c"]), "Volume": nan(e["v"]), "Dividends": 0, "Stock Splits": 0,
    }).astype({"Open": float, "High": float, "Low": float, "Close": float, "Volume": float})


def main(dates):
    stocks = load_stock_list(os.path.join(ROOT, "api", "TA_Tracking_List.csv"))
    os.makedirs(os.path.join(OUT, "indicators"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "pipeline"), exist_ok=True)
    real_fetch = parallel_processor.fetch_stock_data

    for i, d in enumerate(dates):
        as_of = datetime.strptime(d, "%Y-%m-%d")
        parallel_processor.fetch_stock_data = real_fetch
        history = build_history_payload(stocks, as_of)
        frames = {e["Ticker"]: frame_from_entry(e) for e in history["tickers"] if e["d"]}

        # Serve the exported bars to the production pipeline: no re-fetch
        parallel_processor.fetch_stock_data = lambda t, end, exchange="": frames.get(t)
        response = api_app._run_analysis(as_of)
        print(f"{d}: {len(response['results'])} results, {len(response['errors'])} errors {response['errors'][:3]}")

        with open(os.path.join(OUT, "pipeline", f"{d}.json"), "w", encoding="utf-8") as f:
            json.dump({"history": history, "expected": response}, f, ensure_ascii=False, separators=(",", ":"))

        if i == len(dates) - 1:
            for t in SAMPLE:
                ind = calculate_all_indicators(frames[t])
                cols = {c: [clean(v) for v in ind[c]] for c in ind.columns if c not in ("Date", "Dividends", "Stock Splits")}
                with open(os.path.join(OUT, "indicators", f"{t.replace('^', '_')}.json"), "w", encoding="utf-8") as f:
                    json.dump({"ticker": t, "columns": cols}, f, separators=(",", ":"))


if __name__ == "__main__":
    main(sys.argv[1:])
