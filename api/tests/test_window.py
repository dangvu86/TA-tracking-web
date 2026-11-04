from datetime import datetime, timedelta

import pandas as pd

from src import data_fetcher


def _frame(dates):
    return pd.DataFrame({
        "Date": pd.to_datetime(dates), "Open": 1.0, "High": 1.0, "Low": 1.0,
        "Close": 1.0, "Volume": 1.0, "Dividends": 0, "Stock Splits": 0,
    })


def test_window_is_anchored_to_end_date(monkeypatch):
    dates = ["2025-11-01", "2025-11-03", "2025-11-04", "2026-11-03", "2026-11-04", "2026-11-05"]
    monkeypatch.setattr(data_fetcher, "fetch_fireant_data", lambda t, p: _frame(dates))
    data_fetcher._cache.clear()
    df = data_fetcher.fetch_stock_data("HPG", datetime(2026, 11, 4), exchange="HOSE")
    assert [d.strftime("%Y-%m-%d") for d in df["Date"]] == ["2025-11-04", "2026-11-03", "2026-11-04"]


def test_fetch_span_covers_older_analysis_dates(monkeypatch):
    seen = []

    def fake(t, p):
        seen.append(p)
        return _frame(["2026-01-05"])

    monkeypatch.setattr(data_fetcher, "fetch_fireant_data", fake)
    data_fetcher._cache.clear()
    end = datetime.now() - timedelta(days=30)
    data_fetcher.fetch_stock_data("HPG", end, exchange="HOSE")
    assert seen and seen[0] >= 365 + 30
