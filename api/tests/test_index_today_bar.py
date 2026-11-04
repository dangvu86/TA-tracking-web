from datetime import date, datetime

import pandas as pd

from src import data_fetcher


def _frame(rows):
    df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    df["Date"] = pd.to_datetime(df["Date"])
    df["Dividends"] = 0
    df["Stock Splits"] = 0
    return df


FA = [("2026-11-03", 1710.04, 1713.02, 1696.23, 1699.62, 515501785.0),
      ("2026-11-04", 1697.71, 1738.24, 1692.36, 1732.51, 609085757.0)]   # stale at 15:00
VND = [("2026-11-03", 1710.04, 1713.02, 1696.23, 1699.62, 515501785.0),
       ("2026-11-04", 1697.71, 1738.24, 1692.36, 1733.02, 785061515.0)]


def _setup(monkeypatch, today, vnd=VND):
    monkeypatch.setattr(data_fetcher, "fetch_fireant_data", lambda t, p: _frame(FA))
    monkeypatch.setattr(data_fetcher, "fetch_tv_index_data", lambda s, p, use_cache=True: _frame(vnd) if vnd else None)
    monkeypatch.setattr(data_fetcher, "_vn_today", lambda: today)
    data_fetcher._cache.clear()


def test_today_vnindex_bar_comes_from_vndirect(monkeypatch):
    _setup(monkeypatch, date(2026, 11, 4))
    df = data_fetcher.fetch_stock_data("VNINDEX", datetime(2026, 11, 4))
    assert list(df["Close"]) == [1699.62, 1733.02]
    assert df["Volume"].iloc[-1] == 785061515.0
    assert list(df.columns) == ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]


def test_past_dates_stay_on_fireant(monkeypatch):
    _setup(monkeypatch, date(2026, 11, 5))
    df = data_fetcher.fetch_stock_data("VNINDEX", datetime(2026, 11, 4))
    assert df["Close"].iloc[-1] == 1732.51


def test_vndirect_down_keeps_fireant(monkeypatch):
    _setup(monkeypatch, date(2026, 11, 4), vnd=None)
    df = data_fetcher.fetch_stock_data("VNINDEX", datetime(2026, 11, 4))
    assert df["Close"].iloc[-1] == 1732.51


def test_us_index_ignores_bar_dated_analysis_day(monkeypatch):
    class FakeTicker:
        def __init__(self, t):
            pass

        def history(self, start, end):
            idx = pd.DatetimeIndex(["2026-11-02", "2026-11-03", "2026-11-04"], name="Date").tz_localize("America/Chicago")
            return pd.DataFrame({"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": [21.15, 20.83, 19.06],
                                 "Volume": 0, "Dividends": 0, "Stock Splits": 0}, index=idx)

    monkeypatch.setattr(data_fetcher.yf, "Ticker", FakeTicker)
    data_fetcher._cache.clear()
    df = data_fetcher.fetch_stock_data("^VIX", datetime(2026, 11, 4))
    assert list(df["Close"]) == [21.15, 20.83]
