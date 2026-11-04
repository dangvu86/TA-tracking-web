from datetime import datetime, timezone

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app as api_app
from src import history_export


def _df():
    return pd.DataFrame({
        "Date": pd.to_datetime(["2026-11-03", "2026-11-04"]),
        "Open": [1.0, 2.0], "High": [1.5, 2.5], "Low": [0.5, 1.5],
        "Close": [1.2, 2.2], "Volume": [100.0, 200.0],
    })


def test_build_history_payload_columnar(monkeypatch):
    monkeypatch.setattr(history_export, "fetch_stock_data", lambda t, d, exchange="": _df())
    stocks = pd.DataFrame([{"Sector": "CK", "Ticker": "SSI", "Exchange": "HOSE"}])
    p = history_export.build_history_payload(stocks, datetime(2026, 11, 4))
    assert p["as_of"] == "2026-11-04"
    assert p["window_days"] == 365
    t = p["tickers"][0]
    assert (t["Ticker"], t["Sector"], t["Exchange"]) == ("SSI", "CK", "HOSE")
    assert t["d"] == ["2026-11-03", "2026-11-04"]
    assert t["c"] == [1.2, 2.2] and t["v"] == [100.0, 200.0]


def test_morning_payload_drops_todays_bar(monkeypatch):
    monkeypatch.setattr(history_export, "fetch_stock_data", lambda t, d, exchange="": _df())
    stocks = pd.DataFrame([{"Sector": "CK", "Ticker": "SSI", "Exchange": "HOSE"}])
    p = history_export.build_history_payload(stocks, datetime(2026, 11, 4), include_as_of=False)
    assert p["tickers"][0]["d"] == ["2026-11-03"]


def test_index_rows_keep_raw_exchange_for_fetch(monkeypatch):
    seen = []
    monkeypatch.setattr(history_export, "fetch_stock_data",
                        lambda t, d, exchange="": seen.append(exchange) or _df())
    stocks = pd.DataFrame([{"Sector": "Index", "Ticker": "VNINDEX", "Exchange": float("nan")}])
    p = history_export.build_history_payload(stocks, datetime(2026, 11, 4))
    assert pd.isna(seen[0])
    assert p["tickers"][0]["Exchange"] == ""


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("GENERATE_TOKEN", "t")
    monkeypatch.setenv("GCS_BUCKET", "b")
    return TestClient(api_app.app)


def _stub_generate(monkeypatch, calls, fail=False):
    monkeypatch.setattr(api_app, "_run_analysis", lambda d: {"meta": {"total_stocks": 1, "elapsed_seconds": 0}, "errors": []})
    monkeypatch.setattr(api_app, "upload_analysis", lambda *a, **k: True)

    def fake_upload(bucket, path, payload, cache):
        if fail:
            raise RuntimeError("boom")
        calls.append((path, payload))
        return True

    monkeypatch.setattr(api_app, "upload_history", fake_upload)
    monkeypatch.setattr(api_app.parallel_processor, "used_frames", {"SSI": _df()})


def test_generate_survives_history_failure(client, monkeypatch):
    _stub_generate(monkeypatch, [], fail=True)
    r = client.post("/api/generate?date=2026-11-04", headers={"X-Generate-Token": "t"})
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_generate_writes_final_history(client, monkeypatch):
    calls = []
    _stub_generate(monkeypatch, calls)
    r = client.post("/api/generate?date=2026-11-04", headers={"X-Generate-Token": "t"})
    assert r.status_code == 200
    assert len(calls) == 1
    path, payload = calls[0]
    assert path == "history/final/2026-11-04.json"
    assert payload["as_of"] == "2026-11-04"
    csv = api_app.load_stock_list("TA_Tracking_List.csv")
    assert len(payload["tickers"]) == len(csv)


@pytest.mark.parametrize("utc_now, expected", [
    (datetime(2026, 11, 4, 1, 45, tzinfo=timezone.utc), "2026-11-04"),
    (datetime(2026, 11, 4, 17, 30, tzinfo=timezone.utc), "2026-11-05"),
])
def test_history_base_uses_vietnam_date(client, monkeypatch, utc_now, expected):
    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return utc_now.astimezone(tz)

    seen = {}
    monkeypatch.setattr(api_app, "datetime", FakeDatetime)
    monkeypatch.setattr(api_app, "build_history_payload",
                        lambda df, as_of, include_as_of=True, frames=None: seen.update(as_of=as_of, inc=include_as_of)
                        or {"as_of": as_of.strftime("%Y-%m-%d"), "window_days": 365, "tickers": []})
    monkeypatch.setattr(api_app, "upload_history", lambda b, path, p, c: seen.update(path=path) or True)
    r = client.post("/api/history-base", headers={"X-Generate-Token": "t"})
    assert r.status_code == 200
    assert seen["as_of"].strftime("%Y-%m-%d") == expected
    assert seen["inc"] is False and seen["path"] == "history/base.json"


def test_history_base_rejects_bad_token(client):
    assert client.post("/api/history-base", headers={"X-Generate-Token": "x"}).status_code == 401
