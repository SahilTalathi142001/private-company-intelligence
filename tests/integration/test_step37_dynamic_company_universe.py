import gzip
import json
from pathlib import Path

import pandas as pd

from company_platform.multi_company import dynamic_universe


class FakeHTTPResponse:
    def __init__(self, body, content_encoding=""):
        self.body = body
        self.content_encoding = content_encoding

    def read(self):
        return self.body

    def getheader(self, name, default=None):
        if name == "Content-Encoding":
            return self.content_encoding or default
        return default


def test_http_response_decodes_gzip_json():
    payload = {"AAPL": {"ticker": "AAPL"}}
    response = FakeHTTPResponse(
        gzip.compress(json.dumps(payload).encode("utf-8")),
        "gzip",
    )

    assert dynamic_universe._HTTPResponse(response).json() == payload


def test_http_response_reads_uncompressed_json():
    payload = {"AAPL": {"ticker": "AAPL"}}
    response = FakeHTTPResponse(
        json.dumps(payload).encode("utf-8")
    )

    assert dynamic_universe._HTTPResponse(response).json() == payload


def fake_sec_master():
    return pd.DataFrame(
        [
            {
                "cik": f"{i:010d}",
                "ticker": ticker,
                "company_name": f"{ticker} CURRENT NAME",
            }
            for i, ticker in enumerate(
                dynamic_universe.DEFAULT_SELECTION,
                start=1,
            )
        ]
    )


def test_selection_contains_exactly_20_tickers():
    tickers = dynamic_universe.load_selection()

    assert len(tickers) == 20
    assert len(set(tickers)) == 20


def test_sec_master_resolves_dynamic_identity():
    master = fake_sec_master()

    universe = (
        dynamic_universe.resolve_target_universe(
            master,
            dynamic_universe.DEFAULT_SELECTION,
        )
    )

    assert len(universe) == 20

    assert universe["ticker"].nunique() == 20
    assert universe["cik"].nunique() == 20

    assert (
        universe["company_id"]
        == "SEC_" + universe["cik"]
    ).all()


def test_company_names_come_from_sec_master():
    master = fake_sec_master()

    universe = (
        dynamic_universe.resolve_target_universe(
            master,
            dynamic_universe.DEFAULT_SELECTION,
        )
    )

    for row in universe.itertuples():
        assert row.company_name == (
            f"{row.ticker} CURRENT NAME"
        )


def test_missing_sec_ticker_is_rejected():
    master = fake_sec_master()

    master = master[
        master["ticker"] != "AAPL"
    ]

    try:
        dynamic_universe.resolve_target_universe(
            master,
            dynamic_universe.DEFAULT_SELECTION,
        )
    except ValueError as exc:
        assert "AAPL" in str(exc)
    else:
        raise AssertionError(
            "Expected missing ticker validation"
        )


def test_duplicate_cik_is_rejected():
    master = fake_sec_master()

    master.loc[
        master["ticker"] == "MSFT",
        "cik",
    ] = master.loc[
        master["ticker"] == "AAPL",
        "cik",
    ].iloc[0]

    universe = (
        dynamic_universe.resolve_target_universe(
            master,
            dynamic_universe.DEFAULT_SELECTION,
        )
    )

    errors = (
        dynamic_universe.validate_target_universe(
            universe
        )
    )

    assert "DUPLICATE_CIK" in errors


def test_audit_is_written(tmp_path, monkeypatch):
    monkeypatch.setattr(
        dynamic_universe,
        "audit_path",
        lambda: (
            tmp_path / "universe_audit.parquet"
        ),
    )

    result = {
        "version": "37.0.0",
        "refreshed_at_utc": "2026-10-06T00:00:00+00:00",
        "companies": [
            {
                "ticker": "AAPL",
                "cik": "0000000001",
                "company_id": "SEC_0000000001",
                "company_name": "Apple CURRENT NAME",
            }
        ],
        "failed": [],
    }

    path = dynamic_universe.write_audit(
        result
    )

    assert path.exists()

    df = pd.read_parquet(path)

    assert len(df) == 1
    assert df.iloc[0]["ticker"] == "AAPL"
    assert (
        df.iloc[0]["company_id"]
        == "SEC_0000000001"
    )