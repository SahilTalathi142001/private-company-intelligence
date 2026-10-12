import gzip
import json
from importlib import import_module
from pathlib import Path
import sys

import pandas as pd
import pytest


def _load_module():
    try:
        return import_module(
            "company_platform.multi_company.large_universe"
        )
    except ModuleNotFoundError as exc:
        if exc.name != "company_platform":
            raise

    root = Path(__file__).resolve().parents[2]

    for candidate in (
        root,
        root / "src",
    ):
        candidate = str(candidate)

        if candidate not in sys.path:
            sys.path.insert(
                0,
                candidate,
            )

    return import_module(
        "company_platform.multi_company.large_universe"
    )


module = _load_module()


class FakeSECResponse:
    def __init__(self, body, content_encoding):
        self.body = body
        self.content_encoding = content_encoding

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.body

    def getheader(self, name, default=None):
        if name == "Content-Encoding":
            return self.content_encoding or default
        return default


def test_fetch_sec_company_master_decodes_gzip(
    monkeypatch,
):
    payload = {
        "0": {
            "cik_str": 320193,
            "ticker": "AAPL",
            "title": "Apple Inc.",
        }
    }
    response = FakeSECResponse(
        gzip.compress(
            json.dumps(payload).encode("utf-8")
        ),
        "gzip",
    )
    monkeypatch.setattr(
        module,
        "_user_agent",
        lambda: "test@example.com",
    )
    monkeypatch.setattr(
        module,
        "urlopen",
        lambda request, timeout: response,
    )

    master = module.fetch_sec_company_master()

    assert master.to_dict("records") == [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
        }
    ]


def _master():
    return pd.DataFrame(
        [
            {
                "cik": f"{i:010d}",
                "ticker": f"T{i}",
                "company_name": f"Company {i}",
            }
            for i in range(1, 51)
        ]
    )


def test_load_selection_matches_config():
    count, tickers = (
        module.load_selection()
    )

    assert count == 50
    assert len(tickers) == 50
    assert len(set(tickers)) == 50


def test_resolve_larger_universe():
    master = _master()

    tickers = [
        f"T{i}"
        for i in range(1, 51)
    ]

    universe = (
        module.resolve_target_universe(
            master,
            tickers,
        )
    )

    assert len(universe) == 50
    assert universe["ticker"].nunique() == 50
    assert universe["cik"].nunique() == 50

    assert (
        universe["company_id"]
        == "SEC_"
        + universe["cik"]
        .astype(str)
        .str.zfill(10)
    ).all()


def test_missing_ticker_fails():
    master = _master()

    tickers = [
        f"T{i}"
        for i in range(1, 50)
    ]

    tickers.append(
        "NOT_REAL"
    )

    with pytest.raises(
        ValueError,
        match="not found",
    ):
        module.resolve_target_universe(
            master,
            tickers,
        )


def test_duplicate_cik_is_detected():
    master = _master()

    master.loc[
        master["ticker"] == "T50",
        "cik",
    ] = "0000000001"

    universe = (
        module.resolve_target_universe(
            master,
            [
                f"T{i}"
                for i in range(1, 51)
            ],
        )
    )

    errors = module.validate_universe(
        universe,
        50,
    )

    assert "DUPLICATE_CIK" in errors


def test_wrong_company_count_is_detected():
    master = _master()

    universe = (
        module.resolve_target_universe(
            master,
            [
                f"T{i}"
                for i in range(1, 50)
            ],
        )
    )

    errors = module.validate_universe(
        universe,
        50,
    )

    assert any(
        x.startswith(
            "EXPECTED_COMPANY_COUNT"
        )
        for x in errors
    )


def test_duplicate_ticker_is_detected():
    master = _master()

    master.loc[
        master["ticker"] == "T50",
        "ticker",
    ] = "T49"

    universe = (
        master[
            master["ticker"].isin(
                [
                    f"T{i}"
                    for i in range(1, 50)
                ]
            )
        ]
        .copy()
    )

    universe["company_id"] = (
        "SEC_"
        + universe["cik"]
        .astype(str)
        .str.zfill(10)
    )

    errors = module.validate_universe(
        universe,
        50,
    )

    assert "DUPLICATE_TICKER" in errors


def test_audit_is_written(
    tmp_path,
    monkeypatch,
):
    audit = (
        tmp_path
        / "universe_audit.parquet"
    )

    monkeypatch.setattr(
        module,
        "audit_path",
        lambda: audit,
    )

    result = {
        "version": "38.0.0",
        "refreshed_at_utc":
            "2026-10-07T00:00:00+00:00",
        "target_count": 50,
        "resolved_count": 50,
        "companies": [
            {
                "ticker": "T1",
                "cik": "0000000001",
                "company_id":
                    "SEC_0000000001",
                "company_name":
                    "Company 1",
            }
        ],
        "failed": [],
    }

    path = module.write_audit(
        result
    )

    assert path == audit
    assert audit.exists()

    data = pd.read_parquet(
        audit
    )

    assert len(data) == 1
    assert data.iloc[0]["ticker"] == "T1"