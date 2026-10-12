"""
STEP 17 - LARGER COMPANY UNIVERSE

Version: 38.0.0

Purpose:
    Expand the production company universe beyond the controlled
    20-company Step 37 universe.

Design:
    - SEC remains authoritative for CIK/company name/ticker.
    - Configuration controls selection only.
    - Existing company registry remains authoritative.
    - Existing register_company() is reused.
    - Existing EventStore / EventConsumer / Orchestrator are untouched.
    - Existing Step 36 SEC live refresh automatically sees newly
      registered ACTIVE companies.
    - Selection size is configuration-driven.

This implementation currently activates 50 companies.
The same controller can later activate 100/250/etc.
"""

from __future__ import annotations

import argparse
import gzip
import importlib
import json
import os
import tempfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from urllib.request import Request, urlopen


pd = importlib.import_module("pandas")


VERSION = "38.0.0"

SEC_COMPANY_TICKERS_URL = (
    "https://www.sec.gov/files/company_tickers.json"
)


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


def _utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def _user_agent() -> str:
    load_dotenv(
        _root() / ".env"
    )

    value = os.getenv(
        "SEC_USER_AGENT"
    )

    if not value or "@" not in value:
        raise ValueError(
            "SEC_USER_AGENT must contain a "
            "descriptive application name and "
            "contact email."
        )

    return value


def selection_path() -> Path:
    return (
        _root()
        / "config"
        / "company_universe_50.json"
    )


def company_master_path() -> Path:
    return (
        _root()
        / "data"
        / "silver"
        / "company"
        / "company.parquet"
    )


def audit_path() -> Path:
    return (
        _root()
        / "data"
        / "gold"
        / "multi_company"
        / "large_universe"
        / "universe_audit.parquet"
    )


def load_selection() -> tuple[int, list[str]]:
    """
    Load scalable universe configuration.

    Configuration controls selection.
    SEC controls identity.
    """

    path = selection_path()

    if not path.exists():
        raise FileNotFoundError(
            f"Universe configuration not found: {path}"
        )

    payload = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    target_count = int(
        payload.get(
            "target_count",
            0,
        )
    )

    tickers = payload.get(
        "target_tickers",
        [],
    )

    if target_count <= 0:
        raise ValueError(
            "target_count must be greater than zero."
        )

    if not isinstance(
        tickers,
        list,
    ):
        raise ValueError(
            "target_tickers must be a JSON list."
        )

    normalized = [
        str(x)
        .strip()
        .upper()
        for x in tickers
        if str(x).strip()
    ]

    normalized = sorted(
        set(normalized)
    )

    if len(normalized) != target_count:
        raise ValueError(
            "Universe configuration mismatch: "
            f"target_count={target_count}, "
            f"unique_tickers={len(normalized)}"
        )

    return (
        target_count,
        normalized,
    )


def fetch_sec_company_master(
    *,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Fetch the current SEC company ticker master.

    SEC is the authoritative identity source.
    """

    request = Request(
        SEC_COMPANY_TICKERS_URL,
        headers={
            "User-Agent": _user_agent(),
            "Accept-Encoding": "gzip, deflate",
        },
    )

    with urlopen(
        request,
        timeout=timeout,
    ) as response:
        body = response.read()
        content_encoding = response.getheader(
            "Content-Encoding",
            "",
        )

        for encoding in reversed(
            [
                value.strip().lower()
                for value in content_encoding.split(",")
                if value.strip()
            ]
        ):
            if encoding == "identity":
                continue
            if encoding == "gzip":
                body = gzip.decompress(body)
            elif encoding == "deflate":
                try:
                    body = zlib.decompress(body)
                except zlib.error:
                    body = zlib.decompress(
                        body,
                        -zlib.MAX_WBITS,
                    )
            else:
                raise ValueError(
                    "Unsupported SEC response encoding: "
                    f"{encoding}"
                )

        payload = json.loads(
            body.decode("utf-8")
        )

    rows: list[dict[str, str]] = []

    for value in payload.values():
        cik = str(
            value.get(
                "cik_str",
                "",
            )
        ).zfill(10)

        ticker = str(
            value.get(
                "ticker",
                "",
            )
        ).strip().upper()

        company_name = str(
            value.get(
                "title",
                "",
            )
        ).strip()

        if not cik:
            continue

        if not ticker:
            continue

        if not company_name:
            continue

        rows.append(
            {
                "cik": cik,
                "ticker": ticker,
                "company_name": company_name,
            }
        )

    if not rows:
        raise RuntimeError(
            "SEC company master returned "
            "no usable records."
        )

    master = pd.DataFrame(rows)

    duplicate_tickers = (
        master.groupby("ticker")["cik"]
        .nunique()
    )

    ambiguous = duplicate_tickers[
        duplicate_tickers > 1
    ]

    if not ambiguous.empty:
        raise ValueError(
            "SEC company master contains "
            "tickers mapped to multiple CIKs: "
            + ", ".join(
                ambiguous.index.tolist()
            )
        )

    return (
        master
        .sort_values(
            ["ticker", "cik"]
        )
        .drop_duplicates(
            subset=["ticker"],
            keep="first",
        )
        .reset_index(drop=True)
    )


def write_company_master(
    master: pd.DataFrame,
) -> Path:
    """
    Persist the SEC master atomically.

    This is the same master used by the
    existing company registry.
    """

    path = company_master_path()

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    required = {
        "cik",
        "ticker",
        "company_name",
    }

    missing = required.difference(
        master.columns
    )

    if missing:
        raise ValueError(
            "SEC company master missing columns: "
            + ", ".join(
                sorted(missing)
            )
        )

    output = master[
        [
            "cik",
            "ticker",
            "company_name",
        ]
    ].copy()

    output["cik"] = (
        output["cik"]
        .astype(str)
        .str.zfill(10)
    )

    output["ticker"] = (
        output["ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    output["company_name"] = (
        output["company_name"]
        .astype(str)
        .str.strip()
    )

    fd, temp_name = tempfile.mkstemp(
        suffix=".parquet",
        dir=str(
            path.parent
        ),
    )

    os.close(fd)

    temp_path = Path(
        temp_name
    )

    try:
        output.to_parquet(
            temp_path,
            index=False,
        )

        temp_path.replace(
            path
        )

    finally:
        if temp_path.exists():
            temp_path.unlink()

    return path


def resolve_target_universe(
    master: pd.DataFrame,
    tickers: list[str],
) -> pd.DataFrame:
    """
    Resolve configured tickers against
    the current SEC master.
    """

    wanted = {
        str(x)
        .strip()
        .upper()
        for x in tickers
    }

    selected = master[
        master["ticker"]
        .astype(str)
        .str.upper()
        .isin(wanted)
    ].copy()

    found = set(
        selected["ticker"]
        .astype(str)
        .str.upper()
    )

    missing = sorted(
        wanted - found
    )

    if missing:
        raise ValueError(
            "Configured target tickers were not "
            "found in the current SEC company master: "
            + ", ".join(missing)
        )

    if len(selected) != len(wanted):
        raise ValueError(
            "Target universe did not resolve "
            "one-to-one from SEC master."
        )

    selected["company_id"] = (
        "SEC_"
        + selected["cik"]
        .astype(str)
        .str.zfill(10)
    )

    return (
        selected[
            [
                "cik",
                "company_id",
                "ticker",
                "company_name",
            ]
        ]
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def validate_universe(
    universe: pd.DataFrame,
    expected_count: int,
) -> list[str]:

    errors: list[str] = []

    if len(universe) != expected_count:
        errors.append(
            "EXPECTED_COMPANY_COUNT:"
            f"{expected_count}:"
            f"{len(universe)}"
        )

    if universe["ticker"].nunique() != len(
        universe
    ):
        errors.append(
            "DUPLICATE_TICKER"
        )

    if universe["cik"].nunique() != len(
        universe
    ):
        errors.append(
            "DUPLICATE_CIK"
        )

    expected_ids = (
        "SEC_"
        + universe["cik"]
        .astype(str)
        .str.zfill(10)
    )

    if not (
        universe["company_id"]
        .astype(str)
        .eq(expected_ids)
        .all()
    ):
        errors.append(
            "COMPANY_ID_CIK_MISMATCH"
        )

    return errors


def refresh_and_resolve() -> dict[str, Any]:
    """
    Refresh SEC identity data and resolve
    the configured larger universe.
    """

    target_count, tickers = (
        load_selection()
    )

    master = fetch_sec_company_master()

    master_path = (
        write_company_master(
            master
        )
    )

    universe = resolve_target_universe(
        master,
        tickers,
    )

    errors = validate_universe(
        universe,
        target_count,
    )

    if errors:
        raise RuntimeError(
            "Larger company universe validation "
            "failed: "
            + "; ".join(errors)
        )

    return {
        "version": VERSION,
        "status": "READY",
        "refreshed_at_utc": _utc_now(),
        "target_count": target_count,
        "resolved_count": len(
            universe
        ),
        "sec_master_rows": len(
            master
        ),
        "company_master_path": str(
            master_path
        ),
        "companies": universe.to_dict(
            orient="records"
        ),
    }


def activate_universe(
    *,
    source: str = "SEC",
) -> dict[str, Any]:
    """
    Resolve and register the larger universe.

    Existing companies are refreshed through
    the existing register_company() lifecycle.
    """

    from company_platform.registry.company_registry import (
        register_company,
    )

    result = refresh_and_resolve()

    universe = pd.DataFrame(
        result["companies"]
    )

    registered: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    for row in universe.itertuples(
        index=False
    ):

        try:
            record = register_company(
                cik=row.cik,
                source=source,
                preferred_ticker=row.ticker,
            )

            registered.append(
                {
                    "cik": row.cik,
                    "ticker": row.ticker,
                    "company_id": row.company_id,
                    "company_name": row.company_name,
                    "registry_status": record.get(
                        "status"
                    ),
                }
            )

        except Exception as exc:
            failed.append(
                {
                    "cik": row.cik,
                    "ticker": row.ticker,
                    "company_id": row.company_id,
                    "error": str(exc),
                }
            )

    result["registered_count"] = len(
        registered
    )

    result["failed_count"] = len(
        failed
    )

    result["registered"] = registered
    result["failed"] = failed

    result["status"] = (
        "READY"
        if not failed
        else "NOT_READY"
    )

    write_audit(result)

    return result


def write_audit(
    result: dict[str, Any],
) -> Path:

    path = audit_path()

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    rows: list[dict[str, Any]] = []

    failures = {
        item["cik"]: item
        for item in result.get(
            "failed",
            [],
        )
    }

    for company in result.get(
        "companies",
        [],
    ):

        failure = failures.get(
            company["cik"]
        )

        rows.append(
            {
                "version": result[
                    "version"
                ],
                "refreshed_at_utc": result[
                    "refreshed_at_utc"
                ],
                "target_count": result[
                    "target_count"
                ],
                "resolved_count": result[
                    "resolved_count"
                ],
                "ticker": company[
                    "ticker"
                ],
                "cik": company[
                    "cik"
                ],
                "company_id": company[
                    "company_id"
                ],
                "company_name": company[
                    "company_name"
                ],
                "status": (
                    "FAILED"
                    if failure
                    else "REGISTERED"
                ),
                "error": (
                    failure["error"]
                    if failure
                    else None
                ),
            }
        )

    pd.DataFrame(
        rows
    ).to_parquet(
        path,
        index=False,
    )

    return path


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Step 17 - Larger SEC-backed "
            "company universe"
        )
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "Refresh SEC master and resolve "
            "the configured universe."
        ),
    )

    parser.add_argument(
        "--activate",
        action="store_true",
        help=(
            "Refresh SEC master and register "
            "the configured universe."
        ),
    )

    args = parser.parse_args()

    if args.activate:
        result = activate_universe()
    else:
        result = refresh_and_resolve()

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()