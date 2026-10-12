"""
Step 37 / Audit #14
Dynamic SEC-backed 20-company activation universe.

SEC is authoritative for:
    - CIK
    - company name
    - ticker

The configured tickers define WHICH companies we want active.
They do not define company identity.

No CIK/company name is hard-coded.
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
from typing import TYPE_CHECKING, Any
from urllib.request import Request, urlopen

from dotenv import load_dotenv

if TYPE_CHECKING:
    import pandas as pd


class _HTTPResponse:
    """Minimal response adapter for the existing SEC fetch call."""

    def __init__(self, response: Any) -> None:
        self._response = response

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        body = self._response.read()
        content_encoding = self._response.getheader(
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
                    body = zlib.decompress(body, -zlib.MAX_WBITS)
            else:
                raise ValueError(
                    f"Unsupported SEC response encoding: {encoding}"
                )

        return json.loads(body.decode("utf-8"))


class _RequestsCompat:
    """Provide the existing requests.get() interface using urllib."""

    @staticmethod
    def get(
        url: str,
        *,
        headers: dict[str, str],
        timeout: int,
    ) -> _HTTPResponse:
        request = Request(
            url,
            headers=headers,
        )
        return _HTTPResponse(
            urlopen(request, timeout=timeout)
        )


requests = _RequestsCompat()

# Load pandas lazily so the module can be imported by editors/interpreters
# that have not installed the optional dataframe dependency yet.
pd = importlib.import_module("pandas")

from company_platform.registry.company_registry import register_company


VERSION = "37.0.0"

SEC_COMPANY_TICKERS_URL = (
    "https://www.sec.gov/files/company_tickers.json"
)

DEFAULT_SELECTION = [
    "AAPL",
    "MSFT",
    "AMZN",
    "NVDA",
    "GOOGL",
    "META",
    "TSLA",
    "AVGO",
    "ORCL",
    "LLY",
    "JPM",
    "V",
    "WMT",
    "MA",
    "XOM",
    "COST",
    "NFLX",
    "AMD",
    "CRM",
    "ADBE",
]


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _user_agent() -> str:
    load_dotenv(_root() / ".env")

    value = os.getenv("SEC_USER_AGENT")

    if not value or "@" not in value:
        raise ValueError(
            "SEC_USER_AGENT must contain a descriptive "
            "application name and contact email."
        )

    return value


def selection_path() -> Path:
    return (
        _root()
        / "config"
        / "company_universe_20.json"
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
        / "dynamic_universe"
        / "universe_audit.parquet"
    )


def load_selection() -> list[str]:
    """
    Load the desired activation tickers.

    Tickers are selection configuration only.
    Identity always comes from SEC.
    """

    path = selection_path()

    if not path.exists():
        return DEFAULT_SELECTION.copy()

    payload = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    tickers = payload.get(
        "target_tickers",
        [],
    )

    if not isinstance(tickers, list):
        raise ValueError(
            "target_tickers must be a JSON list."
        )

    normalized = sorted(
        {
            str(x).strip().upper()
            for x in tickers
            if str(x).strip()
        }
    )

    if len(normalized) != 20:
        raise ValueError(
            f"Expected exactly 20 target tickers; "
            f"found {len(normalized)}."
        )

    return normalized


def fetch_sec_company_master(
    *,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Fetch the current SEC company ticker master.

    The SEC response is the authoritative identity source.
    """

    response = requests.get(
        SEC_COMPANY_TICKERS_URL,
        headers={
            "User-Agent": _user_agent(),
            "Accept-Encoding": "gzip, deflate",
        },
        timeout=timeout,
    )

    response.raise_for_status()

    payload = response.json()

    rows = []

    for value in payload.values():

        cik = str(
            value.get("cik_str", "")
        ).zfill(10)

        ticker = str(
            value.get("ticker", "")
        ).strip().upper()

        company_name = str(
            value.get("title", "")
        ).strip()

        if not cik or not ticker or not company_name:
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
            "SEC company master returned no usable records."
        )

    df = pd.DataFrame(rows)

    # SEC company_tickers.json can contain multiple listings.
    # One ticker must resolve to exactly one CIK.
    duplicate_tickers = (
        df.groupby("ticker")["cik"]
        .nunique()
    )

    ambiguous = duplicate_tickers[
        duplicate_tickers > 1
    ]

    if not ambiguous.empty:
        raise ValueError(
            "SEC company master contains tickers "
            "mapped to multiple CIKs: "
            + ", ".join(
                ambiguous.index.tolist()
            )
        )

    return (
        df.sort_values(
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
    Replace the local company master atomically.

    This is the same master consumed by company_registry.resolve_company().
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
            + ", ".join(sorted(missing))
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
        dir=str(path.parent),
    )

    os.close(fd)

    temp_path = Path(temp_name)

    try:

        output.to_parquet(
            temp_path,
            index=False,
        )

        temp_path.replace(path)

    finally:

        if temp_path.exists():
            temp_path.unlink()

    return path


def resolve_target_universe(
    master: pd.DataFrame,
    tickers: list[str],
) -> pd.DataFrame:
    """
    Resolve configured selection against current SEC identity data.
    """

    wanted = {
        str(x).strip().upper()
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
            "Configured target tickers were not found "
            "in the current SEC company master: "
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


def validate_target_universe(
    universe: pd.DataFrame,
) -> list[str]:

    errors: list[str] = []

    if len(universe) != 20:
        errors.append(
            f"EXPECTED_20_RESOLVED_COMPANIES:{len(universe)}"
        )

    if universe["ticker"].nunique() != len(universe):
        errors.append(
            "DUPLICATE_TICKER"
        )

    if universe["cik"].nunique() != len(universe):
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
    Main discovery operation.

    1. Fetch current SEC master.
    2. Persist it to the existing company master.
    3. Resolve configured 20 tickers.
    4. Validate exact identity.
    """

    tickers = load_selection()

    master = fetch_sec_company_master()

    master_path = write_company_master(
        master
    )

    universe = resolve_target_universe(
        master,
        tickers,
    )

    errors = validate_target_universe(
        universe
    )

    if errors:
        raise RuntimeError(
            "Dynamic company universe validation failed: "
            + "; ".join(errors)
        )

    return {
        "version": VERSION,
        "status": "READY",
        "refreshed_at_utc": _utc_now(),
        "sec_master_rows": len(master),
        "target_count": len(tickers),
        "resolved_count": len(universe),
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
    Resolve current SEC identities and register missing companies.

    Existing companies are refreshed through register_company().
    """

    result = refresh_and_resolve()

    universe = pd.DataFrame(
        result["companies"]
    )

    registered = []
    failed = []

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

    rows = []

    for company in result.get(
        "companies",
        [],
    ):

        failure = next(
            (
                x
                for x in result.get(
                    "failed",
                    [],
                )
                if x["cik"]
                == company["cik"]
            ),
            None,
        )

        rows.append(
            {
                "version": result["version"],
                "refreshed_at_utc": result[
                    "refreshed_at_utc"
                ],
                "ticker": company["ticker"],
                "cik": company["cik"],
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

    pd.DataFrame(rows).to_parquet(
        path,
        index=False,
    )

    return path


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Dynamic SEC-backed 20-company "
            "universe activation"
        )
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh SEC master and resolve 20 targets.",
    )

    parser.add_argument(
        "--activate-20",
        action="store_true",
        help="Refresh SEC master and register the 20 targets.",
    )

    args = parser.parse_args()

    if args.activate_20:

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