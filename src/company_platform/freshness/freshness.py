"""
STEP 15 - PLATFORM DATA FRESHNESS
=================================

Read-only freshness evaluation for the active company universe.

Responsibilities
----------------
1. Read the authoritative company registry.
2. Read Step 36 SEC live-refresh coverage.
3. Evaluate freshness per company.
4. Produce deterministic company and platform freshness artifacts.

This component MUST NOT:
- trigger SEC refreshes
- emit events
- execute pipeline tasks
- retry failures
- modify company lifecycle state
- schedule execution

Freshness statuses:
- FRESH
- STALE
- NEVER_REFRESHED
- ERROR
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


VERSION = "15.0.0"

ROOT = Path(__file__).resolve().parents[3]

REGISTRY_PATH = (
    ROOT
    / "data"
    / "silver"
    / "platform"
    / "company_registry.parquet"
)

SEC_COVERAGE_PATH = (
    ROOT
    / "data"
    / "gold"
    / "sec_live_refresh"
    / "refresh_coverage.parquet"
)

OUTPUT_DIR = (
    ROOT
    / "data"
    / "gold"
    / "platform_freshness"
)

COMPANY_OUTPUT_PATH = (
    OUTPUT_DIR
    / "company_freshness.parquet"
)

SUMMARY_OUTPUT_PATH = (
    OUTPUT_DIR
    / "summary.parquet"
)


FRESHNESS_STATUSES = {
    "FRESH",
    "STALE",
    "NEVER_REFRESHED",
    "ERROR",
}


def utc_now() -> datetime:
    """Return timezone-aware current UTC timestamp."""
    return datetime.now(timezone.utc)


def parse_utc(value) -> Optional[datetime]:
    """
    Parse a UTC timestamp safely.

    Invalid, blank, or missing values return None.
    """
    if value is None:
        return None

    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text:
        return None

    try:
        timestamp = pd.to_datetime(
            text,
            utc=True,
            errors="coerce",
        )

        if pd.isna(timestamp):
            return None

        return timestamp.to_pydatetime()
    except Exception:
        return None


def load_registry(path: Path = REGISTRY_PATH) -> pd.DataFrame:
    """Load and validate the company registry."""
    if not path.exists():
        raise FileNotFoundError(
            f"Company registry not found: {path}"
        )

    df = pd.read_parquet(path)

    required = {
        "company_id",
        "cik",
        "company_name",
        "ticker",
        "status",
        "ingestion_enabled",
    }

    missing = sorted(required.difference(df.columns))

    if missing:
        raise ValueError(
            "Company registry is missing required columns: "
            + ", ".join(missing)
        )

    return df.copy()


def load_sec_coverage(
    path: Path = SEC_COVERAGE_PATH,
) -> pd.DataFrame:
    """
    Load Step 36 SEC refresh coverage.

    Missing coverage is represented as an empty dataframe so that
    the freshness evaluator can classify companies as NEVER_REFRESHED.
    """
    if not path.exists():
        return pd.DataFrame()

    return pd.read_parquet(path)


def latest_sec_coverage(
    coverage: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return the latest Step 36 coverage record per company.

    Coverage is sorted deterministically by:
        company_id
        refreshed_at_utc
        refresh_id
    """
    if coverage.empty:
        return coverage.copy()

    required = {
        "company_id",
        "cik",
        "status",
        "refreshed_at_utc",
    }

    missing = sorted(required.difference(coverage.columns))

    if missing:
        raise ValueError(
            "SEC refresh coverage is missing required columns: "
            + ", ".join(missing)
        )

    result = coverage.copy()

    result["_refresh_timestamp"] = result[
        "refreshed_at_utc"
    ].apply(parse_utc)

    result = result.sort_values(
        [
            "company_id",
            "_refresh_timestamp",
        ],
        kind="stable",
    )

    result = (
        result
        .drop_duplicates(
            subset=["company_id"],
            keep="last",
        )
        .drop(columns=["_refresh_timestamp"])
        .reset_index(drop=True)
    )

    return result


def evaluate_status(
    *,
    refreshed_at: Optional[datetime],
    coverage_status: Optional[str],
    now: datetime,
    max_age_minutes: int,
) -> tuple[str, Optional[float]]:
    """
    Determine freshness status and age in minutes.
    """
    status = (
        str(coverage_status).strip().upper()
        if coverage_status is not None
        else ""
    )

    if status == "FAILED":
        return "ERROR", None

    if refreshed_at is None:
        return "NEVER_REFRESHED", None

    age_seconds = (
        now - refreshed_at
    ).total_seconds()

    age_minutes = max(
        0.0,
        age_seconds / 60.0,
    )

    if age_minutes <= max_age_minutes:
        return "FRESH", age_minutes

    return "STALE", age_minutes


def build_company_freshness(
    registry: pd.DataFrame,
    coverage: pd.DataFrame,
    *,
    now: Optional[datetime] = None,
    max_age_minutes: int = 120,
) -> pd.DataFrame:
    """
    Build the company-level freshness contract.

    Only active + ingestion-enabled companies are evaluated.
    """
    if max_age_minutes < 0:
        raise ValueError(
            "max_age_minutes must be >= 0"
        )

    now = now or utc_now()

    active = registry[
        (
            registry["status"]
            .astype(str)
            .str.upper()
            == "ACTIVE"
        )
        &
        (
            registry["ingestion_enabled"]
            .fillna(False)
            .astype(bool)
        )
    ].copy()

    latest = latest_sec_coverage(coverage)

    rows = []

    for record in active.to_dict("records"):
        company_id = str(
            record["company_id"]
        )

        company_coverage = latest[
            latest["company_id"].astype(str)
            == company_id
        ]

        if company_coverage.empty:
            refreshed_at = None
            coverage_status = None
            refresh_id = None
            last_error = record.get(
                "last_error"
            )
        else:
            coverage_record = (
                company_coverage.iloc[0]
            )

            refreshed_at = parse_utc(
                coverage_record[
                    "refreshed_at_utc"
                ]
            )

            coverage_status = (
                coverage_record["status"]
            )

            refresh_id = (
                coverage_record.get(
                    "refresh_id"
                )
            )

            last_error = (
                coverage_record.get(
                    "last_error"
                )
            )

            if pd.isna(last_error):
                last_error = None

        freshness_status, age_minutes = (
            evaluate_status(
                refreshed_at=refreshed_at,
                coverage_status=coverage_status,
                now=now,
                max_age_minutes=max_age_minutes,
            )
        )

        rows.append(
            {
                "version": VERSION,
                "evaluated_at_utc": now.isoformat(),
                "company_id": company_id,
                "cik": str(record["cik"]).zfill(10),
                "ticker": str(record["ticker"]),
                "company_name": str(
                    record["company_name"]
                ),
                "company_status": str(
                    record["status"]
                ),
                "ingestion_enabled": bool(
                    record["ingestion_enabled"]
                ),
                "refresh_id": refresh_id,
                "last_refresh_status": (
                    coverage_status
                ),
                "last_refresh_utc": (
                    refreshed_at.isoformat()
                    if refreshed_at
                    else None
                ),
                "age_minutes": age_minutes,
                "max_age_minutes": (
                    max_age_minutes
                ),
                "freshness_status": (
                    freshness_status
                ),
                "last_error": last_error,
            }
        )

    result = pd.DataFrame(rows)

    if result.empty:
        return pd.DataFrame(
            columns=[
                "version",
                "evaluated_at_utc",
                "company_id",
                "cik",
                "ticker",
                "company_name",
                "company_status",
                "ingestion_enabled",
                "refresh_id",
                "last_refresh_status",
                "last_refresh_utc",
                "age_minutes",
                "max_age_minutes",
                "freshness_status",
                "last_error",
            ]
        )

    return (
        result
        .sort_values(
            ["ticker", "company_id"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def build_summary(
    company_freshness: pd.DataFrame,
    *,
    now: Optional[datetime] = None,
    max_age_minutes: int = 120,
) -> pd.DataFrame:
    """Build one deterministic platform-level freshness summary."""
    now = now or utc_now()

    total = len(company_freshness)

    counts = {
        status: int(
            (
                company_freshness[
                    "freshness_status"
                ]
                == status
            ).sum()
        )
        for status in sorted(
            FRESHNESS_STATUSES
        )
    }

    fresh = counts["FRESH"]

    if total == 0:
        coverage_pct = 0.0
    else:
        coverage_pct = (
            fresh / total
        ) * 100.0

    if counts["ERROR"] > 0:
        platform_status = "ERROR"
    elif counts["NEVER_REFRESHED"] > 0:
        platform_status = "NEVER_REFRESHED"
    elif counts["STALE"] > 0:
        platform_status = "STALE"
    else:
        platform_status = "FRESH"

    return pd.DataFrame(
        [
            {
                "version": VERSION,
                "evaluated_at_utc": now.isoformat(),
                "max_age_minutes": (
                    max_age_minutes
                ),
                "companies_evaluated": total,
                "fresh_count": counts["FRESH"],
                "stale_count": counts["STALE"],
                "never_refreshed_count": (
                    counts["NEVER_REFRESHED"]
                ),
                "error_count": counts["ERROR"],
                "freshness_coverage_pct": (
                    coverage_pct
                ),
                "platform_freshness_status": (
                    platform_status
                ),
            }
        ]
    )


def write_outputs(
    company_freshness: pd.DataFrame,
    summary: pd.DataFrame,
    *,
    company_path: Path = COMPANY_OUTPUT_PATH,
    summary_path: Path = SUMMARY_OUTPUT_PATH,
) -> tuple[Path, Path]:
    """Persist freshness artifacts."""
    company_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    company_freshness.to_parquet(
        company_path,
        index=False,
    )

    summary.to_parquet(
        summary_path,
        index=False,
    )

    return company_path, summary_path


def run_check(
    *,
    registry_path: Path = REGISTRY_PATH,
    coverage_path: Path = SEC_COVERAGE_PATH,
    company_output_path: Path = COMPANY_OUTPUT_PATH,
    summary_output_path: Path = SUMMARY_OUTPUT_PATH,
    max_age_minutes: int = 120,
    now: Optional[datetime] = None,
) -> dict:
    """
    Execute the read-only freshness evaluation.
    """
    now = now or utc_now()

    registry = load_registry(
        registry_path
    )

    coverage = load_sec_coverage(
        coverage_path
    )

    company_freshness = (
        build_company_freshness(
            registry,
            coverage,
            now=now,
            max_age_minutes=max_age_minutes,
        )
    )

    summary = build_summary(
        company_freshness,
        now=now,
        max_age_minutes=max_age_minutes,
    )

    company_path, summary_path = (
        write_outputs(
            company_freshness,
            summary,
            company_path=company_output_path,
            summary_path=summary_output_path,
        )
    )

    result = summary.iloc[0].to_dict()

    result["company_output"] = str(
        company_path
    )
    result["summary_output"] = str(
        summary_path
    )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Step 15 - Platform Data Freshness"
        )
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="Evaluate current platform freshness.",
    )

    parser.add_argument(
        "--max-age-minutes",
        type=int,
        default=120,
        help=(
            "Maximum age for FRESH status. "
            "Default: 120 minutes."
        ),
    )

    args = parser.parse_args()

    if not args.check:
        parser.error(
            "Use --check to evaluate freshness."
        )

    result = run_check(
        max_age_minutes=(
            args.max_age_minutes
        )
    )

    print(
        "========================================"
    )
    print(
        "STEP 15 - DATA FRESHNESS"
    )
    print(
        "========================================"
    )
    print(
        f"Version              : {VERSION}"
    )
    print(
        f"Platform status      : "
        f"{result['platform_freshness_status']}"
    )
    print(
        f"Companies evaluated  : "
        f"{result['companies_evaluated']}"
    )
    print(
        f"Fresh                : "
        f"{result['fresh_count']}"
    )
    print(
        f"Stale                : "
        f"{result['stale_count']}"
    )
    print(
        f"Never refreshed      : "
        f"{result['never_refreshed_count']}"
    )
    print(
        f"Errors               : "
        f"{result['error_count']}"
    )
    print(
        f"Freshness coverage   : "
        f"{result['freshness_coverage_pct']:.2f}%"
    )
    print(
        f"Threshold            : "
        f"{result['max_age_minutes']} minutes"
    )
    print()
    print(
        f"Company freshness    : "
        f"{result['company_output']}"
    )
    print(
        f"Summary              : "
        f"{result['summary_output']}"
    )
    print(
        "========================================"
    )


if __name__ == "__main__":
    main()