from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[3]

REGISTRY_DIR = ROOT / "data" / "silver" / "platform"

REGISTRY_PATH = (
    REGISTRY_DIR / "company_registry.parquet"
)

COMPANY_MASTER_PATH = (
    ROOT
    / "data"
    / "silver"
    / "company"
    / "company.parquet"
)


# ---------------------------------------------------------------------------
# Controlled vocabularies
# ---------------------------------------------------------------------------

COMPANY_SOURCES = {
    "SEC",
    "USER",
    "IMPORT",
    "SYSTEM",
}

COMPANY_STATUSES = {
    "ACTIVE",
    "INACTIVE",
    "SUSPENDED",
}

ONBOARDING_STATUSES = {
    "PENDING",
    "RUNNING",
    "ACTIVE",
    "PARTIAL",
    "FAILED",
    "DISABLED",
}


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    """Return the current UTC timestamp as ISO-8601 text."""
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(cik: str) -> str:
    """
    Normalize an SEC CIK to the canonical 10-digit representation.

    Examples
    --------
    320193       -> 0000320193
    0000320193   -> 0000320193
    """
    value = str(cik).strip()

    if not value:
        raise ValueError("CIK cannot be empty")

    if not re.fullmatch(r"\d+", value):
        raise ValueError(
            f"Invalid SEC CIK: {cik!r}. "
            "CIK must contain digits only."
        )

    return value.zfill(10)


def company_id_from_cik(cik: str) -> str:
    """
    Build the current SEC-backed platform company ID.

    This is intentionally SEC-specific for the current Phase 1
    architecture. A future multi-source identity model can separate
    platform company identity from source identifiers.
    """
    normalized = normalize_cik(cik)
    return f"SEC_{normalized}"


# ---------------------------------------------------------------------------
# Registry schema
# ---------------------------------------------------------------------------

REGISTRY_COLUMNS = [
    "company_id",
    "cik",
    "company_name",
    "ticker",
    "source",
    "status",
    "onboarding_status",
    "ingestion_enabled",
    "created_at_utc",
    "updated_at_utc",
    "last_successful_run_utc",
    "last_error",
]


def _empty_registry() -> pd.DataFrame:
    """Return an empty registry with the canonical schema."""
    return pd.DataFrame(columns=REGISTRY_COLUMNS)


def _validate_registry_schema(df: pd.DataFrame) -> None:
    """Validate the minimum registry schema."""
    missing = [
        column
        for column in REGISTRY_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Company registry schema is missing columns: "
            + ", ".join(missing)
        )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_registry() -> pd.DataFrame:
    """
    Load the company registry.

    Missing registry is treated as an empty registry.
    """
    if not REGISTRY_PATH.exists():
        return _empty_registry()

    df = pd.read_parquet(REGISTRY_PATH)

    _validate_registry_schema(df)

    return df


def save_registry(df: pd.DataFrame) -> None:
    """Persist the company registry."""
    _validate_registry_schema(df)

    REGISTRY_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Keep deterministic column ordering.
    df = df[REGISTRY_COLUMNS].copy()

    df.to_parquet(
        REGISTRY_PATH,
        index=False,
    )


# ---------------------------------------------------------------------------
# Company resolution
# ---------------------------------------------------------------------------

def resolve_company(cik: str,
    preferred_ticker: Optional[str] = None,) -> dict:
    """
    Resolve a CIK against the existing company master.

    The company master is:

        data/silver/company/company.parquet

    Current schema:

        cik
        ticker
        company_name

    The registry therefore does not become another SEC ingestion
    implementation.
    """
    normalized_cik = normalize_cik(cik)

    if not COMPANY_MASTER_PATH.exists():
        raise FileNotFoundError(
            f"Company master not found: "
            f"{COMPANY_MASTER_PATH}"
        )

    master = pd.read_parquet(
        COMPANY_MASTER_PATH
    )

    required_columns = {
        "cik",
        "ticker",
        "company_name",
    }

    missing = required_columns.difference(
        master.columns
    )

    if missing:
        raise ValueError(
            "Company master is missing required columns: "
            + ", ".join(sorted(missing))
        )

    cik_series = (
        master["cik"]
        .astype(str)
        .str.strip()
        .str.zfill(10)
    )

    matches = master[
        cik_series == normalized_cik
    ]

    if matches.empty:
        raise ValueError(
            f"CIK {normalized_cik} does not exist "
            "in the company master."
        )

    if len(matches) > 1:
        preferred = None

        if preferred_ticker is not None:
            preferred = (
                str(preferred_ticker)
                .strip()
                .upper()
            )

        # If no explicit ticker was supplied, use the ticker
        # already associated with this CIK in the registry.
        if not preferred:
            try:
                registry = load_registry()

                registry_matches = registry[
                    registry["cik"]
                    .astype(str)
                    .str.strip()
                    .str.zfill(10)
                    == normalized_cik
                ]

                registry_tickers = (
                    registry_matches["ticker"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .unique()
                    .tolist()
                )

                if len(registry_tickers) == 1:
                    preferred = registry_tickers[0]

            except Exception:
                preferred = None

        if not preferred:
            raise ValueError(
                f"CIK {normalized_cik} has "
                f"{len(matches)} records in the company master. "
                "Add an explicit preferred ticker."
            )

        selected = matches[
            matches["ticker"]
            .astype(str)
            .str.strip()
            .str.upper()
            == preferred
        ]

        if len(selected) != 1:
            available = sorted(
                matches["ticker"]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .unique()
                .tolist()
            )

            raise ValueError(
                f"Preferred ticker {preferred} is not unique "
                f"for CIK {normalized_cik}. "
                f"Available tickers: {available}"
            )

        row = selected.iloc[0]

    else:
        row = matches.iloc[0]

    ticker = row["ticker"]

    if pd.isna(ticker):
        ticker = None
    else:
        ticker = str(ticker).strip()

    company_name = row["company_name"]

    if pd.isna(company_name):
        raise ValueError(
            f"Company master contains a null company name "
            f"for CIK {normalized_cik}."
        )

    company_name = str(company_name).strip()

    if not company_name:
        raise ValueError(
            f"Company master contains an empty company name "
            f"for CIK {normalized_cik}."
        )

    return {
        "company_id": company_id_from_cik(
            normalized_cik
        ),
        "cik": normalized_cik,
        "company_name": company_name,
        "ticker": ticker,
    }


# ---------------------------------------------------------------------------
# Registry lifecycle
# ---------------------------------------------------------------------------

def register_company(
    cik: str,
    source: str = "USER",
   preferred_ticker: Optional[str] = None,
) -> dict:
    """
    Register a company for platform processing.

    Registration and onboarding are intentionally separate.

    Newly registered company:

        status            = ACTIVE
        onboarding_status = PENDING
        ingestion_enabled = True

    The company becomes ACTIVE from an onboarding perspective only
    after the orchestration lifecycle completes successfully.
    """
    if source not in COMPANY_SOURCES:
        raise ValueError(
            f"Invalid company source: {source}. "
            f"Allowed values: {sorted(COMPANY_SOURCES)}"
        )

    company = resolve_company(cik,
    preferred_ticker=preferred_ticker,)

    now = _utc_now()

    registry = load_registry()

    existing = registry[
        registry["company_id"].astype(str)
        == company["company_id"]
    ]

    # ------------------------------------------------------------------
    # Existing company
    # ------------------------------------------------------------------

    if not existing.empty:
        if len(existing) > 1:
            raise ValueError(
                f"Duplicate company_id in registry: "
                f"{company['company_id']}"
            )

        idx = existing.index[0]

        # Refresh identity fields from the authoritative company master.
        registry.loc[idx, "cik"] = company["cik"]
        registry.loc[idx, "company_name"] = company["company_name"]
        registry.loc[idx, "ticker"] = company["ticker"]

        registry.loc[idx, "source"] = (
            registry.loc[idx, "source"]
            if pd.notna(registry.loc[idx, "source"])
            else source
        )

        registry.loc[idx, "ingestion_enabled"] = True
        registry.loc[idx, "updated_at_utc"] = now

        # Do not automatically reset ACTIVE/PARTIAL/FAILED lifecycle
        # state merely because register_company() was called again.
        #
        # If an explicit onboarding request is made, the onboarding
        # service/orchestrator can move it to RUNNING.
        save_registry(registry)

        return registry.loc[idx].to_dict()

    # ------------------------------------------------------------------
    # New company
    # ------------------------------------------------------------------

    record = {
        "company_id": company["company_id"],
        "cik": company["cik"],
        "company_name": company["company_name"],
        "ticker": company["ticker"],
        "source": source,
        "status": "ACTIVE",
        "onboarding_status": "PENDING",
        "ingestion_enabled": True,
        "created_at_utc": now,
        "updated_at_utc": now,
        "last_successful_run_utc": None,
        "last_error": None,
    }

    new_row = pd.DataFrame(
        [record],
        columns=REGISTRY_COLUMNS,
    )

    registry = pd.concat(
        [
            registry,
            new_row,
        ],
        ignore_index=True,
    )

    save_registry(registry)

    return record


def mark_onboarding_running(
    company_id: str,
) -> None:
    """
    Mark company onboarding as RUNNING.
    """
    _update_onboarding_state(
        company_id=company_id,
        onboarding_status="RUNNING",
        status="ACTIVE",
        last_error=None,
    )


def mark_onboarding_active(
    company_id: str,
    successful_run_utc: Optional[str] = None,
) -> None:
    """
    Mark company onboarding as fully ACTIVE.
    """
    successful_run_utc = (
        successful_run_utc
        or _utc_now()
    )

    _update_onboarding_state(
        company_id=company_id,
        onboarding_status="ACTIVE",
        status="ACTIVE",
        last_error=None,
        last_successful_run_utc=successful_run_utc,
    )


def mark_onboarding_partial(
    company_id: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Mark company onboarding as PARTIAL.

    The company itself remains ACTIVE, but the platform has not
    completed every capability in its onboarding dependency graph.
    """
    _update_onboarding_state(
        company_id=company_id,
        onboarding_status="PARTIAL",
        status="ACTIVE",
        last_error=error_message,
    )


def mark_onboarding_failed(
    company_id: str,
    error_message: str,
) -> None:
    """
    Mark company onboarding as FAILED.
    """
    _update_onboarding_state(
        company_id=company_id,
        onboarding_status="FAILED",
        status="SUSPENDED",
        last_error=str(error_message),
    )


def mark_onboarding_disabled(
    company_id: str,
    reason: Optional[str] = None,
) -> None:
    """
    Disable onboarding for a company.
    """
    _update_onboarding_state(
        company_id=company_id,
        onboarding_status="DISABLED",
        status="INACTIVE",
        last_error=reason,
    )


def _update_onboarding_state(
    company_id: str,
    onboarding_status: str,
    status: str,
    last_error: Optional[str] = None,
    last_successful_run_utc: Optional[str] = None,
) -> None:
    """
    Internal lifecycle state update.
    """
    if onboarding_status not in ONBOARDING_STATUSES:
        raise ValueError(
            f"Invalid onboarding status: "
            f"{onboarding_status}"
        )

    if status not in COMPANY_STATUSES:
        raise ValueError(
            f"Invalid company status: {status}"
        )

    registry = load_registry()

    matches = registry[
        registry["company_id"].astype(str)
        == str(company_id)
    ]

    if matches.empty:
        raise KeyError(
            f"Company not found in registry: "
            f"{company_id}"
        )

    if len(matches) > 1:
        raise ValueError(
            f"Duplicate company_id in registry: "
            f"{company_id}"
        )

    idx = matches.index[0]

    registry.loc[idx, "status"] = status
    registry.loc[idx, "onboarding_status"] = (
        onboarding_status
    )
    registry.loc[idx, "updated_at_utc"] = _utc_now()

    if last_error is not None:
        registry.loc[idx, "last_error"] = (
            str(last_error)
        )
    elif onboarding_status in {
        "RUNNING",
        "ACTIVE",
    }:
        registry.loc[idx, "last_error"] = None

    if last_successful_run_utc is not None:
        registry.loc[idx, "last_successful_run_utc"] = (
            last_successful_run_utc
        )

    save_registry(registry)


# ---------------------------------------------------------------------------
# Registry lookup helpers
# ---------------------------------------------------------------------------

def get_company(
    company_id: str,
) -> Optional[dict]:
    """
    Return a company registry record by company_id.
    """
    registry = load_registry()

    matches = registry[
        registry["company_id"].astype(str)
        == str(company_id)
    ]

    if matches.empty:
        return None

    if len(matches) > 1:
        raise ValueError(
            f"Duplicate company_id in registry: "
            f"{company_id}"
        )

    return matches.iloc[0].to_dict()


def get_company_by_cik(
    cik: str,
) -> Optional[dict]:
    """
    Return a company registry record by CIK.
    """
    normalized_cik = normalize_cik(cik)

    registry = load_registry()

    matches = registry[
        registry["cik"]
        .astype(str)
        .str.zfill(10)
        == normalized_cik
    ]

    if matches.empty:
        return None

    if len(matches) > 1:
        raise ValueError(
            f"Duplicate CIK in registry: "
            f"{normalized_cik}"
        )

    return matches.iloc[0].to_dict()


def list_companies() -> pd.DataFrame:
    """
    Return the complete company registry.
    """
    return load_registry().copy()