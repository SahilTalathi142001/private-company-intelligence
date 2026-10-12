from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "data" / "silver" / "platform" / "company_registry.parquet"

REQUIRED_COLUMNS = {"company_id", "cik", "status", "ingestion_enabled"}
TRUE_VALUES = {"true", "1", "yes", "y", "on"}
FALSE_VALUES = {"false", "0", "no", "n", "off", ""}
SCOPE_STATUS = "ACTIVE"


@dataclass(frozen=True)
class ExecutionScope:
    """Immutable production execution scope resolved from the company registry."""

    ciks: tuple[str, ...]
    source: str
    status: str = SCOPE_STATUS

    @property
    def count(self) -> int:
        return len(self.ciks)

    def as_list(self) -> List[str]:
        return list(self.ciks)


def normalize_cik(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(10) if digits else ""


def normalize_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return False


def resolve_execution_scope() -> ExecutionScope:
    """Resolve the authoritative production universe.

    IMPORTANT: ``company.parquet`` is a reference/company-identity master and
    must never determine production execution scope.  Only the active,
    ingestion-enabled records in ``company_registry.parquet`` do so.

    This deliberately has no expected-count parameter: scaling 5 -> 20 -> 50
    -> N companies must be a registry/configuration change, not a code change.
    """
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Company registry not found: {REGISTRY_PATH}")

    df = pd.read_parquet(REGISTRY_PATH)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "Company registry missing required columns: "
            + ", ".join(sorted(missing))
        )

    work = df.copy()
    work["cik"] = work["cik"].map(normalize_cik)
    work["status"] = work["status"].astype(str).str.strip().str.upper()
    work["ingestion_enabled"] = work["ingestion_enabled"].map(normalize_bool)

    work = work[
        (work["status"] == SCOPE_STATUS)
        & work["ingestion_enabled"]
        & work["cik"].ne("")
    ].copy()

    duplicates = (
        work.loc[work["cik"].duplicated(keep=False), "cik"]
        .drop_duplicates()
        .tolist()
    )
    if duplicates:
        raise ValueError(f"Duplicate active registry CIKs: {sorted(duplicates)}")

    ciks = tuple(sorted(work["cik"].unique().tolist()))
    if not ciks:
        raise RuntimeError("Production company execution scope is empty.")

    return ExecutionScope(ciks=ciks, source=str(REGISTRY_PATH))


def load_active_company_ciks() -> List[str]:
    """Backward-compatible list API over the unified execution-scope resolver."""
    return resolve_execution_scope().as_list()


def assert_company_scope(expected_count: int | None = None) -> List[str]:
    """Return the active production scope.

    ``expected_count`` remains only for legacy tests/migration checks. New
    production code must not pass a fixed count.
    """
    companies = load_active_company_ciks()
    if expected_count is not None and len(companies) != expected_count:
        raise RuntimeError(
            f"Expected {expected_count} companies, found {len(companies)}."
        )
    return companies
