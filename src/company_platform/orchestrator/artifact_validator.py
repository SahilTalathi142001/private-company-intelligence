"""Validate persisted outputs before an incremental component is allowed to run.

The validator distinguishes between:
    1. Required artifacts that must contain the requested company.
    2. Sparse enrichment artifacts where the company may legitimately have
       no observations.

SEC Form D is a sparse enrichment source. A company without a Form D filing
must be treated as valid NO_DATA rather than as a pipeline failure.

The paths below are taken from integration_v2/components.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]


def _path(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _cik_from_company_id(company_id: str) -> str:
    """Extract a normalized 10-digit CIK from SEC_<CIK> company IDs."""
    return str(company_id).replace("SEC_", "").zfill(10)


def _company_file(component_id: str, cik: str) -> list[Path]:
    """Return persisted output paths for company-scoped components."""

    paths = {
        "company_registry": [
            _path(
                "data",
                "silver",
                "platform",
                "company_registry.parquet",
            ),
        ],
        "sec_ingestion": [
            _path(
                "data",
                "bronze",
                "sec",
                "submissions",
                f"{cik}.json",
            ),
            _path(
                "data",
                "bronze",
                "sec",
                "companyfacts",
                f"{cik}.json",
            ),
        ],
        "financial_normalization": [
            _path(
                "data",
                "silver",
                "financial_facts",
                f"cik={cik}",
                "financial_facts.parquet",
            ),
            _path(
                "data",
                "gold",
                "financials",
                f"cik={cik}",
                "financials.parquet",
            ),
        ],
        "quarter_derivation": [
            _path(
                "data",
                "gold",
                "financials_quarterly",
                f"cik={cik}",
                "financials_quarterly.parquet",
            ),
            _path(
                "data",
                "gold",
                "financials_quarterly_final",
                f"cik={cik}",
                "financials_quarterly.parquet",
            ),
            _path(
                "data",
                "gold",
                "financials_analytics",
                f"cik={cik}",
                "financials_analytics.parquet",
            ),
        ],
        "financial_features": [
            _path(
                "data",
                "gold",
                "financials_features",
                f"cik={cik}",
                "financial_features.parquet",
            ),
        ],
    }

    return paths.get(component_id, [])


# ---------------------------------------------------------------------------
# Shared artifacts
# ---------------------------------------------------------------------------
#
# These components write shared parquet tables rather than one parquet file
# per company.
#
_SHARED = {
    "source_registry": [
        _path(
            "data",
            "silver",
            "platform",
            "source_registry.parquet",
        ),
    ],
    "financial_decision": [
        _path(
            "data",
            "gold",
            "financial_decision_intelligence",
            "decision_signals",
            "financial_decision_signals.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_decision_intelligence",
            "company_financial_decision_snapshot",
            "company_financial_decision_snapshot.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_decision_intelligence",
            "financial_decision_summary",
            "financial_decision_summary.parquet",
        ),
    ],
    "company_profile": [
        _path(
            "data",
            "gold",
            "financial_company_intelligence",
            "company_intelligence_profile",
            "company_intelligence_profile.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence",
            "company_intelligence_history",
            "company_intelligence_history.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence",
            "company_intelligence_signals",
            "company_intelligence_signals.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence",
            "company_intelligence_summary",
            "company_intelligence_summary.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence",
            "company_intelligence_lineage",
            "company_intelligence_lineage.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence_dq",
            "profile_quality_audit",
            "profile_quality_audit.parquet",
        ),
        _path(
            "data",
            "gold",
            "financial_company_intelligence_dq",
            "profile_dq_summary",
            "profile_dq_summary.parquet",
        ),
    ],
    "form_d_ingestion": [
        _path(
            "data",
            "gold",
            "funding_capital_intelligence",
            "funding_events",
            "funding_events.parquet",
        ),
        _path(
            "data",
            "gold",
            "funding_capital_intelligence",
            "company_funding_snapshot",
            "company_funding_snapshot.parquet",
        ),
    ],
    "investor_ownership": [
        _path(
            "data",
            "gold",
            "investor_ownership_intelligence",
            "ownership_snapshot",
            "ownership_snapshot.parquet",
        ),
    ],
    "valuation": [
        _path(
            "data",
            "gold",
            "valuation_intelligence",
            "company_valuation_snapshot",
            "company_valuation_snapshot.parquet",
        ),
    ],
    "investment_intelligence": [
        _path(
            "data",
            "gold",
            "unified_investment_intelligence",
            "investment_profile",
            "investment_profile.parquet",
        ),
        _path(
            "data",
            "gold",
            "unified_investment_intelligence",
            "investment_signals",
            "investment_signals.parquet",
        ),
        _path(
            "data",
            "gold",
            "unified_investment_intelligence",
            "investment_summary",
            "investment_summary.parquet",
        ),
    ],
    "unified_company_view": [
        _path(
            "data",
            "gold",
            "unified_company_intelligence",
            "unified_company_view",
            "unified_company_view.parquet",
        ),
    ],
}


# ---------------------------------------------------------------------------
# Global metadata artifacts
# ---------------------------------------------------------------------------
#
# These files are shared and do not need a company-specific CIK row.
#
_GLOBAL_METADATA = {
    "financial_decision": {
        "financial_decision_summary.parquet",
    },
    "company_profile": {
        "company_intelligence_summary.parquet",
        "company_intelligence_lineage.parquet",
        "profile_dq_summary.parquet",
    },
    "source_registry": {
        "source_registry.parquet",
    },
    "investment_intelligence": {
        "investment_summary.parquet",
    },
}


# ---------------------------------------------------------------------------
# Sparse enrichment artifacts
# ---------------------------------------------------------------------------
#
# These are valid even when the requested company has no observation.
#
# Form D is intentionally sparse:
#
#     Company has Form D filing
#         -> AVAILABLE
#
#     Company has no Form D filing
#         -> NO_DATA
#
# Absence of a company row is therefore NOT an artifact validation failure.
#
_SPARSE_COMPANY_COMPONENTS = {
    "form_d_ingestion",
    "investor_ownership",
    "valuation",
}


def _normalize_cik_series(series: pd.Series) -> pd.Series:
    """Normalize a parquet CIK column to 10-digit strings."""

    return (
        series.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(10)
    )


def _find_cik_column(frame: pd.DataFrame) -> str | None:
    """Find a supported company CIK column."""

    candidates = [
        "cik",
        "CIK",
        "issuer_cik",
        "ISSUER_CIK",
        "issuer_cik_normalized",
        "ISSUER_CIK_NORMALIZED",
    ]

    for column in candidates:
        if column in frame.columns:
            return column

    return None


def _validate_parquet(
    path: Path,
    cik: str,
    require_identity: bool = True,
) -> tuple[bool, str]:
    """Strictly validate a persisted parquet artifact."""

    if not path.is_file():
        return False, f"MISSING:{path}"

    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        return False, f"UNREADABLE:{path}:{type(exc).__name__}"

    if frame.empty:
        return False, f"EMPTY:{path}"

    if not require_identity:
        return True, "OK"

    cik_column = _find_cik_column(frame)

    if cik_column is not None:
        matches = _normalize_cik_series(frame[cik_column]) == cik

        if not matches.any():
            return (
                False,
                f"COMPANY_NOT_PRESENT:{path}:CIK={cik}",
            )

        return True, "OK"

    if f"cik={cik}" in str(path):
        return True, "OK"

    return (
        False,
        f"IDENTITY_UNVERIFIABLE:{path}:NO_CIK_COLUMN",
    )


def _validate_sparse_company_parquet(
    path: Path,
    cik: str,
) -> tuple[bool, str]:
    """Validate a sparse company enrichment artifact.

    The artifact itself must exist, be readable, and contain data.

    However, absence of the requested company is valid and is interpreted
    as NO_DATA rather than as COMPANY_NOT_PRESENT failure.
    """

    if not path.is_file():
        return False, f"MISSING:{path}"

    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        return False, f"UNREADABLE:{path}:{type(exc).__name__}"

    if frame.empty:
        return False, f"EMPTY:{path}"

    cik_column = _find_cik_column(frame)

    if cik_column is None:
        return (
            False,
            f"IDENTITY_UNVERIFIABLE:{path}:NO_CIK_COLUMN",
        )

    matches = _normalize_cik_series(frame[cik_column]) == cik

    if matches.any():
        return True, "AVAILABLE"

    # IMPORTANT:
    # A missing company row is valid for sparse enrichment sources.
    return (
        True,
        f"NO_DATA:COMPANY_NOT_PRESENT:{path}:CIK={cik}",
    )


def _validate_json(path: Path) -> tuple[bool, str]:
    """Validate a JSON artifact."""

    import json

    if not path.is_file():
        return False, f"MISSING:{path}"

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        return False, f"UNREADABLE:{path}:{type(exc).__name__}"

    if not payload:
        return False, f"EMPTY:{path}"

    return True, "OK"


def _validate_sparse_component(
    component_id: str,
    paths: list[Path],
    cik: str,
) -> tuple[bool, str]:
    """Validate sparse enrichment artifacts.

    All declared artifact files must exist and be readable.

    The company itself may be absent from the artifact and that is treated
    as valid NO_DATA.
    """

    results = [
        _validate_sparse_company_parquet(path, cik)
        for path in paths
    ]

    failures = [
        reason
        for valid, reason in results
        if not valid
    ]

    if failures:
        return False, ";".join(failures)

    available = [
        reason
        for valid, reason in results
        if valid and reason == "AVAILABLE"
    ]

    if available:
        return True, "AVAILABLE"

    no_data = [
        reason
        for valid, reason in results
        if valid and reason.startswith("NO_DATA:")
    ]

    if no_data:
        return True, "NO_DATA"

    return True, "AVAILABLE"


def validate_artifact(
    company_id: str,
    component_id: str,
    pipeline_version: str,
) -> tuple[bool, str]:
    """Return (valid, reason) for a prerequisite's persisted output.

    Parameters
    ----------
    company_id:
        Company identifier, normally SEC_<10-digit CIK>.

    component_id:
        Component whose persisted artifact is being validated.

    pipeline_version:
        Accepted to match the orchestrator callback contract.

    Returns
    -------
    tuple[bool, str]
        True when the prerequisite is usable.

        For sparse enrichment components, True + NO_DATA is valid.

        False indicates a genuine artifact problem such as:
            - missing file
            - unreadable file
            - empty artifact
            - malformed identity information
            - corrupted prerequisite
    """

    # Current component outputs do not persist a version manifest, so
    # pipeline-version compatibility cannot yet be asserted here.
    _ = pipeline_version

    cik = _cik_from_company_id(company_id)

    # ------------------------------------------------------------------
    # Company-scoped artifacts
    # ------------------------------------------------------------------

    scoped = _company_file(component_id, cik)

    if scoped:
        checks = [
            (
                _validate_json(path)
                if path.suffix == ".json"
                else _validate_parquet(path, cik)
            )
            for path in scoped
        ]

        failures = [
            reason
            for valid, reason in checks
            if not valid
        ]

        if failures:
            return False, ";".join(failures)

        return True, "OK"

    # ------------------------------------------------------------------
    # Shared artifacts
    # ------------------------------------------------------------------

    shared = _SHARED.get(component_id)

    if not shared:
        return False, f"NO_ARTIFACT_CONTRACT:{component_id}"

    # ------------------------------------------------------------------
    # Sparse enrichment
    # ------------------------------------------------------------------

    if component_id in _SPARSE_COMPANY_COMPONENTS:
        return _validate_sparse_component(
            component_id=component_id,
            paths=shared,
            cik=cik,
        )

    # ------------------------------------------------------------------
    # Normal shared artifacts
    # ------------------------------------------------------------------

    global_metadata = _GLOBAL_METADATA.get(
        component_id,
        set(),
    )

    checks = [
        _validate_parquet(
            path,
            cik,
            require_identity=path.name not in global_metadata,
        )
        for path in shared
    ]

    failures = [
        reason
        for valid, reason in checks
        if not valid
    ]

    if failures:
        return False, ";".join(failures)

    return True, "OK"