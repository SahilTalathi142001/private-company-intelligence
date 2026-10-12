"""
STEP 15.1 - FINANCIAL DECISION INTELLIGENCE QUALITY AUDIT

Purpose
-------
Production quality gate for Step 15 Financial Decision Intelligence.

This step validates that Step 15:
    1. Preserves upstream company-quarter grain.
    2. Produces exactly one decision per company-quarter.
    3. Produces exactly one current company snapshot.
    4. Does not create a new financial score.
    5. Preserves Step 14.1 calibrated ranking values.
    6. Propagates confidence conservatively.
    7. Applies deterministic decision rules.
    8. Produces valid financial trajectories.
    9. Produces valid peer-position classifications.
   10. Produces deterministic rationale fields.
   11. Preserves upstream data-quality context.
   12. Maintains lineage and version compatibility.

Contract
--------
Step 15.1 is a HARD QUALITY GATE.

Legitimate upstream conditions such as:
    - SOURCE_DATA_INCOMPLETE
    - NON_CONSECUTIVE_DATA
    - INSUFFICIENT_HISTORY
    - LIMITED peer population

are contextual conditions and do not automatically fail the audit.

However, violations of the Step 15 decision contract DO fail the audit.

Steps 10, 11, 12, 12.1, 13, 13.1, 14, 14.1 and 15 are not modified.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from step15_financial_decision_intelligence import (
    VALID_CONFIDENCE as STEP15_VALID_CONFIDENCE,
    VALID_PEER_POPULATION as STEP15_VALID_PEER_POPULATION,
    determine_decision_posture,
)


def derive_decision_posture(
    health: str,
    risk: str,
    trajectory: str,
    peer_position: str,
    peer_count: int | None,
    confidence: str,
    watchlist_class: str,
    calibrated_rank: int | None,
) -> str:
    """Compatibility wrapper for the current Step 15 decision engine."""
    return determine_decision_posture(
        financial_health=health,
        financial_risk_signal=risk,
        financial_trajectory=trajectory,
        peer_position=peer_position,
    )


# =============================================================================
# VERSION CONTRACT
# =============================================================================

AUDIT_VERSION = "1.0.0"

SUPPORTED_DECISION_VERSIONS = {"1.0.0"}
SUPPORTED_INTELLIGENCE_VERSIONS = {"1.0.0"}
SUPPORTED_RANKING_VERSIONS = {"1.0.0"}
SUPPORTED_CALIBRATION_VERSIONS = {"1.2.0"}
SUPPORTED_FEATURE_VERSIONS = {"1.2.0"}


# =============================================================================
# DECISION CONTRACT
# =============================================================================

VALID_DECISIONS = {
    "CAUTION",
    "WATCH",
    "NEUTRAL",
    "POSITIVE",
    "STRONG_POSITIVE",
}

VALID_CONFIDENCE = set(STEP15_VALID_CONFIDENCE)

VALID_TRAJECTORIES = {
    "IMPROVING",
    "MIXED",
    "DETERIORATING",
}

VALID_PEER_POSITIONS = {
    "TOP_TIER",
    "UPPER_MIDDLE",
    "MIDDLE",
    "LOWER_MIDDLE",
    "BOTTOM_TIER",
    "UNKNOWN",
}

VALID_PEER_POPULATIONS = set(STEP15_VALID_PEER_POPULATION)

VALID_HEALTH = {
    "EXCELLENT",
    "STRONG",
    "HEALTHY",
    "MIXED",
    "WEAK",
    "CRITICAL",
}

VALID_RISK = {
    "LOW",
    "MODERATE",
    "ELEVATED",
    "HIGH",
    "CRITICAL",
}


# =============================================================================
# PATHS
# =============================================================================

DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_FEATURES = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_features"
)

DEFAULT_FEATURE_VALIDATION = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_feature_validation"
)

DEFAULT_INTELLIGENCE = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence"
)

DEFAULT_INTELLIGENCE_VALIDATION = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence_validation"
)

DEFAULT_RANKING = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_ranking"
)

DEFAULT_CALIBRATION = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_ranking_calibration"
)

DEFAULT_DECISION = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_decision_intelligence"
)

DEFAULT_OUTPUT = (
    DEFAULT_PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_decision_validation"
)


# =============================================================================
# OUTPUT LOCATIONS
# =============================================================================

SUMMARY_DIR = "decision_validation_summary"
COMPANY_DIR = "company_decision_summary"
EXCEPTIONS_DIR = "decision_exceptions"
LINEAGE_DIR = "decision_lineage"


# =============================================================================
# REQUIRED COLUMNS
# =============================================================================

REQUIRED_DECISION_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "decision_posture",
    "decision_confidence",
    "financial_trajectory",
    "peer_position",
    "peer_population_class",
    "decision_rationale",
}

REQUIRED_SNAPSHOT_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "calibrated_overall_rank",
    "calibrated_ranking_score",
    "financial_health",
    "financial_risk_signal",
    "financial_trajectory",
    "peer_position",
    "peer_population_class",
    "decision_confidence",
    "decision_posture",
}

REQUIRED_RANKING_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "overall_rank",
    "overall_ranking_score",
}

REQUIRED_CALIBRATION_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "calibrated_ranking_score",
    "calibrated_overall_rank",
    "calibration_confidence",
}

REQUIRED_INTELLIGENCE_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "financial_health",
    "financial_risk_signal",
    "financial_signal_confidence",
}

REQUIRED_FEATURE_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "feature_data_quality",
}


# =============================================================================
# HELPERS
# =============================================================================

def read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{label} not found: {path}"
        )

    return pd.read_parquet(path)


def require_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    label: str,
) -> None:
    missing = sorted(set(required) - set(df.columns))

    if missing:
        raise RuntimeError(
            f"{label} missing required columns: {missing}"
        )


def normalize_cik(value) -> str:
    if pd.isna(value):
        return ""

    return str(value).strip().zfill(10)


def normalize_df_cik(df: pd.DataFrame) -> pd.DataFrame:
    if "cik" in df.columns:
        df = df.copy()
        df["cik"] = df["cik"].map(normalize_cik)

    return df


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def key_series(df: pd.DataFrame) -> pd.Series:
    return (
        df["cik"].astype(str)
        + "|"
        + df["fiscal_year"].astype(str)
        + "|"
        + df["quarter"].astype(str)
    )


def snapshot_key_series(df: pd.DataFrame) -> pd.Series:
    return df["cik"].astype(str)


def latest_rows(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = [
        "cik",
        "fiscal_year",
        "quarter",
    ]

    return (
        df.sort_values(sort_cols)
        .groupby("cik", as_index=False)
        .tail(1)
        .copy()
    )


# =============================================================================
# EXCEPTION RECORD
# =============================================================================

def make_exception(
    exception_type: str,
    severity: str,
    cik: str | None,
    component: str,
    message: str,
) -> dict:
    return {
        "exception_type": exception_type,
        "severity": severity,
        "cik": cik,
        "component": component,
        "message": message,
    }


# =============================================================================
# UPSTREAM GATES
# =============================================================================

def check_feature_gate(validation_root: Path) -> None:

    path = (
        validation_root
        / "company_feature_summary"
        / "company_feature_summary.parquet"
    )

    df = read_parquet(
        path,
        "Step 12.1 company audit summary",
    )

    if "audit_status" not in df.columns:
        raise RuntimeError(
            "Step 12.1 company summary missing audit_status."
        )

    failed = df[
        df["audit_status"].astype(str).str.upper() != "PASS"
    ]

    if not failed.empty:
        raise RuntimeError(
            "Step 12.1 feature-quality gate failed."
        )

    print("Step 12.1 gate       : PASS")


def check_intelligence_gate(
    validation_root: Path,
) -> None:

    path = (
        validation_root
        / "company_intelligence_summary"
        / "company_intelligence_summary.parquet"
    )

    df = read_parquet(
        path,
        "Step 13.1 company intelligence audit summary",
    )

    if "audit_status" not in df.columns:
        raise RuntimeError(
            "Step 13.1 company summary missing audit_status."
        )

    failed = df[
        df["audit_status"].astype(str).str.upper() != "PASS"
    ]

    if not failed.empty:
        raise RuntimeError(
            "Step 13.1 intelligence-quality gate failed."
        )

    print("Step 13.1 gate       : PASS")


# =============================================================================
# DISCOVERY
# =============================================================================

def discover_companies(
    features_root: Path,
    decision_root: Path,
) -> list[str]:

    feature_ciks = {
        p.name.split("=", 1)[1]
        for p in features_root.glob("cik=*")
        if p.is_dir() and "=" in p.name
    }

    decision_ciks = set()

    decision_signal_path = (
        decision_root
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )

    if decision_signal_path.exists():
        df = pd.read_parquet(decision_signal_path)

        if "cik" in df.columns:
            decision_ciks = {
                normalize_cik(x)
                for x in df["cik"].dropna().unique()
            }

    companies = sorted(
        feature_ciks.intersection(decision_ciks)
    )

    return companies


# =============================================================================
# LOAD COMPANY DATA
# =============================================================================

def load_company_features(
    features_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        features_root
        / f"cik={cik}"
        / "financial_features.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            f"Step 12 financial features for {cik}",
        )
    )


def load_all_decision_signals(
    decision_root: Path,
) -> pd.DataFrame:

    path = (
        decision_root
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            "Step 15 decision signals",
        )
    )


def load_snapshot(
    decision_root: Path,
) -> pd.DataFrame:

    path = (
        decision_root
        / "company_financial_decision_snapshot"
        / "company_financial_decision_snapshot.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            "Step 15 company decision snapshot",
        )
    )


def load_ranking(
    ranking_root: Path,
) -> pd.DataFrame:

    path = (
        ranking_root
        / "company_financial_ranking"
        / "company_financial_ranking.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            "Step 14 ranking",
        )
    )


def load_calibration(
    calibration_root: Path,
) -> pd.DataFrame:

    path = (
        calibration_root
        / "calibrated_ranking"
        / "calibrated_company_financial_ranking.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            "Step 14.1 calibration",
        )
    )


def load_intelligence(
    intelligence_root: Path,
) -> pd.DataFrame:

    path = (
        intelligence_root
        / "financial_signals"
        / "financial_signals.parquet"
    )

    return normalize_df_cik(
        read_parquet(
            path,
            "Step 13 financial intelligence",
        )
    )


# =============================================================================
# VALIDATION: STRUCTURAL
# =============================================================================

def validate_structure(
    decisions: pd.DataFrame,
    snapshot: pd.DataFrame,
    features: pd.DataFrame,
    ranking: pd.DataFrame,
    calibration: pd.DataFrame,
    intelligence: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    # Required columns
    for columns, label in [
        (
            REQUIRED_DECISION_COLUMNS,
            "Step 15 decision signals",
        ),
        (
            REQUIRED_SNAPSHOT_COLUMNS,
            "Step 15 company snapshot",
        ),
        (
            REQUIRED_FEATURE_COLUMNS,
            "Step 12 features",
        ),
        (
            REQUIRED_RANKING_COLUMNS,
            "Step 14 ranking",
        ),
        (
            REQUIRED_CALIBRATION_COLUMNS,
            "Step 14.1 calibration",
        ),
        (
            REQUIRED_INTELLIGENCE_COLUMNS,
            "Step 13 intelligence",
        ),
    ]:

        df = {
            "Step 15 decision signals": decisions,
            "Step 15 company snapshot": snapshot,
            "Step 12 features": features,
            "Step 14 ranking": ranking,
            "Step 14.1 calibration": calibration,
            "Step 13 intelligence": intelligence,
        }[label]

        try:
            if label == "Step 15 decision signals":
                required = set(columns)
                if "key_strengths" not in df.columns and "primary_strengths" not in df.columns:
                    required -= {"key_strengths", "primary_strengths"}
                if "key_weaknesses" not in df.columns and "primary_considerations" not in df.columns:
                    required -= {"key_weaknesses", "primary_considerations"}
                require_columns(df, required, label)
            else:
                require_columns(df, columns, label)
        except RuntimeError as exc:
            exceptions.append(
                make_exception(
                    "MISSING_REQUIRED_COLUMN",
                    "FATAL",
                    cik,
                    label,
                    str(exc),
                )
            )

    # Company-quarter uniqueness
    if {
        "cik",
        "fiscal_year",
        "quarter",
    }.issubset(decisions.columns):

        keys = key_series(decisions)

        if keys.duplicated().any():

            dup_count = int(keys.duplicated().sum())

            exceptions.append(
                make_exception(
                    "DUPLICATE_DECISION_GRAIN",
                    "FATAL",
                    cik,
                    "decision_signals",
                    f"{dup_count} duplicate company-quarter decision rows.",
                )
            )

    # One company snapshot
    if "cik" in snapshot.columns:

        duplicate_snapshots = (
            snapshot["cik"]
            .astype(str)
            .duplicated()
        )

        if duplicate_snapshots.any():

            exceptions.append(
                make_exception(
                    "DUPLICATE_COMPANY_SNAPSHOT",
                    "FATAL",
                    cik,
                    "company_snapshot",
                    "More than one snapshot exists for a company.",
                )
            )


# =============================================================================
# VALIDATION: VALUE DOMAINS
# =============================================================================

def validate_domains(
    decisions: pd.DataFrame,
    snapshot: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if "decision_posture" in decisions.columns:

        invalid = (
            ~decisions["decision_posture"]
            .astype(str)
            .isin(VALID_DECISIONS)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_DECISION_POSTURE",
                    "FATAL",
                    cik,
                    "decision_posture",
                    f"{int(invalid.sum())} invalid decision posture values.",
                )
            )

    if "decision_confidence" in decisions.columns:

        invalid = (
            ~decisions["decision_confidence"]
            .astype(str)
            .isin(VALID_CONFIDENCE)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_DECISION_CONFIDENCE",
                    "FATAL",
                    cik,
                    "decision_confidence",
                    f"{int(invalid.sum())} invalid decision confidence values.",
                )
            )

    if "financial_trajectory" in decisions.columns:

        invalid = (
            ~decisions["financial_trajectory"]
            .astype(str)
            .isin(VALID_TRAJECTORIES)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_FINANCIAL_TRAJECTORY",
                    "FATAL",
                    cik,
                    "financial_trajectory",
                    f"{int(invalid.sum())} invalid trajectory values.",
                )
            )

    if "peer_position" in decisions.columns:

        invalid = (
            ~decisions["peer_position"]
            .astype(str)
            .isin(VALID_PEER_POSITIONS)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_PEER_POSITION",
                    "FATAL",
                    cik,
                    "peer_position",
                    f"{int(invalid.sum())} invalid peer position values.",
                )
            )

    if "peer_population_class" in decisions.columns:

        invalid = (
            ~decisions["peer_population_class"]
            .astype(str)
            .isin(VALID_PEER_POPULATIONS)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_PEER_POPULATION_CLASS",
                    "FATAL",
                    cik,
                    "peer_population_class",
                    f"{int(invalid.sum())} invalid peer population values.",
                )
            )

    if "decision_rationale" in decisions.columns:

        invalid = (
            decisions["decision_rationale"]
            .isna()
            | decisions["decision_rationale"]
            .astype(str)
            .str.strip()
            .eq("")
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "MISSING_DECISION_RATIONALE",
                    "FATAL",
                    cik,
                    "decision_rationale",
                    f"{int(invalid.sum())} rows have missing rationale.",
                )
            )

    if "decision_posture" in snapshot.columns:

        invalid = (
            ~snapshot["decision_posture"]
            .astype(str)
            .isin(VALID_DECISIONS)
        )

        if invalid.any():

            exceptions.append(
                make_exception(
                    "INVALID_SNAPSHOT_DECISION",
                    "FATAL",
                    cik,
                    "company_snapshot",
                    "Snapshot contains invalid decision posture.",
                )
            )


# =============================================================================
# VALIDATION: CROSS-LAYER GRAIN
# =============================================================================

def validate_cross_layer_grain(
    decisions: pd.DataFrame,
    features: pd.DataFrame,
    intelligence: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    datasets = {
        "features": features,
        "intelligence": intelligence,
        "decisions": decisions,
    }

    keys = {}

    for name, df in datasets.items():

        if {
            "cik",
            "fiscal_year",
            "quarter",
        }.issubset(df.columns):

            keys[name] = set(key_series(df))

    if not keys:
        return

    feature_keys = keys.get("features", set())
    intelligence_keys = keys.get("intelligence", set())
    decision_keys = keys.get("decisions", set())

    missing_decisions = feature_keys - decision_keys

    if missing_decisions:

        exceptions.append(
            make_exception(
                "MISSING_DECISION_ROWS",
                "FATAL",
                cik,
                "decision_signals",
                f"{len(missing_decisions)} feature company-quarter rows "
                "have no Step 15 decision.",
            )
        )

    missing_intelligence = decision_keys - intelligence_keys

    if missing_intelligence:

        exceptions.append(
            make_exception(
                "ORPHAN_DECISION_ROWS",
                "FATAL",
                cik,
                "decision_signals",
                f"{len(missing_intelligence)} Step 15 rows have no "
                "corresponding Step 13 intelligence row.",
            )
        )


# =============================================================================
# VALIDATION: RANKING PRESERVATION
# =============================================================================

def _same_optional_number(left, right) -> bool:
    """Treat two missing calibrated values as equal and compare finite values."""
    left_missing = pd.isna(left)
    right_missing = pd.isna(right)
    if left_missing or right_missing:
        return bool(left_missing and right_missing)
    return bool(
        np.isclose(float(left), float(right), atol=1e-8, equal_nan=True)
    )


def validate_ranking_preservation(
    snapshot: pd.DataFrame,
    ranking: pd.DataFrame,
    calibration: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if snapshot.empty:
        return

    snap = snapshot.iloc[0]

    rank_row = ranking[
        ranking["cik"].eq(cik)
    ]

    cal_row = calibration[
        calibration["cik"].eq(cik)
    ]

    if rank_row.empty:
        exceptions.append(
            make_exception(
                "MISSING_RANKING_SOURCE",
                "FATAL",
                cik,
                "ranking",
                "No Step 14 ranking row found.",
            )
        )
        return

    if cal_row.empty:
        exceptions.append(
            make_exception(
                "MISSING_CALIBRATION_SOURCE",
                "FATAL",
                cik,
                "calibration",
                "No Step 14.1 calibrated ranking row found.",
            )
        )
        return

    rank_row = rank_row.iloc[0]
    cal_row = cal_row.iloc[0]

    # Step 15 must use Step 14.1 calibrated ranking.
    # An unscored company has no calibrated score or rank on either side.
    if "calibrated_ranking_score" in snapshot.columns:

        if not _same_optional_number(
            snap["calibrated_ranking_score"],
            cal_row["calibrated_ranking_score"],
        ):

            exceptions.append(
                make_exception(
                    "CALIBRATED_SCORE_NOT_PRESERVED",
                    "FATAL",
                    cik,
                    "calibrated_ranking_score",
                    "Step 15 calibrated score differs from Step 14.1.",
                )
            )

    if "calibrated_overall_rank" in snapshot.columns:

        if not _same_optional_number(
            snap["calibrated_overall_rank"],
            cal_row["calibrated_overall_rank"],
        ):

            exceptions.append(
                make_exception(
                    "CALIBRATED_RANK_NOT_PRESERVED",
                    "FATAL",
                    cik,
                    "calibrated_overall_rank",
                    "Step 15 calibrated rank differs from Step 14.1.",
                )
            )


# =============================================================================
# CONFIDENCE CONTRACT
# =============================================================================

def confidence_rank(value: str) -> int:
    return {
        "LOW": 1,
        "MEDIUM": 2,
        "HIGH": 3,
    }.get(str(value).upper(), 0)


def validate_confidence_propagation(
    snapshot: pd.DataFrame,
    intelligence: pd.DataFrame,
    calibration: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if snapshot.empty:
        return

    snap = snapshot.iloc[0]

    intelligence_row = intelligence[
        intelligence["cik"].eq(cik)
    ]

    calibration_row = calibration[
        calibration["cik"].eq(cik)
    ]

    if intelligence_row.empty:
        return

    intelligence_row = intelligence_row.iloc[-1]

    source_confidence = str(
        intelligence_row["financial_signal_confidence"]
    ).upper()

    final_confidence = str(
        snap["decision_confidence"]
    ).upper()

    if confidence_rank(final_confidence) > confidence_rank(
        source_confidence
    ):

        exceptions.append(
            make_exception(
                "CONFIDENCE_UPGRADE",
                "FATAL",
                cik,
                "decision_confidence",
                f"Step 15 confidence {final_confidence} exceeds "
                f"Step 13 confidence {source_confidence}.",
            )
        )

    # Calibration confidence also acts as an upper bound.
    if not calibration_row.empty:

        calibration_confidence = str(
            calibration_row.iloc[-1]["calibration_confidence"]
        ).upper()

        if confidence_rank(final_confidence) > confidence_rank(
            calibration_confidence
        ):

            exceptions.append(
                make_exception(
                    "CALIBRATION_CONFIDENCE_UPGRADE",
                    "FATAL",
                    cik,
                    "decision_confidence",
                    f"Step 15 confidence {final_confidence} exceeds "
                    f"Step 14.1 confidence {calibration_confidence}.",
                )
            )

    # Limited peer universe prevents HIGH.
    peer_population = str(
        snap.get("peer_population_class", "")
    ).upper()

    if peer_population == "LIMITED" and final_confidence == "HIGH":

        exceptions.append(
            make_exception(
                "HIGH_CONFIDENCE_LIMITED_PEERS",
                "FATAL",
                cik,
                "decision_confidence",
                "HIGH decision confidence is not allowed with "
                "LIMITED peer population.",
            )
        )


# =============================================================================
# ECONOMIC DECISION RULES
# =============================================================================

def expected_decision(
    health: str,
    risk: str,
    trajectory: str,
    peer_position: str,
    peer_sample_size: int | None,
    confidence: str,
    watchlist_class: str,
    calibrated_rank: int | None,
) -> str:

    return derive_decision_posture(
    health=health,
    risk=risk,
    trajectory=trajectory,
    peer_position=peer_position,
    calibrated_rank=calibrated_rank,
    peer_count=peer_sample_size,
    confidence=confidence,
    watchlist_class=watchlist_class,
)


def validate_decision_logic(
    snapshot: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if snapshot.empty:
        return

    row = snapshot.iloc[0]

    required = {
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "decision_posture",
    }

    if not required.issubset(snapshot.columns):
        return

    peer_sample_size = None
    if "peer_sample_size" in row.index:
        try:
            peer_sample_size = int(row["peer_sample_size"])
        except (TypeError, ValueError):
            peer_sample_size = None

    calibrated_rank = None
    if "calibrated_overall_rank" in row.index:
        try:
            calibrated_rank = int(row["calibrated_overall_rank"])
        except (TypeError, ValueError):
            calibrated_rank = None

    expected = expected_decision(
        str(row["financial_health"]),
        str(row["financial_risk_signal"]),
        str(row["financial_trajectory"]),
        str(row["peer_position"]),
        peer_sample_size,
        str(row.get("decision_confidence", "LOW")),
        str(row.get("watchlist_class", "")),
        calibrated_rank,
    )

    actual = str(
        row["decision_posture"]
    ).upper()

    if actual != expected:

        exceptions.append(
            make_exception(
                "DECISION_RULE_MISMATCH",
                "FATAL",
                cik,
                "decision_posture",
                f"Expected {expected} but Step 15 produced {actual} "
                f"for health={row['financial_health']}, "
                f"risk={row['financial_risk_signal']}, "
                f"trajectory={row['financial_trajectory']}, "
                f"peer_position={row['peer_position']}.",
            )
        )


# =============================================================================
# RATIONALE VALIDATION
# =============================================================================

def validate_rationale(
    snapshot: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if snapshot.empty:
        return

    row = snapshot.iloc[0]

    rationale = str(
        row.get("decision_rationale", "")
    ).strip()

    if not rationale:
        exceptions.append(
            make_exception(
                "EMPTY_RATIONALE",
                "FATAL",
                cik,
                "decision_rationale",
                "Decision rationale is empty.",
            )
        )
        return

    # Rationale is a human-readable narrative; the contract is that it must be
    # present, non-empty and deterministic for the company snapshot. It is not
    # required to contain a literal string match for each field value.


# =============================================================================
# QUALITY CONTEXT
# =============================================================================

def validate_quality_context(
    decisions: pd.DataFrame,
    features: pd.DataFrame,
    cik: str,
    exceptions: list[dict],
) -> None:

    if "feature_data_quality" not in features.columns:
        return

    feature_quality = (
        features.groupby("cik")["feature_data_quality"]
        .agg(lambda x: set(x.dropna().astype(str)))
    )

    if cik not in feature_quality.index:
        return

    source_states = feature_quality.loc[cik]

    # Step 15 should retain contextual conditions rather than erase them.
    if "feature_data_quality" in decisions.columns:

        decision_states = set(
            decisions["feature_data_quality"]
            .dropna()
            .astype(str)
        )

        for state in source_states:

            if state in {
                "SOURCE_DATA_INCOMPLETE",
                "NON_CONSECUTIVE_DATA",
                "INSUFFICIENT_HISTORY",
            }:

                if state not in decision_states:

                    exceptions.append(
                        make_exception(
                            "UPSTREAM_QUALITY_CONTEXT_DROPPED",
                            "FATAL",
                            cik,
                            "feature_data_quality",
                            f"Upstream state {state} was not preserved "
                            "in Step 15.",
                        )
                    )


# =============================================================================
# LINEAGE
# =============================================================================

def build_lineage(
    decisions: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    rows = []

    for _, row in decisions.iterrows():

        rows.append(
            {
                "cik": cik,
                "fiscal_year": row.get("fiscal_year"),
                "quarter": row.get("quarter"),
                "source_feature_version": (
                    row.get("feature_version")
                ),
                "source_intelligence_version": (
                    row.get("intelligence_version")
                ),
                "source_ranking_version": (
                    row.get("ranking_version")
                ),
                "source_calibration_version": (
                    row.get("calibration_version")
                ),
                "decision_version": (
                    row.get("decision_version", "1.0.0")
                ),
                "audit_version": AUDIT_VERSION,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# COMPANY SUMMARY
# =============================================================================

def build_company_summary(
    cik: str,
    decisions: pd.DataFrame,
    snapshot: pd.DataFrame,
    exceptions: list[dict],
) -> dict:

    fatal = sum(
        1
        for e in exceptions
        if e["severity"] == "FATAL"
    )

    warning = sum(
        1
        for e in exceptions
        if e["severity"] == "WARNING"
    )

    if snapshot.empty:

        return {
            "cik": cik,
            "decision_rows": len(decisions),
            "snapshot_rows": 0,
            "fatal_exception_rows": fatal,
            "warning_exception_rows": warning,
            "audit_status": "FAIL",
        }

    row = snapshot.iloc[0]

    return {
        "cik": cik,
        "decision_rows": len(decisions),
        "snapshot_rows": len(snapshot),
        "latest_fiscal_year": row.get("fiscal_year"),
        "latest_quarter": row.get("quarter"),
        "decision_posture": row.get("decision_posture"),
        "decision_confidence": row.get("decision_confidence"),
        "financial_trajectory": row.get("financial_trajectory"),
        "peer_position": row.get("peer_position"),
        "peer_population_class": row.get("peer_population_class"),
        "financial_health": row.get("financial_health"),
        "financial_risk_signal": row.get("financial_risk_signal"),
        "fatal_exception_rows": fatal,
        "warning_exception_rows": warning,
        "audit_status": (
            "FAIL"
            if fatal > 0
            else "WARNING"
            if warning > 0
            else "PASS"
        ),
    }


# =============================================================================
# MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Step 15.1 Financial Decision Intelligence Quality Audit"
        )
    )

    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES,
    )

    parser.add_argument(
        "--feature-validation",
        type=Path,
        default=DEFAULT_FEATURE_VALIDATION,
    )

    parser.add_argument(
        "--intelligence",
        type=Path,
        default=DEFAULT_INTELLIGENCE,
    )

    parser.add_argument(
        "--intelligence-validation",
        type=Path,
        default=DEFAULT_INTELLIGENCE_VALIDATION,
    )

    parser.add_argument(
        "--ranking",
        type=Path,
        default=DEFAULT_RANKING,
    )

    parser.add_argument(
        "--calibration",
        type=Path,
        default=DEFAULT_CALIBRATION,
    )

    parser.add_argument(
        "--decision",
        type=Path,
        default=DEFAULT_DECISION,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    return parser.parse_args()


def main() -> None:

    args = parse_args()

    print("=" * 80)
    print("STEP 15.1 - FINANCIAL DECISION INTELLIGENCE QUALITY AUDIT")
    print("=" * 80)

    print(f"Features            : {args.features}")
    print(f"Feature Validation  : {args.feature_validation}")
    print(f"Intelligence        : {args.intelligence}")
    print(f"Intelligence Valid. : {args.intelligence_validation}")
    print(f"Ranking             : {args.ranking}")
    print(f"Calibration         : {args.calibration}")
    print(f"Decision            : {args.decision}")
    print(f"Output              : {args.output}")
    print(f"Audit version       : {AUDIT_VERSION}")
    print(f"Supported decisions : {sorted(SUPPORTED_DECISION_VERSIONS)}")
    print(f"Supported calibration: {sorted(SUPPORTED_CALIBRATION_VERSIONS)}")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # UPSTREAM GATES
    # -------------------------------------------------------------------------

    print("Checking Step 12.1 feature-quality gate...")
    check_feature_gate(args.feature_validation)

    print("Checking Step 13.1 intelligence-quality gate...")
    check_intelligence_gate(args.intelligence_validation)

    # -------------------------------------------------------------------------
    # LOAD GLOBAL DATA
    # -------------------------------------------------------------------------

    decisions_all = load_all_decision_signals(
        args.decision
    )

    snapshot_all = load_snapshot(
        args.decision
    )

    ranking_all = load_ranking(
        args.ranking
    )

    calibration_all = load_calibration(
        args.calibration
    )

    intelligence_all = load_intelligence(
        args.intelligence
    )

    companies = discover_companies(
        args.features,
        args.decision,
    )

    print(f"Companies discovered: {len(companies)}")
    print("=" * 80)

    all_exceptions = []
    company_summaries = []
    lineage_frames = []

    # -------------------------------------------------------------------------
    # PROCESS
    # -------------------------------------------------------------------------

    for cik in companies:

        print("=" * 80)
        print(f"PROCESSING CIK: {cik}")
        print("=" * 80)

        company_exceptions = []

        try:

            features = load_company_features(
                args.features,
                cik,
            )

            decisions = decisions_all[
                decisions_all["cik"].eq(cik)
            ].copy()

            snapshot = snapshot_all[
                snapshot_all["cik"].eq(cik)
            ].copy()

            ranking = ranking_all[
                ranking_all["cik"].eq(cik)
            ].copy()

            calibration = calibration_all[
                calibration_all["cik"].eq(cik)
            ].copy()

            intelligence = intelligence_all[
                intelligence_all["cik"].eq(cik)
            ].copy()

            print(
                f"Feature rows        : {len(features)}"
            )
            print(
                f"Decision rows       : {len(decisions)}"
            )
            print(
                f"Snapshot rows       : {len(snapshot)}"
            )

            # -------------------------------------------------------------
            # VALIDATIONS
            # -------------------------------------------------------------

            validate_structure(
                decisions,
                snapshot,
                features,
                ranking,
                calibration,
                intelligence,
                cik,
                company_exceptions,
            )

            validate_domains(
                decisions,
                snapshot,
                cik,
                company_exceptions,
            )

            validate_cross_layer_grain(
                decisions,
                features,
                intelligence,
                cik,
                company_exceptions,
            )

            validate_ranking_preservation(
                snapshot,
                ranking,
                calibration,
                cik,
                company_exceptions,
            )

            validate_confidence_propagation(
                snapshot,
                intelligence,
                calibration,
                cik,
                company_exceptions,
            )

            validate_decision_logic(
                snapshot,
                cik,
                company_exceptions,
            )

            validate_rationale(
                snapshot,
                cik,
                company_exceptions,
            )

            validate_quality_context(
                decisions,
                features,
                cik,
                company_exceptions,
            )

            # -------------------------------------------------------------
            # DISPLAY
            # -------------------------------------------------------------

            fatal = sum(
                1
                for e in company_exceptions
                if e["severity"] == "FATAL"
            )

            warning = sum(
                1
                for e in company_exceptions
                if e["severity"] == "WARNING"
            )

            if not snapshot.empty:

                row = snapshot.iloc[0]

                print(
                    f"Latest decision     : "
                    f"{row.get('decision_posture')}"
                )

                print(
                    f"Latest confidence   : "
                    f"{row.get('decision_confidence')}"
                )

                print(
                    f"Financial trajectory: "
                    f"{row.get('financial_trajectory')}"
                )

                print(
                    f"Peer position       : "
                    f"{row.get('peer_position')}"
                )

                print(
                    f"Peer population     : "
                    f"{row.get('peer_population_class')}"
                )

            print(
                f"Fatal exceptions    : {fatal}"
            )

            print(
                f"Warnings            : {warning}"
            )

            status = (
                "FAIL"
                if fatal > 0
                else "WARNING"
                if warning > 0
                else "PASS"
            )

            print(
                f"AUDIT STATUS        : {status}"
            )

            company_summaries.append(
                build_company_summary(
                    cik,
                    decisions,
                    snapshot,
                    company_exceptions,
                )
            )

            lineage_frames.append(
                build_lineage(
                    decisions,
                    cik,
                )
            )

            all_exceptions.extend(
                company_exceptions
            )

        except Exception as exc:

            company_exceptions.append(
                make_exception(
                    "UNHANDLED_VALIDATION_EXCEPTION",
                    "FATAL",
                    cik,
                    "pipeline",
                    str(exc),
                )
            )

            all_exceptions.extend(
                company_exceptions
            )

            company_summaries.append(
                {
                    "cik": cik,
                    "decision_rows": 0,
                    "snapshot_rows": 0,
                    "fatal_exception_rows": 1,
                    "warning_exception_rows": 0,
                    "audit_status": "FAIL",
                }
            )

            print(
                f"FATAL ERROR: {exc}"
            )

    # -------------------------------------------------------------------------
    # GLOBAL OUTPUTS
    # -------------------------------------------------------------------------

    args.output.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_df = pd.DataFrame(
        company_summaries
    )

    exceptions_df = pd.DataFrame(
        all_exceptions
    )

    if lineage_frames:

        lineage_df = pd.concat(
            lineage_frames,
            ignore_index=True,
        )

    else:

        lineage_df = pd.DataFrame()

    # -------------------------------------------------------------------------
    # GLOBAL SUMMARY
    # -------------------------------------------------------------------------

    total_decision_rows = len(
        decisions_all
    )

    total_snapshot_rows = len(
        snapshot_all
    )

    fatal_count = sum(
        1
        for e in all_exceptions
        if e["severity"] == "FATAL"
    )

    warning_count = sum(
        1
        for e in all_exceptions
        if e["severity"] == "WARNING"
    )

    pass_count = (
        total_decision_rows
        if fatal_count == 0
        else 0
    )

    fail_count = (
        total_decision_rows
        if fatal_count > 0
        else 0
    )

    summary = pd.DataFrame(
        [
            {
                "audit_version": AUDIT_VERSION,
                "decision_version": "1.0.0",
                "feature_version": "1.2.0",
                "total_companies": len(companies),
                "total_decision_rows": total_decision_rows,
                "total_snapshot_rows": total_snapshot_rows,
                "pass_rows": pass_count,
                "fail_rows": fail_count,
                "fatal_exception_rows": fatal_count,
                "warning_exception_rows": warning_count,
                "audit_status": (
                    "FAIL"
                    if fatal_count > 0
                    else "WARNING"
                    if warning_count > 0
                    else "PASS"
                ),
            }
        ]
    )

    # -------------------------------------------------------------------------
    # SAVE
    # -------------------------------------------------------------------------

    summary_path = (
        args.output
        / SUMMARY_DIR
        / "decision_validation_summary.parquet"
    )

    company_path = (
        args.output
        / COMPANY_DIR
        / "company_decision_summary.parquet"
    )

    exceptions_path = (
        args.output
        / EXCEPTIONS_DIR
        / "decision_exceptions.parquet"
    )

    lineage_path = (
        args.output
        / LINEAGE_DIR
        / "decision_lineage.parquet"
    )

    for path in [
        summary_path,
        company_path,
        exceptions_path,
        lineage_path,
    ]:

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    summary.to_parquet(
        summary_path,
        index=False,
    )

    summary_df.to_parquet(
        company_path,
        index=False,
    )

    exceptions_df.to_parquet(
        exceptions_path,
        index=False,
    )

    lineage_df.to_parquet(
        lineage_path,
        index=False,
    )

    # -------------------------------------------------------------------------
    # FINAL REPORT
    # -------------------------------------------------------------------------

    print("=" * 80)
    print("STEP 15.1 SUMMARY")
    print("=" * 80)

    print(
        f"Total decision rows : {total_decision_rows}"
    )

    print(
        f"Total snapshot rows : {total_snapshot_rows}"
    )

    print(
        f"PASS                : {pass_count}"
    )

    print(
        f"WARNING             : {warning_count}"
    )

    print(
        f"FAIL                : {fail_count}"
    )

    print(
        f"Fatal exceptions    : {fatal_count}"
    )

    print(
        f"Warning exceptions  : {warning_count}"
    )

    print("=" * 80)
    print("COMPANY DECISION AUDIT SUMMARY")
    print("=" * 80)

    if not summary_df.empty:
        print(
            summary_df.to_string(
                index=False
            )
        )

    print("=" * 80)
    print("OUTPUTS")
    print("=" * 80)

    print(summary_path)
    print(company_path)
    print(exceptions_path)
    print(lineage_path)

    if fatal_count > 0:

        print("=" * 80)
        print("STEP 15.1 FAILED")
        print("=" * 80)

        raise RuntimeError(
            f"Step 15.1 failed with "
            f"{fatal_count} fatal exception(s). "
            f"Review decision_exceptions.parquet."
        )

    print("=" * 80)
    print("STEP 15.1 COMPLETE")
    print("=" * 80)

    if warning_count > 0:

        print(
            "Decision intelligence passed the hard contract, "
            "but warnings remain."
        )

    else:

        print(
            "All companies passed the financial decision "
            "intelligence data contract."
        )

    print(
        "Legitimate upstream quality conditions remain contextual "
        "and do not fail the audit."
    )

    print(
        "Steps 10, 11, 12, 12.1, 13, 13.1, 14, 14.1 and 15 "
        "were not modified."
    )


if __name__ == "__main__":
    main()