# =============================================================================
# STEP 15 - FINANCIAL DECISION INTELLIGENCE
# =============================================================================
#
# Purpose
# -------
# Convert validated financial intelligence + peer ranking information into
# deterministic decision interpretation.
#
# IMPORTANT CONTRACT
# ------------------
# 1. NO NEW FINANCIAL SCORE
# 2. DETERMINISTIC DECISION INTERPRETATION
# 3. PEER CONFIDENCE PROPAGATED
# 4. STEP 12.1 AND STEP 13.1 REMAIN UPSTREAM QUALITY GATES
# 5. STEP 14 / 14.1 ARE INPUTS, NOT RECOMPUTED HERE
#
# Inputs
# ------
# Step 12:
#   data/gold/financials_features
#
# Step 12.1:
#   data/gold/financials_feature_validation
#
# Step 13:
#   data/gold/financial_intelligence
#
# Step 13.1:
#   data/gold/financial_intelligence_validation
#
# Step 14:
#   data/gold/financial_ranking
#
# Step 14.1:
#   data/gold/financial_ranking_calibration
#
# Outputs
# -------
# data/gold/financial_decision_intelligence/
#   decision_signals/
#       financial_decision_signals.parquet
#
#   company_financial_decision_snapshot/
#       company_financial_decision_snapshot.parquet
#
#   financial_decision_summary/
#       financial_decision_summary.parquet
#
#   financial_decision_watchlist/
#       financial_decision_watchlist.parquet
#
#   decision_lineage/
#       financial_decision_lineage.parquet
#
# Decision vocabulary
# -------------------
# POSITIVE
# NEUTRAL
# WATCH
# CAUTION
#
# Financial trajectory vocabulary
# --------------------------------
# IMPROVING
# MIXED
# DETERIORATING
#
# =============================================================================


from __future__ import annotations

import argparse
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# =============================================================================
# VERSION / CONTRACT
# =============================================================================

DECISION_VERSION = "1.2.0"

SUPPORTED_FEATURE_VERSIONS = {
    "1.2.0",
}

SUPPORTED_INTELLIGENCE_VERSIONS = {
    "1.0.0",
}

SUPPORTED_RANKING_VERSIONS = {
    "1.0.0",
}

SUPPORTED_CALIBRATION_VERSIONS = {
    "1.2.0",
}


VALID_DECISIONS = {
    "POSITIVE",
    "NEUTRAL",
    "WATCH",
    "CAUTION",
}


VALID_TRAJECTORIES = {
    "IMPROVING",
    "MIXED",
    "DETERIORATING",
}


VALID_CONFIDENCE = {
    "LOW",
    "MEDIUM",
    "HIGH",
}


VALID_PEER_POSITIONS = {
    "TOP_TIER",
    "UPPER_MIDDLE",
    "MIDDLE",
    "LOWER_MIDDLE",
    "BOTTOM_TIER",
    "UNKNOWN",
}


VALID_PEER_POPULATION = {
    "ROBUST",
    "LIMITED",
    "UNKNOWN",
}


VALID_REASON_CODES = {
    "CRITICAL_RISK",
    "HIGH_RISK_DETERIORATING",
    "HIGH_RISK_MONITORING",
    "DETERIORATING_WITH_RISK",
    "DETERIORATING",
    "WEAK_PEER_MODERATE_RISK",
    "WEAK_PEER_MIXED",
    "EXCELLENT_LOW_RISK_IMPROVING",
    "STRONG_LOW_RISK_IMPROVING",
    "STRONG_LOW_RISK_MIXED",
    "HEALTHY_LOW_RISK_IMPROVING",
    "BALANCED_MIXED_PROFILE",
}


DECISION_PRIORITY = {
    "CAUTION": 1,
    "WATCH": 2,
    "NEUTRAL": 3,
    "POSITIVE": 4,
}


# =============================================================================
# PROJECT PATHS
# =============================================================================

DEFAULT_BASE_DIR = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence"
)


DEFAULT_FEATURES_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financials_features"
)


DEFAULT_FEATURE_VALIDATION_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financials_feature_validation"
)


DEFAULT_INTELLIGENCE_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financial_intelligence"
)


DEFAULT_INTELLIGENCE_VALIDATION_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financial_intelligence_validation"
)


DEFAULT_RANKING_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financial_ranking"
)


DEFAULT_CALIBRATION_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financial_ranking_calibration"
)


DEFAULT_OUTPUT_DIR = (
    DEFAULT_BASE_DIR
    / "data"
    / "gold"
    / "financial_decision_intelligence"
)


# =============================================================================
# OUTPUT PATHS
# =============================================================================

def output_paths(output_root: Path) -> dict[str, Path]:

    return {
        "signals": (
            output_root
            / "decision_signals"
            / "financial_decision_signals.parquet"
        ),

        "snapshot": (
            output_root
            / "company_financial_decision_snapshot"
            / "company_financial_decision_snapshot.parquet"
        ),

        "summary": (
            output_root
            / "financial_decision_summary"
            / "financial_decision_summary.parquet"
        ),

        "watchlist": (
            output_root
            / "financial_decision_watchlist"
            / "financial_decision_watchlist.parquet"
        ),

        "lineage": (
            output_root
            / "decision_lineage"
            / "financial_decision_lineage.parquet"
        ),
    }


# =============================================================================
# CONSTANTS
# =============================================================================

QUALITY_COMPLETE = "COMPLETE"
QUALITY_PARTIAL = "PARTIAL"
QUALITY_SOURCE_INCOMPLETE = "SOURCE_DATA_INCOMPLETE"
QUALITY_NON_CONSECUTIVE = "NON_CONSECUTIVE_DATA"
QUALITY_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"


CONFIDENCE_ORDER = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
}


PEER_POSITION_MAP = {
    1: "TOP_TIER",
    2: "UPPER_MIDDLE",
    3: "MIDDLE",
    4: "LOWER_MIDDLE",
    5: "BOTTOM_TIER",
}


# =============================================================================
# LOGGING
# =============================================================================

def log(message: str = "") -> None:
    print(message)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def normalize_cik(value) -> str:

    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    return text.zfill(10)


def safe_float(value) -> Optional[float]:

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

        return float(value)

    except Exception:

        return None


def safe_int(value) -> Optional[int]:

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

        return int(value)

    except Exception:

        return None


def normalize_text(value) -> str:

    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return str(value).strip()


def normalize_upper(value) -> str:

    return normalize_text(value).upper()


def first_existing(
    df: pd.DataFrame,
    candidates: list[str],
) -> Optional[str]:

    for column in candidates:

        if column in df.columns:
            return column

    return None


def ensure_directory(path: Path) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )


def read_parquet(
    path: Path,
    label: str,
) -> pd.DataFrame:

    if not path.exists():

        raise FileNotFoundError(
            f"{label} not found: {path}"
        )

    return pd.read_parquet(path)


def write_parquet(
    df: pd.DataFrame,
    path: Path,
) -> None:

    ensure_directory(path)

    df.to_parquet(
        path,
        index=False,
    )


# =============================================================================
# VALIDATION GATES
# =============================================================================

def find_company_summary_path(
    validation_root: Path,
) -> Path:

    return (
        validation_root
        / "company_feature_summary"
        / "company_feature_summary.parquet"
    )


def find_feature_validation_summary_path(
    validation_root: Path,
) -> Path:

    return (
        validation_root
        / "feature_validation_summary"
        / "feature_validation_summary.parquet"
    )


def load_validation_gate(
    validation_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    company_path = find_company_summary_path(
        validation_root
    )

    summary_path = find_feature_validation_summary_path(
        validation_root
    )

    company_df = read_parquet(
        company_path,
        "Step 12.1 company audit summary",
    )

    summary_df = read_parquet(
        summary_path,
        "Step 12.1 validation summary",
    )

    if "audit_status" in company_df.columns:

        failed = company_df[
            company_df["audit_status"]
            .astype(str)
            .str.upper()
            != "PASS"
        ]

        if not failed.empty:

            raise RuntimeError(
                "Step 12.1 company gate failed:\n"
                f"{failed.to_string(index=False)}"
            )

    if "audit_status" in summary_df.columns:

        failed = summary_df[
            summary_df["audit_status"]
            .astype(str)
            .str.upper()
            != "PASS"
        ]

        if not failed.empty:

            raise RuntimeError(
                "Step 12.1 summary gate failed."
            )

    return company_df, summary_df


def find_intelligence_validation_summary_path(
    validation_root: Path,
) -> Path:

    return (
        validation_root
        / "company_intelligence_summary"
        / "company_intelligence_summary.parquet"
    )


def find_intelligence_validation_summary_path_2(
    validation_root: Path,
) -> Path:

    return (
        validation_root
        / "intelligence_validation_summary"
        / "intelligence_validation_summary.parquet"
    )


def load_intelligence_validation_gate(
    validation_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    company_path = (
        find_intelligence_validation_summary_path(
            validation_root
        )
    )

    summary_path = (
        find_intelligence_validation_summary_path_2(
            validation_root
        )
    )

    company_df = read_parquet(
        company_path,
        "Step 13.1 company audit summary",
    )

    summary_df = read_parquet(
        summary_path,
        "Step 13.1 validation summary",
    )

    if "audit_status" in company_df.columns:

        failed = company_df[
            company_df["audit_status"]
            .astype(str)
            .str.upper()
            != "PASS"
        ]

        if not failed.empty:

            raise RuntimeError(
                "Step 13.1 company gate failed:\n"
                f"{failed.to_string(index=False)}"
            )

    if "audit_status" in summary_df.columns:

        statuses = (
            summary_df["audit_status"]
            .astype("string")
            .str.upper()
            .dropna()
            .unique()
            .tolist()
        )

        # WARNING is acceptable for global fixture-count mismatches in
        # single-company runs, provided there are no failed rows or fatal
        # exceptions. The company-level audit above remains strict PASS.
        if any(status not in {"PASS", "WARNING"} for status in statuses):
            raise RuntimeError(
                "Step 13.1 summary gate failed. "
                f"Observed statuses: {statuses}"
            )

        for column in ["fail_rows", "fatal_exceptions"]:
            if column in summary_df.columns:
                counts = pd.to_numeric(
                    summary_df[column], errors="coerce"
                )
                if counts.isna().any() or (counts > 0).any():
                    raise RuntimeError(
                        "Step 13.1 summary gate failed: "
                        f"invalid or positive {column}."
                    )

    return company_df, summary_df


# =============================================================================
# COMPANY DISCOVERY
# =============================================================================

def discover_companies(
    features_root: Path,
) -> list[str]:

    companies = []

    if not features_root.exists():
        raise FileNotFoundError(
            f"Features directory not found: {features_root}"
        )

    for path in features_root.glob(
        "cik=*/financial_features.parquet"
    ):

        cik = normalize_cik(
            path.parent.name.replace(
                "cik=",
                "",
            )
        )

        if cik:
            companies.append(cik)

    companies = sorted(
        set(companies)
    )

    if not companies:

        raise RuntimeError(
            "No Step 12 feature companies discovered."
        )

    return companies


# =============================================================================
# FEATURE LOADING
# =============================================================================

def load_features(
    features_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        features_root
        / f"cik={cik}"
        / "financial_features.parquet"
    )

    df = read_parquet(
        path,
        f"Step 12 features for {cik}",
    )

    if df.empty:

        raise ValueError(
            f"Step 12 features are empty for {cik}."
        )

    out = df.copy()

    if "cik" not in out.columns:

        out["cik"] = cik

    out["cik"] = out["cik"].map(
        normalize_cik
    )

    if set(out["cik"].dropna().unique()) != {cik}:

        raise ValueError(
            f"Feature CIK mismatch for {cik}: "
            f"{sorted(out['cik'].dropna().unique().tolist())}"
        )

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "feature_data_quality",
        "feature_version",
    ]

    missing = [
        c for c in required
        if c not in out.columns
    ]

    if missing:

        raise KeyError(
            f"Step 12 features missing columns for {cik}: "
            f"{missing}"
        )

    out["fiscal_year"] = pd.to_numeric(
        out["fiscal_year"],
        errors="coerce",
    ).astype("Int64")

    out["quarter"] = (
        out["quarter"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    out["quarter_num"] = (
        out["quarter"]
        .map({
            "Q1": 1,
            "Q2": 2,
            "Q3": 3,
            "Q4": 4,
        })
        .astype("Int64")
    )

    feature_versions = set(
        out["feature_version"]
        .dropna()
        .astype(str)
        .unique()
    )

    unsupported = (
        feature_versions
        - SUPPORTED_FEATURE_VERSIONS
    )

    if unsupported:

        raise ValueError(
            f"Unsupported feature versions for {cik}: "
            f"{sorted(unsupported)}"
        )

    duplicate_mask = out.duplicated(
        subset=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        keep=False,
    )

    if duplicate_mask.any():

        dupes = (
            out.loc[
                duplicate_mask,
                [
                    "cik",
                    "fiscal_year",
                    "quarter",
                ],
            ]
            .drop_duplicates()
        )

        raise ValueError(
            f"Step 12 feature grain violation for {cik}:\n"
            f"{dupes.to_string(index=False)}"
        )

    out = out.sort_values(
        [
            "fiscal_year",
            "quarter_num",
        ]
    ).reset_index(drop=True)

    return out


# =============================================================================
# STEP 13 INTELLIGENCE LOADING
# =============================================================================

def load_intelligence(
    intelligence_root: Path,
    cik: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    signals_path = (
        intelligence_root
        / "financial_signals"
        / "financial_signals.parquet"
    )

    snapshot_path = (
        intelligence_root
        / "company_financial_snapshot"
        / "company_financial_snapshot.parquet"
    )

    all_signals = read_parquet(
        signals_path,
        "Step 13 financial signals",
    )

    all_snapshots = read_parquet(
        snapshot_path,
        "Step 13 company snapshot",
    )

    if "cik" not in all_signals.columns:

        raise KeyError(
            "Step 13 signals missing cik."
        )

    if "cik" not in all_snapshots.columns:

        raise KeyError(
            "Step 13 snapshot missing cik."
        )

    signals = all_signals[
        all_signals["cik"].map(
            normalize_cik
        ) == cik
    ].copy()

    snapshot = all_snapshots[
        all_snapshots["cik"].map(
            normalize_cik
        ) == cik
    ].copy()

    if signals.empty:

        raise ValueError(
            f"Step 13 signals missing for {cik}."
        )

    if len(snapshot) != 1:

        raise ValueError(
            f"Step 13 snapshot must contain exactly "
            f"one row for {cik}; found {len(snapshot)}."
        )

    if "intelligence_version" in signals.columns:

        versions = set(
            signals["intelligence_version"]
            .dropna()
            .astype(str)
            .unique()
        )

        unsupported = (
            versions
            - SUPPORTED_INTELLIGENCE_VERSIONS
        )

        if unsupported:

            raise ValueError(
                f"Unsupported Step 13 intelligence versions "
                f"for {cik}: {sorted(unsupported)}"
            )

    if "intelligence_version" in snapshot.columns:

        versions = set(
            snapshot["intelligence_version"]
            .dropna()
            .astype(str)
            .unique()
        )

        unsupported = (
            versions
            - SUPPORTED_INTELLIGENCE_VERSIONS
        )

        if unsupported:

            raise ValueError(
                f"Unsupported Step 13 snapshot versions "
                f"for {cik}: {sorted(unsupported)}"
            )

    return (
        signals,
        snapshot,
    )


# =============================================================================
# STEP 14 RANKING LOADING
# =============================================================================

def load_ranking(
    ranking_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        ranking_root
        / "company_financial_ranking"
        / "company_financial_ranking.parquet"
    )

    all_ranking = read_parquet(
        path,
        "Step 14 financial ranking",
    )

    if "cik" not in all_ranking.columns:

        raise KeyError(
            "Step 14 ranking missing cik."
        )

    ranking = all_ranking[
        all_ranking["cik"].map(
            normalize_cik
        ) == cik
    ].copy()

    if ranking.empty:

        raise ValueError(
            f"Step 14 ranking missing for {cik}."
        )

    if len(ranking) != 1:

        raise ValueError(
            f"Step 14 ranking must contain exactly "
            f"one current row for {cik}; found {len(ranking)}."
        )

    if "ranking_version" in ranking.columns:

        versions = set(
            ranking["ranking_version"]
            .dropna()
            .astype(str)
            .unique()
        )

        unsupported = (
            versions
            - SUPPORTED_RANKING_VERSIONS
        )

        if unsupported:

            raise ValueError(
                f"Unsupported Step 14 ranking versions "
                f"for {cik}: {sorted(unsupported)}"
            )

    return ranking


# =============================================================================
# STEP 14.1 CALIBRATION LOADING
# =============================================================================

def load_calibration(
    calibration_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        calibration_root
        / "calibrated_ranking"
        / "calibrated_company_financial_ranking.parquet"
    )

    all_calibrated = read_parquet(
        path,
        "Step 14.1 calibrated ranking",
    )

    if "cik" not in all_calibrated.columns:

        raise KeyError(
            "Step 14.1 calibrated ranking missing cik."
        )

    calibrated = all_calibrated[
        all_calibrated["cik"].map(
            normalize_cik
        ) == cik
    ].copy()

    if calibrated.empty:

        raise ValueError(
            f"Step 14.1 calibrated ranking missing for {cik}."
        )

    if len(calibrated) != 1:

        raise ValueError(
            f"Step 14.1 calibration must contain exactly "
            f"one current row for {cik}; found {len(calibrated)}."
        )

    if "calibration_version" in calibrated.columns:

        versions = set(
            calibrated["calibration_version"]
            .dropna()
            .astype(str)
            .unique()
        )

        unsupported = (
            versions
            - SUPPORTED_CALIBRATION_VERSIONS
        )

        if unsupported:

            raise ValueError(
                f"Unsupported Step 14.1 calibration versions "
                f"for {cik}: {sorted(unsupported)}"
            )

    return calibrated


# =============================================================================
# PEER POSITION
# =============================================================================

def derive_peer_position(
    ranking: pd.Series,
) -> str:

    rank = safe_int(
        ranking.get("calibrated_overall_rank")
    )

    if rank is None:

        rank = safe_int(
            ranking.get("overall_rank")
        )

    if rank is None:
        return "UNKNOWN"

    if rank in PEER_POSITION_MAP:
        return PEER_POSITION_MAP[rank]

    peer_size = safe_int(
        ranking.get("peer_sample_size")
    )

    if peer_size is None:

        return "UNKNOWN"

    if peer_size <= 0:
        return "UNKNOWN"

    percentile = safe_float(
        ranking.get("ranking_percentile")
    )

    if percentile is None:

        percentile = safe_float(
            ranking.get("calibrated_ranking_percentile")
        )

    if percentile is None:

        percentile = (
            (peer_size - rank + 1)
            / peer_size
            * 100.0
        )

    if percentile >= 80:
        return "TOP_TIER"

    if percentile >= 60:
        return "UPPER_MIDDLE"

    if percentile >= 40:
        return "MIDDLE"

    if percentile >= 20:
        return "LOWER_MIDDLE"

    return "BOTTOM_TIER"


# =============================================================================
# PEER POPULATION
# =============================================================================

def derive_peer_population(
    ranking: pd.Series,
    calibration: pd.Series,
) -> tuple[str, int]:

    peer_size = safe_int(
        calibration.get("peer_sample_size")
    )

    if peer_size is None:

        peer_size = safe_int(
            ranking.get("peer_sample_size")
        )

    if peer_size is None:
        return "UNKNOWN", 0

    if peer_size >= 10:
        return "ROBUST", peer_size

    return "LIMITED", peer_size


# =============================================================================
# FINANCIAL TRAJECTORY
# =============================================================================

def determine_financial_trajectory(
    revenue_growth_signal: str,
    profitability_signal: str,
    momentum_signal: str,
) -> str:
    """
    Deterministic trajectory interpretation.

    Positive signals:
        ACCELERATING
        STRONG_ACCELERATION
        EXPANDING
        STRONG_EXPANSION
        POSITIVE
        STRONG_POSITIVE

    Negative signals:
        DECELERATING
        CONTRACTING
        NEGATIVE
        STRONG_NEGATIVE
    """

    signals = [
        normalize_upper(revenue_growth_signal),
        normalize_upper(profitability_signal),
        normalize_upper(momentum_signal),
    ]

    positive = 0
    negative = 0

    for signal in signals:

        if signal in {
            "ACCELERATING",
            "STRONG_ACCELERATION",
            "EXPANDING",
            "STRONG_EXPANSION",
            "POSITIVE",
            "STRONG_POSITIVE",
        }:

            positive += 1

        elif signal in {
            "DECELERATING",
            "CONTRACTING",
            "NEGATIVE",
            "STRONG_NEGATIVE",
        }:

            negative += 1

    if positive >= 2 and negative == 0:
        return "IMPROVING"

    if negative >= 2 and positive == 0:
        return "DETERIORATING"

    return "MIXED"


# =============================================================================
# DECISION POSTURE
# =============================================================================

def determine_decision_posture(
    financial_health: str,
    financial_risk_signal: str,
    financial_trajectory: str,
    peer_position: str,
) -> str:
    """
    Deterministic decision interpretation.

    This function does NOT create a financial score.

    Priority:
        1. Critical/high risk
        2. Deteriorating trajectory
        3. Weak peer position with risk
        4. Strong positive fundamentals
        5. Healthy + low risk + improving
        6. Strong/excellent + low risk + mixed
        7. Neutral fallback
    """

    health = normalize_upper(
        financial_health
    )

    risk = normalize_upper(
        financial_risk_signal
    )

    trajectory = normalize_upper(
        financial_trajectory
    )

    peer = normalize_upper(
        peer_position
    )

    # -------------------------------------------------------------------------
    # 1. Critical risk
    # -------------------------------------------------------------------------

    if risk == "CRITICAL":

        return "CAUTION"

    # -------------------------------------------------------------------------
    # 2. High risk
    # -------------------------------------------------------------------------

    if risk == "HIGH":

        if trajectory == "DETERIORATING":
            return "CAUTION"

        return "WATCH"

    # -------------------------------------------------------------------------
    # 3. Deteriorating trajectory
    # -------------------------------------------------------------------------

    if trajectory == "DETERIORATING":

        if risk in {
            "ELEVATED",
            "MODERATE",
        }:

            return "CAUTION"

        return "WATCH"

    # -------------------------------------------------------------------------
    # 4. Bottom-tier + moderate/elevated risk
    # -------------------------------------------------------------------------

    if (
        peer == "BOTTOM_TIER"
        and risk in {
            "MODERATE",
            "ELEVATED",
        }
    ):

        return "WATCH"

    # -------------------------------------------------------------------------
    # 5. Bottom-tier + mixed trajectory
    # -------------------------------------------------------------------------

    if (
        peer == "BOTTOM_TIER"
        and trajectory == "MIXED"
        and health in {
            "HEALTHY",
            "STRONG",
            "MIXED",
            "WEAK",
        }
    ):

        return "WATCH"

    # -------------------------------------------------------------------------
    # 6. Excellent + low risk + improving + strong peer position
    # -------------------------------------------------------------------------

    if (
        health == "EXCELLENT"
        and risk == "LOW"
        and trajectory == "IMPROVING"
        and peer in {
            "TOP_TIER",
            "UPPER_MIDDLE",
        }
    ):

        return "POSITIVE"

    # -------------------------------------------------------------------------
    # 7. Strong + low risk + improving/mixed
    # -------------------------------------------------------------------------

    if (
        health == "STRONG"
        and risk == "LOW"
        and trajectory in {
            "IMPROVING",
            "MIXED",
        }
        and peer in {
            "TOP_TIER",
            "UPPER_MIDDLE",
        }
    ):

        return "POSITIVE"

    # -------------------------------------------------------------------------
    # 8. Healthy + low risk + improving
    # -------------------------------------------------------------------------

    if (
        health in {
            "EXCELLENT",
            "STRONG",
            "HEALTHY",
        }
        and risk == "LOW"
        and trajectory == "IMPROVING"
    ):

        return "POSITIVE"

    # -------------------------------------------------------------------------
    # 9. Excellent/strong + low risk + mixed
    # -------------------------------------------------------------------------

    if (
        health in {
            "EXCELLENT",
            "STRONG",
        }
        and risk == "LOW"
        and trajectory == "MIXED"
    ):

        return "POSITIVE"

    # -------------------------------------------------------------------------
    # 10. Default
    # -------------------------------------------------------------------------

    return "NEUTRAL"


# =============================================================================
# DECISION CONFIDENCE
# =============================================================================

def cap_decision_confidence(
    intelligence_confidence: str,
    calibration_confidence: str,
    peer_population_class: str,
) -> str:
    """
    Conservative confidence propagation.

    Decision confidence can never exceed the weakest material upstream
    confidence.

    LIMITED peer populations cannot produce HIGH decision confidence.
    """

    intelligence = normalize_upper(
        intelligence_confidence
    )

    calibration = normalize_upper(
        calibration_confidence
    )

    peer_population = normalize_upper(
        peer_population_class
    )

    intelligence_level = CONFIDENCE_ORDER.get(
        intelligence,
        1,
    )

    calibration_level = CONFIDENCE_ORDER.get(
        calibration,
        1,
    )

    final_level = min(
        intelligence_level,
        calibration_level,
    )

    if peer_population == "LIMITED":

        final_level = min(
            final_level,
            CONFIDENCE_ORDER["MEDIUM"],
        )

    reverse = {
        1: "LOW",
        2: "MEDIUM",
        3: "HIGH",
    }

    return reverse[final_level]


# =============================================================================
# DECISION REASON CODE / PRIORITY
# =============================================================================

def determine_decision_reason_code(
    decision_posture: str,
    financial_health: str,
    financial_risk_signal: str,
    financial_trajectory: str,
    peer_position: str,
) -> str:
    """Return a deterministic machine-readable explanation for the posture."""

    posture = normalize_upper(decision_posture)
    health = normalize_upper(financial_health)
    risk = normalize_upper(financial_risk_signal)
    trajectory = normalize_upper(financial_trajectory)
    peer = normalize_upper(peer_position)

    if risk == "CRITICAL":
        return "CRITICAL_RISK"

    if risk == "HIGH":
        if trajectory == "DETERIORATING":
            return "HIGH_RISK_DETERIORATING"
        return "HIGH_RISK_MONITORING"

    if trajectory == "DETERIORATING":
        if risk in {"ELEVATED", "MODERATE"}:
            return "DETERIORATING_WITH_RISK"
        return "DETERIORATING"

    if peer == "BOTTOM_TIER" and risk in {"MODERATE", "ELEVATED"}:
        return "WEAK_PEER_MODERATE_RISK"

    if peer == "BOTTOM_TIER" and trajectory == "MIXED":
        return "WEAK_PEER_MIXED"

    if (
        health == "EXCELLENT"
        and risk == "LOW"
        and trajectory == "IMPROVING"
        and peer in {"TOP_TIER", "UPPER_MIDDLE"}
    ):
        return "EXCELLENT_LOW_RISK_IMPROVING"

    if (
        health == "STRONG"
        and risk == "LOW"
        and trajectory == "IMPROVING"
        and peer in {"TOP_TIER", "UPPER_MIDDLE"}
    ):
        return "STRONG_LOW_RISK_IMPROVING"

    if (
        health == "STRONG"
        and risk == "LOW"
        and trajectory == "MIXED"
    ):
        return "STRONG_LOW_RISK_MIXED"

    if (
        health in {"EXCELLENT", "STRONG", "HEALTHY"}
        and risk == "LOW"
        and trajectory == "IMPROVING"
    ):
        return "HEALTHY_LOW_RISK_IMPROVING"

    return "BALANCED_MIXED_PROFILE"


def derive_decision_priority(decision_posture: str) -> int:
    """Return deterministic UI/action priority; lower number means higher attention."""
    return DECISION_PRIORITY.get(
        normalize_upper(decision_posture),
        DECISION_PRIORITY["NEUTRAL"],
    )


# =============================================================================
# STRENGTHS
# =============================================================================

def build_primary_strengths(
    row: pd.Series,
) -> list[str]:

    strengths = []

    revenue_signal = normalize_upper(
        row.get("revenue_growth_signal")
    )

    profitability_signal = normalize_upper(
        row.get("profitability_signal")
    )

    consistency_signal = normalize_upper(
        row.get("growth_consistency_signal")
    )

    momentum_signal_value = normalize_upper(
        row.get("momentum_signal")
    )

    risk = normalize_upper(
        row.get("financial_risk_signal")
    )

    health = normalize_upper(
        row.get("financial_health")
    )

    if revenue_signal in {
        "ACCELERATING",
        "STRONG_ACCELERATION",
    }:

        strengths.append(
            "Revenue growth is accelerating"
        )

    elif revenue_signal == "STABLE":

        strengths.append(
            "Revenue growth is stable"
        )

    if profitability_signal in {
        "EXPANDING",
        "STRONG_EXPANSION",
    }:

        strengths.append(
            "Profitability is expanding"
        )

    elif profitability_signal == "STABLE":

        strengths.append(
            "Profitability is stable"
        )

    if consistency_signal == "CONSISTENT":

        strengths.append(
            "Growth pattern is consistent"
        )

    elif consistency_signal == "MIXED":

        strengths.append(
            "Growth pattern is mixed but observable"
        )

    if momentum_signal_value in {
        "POSITIVE",
        "STRONG_POSITIVE",
    }:

        strengths.append(
            "Recent operating momentum is positive"
        )

    if risk == "LOW":

        strengths.append(
            "Financial risk signal is low"
        )

    if health in {
        "EXCELLENT",
        "STRONG",
    }:

        strengths.append(
            "Overall financial health is strong"
        )

    elif health == "HEALTHY":

        strengths.append(
            "Overall financial health is healthy"
        )

    return strengths


# =============================================================================
# CONSIDERATIONS
# =============================================================================

def build_primary_considerations(
    row: pd.Series,
    decision_posture: str,
) -> list[str]:

    considerations = []

    revenue_signal = normalize_upper(
        row.get("revenue_growth_signal")
    )

    profitability_signal = normalize_upper(
        row.get("profitability_signal")
    )

    risk = normalize_upper(
        row.get("financial_risk_signal")
    )

    trajectory = normalize_upper(
        row.get("financial_trajectory")
    )

    peer_position = normalize_upper(
        row.get("peer_position")
    )

    confidence = normalize_upper(
        row.get("decision_confidence")
    )

    if revenue_signal == "DECELERATING":

        considerations.append(
            "Revenue growth is decelerating"
        )

    elif revenue_signal == "CONTRACTING":

        considerations.append(
            "Revenue growth is contracting"
        )

    if profitability_signal == "CONTRACTING":

        considerations.append(
            "Profitability is contracting"
        )

    elif profitability_signal == "DECELERATING":

        considerations.append(
            "Profitability is decelerating"
        )

    if risk == "MODERATE":

        considerations.append(
            "Financial risk is moderate"
        )

    elif risk == "ELEVATED":

        considerations.append(
            "Financial risk is elevated"
        )

    elif risk in {
        "HIGH",
        "CRITICAL",
    }:

        considerations.append(
            "Financial risk requires attention"
        )

    if trajectory == "DETERIORATING":

        considerations.append(
            "Financial trajectory is deteriorating"
        )

    if peer_position == "BOTTOM_TIER":

        considerations.append(
            "Peer position is bottom tier"
        )

    elif peer_position == "LOWER_MIDDLE":

        considerations.append(
            "Peer position is lower middle"
        )

    if decision_posture == "WATCH":

        if not any(
            "attention" in x.lower()
            for x in considerations
        ):

            considerations.append(
                "Current financial position warrants monitoring"
            )

    if decision_posture == "CAUTION":

        considerations.append(
            "Current conditions warrant heightened caution"
        )

    return considerations


# =============================================================================
# RATIONALE
# =============================================================================

def build_decision_rationale(
    decision_posture: str,
    financial_health: str,
    financial_risk_signal: str,
    financial_trajectory: str,
    peer_position: str,
    decision_confidence: str,
    primary_strengths: list[str],
    primary_considerations: list[str],
) -> str:

    health = normalize_text(
        financial_health
    ).lower()

    risk = normalize_text(
        financial_risk_signal
    ).lower()

    trajectory = normalize_text(
        financial_trajectory
    ).lower()

    peer = normalize_upper(
        peer_position
    )

    posture = normalize_text(
        decision_posture
    ).lower()

    confidence = normalize_text(
        decision_confidence
    ).lower()

    if trajectory[:1] in {
        "a",
        "e",
        "i",
        "o",
        "u",
    }:

        trajectory_phrase = (
            f"an {trajectory}"
        )

    else:

        trajectory_phrase = (
            f"a {trajectory}"
        )

    peer_text = {
        "TOP_TIER": "the top tier",
        "UPPER_MIDDLE": "the upper middle",
        "MIDDLE": "the middle",
        "LOWER_MIDDLE": "the lower middle",
        "BOTTOM_TIER": "the bottom tier",
    }.get(
        peer,
        "the current peer universe",
    )

    rationale = (
        f"Decision posture is {posture} "
        f"based on {health} financial health, "
        f"{risk} financial risk and "
        f"{trajectory_phrase} trajectory. "
        f"The company ranks in {peer_text} "
        f"of the current peer universe."
    )

    if primary_strengths:

        rationale += (
            " Primary strengths: "
            + ". ".join(primary_strengths)
            + "."
        )

    if primary_considerations:

        rationale += (
            " Primary considerations: "
            + ". ".join(primary_considerations)
            + "."
        )

    rationale += (
        f" Decision confidence is {confidence}."
    )

    return rationale


# =============================================================================
# BUILD DECISION ROW
# =============================================================================

def build_decision_row(
    row: pd.Series,
    ranking: pd.Series,
    calibration: pd.Series,
    cik: str,
) -> dict:

    peer_position = derive_peer_position(
        ranking
    )

    peer_population_class, peer_sample_size = (
        derive_peer_population(
            ranking,
            calibration,
        )
    )

    trajectory = determine_financial_trajectory(
        row.get("revenue_growth_signal"),
        row.get("profitability_signal"),
        row.get("momentum_signal"),
    )

    temporary = row.copy()

    temporary["financial_trajectory"] = trajectory
    temporary["peer_position"] = peer_position
    temporary["peer_population_class"] = (
        peer_population_class
    )

    decision_posture = determine_decision_posture(
        financial_health=row.get(
            "financial_health"
        ),
        financial_risk_signal=row.get(
            "financial_risk_signal"
        ),
        financial_trajectory=trajectory,
        peer_position=peer_position,
    )

    intelligence_confidence = normalize_upper(
        row.get(
            "financial_signal_confidence"
        )
    )

    calibration_confidence = normalize_upper(
        calibration.get(
            "calibration_confidence"
        )
    )

    decision_confidence = (
        cap_decision_confidence(
            intelligence_confidence,
            calibration_confidence,
            peer_population_class,
        )
    )

    temporary["decision_confidence"] = (
        decision_confidence
    )

    primary_strengths = (
        build_primary_strengths(
            temporary
        )
    )

    primary_considerations = (
        build_primary_considerations(
            temporary,
            decision_posture,
        )
    )

    rationale = build_decision_rationale(
        decision_posture=decision_posture,
        financial_health=row.get(
            "financial_health"
        ),
        financial_risk_signal=row.get(
            "financial_risk_signal"
        ),
        financial_trajectory=trajectory,
        peer_position=peer_position,
        decision_confidence=decision_confidence,
        primary_strengths=primary_strengths,
        primary_considerations=primary_considerations,
    )

    decision_reason_code = determine_decision_reason_code(
        decision_posture=decision_posture,
        financial_health=row.get("financial_health"),
        financial_risk_signal=row.get("financial_risk_signal"),
        financial_trajectory=trajectory,
        peer_position=peer_position,
    )

    decision_priority = derive_decision_priority(
        decision_posture
    )

    calibrated_score = safe_float(
        calibration.get(
            "calibrated_ranking_score"
        )
    )

    calibrated_display_score = safe_float(
        calibration.get(
            "calibrated_display_score"
        )
    )

    calibrated_rank = safe_int(
        calibration.get(
            "calibrated_overall_rank"
        )
    )

    base_score = safe_float(
        calibration.get(
            "base_financial_score"
        )
    )

    calibration_adjustment = safe_float(
        calibration.get(
            "calibration_adjustment"
        )
    )

    calibrated_health = normalize_text(
        calibration.get(
            "calibrated_financial_health"
        )
    )

    company_archetype = normalize_text(
        calibration.get(
            "company_archetype"
        )
    )

    feature_quality = normalize_upper(
        row.get(
            "feature_data_quality"
        )
    )

    decision_quality = (
        "SOURCE_DATA_INCOMPLETE"
        if feature_quality == QUALITY_SOURCE_INCOMPLETE
        else feature_quality
    )

    return {
        # ---------------------------------------------------------------------
        # Grain
        # ---------------------------------------------------------------------

        "cik": cik,

        "fiscal_year": safe_int(
            row.get("fiscal_year")
        ),

        "quarter": normalize_upper(
            row.get("quarter")
        ),

        "quarter_num": safe_int(
            row.get("quarter_num")
        ),

        # ---------------------------------------------------------------------
        # Core Step 13 intelligence
        # ---------------------------------------------------------------------

        "financial_score": safe_float(
            row.get("financial_score")
        ),

        "financial_health": normalize_upper(
            row.get("financial_health")
        ),

        "financial_risk_signal": normalize_upper(
            row.get("financial_risk_signal")
        ),

        "financial_signal_confidence": (
            intelligence_confidence
        ),

        "revenue_growth_signal": normalize_upper(
            row.get("revenue_growth_signal")
        ),

        "profitability_signal": normalize_upper(
            row.get("profitability_signal")
        ),

        "growth_consistency_signal": normalize_upper(
            row.get("growth_consistency_signal")
        ),

        "momentum_signal": normalize_upper(
            row.get("momentum_signal")
        ),

        # ---------------------------------------------------------------------
        # Feature context
        # ---------------------------------------------------------------------

        "feature_data_quality": feature_quality,

        "feature_version": normalize_text(
            row.get("feature_version")
        ),

        # ---------------------------------------------------------------------
        # Ranking context
        # ---------------------------------------------------------------------

        "overall_rank": safe_int(
            ranking.get("overall_rank")
        ),

        "overall_ranking_score": safe_float(
            ranking.get(
                "overall_ranking_score"
            )
        ),

        "ranking_percentile": safe_float(
            ranking.get(
                "ranking_percentile"
            )
        ),

        "ranking_confidence": normalize_upper(
            ranking.get(
                "ranking_confidence"
            )
        ),

        "company_archetype": company_archetype,

        # ---------------------------------------------------------------------
        # Calibration context
        # ---------------------------------------------------------------------

        "base_financial_score": base_score,

        "calibration_adjustment": (
            calibration_adjustment
        ),

        "calibrated_ranking_score": (
            calibrated_score
        ),

        "calibrated_display_score": (
            calibrated_display_score
        ),

        "calibrated_overall_rank": (
            calibrated_rank
        ),

        "calibration_confidence": normalize_upper(
            calibration.get(
                "calibration_confidence"
            )
        ),

        "calibration_version": normalize_text(
            calibration.get(
                "calibration_version"
            )
        ),

        # ---------------------------------------------------------------------
        # Peer interpretation
        # ---------------------------------------------------------------------

        "peer_position": peer_position,

        "peer_population_class": (
            peer_population_class
        ),

        "peer_sample_size": (
            peer_sample_size
        ),

        # ---------------------------------------------------------------------
        # Decision interpretation
        # ---------------------------------------------------------------------

        "financial_trajectory": trajectory,

        "decision_posture": decision_posture,

        "decision_priority": decision_priority,

        "decision_reason_code": decision_reason_code,

        "decision_confidence": (
            decision_confidence
        ),

        "decision_quality": decision_quality,

        "primary_strengths": (
            " | ".join(primary_strengths)
            if primary_strengths
            else ""
        ),

        "primary_considerations": (
            " | ".join(primary_considerations)
            if primary_considerations
            else ""
        ),

        "decision_rationale": rationale,

        # ---------------------------------------------------------------------
        # Governance
        # ---------------------------------------------------------------------

        "decision_version": DECISION_VERSION,

        "decision_generated_at_utc": (
            utc_now_iso()
        ),

        "decision_rule_version": "1.2.0",

        "decision_score_created": False,

        "upstream_financial_score_source": (
            "Step 13 financial_score"
        ),

        "upstream_ranking_source": (
            "Step 14 financial ranking"
        ),

        "upstream_calibration_source": (
            "Step 14.1 calibrated ranking"
        ),
    }


# =============================================================================
# BUILD DECISION SIGNALS
# =============================================================================

def build_decision_signals(
    features: pd.DataFrame,
    intelligence: pd.DataFrame,
    ranking: pd.DataFrame,
    calibration: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    if len(ranking) != 1:

        raise ValueError(
            f"Expected one ranking row for {cik}."
        )

    if len(calibration) != 1:

        raise ValueError(
            f"Expected one calibration row for {cik}."
        )

    ranking_row = ranking.iloc[0]
    calibration_row = calibration.iloc[0]

    # -------------------------------------------------------------------------
    # Validate feature/intelligence grain alignment
    # -------------------------------------------------------------------------

    feature_keys = set(
        zip(
            features["fiscal_year"].astype(str),
            features["quarter"].astype(str),
        )
    )

    intelligence_keys = set(
        zip(
            intelligence["fiscal_year"].astype(str),
            intelligence["quarter"].astype(str),
        )
    )

    if feature_keys != intelligence_keys:

        missing_from_intelligence = (
            feature_keys
            - intelligence_keys
        )

        missing_from_features = (
            intelligence_keys
            - feature_keys
        )

        raise ValueError(
            f"Step 12 / Step 13 grain mismatch for {cik}. "
            f"Missing from intelligence: "
            f"{sorted(missing_from_intelligence)}. "
            f"Missing from features: "
            f"{sorted(missing_from_features)}."
        )

    # -------------------------------------------------------------------------
    # Merge intelligence with features for explicit upstream lineage
    # -------------------------------------------------------------------------

    feature_columns = [
        "fiscal_year",
        "quarter",
        "feature_data_quality",
        "feature_version",
    ]

    feature_columns = [
        c
        for c in feature_columns
        if c in features.columns
    ]

    feature_context = features[
        feature_columns
    ].copy()

    intelligence_context = intelligence.copy()

    for column in [
        "feature_data_quality",
        "feature_version",
    ]:

        if column in intelligence_context.columns:
            intelligence_context = (
                intelligence_context.drop(
                    columns=[column]
                )
            )

    merged = intelligence_context.merge(
        feature_context,
        on=[
            "fiscal_year",
            "quarter",
        ],
        how="left",
        validate="one_to_one",
    )

    if len(merged) != len(intelligence):

        raise ValueError(
            f"Step 12 / Step 13 merge changed row count "
            f"for {cik}: "
            f"{len(intelligence)} -> {len(merged)}"
        )

    # -------------------------------------------------------------------------
    # Build decisions
    # -------------------------------------------------------------------------

    rows = []

    for _, row in merged.iterrows():

        rows.append(
            build_decision_row(
                row=row,
                ranking=ranking_row,
                calibration=calibration_row,
                cik=cik,
            )
        )

    result = pd.DataFrame(rows)

    result = result.sort_values(
        [
            "fiscal_year",
            "quarter_num",
        ]
    ).reset_index(drop=True)

    return result


# =============================================================================
# SNAPSHOT
# =============================================================================

def build_snapshot(
    decisions: pd.DataFrame,
    cik: str,
) -> pd.DataFrame:

    if decisions.empty:

        raise ValueError(
            f"No decision rows available for snapshot: {cik}"
        )

    latest = (
        decisions
        .sort_values(
            [
                "fiscal_year",
                "quarter_num",
            ]
        )
        .iloc[-1]
        .copy()
    )

    row = {
        "cik": cik,

        "fiscal_year": safe_int(
            latest.get("fiscal_year")
        ),

        "quarter": normalize_upper(
            latest.get("quarter")
        ),

        "financial_score": safe_float(
            latest.get("financial_score")
        ),

        "financial_health": normalize_upper(
            latest.get("financial_health")
        ),

        "financial_risk_signal": normalize_upper(
            latest.get("financial_risk_signal")
        ),

        "financial_trajectory": normalize_upper(
            latest.get("financial_trajectory")
        ),

        "peer_position": normalize_upper(
            latest.get("peer_position")
        ),

        "peer_population_class": normalize_upper(
            latest.get("peer_population_class")
        ),

        "peer_sample_size": safe_int(
            latest.get("peer_sample_size")
        ),

        "overall_rank": safe_int(
            latest.get("overall_rank")
        ),

        "overall_ranking_score": safe_float(
            latest.get("overall_ranking_score")
        ),

        "calibrated_overall_rank": safe_int(
            latest.get("calibrated_overall_rank")
        ),

        "calibrated_ranking_score": safe_float(
            latest.get("calibrated_ranking_score")
        ),

        "calibrated_display_score": safe_float(
            latest.get("calibrated_display_score")
        ),

        "calibration_adjustment": safe_float(
            latest.get("calibration_adjustment")
        ),

        "company_archetype": normalize_text(
            latest.get("company_archetype")
        ),

        "decision_posture": normalize_upper(
            latest.get("decision_posture")
        ),

        "decision_priority": safe_int(
            latest.get("decision_priority")
        ),

        "decision_reason_code": normalize_upper(
            latest.get("decision_reason_code")
        ),

        "decision_confidence": normalize_upper(
            latest.get("decision_confidence")
        ),

        "decision_quality": normalize_upper(
            latest.get("decision_quality")
        ),

        "revenue_growth_signal": normalize_upper(
            latest.get("revenue_growth_signal")
        ),

        "profitability_signal": normalize_upper(
            latest.get("profitability_signal")
        ),

        "growth_consistency_signal": normalize_upper(
            latest.get("growth_consistency_signal")
        ),

        "momentum_signal": normalize_upper(
            latest.get("momentum_signal")
        ),

        "decision_rationale": normalize_text(
            latest.get("decision_rationale")
        ),

        "decision_version": DECISION_VERSION,

        "decision_generated_at_utc": (
            utc_now_iso()
        ),

        "decision_score_created": False,
    }

    return pd.DataFrame([row])


# =============================================================================
# COMPANY SUMMARY
# =============================================================================

def build_company_summary(
    decisions: pd.DataFrame,
    snapshot: pd.DataFrame,
    cik: str,
) -> dict:

    quality = (
        decisions["feature_data_quality"]
        .astype(str)
        .str.upper()
    )

    confidence = (
        decisions["decision_confidence"]
        .astype(str)
        .str.upper()
    )

    decision = (
        decisions["decision_posture"]
        .astype(str)
        .str.upper()
    )

    trajectory = (
        decisions["financial_trajectory"]
        .astype(str)
        .str.upper()
    )

    latest = snapshot.iloc[0]

    return {
        "cik": cik,

        "decision_rows": len(decisions),

        "snapshot_rows": len(snapshot),

        "complete_feature_rows": int(
            (quality == QUALITY_COMPLETE).sum()
        ),

        "partial_feature_rows": int(
            (quality == QUALITY_PARTIAL).sum()
        ),

        "source_incomplete_feature_rows": int(
            (
                quality
                == QUALITY_SOURCE_INCOMPLETE
            ).sum()
        ),

        "non_consecutive_feature_rows": int(
            (
                quality
                == QUALITY_NON_CONSECUTIVE
            ).sum()
        ),

        "insufficient_history_feature_rows": int(
            (
                quality
                == QUALITY_INSUFFICIENT_HISTORY
            ).sum()
        ),

        "positive_decision_rows": int(
            (decision == "POSITIVE").sum()
        ),

        "neutral_decision_rows": int(
            (decision == "NEUTRAL").sum()
        ),

        "watch_decision_rows": int(
            (decision == "WATCH").sum()
        ),

        "caution_decision_rows": int(
            (decision == "CAUTION").sum()
        ),

        "high_confidence_rows": int(
            (confidence == "HIGH").sum()
        ),

        "medium_confidence_rows": int(
            (confidence == "MEDIUM").sum()
        ),

        "low_confidence_rows": int(
            (confidence == "LOW").sum()
        ),

        "improving_rows": int(
            (trajectory == "IMPROVING").sum()
        ),

        "mixed_rows": int(
            (trajectory == "MIXED").sum()
        ),

        "deteriorating_rows": int(
            (trajectory == "DETERIORATING").sum()
        ),

        "latest_fiscal_year": safe_int(
            latest.get("fiscal_year")
        ),

        "latest_quarter": normalize_upper(
            latest.get("quarter")
        ),

        "latest_decision": normalize_upper(
            latest.get("decision_posture")
        ),

        "latest_confidence": normalize_upper(
            latest.get("decision_confidence")
        ),

        "latest_trajectory": normalize_upper(
            latest.get("financial_trajectory")
        ),

        "latest_peer_position": normalize_upper(
            latest.get("peer_position")
        ),

        "latest_peer_population_class": normalize_upper(
            latest.get("peer_population_class")
        ),

        "latest_financial_health": normalize_upper(
            latest.get("financial_health")
        ),

        "latest_financial_risk_signal": normalize_upper(
            latest.get("financial_risk_signal")
        ),

        "latest_calibrated_rank": safe_int(
            latest.get(
                "calibrated_overall_rank"
            )
        ),

        "latest_calibrated_score": safe_float(
            latest.get(
                "calibrated_display_score"
            )
        ),

        "decision_version": DECISION_VERSION,
    }


# =============================================================================
# WATCHLIST
# =============================================================================

def build_watchlist(
    decisions: pd.DataFrame,
) -> pd.DataFrame:

    if decisions.empty:

        return pd.DataFrame()

    latest = (
        decisions
        .sort_values(
            [
                "cik",
                "fiscal_year",
                "quarter_num",
            ]
        )
        .groupby(
            "cik",
            as_index=False,
            group_keys=False,
        )
        .tail(1)
        .copy()
    )

    rows = []

    for _, row in latest.iterrows():

        decision = normalize_upper(
            row.get("decision_posture")
        )

        risk = normalize_upper(
            row.get("financial_risk_signal")
        )

        trajectory = normalize_upper(
            row.get("financial_trajectory")
        )

        peer = normalize_upper(
            row.get("peer_position")
        )

        confidence = normalize_upper(
            row.get("decision_confidence")
        )

        reasons = []

        watchlist_class = "NO_ACTION"

        if decision == "CAUTION":

            watchlist_class = "CAUTION"

            reasons.append(
                "Decision posture requires heightened caution"
            )

        elif decision == "WATCH":

            watchlist_class = "WATCH"

            reasons.append(
                "Decision posture requires monitoring"
            )

        if risk in {
            "ELEVATED",
            "HIGH",
            "CRITICAL",
        }:

            reasons.append(
                f"Financial risk is {risk.lower()}"
            )

        if trajectory == "DETERIORATING":

            reasons.append(
                "Financial trajectory is deteriorating"
            )

        if peer == "BOTTOM_TIER":

            reasons.append(
                "Company is in the bottom tier of the peer universe"
            )

        if confidence == "LOW":

            reasons.append(
                "Decision confidence is low"
            )

        # Only retain actual watchlist cases.
        if watchlist_class != "NO_ACTION":

            rows.append(
                {
                    "cik": normalize_cik(
                        row.get("cik")
                    ),

                    "fiscal_year": safe_int(
                        row.get("fiscal_year")
                    ),

                    "quarter": normalize_upper(
                        row.get("quarter")
                    ),

                    "watchlist_class": (
                        watchlist_class
                    ),

                    "decision_posture": decision,

                    "decision_priority": safe_int(
                        row.get("decision_priority")
                    ),

                    "decision_reason_code": normalize_upper(
                        row.get("decision_reason_code")
                    ),

                    "decision_confidence": confidence,

                    "financial_health": normalize_upper(
                        row.get("financial_health")
                    ),

                    "financial_risk_signal": risk,

                    "financial_trajectory": trajectory,

                    "peer_position": peer,

                    "peer_population_class": normalize_upper(
                        row.get(
                            "peer_population_class"
                        )
                    ),

                    "calibrated_overall_rank": safe_int(
                        row.get(
                            "calibrated_overall_rank"
                        )
                    ),

                    "calibrated_display_score": safe_float(
                        row.get(
                            "calibrated_display_score"
                        )
                    ),

                    "watchlist_reason": (
                        " | ".join(reasons)
                    ),

                    "decision_version": (
                        DECISION_VERSION
                    ),
                }
            )

    return pd.DataFrame(rows)


# =============================================================================
# LINEAGE
# =============================================================================

def build_lineage(
    decisions: pd.DataFrame,
) -> pd.DataFrame:

    rows = []

    lineage_map = {
        "financial_score": (
            "Step 13 financial_intelligence."
            "financial_score"
        ),

        "financial_health": (
            "Step 13 financial_intelligence."
            "financial_health"
        ),

        "financial_risk_signal": (
            "Step 13 financial_intelligence."
            "financial_risk_signal"
        ),

        "revenue_growth_signal": (
            "Step 13 financial_intelligence."
            "revenue_growth_signal"
        ),

        "profitability_signal": (
            "Step 13 financial_intelligence."
            "profitability_signal"
        ),

        "growth_consistency_signal": (
            "Step 13 financial_intelligence."
            "growth_consistency_signal"
        ),

        "momentum_signal": (
            "Step 13 financial_intelligence."
            "momentum_signal"
        ),

        "feature_data_quality": (
            "Step 12 financial_features."
            "feature_data_quality"
        ),

        "overall_rank": (
            "Step 14 financial_ranking."
            "overall_rank"
        ),

        "overall_ranking_score": (
            "Step 14 financial_ranking."
            "overall_ranking_score"
        ),

        "calibrated_overall_rank": (
            "Step 14.1 calibrated ranking."
            "calibrated_overall_rank"
        ),

        "calibrated_ranking_score": (
            "Step 14.1 calibrated ranking."
            "calibrated_ranking_score"
        ),

        "calibration_adjustment": (
            "Step 14.1 calibrated ranking."
            "calibration_adjustment"
        ),

        "financial_trajectory": (
            "Deterministic Step 15 interpretation "
            "of Step 13 growth/profitability/momentum signals."
        ),

        "peer_position": (
            "Deterministic interpretation of Step 14.1 "
            "calibrated rank within current peer universe."
        ),

        "peer_population_class": (
            "Step 14.1 peer_sample_size and "
            "peer population threshold."
        ),

        "decision_posture": (
            "Deterministic Step 15 decision rule. "
            "No new financial score."
        ),

        "decision_priority": (
            "Deterministic priority derived from Step 15 decision posture."
        ),

        "decision_reason_code": (
            "Deterministic machine-readable reason for Step 15 decision posture."
        ),

        "decision_confidence": (
            "Conservative minimum of Step 13 intelligence "
            "confidence and Step 14.1 calibration confidence, "
            "capped at MEDIUM for LIMITED peer populations."
        ),

        "decision_rationale": (
            "Deterministic textual interpretation of "
            "decision posture, strengths and considerations."
        ),
    }

    for output_column, source in lineage_map.items():

        rows.append(
            {
                "output_column": output_column,

                "source": source,

                "transformation": (
                    "Step 15 deterministic interpretation"
                ),

                "decision_version": (
                    DECISION_VERSION
                ),

                "new_financial_score_created": False,
            }
        )

    return pd.DataFrame(rows)


# =============================================================================
# FINAL OUTPUT VALIDATION
# =============================================================================

def validate_final_outputs(
    signals: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> None:

    required_signal_columns = [
        "cik",
        "fiscal_year",
        "quarter",
        "decision_posture",
        "decision_confidence",
        "financial_trajectory",
        "peer_position",
        "peer_population_class",
        "decision_priority",
        "decision_reason_code",
        "decision_rationale",
        "decision_version",
    ]

    missing = [
        c
        for c in required_signal_columns
        if c not in signals.columns
    ]

    if missing:

        raise ValueError(
            f"Decision signals missing required columns: "
            f"{missing}"
        )

    duplicate_mask = signals.duplicated(
        subset=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        keep=False,
    )

    if duplicate_mask.any():

        raise ValueError(
            "Step 15 decision grain violation:\n"
            + signals.loc[
                duplicate_mask,
                [
                    "cik",
                    "fiscal_year",
                    "quarter",
                ],
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

    invalid_decisions = set(
        signals["decision_posture"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_DECISIONS

    if invalid_decisions:

        raise ValueError(
            f"Invalid decision posture values: "
            f"{sorted(invalid_decisions)}"
        )

    invalid_reason_codes = set(
        signals["decision_reason_code"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_REASON_CODES

    if invalid_reason_codes:
        raise ValueError(
            f"Invalid decision reason codes: "
            f"{sorted(invalid_reason_codes)}"
        )

    invalid_priorities = set(
        signals["decision_priority"]
        .dropna()
        .astype(int)
        .unique()
    ) - set(DECISION_PRIORITY.values())

    if invalid_priorities:
        raise ValueError(
            f"Invalid decision priorities: "
            f"{sorted(invalid_priorities)}"
        )

    priority_mismatch = signals.apply(
        lambda r: safe_int(r.get("decision_priority"))
        != derive_decision_priority(r.get("decision_posture")),
        axis=1,
    )

    if priority_mismatch.any():
        raise ValueError(
            "Decision priority does not match decision posture."
        )

    invalid_trajectories = set(
        signals["financial_trajectory"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_TRAJECTORIES

    if invalid_trajectories:

        raise ValueError(
            f"Invalid financial trajectory values: "
            f"{sorted(invalid_trajectories)}"
        )

    invalid_confidence = set(
        signals["decision_confidence"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_CONFIDENCE

    if invalid_confidence:

        raise ValueError(
            f"Invalid decision confidence values: "
            f"{sorted(invalid_confidence)}"
        )

    invalid_peer_positions = set(
        signals["peer_position"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_PEER_POSITIONS

    if invalid_peer_positions:

        raise ValueError(
            f"Invalid peer positions: "
            f"{sorted(invalid_peer_positions)}"
        )

    invalid_population = set(
        signals["peer_population_class"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
    ) - VALID_PEER_POPULATION

    if invalid_population:

        raise ValueError(
            f"Invalid peer population classes: "
            f"{sorted(invalid_population)}"
        )

    if "decision_score_created" in signals.columns:

        invalid_score_flag = signals[
            signals["decision_score_created"]
            .astype(bool)
        ]

        if not invalid_score_flag.empty:

            raise ValueError(
                "Step 15 contract violation: "
                "a new financial score was created."
            )

    if len(snapshots) == 0:

        raise ValueError(
            "No company decision snapshots generated."
        )

    snapshot_duplicates = snapshots.duplicated(
        subset=["cik"],
        keep=False,
    )

    if snapshot_duplicates.any():

        raise ValueError(
            "Step 15 snapshot grain violation."
        )


# =============================================================================
# COMPANY RESULT
# =============================================================================

@dataclass
class CompanyResult:

    cik: str

    feature_rows: int

    intelligence_rows: int

    decision_rows: int

    snapshot_rows: int

    complete_rows: int

    partial_rows: int

    source_incomplete_rows: int

    non_consecutive_rows: int

    insufficient_history_rows: int

    high_confidence_rows: int

    medium_confidence_rows: int

    low_confidence_rows: int

    failed: bool = False

    error: str = ""


# =============================================================================
# COMPANY PROCESSING
# =============================================================================

def process_company(
    cik: str,
    features_root: Path,
    intelligence_root: Path,
    ranking_root: Path,
    calibration_root: Path,
) -> tuple[
    CompanyResult,
    pd.DataFrame,
    pd.DataFrame,
]:

    features = load_features(
        features_root,
        cik,
    )

    intelligence, _ = load_intelligence(
        intelligence_root,
        cik,
    )

    ranking = load_ranking(
        ranking_root,
        cik,
    )

    calibration = load_calibration(
        calibration_root,
        cik,
    )

    decisions = build_decision_signals(
        features=features,
        intelligence=intelligence,
        ranking=ranking,
        calibration=calibration,
        cik=cik,
    )

    snapshot = build_snapshot(
        decisions,
        cik,
    )

    quality = (
        decisions["feature_data_quality"]
        .astype(str)
        .str.upper()
    )

    confidence = (
        decisions["decision_confidence"]
        .astype(str)
        .str.upper()
    )

    result = CompanyResult(
        cik=cik,

        feature_rows=len(features),

        intelligence_rows=len(intelligence),

        decision_rows=len(decisions),

        snapshot_rows=len(snapshot),

        complete_rows=int(
            (quality == QUALITY_COMPLETE).sum()
        ),

        partial_rows=int(
            (quality == QUALITY_PARTIAL).sum()
        ),

        source_incomplete_rows=int(
            (
                quality
                == QUALITY_SOURCE_INCOMPLETE
            ).sum()
        ),

        non_consecutive_rows=int(
            (
                quality
                == QUALITY_NON_CONSECUTIVE
            ).sum()
        ),

        insufficient_history_rows=int(
            (
                quality
                == QUALITY_INSUFFICIENT_HISTORY
            ).sum()
        ),

        high_confidence_rows=int(
            (confidence == "HIGH").sum()
        ),

        medium_confidence_rows=int(
            (confidence == "MEDIUM").sum()
        ),

        low_confidence_rows=int(
            (confidence == "LOW").sum()
        ),
    )

    return (
        result,
        decisions,
        snapshot,
    )


# =============================================================================
# ARGUMENT PARSER
# =============================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Step 15 - Financial Decision Intelligence"
        )
    )

    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
    )

    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES_DIR,
    )

    parser.add_argument(
        "--feature-validation",
        type=Path,
        default=DEFAULT_FEATURE_VALIDATION_DIR,
    )

    parser.add_argument(
        "--intelligence",
        type=Path,
        default=DEFAULT_INTELLIGENCE_DIR,
    )

    parser.add_argument(
        "--intelligence-validation",
        type=Path,
        default=DEFAULT_INTELLIGENCE_VALIDATION_DIR,
    )

    parser.add_argument(
        "--ranking",
        type=Path,
        default=DEFAULT_RANKING_DIR,
    )

    parser.add_argument(
        "--calibration",
        type=Path,
        default=DEFAULT_CALIBRATION_DIR,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )

    return parser.parse_args()


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary(
    signals_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
    company_results: list[CompanyResult],
) -> None:

    section(
        "STEP 15 SUMMARY"
    )

    log(
        f"Total companies       : "
        f"{len(snapshot_df)}"
    )

    log(
        f"Total decision rows   : "
        f"{len(signals_df)}"
    )

    log("")

    log("Decision posture:")

    if not signals_df.empty:

        log(
            signals_df[
                "decision_posture"
            ]
            .value_counts()
            .sort_index()
            .to_string()
        )

    log("")

    log("Decision confidence:")

    if not signals_df.empty:

        log(
            signals_df[
                "decision_confidence"
            ]
            .value_counts()
            .sort_index()
            .to_string()
        )

    log("")

    log("Financial trajectory:")

    if not signals_df.empty:

        log(
            signals_df[
                "financial_trajectory"
            ]
            .value_counts()
            .sort_index()
            .to_string()
        )

    log("")

    log("Peer population:")

    if not signals_df.empty:

        latest = (
            signals_df
            .sort_values(
                [
                    "cik",
                    "fiscal_year",
                    "quarter_num",
                ]
            )
            .groupby(
                "cik",
                as_index=False,
            )
            .tail(1)
        )

        log(
            latest[
                "peer_population_class"
            ]
            .value_counts()
            .sort_index()
            .to_string()
        )

    log("")

    section(
        "COMPANY DECISION SNAPSHOT"
    )

    display_columns = [
        "cik",
        "fiscal_year",
        "quarter",
        "calibrated_overall_rank",
        "calibrated_display_score",
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "decision_confidence",
        "decision_posture",
        "decision_priority",
        "decision_reason_code",
        "company_archetype",
    ]

    existing = [
        c
        for c in display_columns
        if c in snapshot_df.columns
    ]

    log(
        snapshot_df[
            existing
        ].to_string(index=False)
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:

    args = parse_args()

    # -------------------------------------------------------------------------
    # Header
    # -------------------------------------------------------------------------

    section(
        "STEP 15 - FINANCIAL DECISION INTELLIGENCE"
    )

    log(
        f"Features            : {args.features}"
    )

    log(
        f"Feature Validation  : "
        f"{args.feature_validation}"
    )

    log(
        f"Intelligence        : "
        f"{args.intelligence}"
    )

    log(
        f"Intelligence Valid. : "
        f"{args.intelligence_validation}"
    )

    log(
        f"Ranking             : "
        f"{args.ranking}"
    )

    log(
        f"Calibration         : "
        f"{args.calibration}"
    )

    log(
        f"Output              : "
        f"{args.output}"
    )

    log(
        f"Decision version    : "
        f"{DECISION_VERSION}"
    )

    log(
        f"Supported features  : "
        f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    log(
        f"Supported intelligence: "
        f"{sorted(SUPPORTED_INTELLIGENCE_VERSIONS)}"
    )

    log(
        f"Supported ranking   : "
        f"{sorted(SUPPORTED_RANKING_VERSIONS)}"
    )

    log(
        f"Supported calibration: "
        f"{sorted(SUPPORTED_CALIBRATION_VERSIONS)}"
    )

    log(
        "Contract            : "
        "NO NEW FINANCIAL SCORE"
    )

    log(
        "Contract            : "
        "DETERMINISTIC DECISION INTERPRETATION"
    )

    log(
        "Contract            : "
        "PEER CONFIDENCE PROPAGATED"
    )

    log(
        "Contract            : "
        "MACHINE-READABLE DECISION REASON + PRIORITY"
    )

    log(
        f"Project root        : "
        f"{args.base_dir}"
    )

    # -------------------------------------------------------------------------
    # Step 12.1 gate
    # -------------------------------------------------------------------------

    log(
        "Checking Step 12.1 feature-quality gate..."
    )

    try:

        load_validation_gate(
            args.feature_validation
        )

        log(
            "Step 12.1 gate       : PASS"
        )

    except Exception:

        log(
            "Step 12.1 gate       : FAIL"
        )

        raise

    # -------------------------------------------------------------------------
    # Step 13.1 gate
    # -------------------------------------------------------------------------

    log(
        "Checking Step 13.1 intelligence-quality gate..."
    )

    try:

        load_intelligence_validation_gate(
            args.intelligence_validation
        )

        log(
            "Step 13.1 gate       : PASS"
        )

    except Exception:

        log(
            "Step 13.1 gate       : FAIL"
        )

        raise

    # -------------------------------------------------------------------------
    # Discover companies
    # -------------------------------------------------------------------------

    discovered = discover_companies(
        args.features
    )

    log(
        f"Companies discovered: "
        f"{len(discovered)}"
    )

    log("")

    # -------------------------------------------------------------------------
    # Processing
    # -------------------------------------------------------------------------

    all_decisions = []

    all_snapshots = []

    results = []

    for cik in discovered:

        section(
            f"PROCESSING CIK: {cik}"
        )

        try:

            (
                result,
                decisions,
                snapshot,
            ) = process_company(
                cik=cik,
                features_root=args.features,
                intelligence_root=args.intelligence,
                ranking_root=args.ranking,
                calibration_root=args.calibration,
            )

            latest = (
                decisions
                .sort_values(
                    [
                        "fiscal_year",
                        "quarter_num",
                    ]
                )
                .iloc[-1]
            )

            log(
                f"Feature rows        : "
                f"{result.feature_rows}"
            )

            log(
                f"Intelligence rows   : "
                f"{result.intelligence_rows}"
            )

            log(
                f"Ranking rows        : 1"
            )

            log(
                f"Calibration rows    : 1"
            )

            log(
                f"Unique company-quarter rows: "
                f"{result.feature_rows}"
            )

            log(
                f"Decision signal rows: "
                f"{result.decision_rows}"
            )

            log(
                f"Latest decision    : "
                f"{latest['decision_posture']}"
            )

            log(
                f"Latest confidence  : "
                f"{latest['decision_confidence']}"
            )

            log(
                f"Financial trajectory: "
                f"{latest['financial_trajectory']}"
            )

            log(
                f"Peer position      : "
                f"{latest['peer_position']}"
            )

            log(
                f"Peer population    : "
                f"{latest['peer_population_class']}"
            )

            log(
                f"Latest rationale   : "
                f"{latest['decision_rationale']}"
            )

            results.append(
                result
            )

            all_decisions.append(
                decisions
            )

            all_snapshots.append(
                snapshot
            )

        except Exception as exc:

            log(
                f"ERROR processing {cik}: "
                f"{exc}"
            )

            traceback.print_exc()

            results.append(
                CompanyResult(
                    cik=cik,
                    feature_rows=0,
                    intelligence_rows=0,
                    decision_rows=0,
                    snapshot_rows=0,
                    complete_rows=0,
                    partial_rows=0,
                    source_incomplete_rows=0,
                    non_consecutive_rows=0,
                    insufficient_history_rows=0,
                    high_confidence_rows=0,
                    medium_confidence_rows=0,
                    low_confidence_rows=0,
                    failed=True,
                    error=str(exc),
                )
            )

    # -------------------------------------------------------------------------
    # Failure gate
    # -------------------------------------------------------------------------

    failed = [
        r
        for r in results
        if r.failed
    ]

    if failed:

        section(
            "STEP 15 FAILED"
        )

        for result in failed:

            log(
                f"{result.cik}: "
                f"{result.error}"
            )

        raise RuntimeError(
            f"Step 15 failed for "
            f"{len(failed)} company/companies."
        )

    # -------------------------------------------------------------------------
    # Combine
    # -------------------------------------------------------------------------

    signals_df = pd.concat(
        all_decisions,
        ignore_index=True,
    )

    snapshot_df = pd.concat(
        all_snapshots,
        ignore_index=True,
    )

    signals_df = signals_df.sort_values(
        [
            "cik",
            "fiscal_year",
            "quarter_num",
        ]
    ).reset_index(drop=True)

    snapshot_df = snapshot_df.sort_values(
        ["cik"]
    ).reset_index(drop=True)

    # -------------------------------------------------------------------------
    # Validate final outputs
    # -------------------------------------------------------------------------

    validate_final_outputs(
        signals_df,
        snapshot_df,
    )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------

    company_summary_df = pd.DataFrame(
        [
            r.__dict__
            for r in results
        ]
    )

    # -------------------------------------------------------------------------
    # Watchlist
    # -------------------------------------------------------------------------

    watchlist_df = build_watchlist(
        signals_df
    )

    # -------------------------------------------------------------------------
    # Lineage
    # -------------------------------------------------------------------------

    lineage_df = build_lineage(
        signals_df
    )

    # -------------------------------------------------------------------------
    # Financial decision summary
    # -------------------------------------------------------------------------

    summary_rows = []

    if not signals_df.empty:

        summary_rows.append(
            {
                "decision_version": DECISION_VERSION,

                "total_companies": int(
                    snapshot_df["cik"]
                    .nunique()
                ),

                "total_decision_rows": int(
                    len(signals_df)
                ),

                "positive_rows": int(
                    (
                        signals_df[
                            "decision_posture"
                        ]
                        .astype(str)
                        .str.upper()
                        == "POSITIVE"
                    ).sum()
                ),

                "neutral_rows": int(
                    (
                        signals_df[
                            "decision_posture"
                        ]
                        .astype(str)
                        .str.upper()
                        == "NEUTRAL"
                    ).sum()
                ),

                "watch_rows": int(
                    (
                        signals_df[
                            "decision_posture"
                        ]
                        .astype(str)
                        .str.upper()
                        == "WATCH"
                    ).sum()
                ),

                "caution_rows": int(
                    (
                        signals_df[
                            "decision_posture"
                        ]
                        .astype(str)
                        .str.upper()
                        == "CAUTION"
                    ).sum()
                ),

                "high_confidence_rows": int(
                    (
                        signals_df[
                            "decision_confidence"
                        ]
                        .astype(str)
                        .str.upper()
                        == "HIGH"
                    ).sum()
                ),

                "medium_confidence_rows": int(
                    (
                        signals_df[
                            "decision_confidence"
                        ]
                        .astype(str)
                        .str.upper()
                        == "MEDIUM"
                    ).sum()
                ),

                "low_confidence_rows": int(
                    (
                        signals_df[
                            "decision_confidence"
                        ]
                        .astype(str)
                        .str.upper()
                        == "LOW"
                    ).sum()
                ),

                "improving_rows": int(
                    (
                        signals_df[
                            "financial_trajectory"
                        ]
                        .astype(str)
                        .str.upper()
                        == "IMPROVING"
                    ).sum()
                ),

                "mixed_rows": int(
                    (
                        signals_df[
                            "financial_trajectory"
                        ]
                        .astype(str)
                        .str.upper()
                        == "MIXED"
                    ).sum()
                ),

                "deteriorating_rows": int(
                    (
                        signals_df[
                            "financial_trajectory"
                        ]
                        .astype(str)
                        .str.upper()
                        == "DETERIORATING"
                    ).sum()
                ),

                "limited_peer_company_rows": int(
                    (
                        signals_df[
                            "peer_population_class"
                        ]
                        .astype(str)
                        .str.upper()
                        == "LIMITED"
                    ).sum()
                ),

                "new_financial_score_created": False,

                "step12_1_gate": "PASS",

                "step13_1_gate": "PASS",

                "generated_at_utc": utc_now_iso(),
            }
        )

    decision_summary_df = pd.DataFrame(
        summary_rows
    )

    # -------------------------------------------------------------------------
    # Outputs
    # -------------------------------------------------------------------------

    paths = output_paths(
        args.output
    )

    write_parquet(
        signals_df,
        paths["signals"],
    )

    write_parquet(
        snapshot_df,
        paths["snapshot"],
    )

    write_parquet(
        decision_summary_df,
        paths["summary"],
    )

    write_parquet(
        watchlist_df,
        paths["watchlist"],
    )

    write_parquet(
        lineage_df,
        paths["lineage"],
    )

    # -------------------------------------------------------------------------
    # Terminal summary
    # -------------------------------------------------------------------------

    print_summary(
        signals_df,
        snapshot_df,
        results,
    )

    section(
        "OUTPUTS"
    )

    log(
        f"Decision signals   : "
        f"{paths['signals']}"
    )

    log(
        f"Company snapshot   : "
        f"{paths['snapshot']}"
    )

    log(
        f"Decision summary   : "
        f"{paths['summary']}"
    )

    log(
        f"Decision watchlist : "
        f"{paths['watchlist']}"
    )

    log(
        f"Decision lineage   : "
        f"{paths['lineage']}"
    )

    section(
        "STEP 15 COMPLETE"
    )

    log(
        f"Decision intelligence version: "
        f"{DECISION_VERSION}"
    )

    log(
        "All companies successfully processed."
    )

    log(
        "Step 12.1 and Step 13.1 remained upstream quality gates."
    )

    log(
        "No new financial score was created."
    )

    log(
        "Steps 10, 11, 12, 12.1, 13, 13.1, "
        "14 and 14.1 were not modified."
    )


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        section(
            "STEP 15 FAILED"
        )

        log(
            str(exc)
        )

        sys.exit(1)