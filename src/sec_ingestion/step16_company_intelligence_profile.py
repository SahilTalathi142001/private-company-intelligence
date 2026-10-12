"""
STEP 16 - COMPANY INTELLIGENCE PROFILE

Purpose
-------
Build a company-facing intelligence profile from the validated outputs of
Steps 12, 13, 14.1 and 15.

Design principles
-----------------
1. Step 16 does NOT create a new financial score.
2. Step 15 remains the authoritative decision interpretation.
3. Step 12 remains the source for quantitative financial features.
4. The latest company profile is one row per company.
5. Historical decision signals are retained through compact trend summaries.
6. Strengths, concerns and profile narrative are deterministic.
7. Peer limitations and upstream quality limitations remain visible.
8. Missing data is represented explicitly; it is never silently fabricated.
9. Every profile field has source/lineage metadata.
10. This layer is designed for downstream Power BI / API / web consumption.

Inputs
------
Step 12:
    financials_features/

Step 15:
    financial_decision_intelligence/
        decision_signals/
        company_financial_decision_snapshot/

Optional company master:
    CSV or Parquet containing:
        cik
        company_name
        [optional: industry, founded_year, headquarters, status]

Outputs
-------
financial_company_intelligence/
    company_intelligence_profile/
        company_intelligence_profile.parquet

    company_intelligence_history/
        company_intelligence_history.parquet

    company_intelligence_signals/
        company_intelligence_signals.parquet

    company_intelligence_summary/
        company_intelligence_summary.parquet

    company_intelligence_lineage/
        company_intelligence_lineage.parquet
"""

from __future__ import annotations

import argparse
import math
import sys
import traceback
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from company_platform.registry.active_company_scope import load_active_company_ciks


# =============================================================================
# VERSION / CONTRACT
# =============================================================================

PROFILE_VERSION = "1.1.0"

SUPPORTED_FEATURE_VERSIONS = {"1.2.0"}
SUPPORTED_DECISION_VERSIONS = {"1.2.0"}

QUALITY_COMPLETE = "COMPLETE"
QUALITY_PARTIAL = "PARTIAL"

VALID_HEALTH = {
    "EXCELLENT",
    "STRONG",
    "HEALTHY",
    "MIXED",
    "WEAK",
    "CRITICAL",
    "INSUFFICIENT_DATA",
}

VALID_RISK = {
    "LOW",
    "MODERATE",
    "ELEVATED",
    "HIGH",
    "CRITICAL",
}

VALID_TRAJECTORY = {
    "IMPROVING",
    "MIXED",
    "DETERIORATING",
}

VALID_DECISIONS = {
    "POSITIVE",
    "NEUTRAL",
    "WATCH",
    "CAUTION",
}

VALID_CONFIDENCE = {
    "HIGH",
    "MEDIUM",
    "LOW",
}

VALID_PEER_POSITION = {
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

DECISION_PRIORITY_MAP = {
    "CAUTION": 1,
    "WATCH": 2,
    "NEUTRAL": 3,
    "POSITIVE": 4,
}

DECISION_ORDER = {
    "CAUTION": 1,
    "WATCH": 2,
    "NEUTRAL": 3,
    "POSITIVE": 4,
}

TRAJECTORY_ORDER = {
    "DETERIORATING": 1,
    "MIXED": 2,
    "IMPROVING": 3,
}


# =============================================================================
# DEFAULT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_FEATURES = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_features"
)

DEFAULT_DECISIONS = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_decision_intelligence"
)

DEFAULT_COMPANY_MASTER = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "company"
    / "company.parquet"
)

DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_company_intelligence"
)


# =============================================================================
# HELPERS
# =============================================================================

def log(message: str) -> None:
    print(message, flush=True)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


def normalize_cik(value) -> str:
    if pd.isna(value):
        return ""

    text = str(value).strip()

    if text.endswith(".0"):
        text = text[:-2]

    digits = "".join(ch for ch in text if ch.isdigit())

    if not digits:
        return ""

    return digits.zfill(10)


def normalize_upper(value) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip().upper()

    if text in {"", "NAN", "NONE", "NA", "<NA>"}:
        return ""

    return text


def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()

    if text.lower() in {"nan", "none", "<na>"}:
        return ""

    return text


def safe_float(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None

    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(value):
        return None

    return value


def safe_int(value) -> Optional[int]:
    if value is None or pd.isna(value):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def read_parquet(path: Path, description: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"{description} not found: {path}"
        )

    df = pd.read_parquet(path)

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"{description} did not load as a DataFrame: {path}"
        )

    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_suffix(".tmp.parquet")

    if temporary.exists():
        temporary.unlink()

    df.to_parquet(
        temporary,
        index=False,
    )

    temporary.replace(path)


def quarter_num(value) -> int:
    quarter = normalize_upper(value)

    mapping = {
        "Q1": 1,
        "Q2": 2,
        "Q3": 3,
        "Q4": 4,
    }

    return mapping.get(quarter, 0)


def latest_row(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        raise ValueError("Cannot select latest row from empty DataFrame.")

    work = df.copy()

    work["_fy_sort"] = pd.to_numeric(
        work["fiscal_year"],
        errors="coerce",
    )

    work["_q_sort"] = work["quarter"].map(quarter_num)

    work = work.sort_values(
        ["_fy_sort", "_q_sort"],
        na_position="last",
    )

    return work.iloc[-1]


def unique_join(values: list[str]) -> str:
    result = []

    for value in values:
        value = normalize_text(value)

        if not value:
            continue

        if value not in result:
            result.append(value)

    return " | ".join(result)


# =============================================================================
# INPUT LOADING
# =============================================================================

def discover_companies(
    decisions_root: Path,
) -> list[str]:
    """Return the authoritative active execution universe.

    Step 15 availability is a data-coverage attribute, not company identity.
    A company with no current financial facts must still receive a profile row
    so downstream company-grain artifacts remain universe-complete.
    """
    companies = load_active_company_ciks()
    if not companies:
        raise ValueError("No active companies are configured in the platform registry.")
    return companies


def load_decision_signals(
    decisions_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        decisions_root
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )

    df = read_parquet(
        path,
        "Step 15 decision signals",
    )

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "decision_posture",
        "decision_confidence",
        "financial_trajectory",
        "financial_health",
        "financial_risk_signal",
        "peer_position",
        "peer_population_class",
        "decision_rationale",
        "decision_version",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(
            "Step 15 decision signals missing required columns: "
            f"{missing}"
        )

    df["cik"] = df["cik"].map(normalize_cik)

    df = df[
        df["cik"] == normalize_cik(cik)
    ].copy()

    if df.empty:
        raise ValueError(
            f"No Step 15 decision signals found for {cik}."
        )

    versions = {
        normalize_text(value)
        for value in df["decision_version"].dropna()
    }

    unsupported = (
        versions
        - SUPPORTED_DECISION_VERSIONS
    )

    if unsupported:
        raise ValueError(
            f"Unsupported Step 15 decision versions for {cik}: "
            f"{sorted(unsupported)}"
        )

    duplicate_mask = df.duplicated(
        subset=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            f"Step 15 decision grain violation for {cik}."
        )

    df["quarter_num"] = df["quarter"].map(quarter_num)

    return df.sort_values(
        ["fiscal_year", "quarter_num"]
    ).reset_index(drop=True)


def load_features(
    features_root: Path,
    cik: str,
) -> pd.DataFrame:

    path = (
        features_root
        / f"cik={normalize_cik(cik)}"
        / "financial_features.parquet"
    )

    if not path.exists():
        candidates = list(
            features_root.glob(
                f"**/*{normalize_cik(cik)}*.parquet"
            )
        )

        if len(candidates) == 1:
            path = candidates[0]

    df = read_parquet(
        path,
        f"Step 12 financial features for {cik}",
    )

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "feature_version",
        "revenue_value",
        "operating_income_value",
        "net_income_value",
        "revenue_yoy_growth_pct",
        "revenue_growth_acceleration",
        "operating_margin",
        "net_margin",
        "ttm_revenue",
        "ttm_operating_margin",
        "ttm_net_margin",
        "revenue_growth_trend",
        "operating_margin_trend",
        "net_margin_trend",
        "revenue_growth_consistent_4q_flag",
        "feature_data_quality",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise KeyError(
            f"Step 12 financial features missing required columns "
            f"for {cik}: {missing}"
        )

    df["cik"] = df["cik"].map(normalize_cik)

    df = df[
        df["cik"] == normalize_cik(cik)
    ].copy()

    if df.empty:
        raise ValueError(
            f"No Step 12 financial features found for {cik}."
        )

    versions = {
        normalize_text(value)
        for value in df["feature_version"].dropna()
    }

    unsupported = (
        versions
        - SUPPORTED_FEATURE_VERSIONS
    )

    if unsupported:
        raise ValueError(
            f"Unsupported Step 12 feature versions for {cik}: "
            f"{sorted(unsupported)}"
        )

    df["quarter_num"] = df["quarter"].map(quarter_num)

    duplicate_mask = df.duplicated(
        subset=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            f"Step 12 feature grain violation for {cik}."
        )

    return df.sort_values(
        ["fiscal_year", "quarter_num"]
    ).reset_index(drop=True)


def load_company_master(
    path: Path,
) -> pd.DataFrame:

    if not path.exists():
        log(
            f"Company master     : not found; "
            f"company_name will default to CIK"
        )
        return pd.DataFrame()

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)

    if "cik" not in df.columns:
        raise KeyError(
            "Company master must contain cik."
        )

    df["cik"] = df["cik"].map(normalize_cik)

    if df["cik"].duplicated().any():
        conflicting_names = (
            df.groupby("cik")["company_name"]
            .nunique(dropna=False)
        )
        if (conflicting_names > 1).any():
            raise ValueError(
                "Company master contains conflicting company names for "
                "duplicate cik values."
            )

        sort_columns = ["cik", "company_name"]
        if "ticker" in df.columns:
            sort_columns.append("ticker")
        df = (
            df.sort_values(sort_columns, na_position="last")
            .drop_duplicates(subset=["cik"], keep="first")
            .reset_index(drop=True)
        )

    return df


# =============================================================================
# FEATURE EXTRACTION
# =============================================================================

def extract_numeric_profile(
    feature: pd.Series,
) -> dict:

    numeric_columns = [
        "revenue_value",
        "operating_income_value",
        "net_income_value",
        "revenue_yoy_growth_pct",
        "revenue_growth_acceleration",
        "operating_margin",
        "net_margin",
        "ttm_revenue",
        "ttm_operating_margin",
        "ttm_net_margin",
        "revenue_growth_4q_avg",
        "operating_margin_4q_avg",
        "net_margin_4q_avg",
        "revenue_growth_volatility_4q",
        "operating_margin_volatility_4q",
        "net_margin_volatility_4q",
        "operating_margin_yoy_change",
        "net_margin_yoy_change",
    ]

    result = {}

    for column in numeric_columns:
        if column in feature.index:
            result[column] = safe_float(
                feature.get(column)
            )
        else:
            result[column] = None

    return result


# =============================================================================
# DETERMINISTIC INTELLIGENCE
# =============================================================================

def build_structured_strengths(
    decision: pd.Series,
    feature: pd.Series,
) -> list[str]:

    strengths = []

    revenue_signal = normalize_upper(
        decision.get("revenue_growth_signal")
    )

    profitability_signal = normalize_upper(
        decision.get("profitability_signal")
    )

    consistency_signal = normalize_upper(
        decision.get("growth_consistency_signal")
    )

    momentum_signal = normalize_upper(
        decision.get("momentum_signal")
    )

    health = normalize_upper(
        decision.get("financial_health")
    )

    risk = normalize_upper(
        decision.get("financial_risk_signal")
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

    if momentum_signal in {
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

    ttm_revenue = safe_float(
        feature.get("ttm_revenue")
    )

    if ttm_revenue is not None and ttm_revenue > 0:
        strengths.append(
            "TTM revenue is available"
        )

    return strengths


def build_structured_concerns(
    decision: pd.Series,
    feature: pd.Series,
) -> list[str]:

    concerns = []

    revenue_signal = normalize_upper(
        decision.get("revenue_growth_signal")
    )

    profitability_signal = normalize_upper(
        decision.get("profitability_signal")
    )

    risk = normalize_upper(
        decision.get("financial_risk_signal")
    )

    trajectory = normalize_upper(
        decision.get("financial_trajectory")
    )

    peer = normalize_upper(
        decision.get("peer_position")
    )

    confidence = normalize_upper(
        decision.get("decision_confidence")
    )

    population = normalize_upper(
        decision.get("peer_population_class")
    )

    if revenue_signal == "DECELERATING":
        concerns.append(
            "Revenue growth is decelerating"
        )
    elif revenue_signal == "CONTRACTING":
        concerns.append(
            "Revenue growth is contracting"
        )

    if profitability_signal == "CONTRACTING":
        concerns.append(
            "Profitability is contracting"
        )
    elif profitability_signal == "DECELERATING":
        concerns.append(
            "Profitability is decelerating"
        )

    if risk == "MODERATE":
        concerns.append(
            "Financial risk is moderate"
        )
    elif risk == "ELEVATED":
        concerns.append(
            "Financial risk is elevated"
        )
    elif risk in {"HIGH", "CRITICAL"}:
        concerns.append(
            f"Financial risk is {risk.lower()}"
        )

    if trajectory == "DETERIORATING":
        concerns.append(
            "Financial trajectory is deteriorating"
        )

    if peer == "BOTTOM_TIER":
        concerns.append(
            "Company is in the bottom tier of the peer universe"
        )
    elif peer == "LOWER_MIDDLE":
        concerns.append(
            "Company is in the lower middle of the peer universe"
        )

    if population == "LIMITED":
        concerns.append(
            "Peer comparison is based on a limited population"
        )

    if confidence == "LOW":
        concerns.append(
            "Decision confidence is low"
        )

    if normalize_upper(
        decision.get("decision_posture")
    ) in {"WATCH", "CAUTION"}:
        concerns.append(
            "Current financial position warrants attention"
        )

    return concerns


def derive_company_archetype(
    decision: pd.Series,
    feature: pd.Series,
) -> str:

    health = normalize_upper(
        decision.get("financial_health")
    )

    risk = normalize_upper(
        decision.get("financial_risk_signal")
    )

    trajectory = normalize_upper(
        decision.get("financial_trajectory")
    )

    revenue_signal = normalize_upper(
        decision.get("revenue_growth_signal")
    )

    profitability_signal = normalize_upper(
        decision.get("profitability_signal")
    )

    if trajectory == "DETERIORATING":
        return "DECLINING"

    if (
        revenue_signal in {
            "ACCELERATING",
            "STRONG_ACCELERATION",
        }
        and profitability_signal in {
            "EXPANDING",
            "STRONG_EXPANSION",
        }
        and health in {"EXCELLENT", "STRONG"}
        and risk == "LOW"
    ):
        return "HIGH_GROWTH_HIGH_PROFITABILITY"

    if (
        revenue_signal in {
            "ACCELERATING",
            "STRONG_ACCELERATION",
        }
        and trajectory == "IMPROVING"
    ):
        return "HIGH_GROWTH_MOMENTUM"

    if (
        revenue_signal in {
            "ACCELERATING",
            "STRONG_ACCELERATION",
        }
        and profitability_signal in {
            "CONTRACTING",
            "DECELERATING",
        }
    ):
        return "GROWTH_WITH_MARGIN_PRESSURE"

    if (
        health in {"EXCELLENT", "STRONG", "HEALTHY"}
        and trajectory == "MIXED"
        and profitability_signal == "STABLE"
        and risk in {"LOW", "MODERATE"}
    ):
        return "PROFITABLE_STABLE"

    if (
        trajectory == "IMPROVING"
        and profitability_signal == "EXPANDING"
        and revenue_signal not in {
            "ACCELERATING",
            "STRONG_ACCELERATION",
        }
    ):
        return "TURNAROUND"

    return "MIXED"


def derive_profile_quality(
    decision: pd.Series,
    feature: pd.Series,
) -> str:

    feature_quality = normalize_upper(
        feature.get("feature_data_quality")
    )

    required_decision_values = [
        decision.get("financial_health"),
        decision.get("financial_risk_signal"),
        decision.get("financial_trajectory"),
        decision.get("decision_posture"),
        decision.get("decision_confidence"),
    ]

    decision_complete = all(
        normalize_text(value)
        for value in required_decision_values
    )

    if (
        feature_quality == QUALITY_COMPLETE
        and decision_complete
    ):
        return QUALITY_COMPLETE

    return QUALITY_PARTIAL


def derive_profile_quality_reason(
    decision: pd.Series,
    feature: pd.Series,
) -> str:
    feature_complete = (
        normalize_upper(feature.get("feature_data_quality"))
        == QUALITY_COMPLETE
    )
    decision_fields = [
        decision.get("financial_health"),
        decision.get("financial_risk_signal"),
        decision.get("financial_trajectory"),
        decision.get("decision_posture"),
        decision.get("decision_confidence"),
        decision.get("decision_reason_code"),
    ]
    decision_complete = all(normalize_text(value) for value in decision_fields)

    if feature_complete and decision_complete:
        return "COMPLETE"
    if not feature_complete and not decision_complete:
        return "PARTIAL_UPSTREAM_DATA"
    if not feature_complete:
        return "PARTIAL_FEATURE_DATA"
    return "PARTIAL_DECISION_DATA"


def build_profile_narrative(
    profile: dict,
    strengths: list[str],
    concerns: list[str],
) -> str:

    health = normalize_text(
        profile.get("financial_health")
    ).lower()

    risk = normalize_text(
        profile.get("financial_risk_signal")
    ).lower()

    trajectory = normalize_text(
        profile.get("financial_trajectory")
    ).lower()

    decision = normalize_text(
        profile.get("decision_posture")
    ).lower()

    peer = normalize_text(
        profile.get("peer_position")
    ).lower().replace("_", " ")

    archetype = normalize_text(
        profile.get("company_archetype")
    ).lower().replace("_", " ")

    parts = [
        (
            f"The company currently shows {health} financial health, "
            f"{risk} financial risk and a {trajectory} financial trajectory."
        ),
        (
            f"The current decision posture is {decision}, "
            f"with the company positioned in the {peer} "
            f"of the current peer universe."
        ),
        (
            f"The company is classified as a {archetype} profile."
        ),
    ]

    if strengths:
        parts.append(
            "Key strengths: "
            + "; ".join(strengths[:4])
            + "."
        )

    if concerns:
        parts.append(
            "Key considerations: "
            + "; ".join(concerns[:4])
            + "."
        )

    return " ".join(parts)


# =============================================================================
# HISTORICAL PROFILE
# =============================================================================

def build_history_summary(
    decisions: pd.DataFrame,
) -> dict:

    total = len(decisions)

    decision_counts = (
        decisions[
            "decision_posture"
        ]
        .map(normalize_upper)
        .value_counts()
        .to_dict()
    )

    trajectory_counts = (
        decisions[
            "financial_trajectory"
        ]
        .map(normalize_upper)
        .value_counts()
        .to_dict()
    )

    confidence_counts = (
        decisions[
            "decision_confidence"
        ]
        .map(normalize_upper)
        .value_counts()
        .to_dict()
    )

    ordered_decisions = [
        normalize_upper(value)
        for value in decisions[
            "decision_posture"
        ].tolist()
    ]

    ordered_trajectories = [
        normalize_upper(value)
        for value in decisions[
            "financial_trajectory"
        ].tolist()
    ]

    decision_changes = sum(
        1
        for previous, current in zip(
            ordered_decisions,
            ordered_decisions[1:],
        )
        if previous != current
    )

    trajectory_changes = sum(
        1
        for previous, current in zip(
            ordered_trajectories,
            ordered_trajectories[1:],
        )
        if previous != current
    )

    positive_ratio = (
        decision_counts.get("POSITIVE", 0) / total
        if total
        else None
    )

    attention_ratio = (
        (
            decision_counts.get("WATCH", 0)
            + decision_counts.get("CAUTION", 0)
        )
        / total
        if total
        else None
    )

    latest = latest_row(decisions)

    return {
        "historical_decision_rows": total,
        "positive_decision_rows": decision_counts.get(
            "POSITIVE", 0
        ),
        "neutral_decision_rows": decision_counts.get(
            "NEUTRAL", 0
        ),
        "watch_decision_rows": decision_counts.get(
            "WATCH", 0
        ),
        "caution_decision_rows": decision_counts.get(
            "CAUTION", 0
        ),
        "improving_trajectory_rows": trajectory_counts.get(
            "IMPROVING", 0
        ),
        "mixed_trajectory_rows": trajectory_counts.get(
            "MIXED", 0
        ),
        "deteriorating_trajectory_rows": trajectory_counts.get(
            "DETERIORATING", 0
        ),
        "high_confidence_rows": confidence_counts.get(
            "HIGH", 0
        ),
        "medium_confidence_rows": confidence_counts.get(
            "MEDIUM", 0
        ),
        "low_confidence_rows": confidence_counts.get(
            "LOW", 0
        ),
        "decision_change_count": decision_changes,
        "trajectory_change_count": trajectory_changes,
        "positive_decision_ratio": positive_ratio,
        "attention_decision_ratio": attention_ratio,
        "historical_first_fiscal_year": safe_int(
            decisions["fiscal_year"].iloc[0]
        ),
        "historical_last_fiscal_year": safe_int(
            latest.get("fiscal_year")
        ),
        "historical_last_quarter": normalize_upper(
            latest.get("quarter")
        ),
    }


# =============================================================================
# COMPANY PROFILE
# =============================================================================

def upstream_data_as_of_utc(
    features_root: Path,
    decisions_root: Path,
    cik: str,
) -> Optional[str]:
    paths = [
        features_root
        / f"cik={cik}"
        / "financial_features.parquet",
        decisions_root
        / "decision_signals"
        / "financial_decision_signals.parquet",
    ]
    timestamps = [
        pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        for path in paths
        if path.exists()
    ]
    return min(timestamps).isoformat() if timestamps else None

def build_no_data_company_profile(
    company_master: pd.DataFrame,
    cik: str,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    """Create an explicit company-grain NO_DATA profile without inference."""
    cik = normalize_cik(cik)
    company_name = cik
    if not company_master.empty:
        match = company_master[company_master["cik"] == cik]
        if not match.empty and normalize_text(match.iloc[0].get("company_name")):
            company_name = normalize_text(match.iloc[0].get("company_name"))

    now = utc_now_iso()
    profile = {
        "cik": cik,
        "company_name": company_name,
        "latest_fiscal_year": None,
        "latest_quarter": None,
        "profile_as_of_period": None,
        "financial_health": None,
        "financial_risk_signal": None,
        "financial_trajectory": None,
        "calibrated_overall_rank": None,
        "calibrated_display_score": None,
        "peer_position": "UNKNOWN",
        "peer_population_class": "UNKNOWN",
        "peer_sample_size": None,
        "decision_posture": None,
        "decision_priority": None,
        "decision_reason_code": None,
        "decision_confidence": None,
        "revenue_growth_signal": None,
        "profitability_signal": None,
        "growth_consistency_signal": None,
        "momentum_signal": None,
        "company_archetype": None,
        "primary_strengths": None,
        "primary_considerations": "NO_FINANCIAL_DATA_AVAILABLE",
        "profile_quality": QUALITY_PARTIAL,
        "profile_quality_reason": "NO_FINANCIAL_DATA_AVAILABLE",
        "profile_narrative": "Financial intelligence is not available for the current processing window.",
        "source_feature_version": None,
        "source_decision_version": None,
        "profile_version": PROFILE_VERSION,
        "profile_generated_at_utc": now,
        "profile_data_as_of_utc": None,
        "profile_age_days": None,
        "profile_freshness_status": "UNKNOWN",
    }
    for column in [
        "revenue_value", "operating_income_value", "net_income_value",
        "revenue_yoy_growth_pct", "revenue_growth_acceleration",
        "operating_margin", "net_margin", "ttm_revenue",
        "ttm_operating_margin", "ttm_net_margin", "revenue_growth_4q_avg",
        "operating_margin_4q_avg", "net_margin_4q_avg",
        "revenue_growth_volatility_4q", "operating_margin_volatility_4q",
        "net_margin_volatility_4q", "operating_margin_yoy_change",
        "net_margin_yoy_change",
    ]:
        profile[column] = None

    history = pd.DataFrame([{
        "cik": cik, "fiscal_year": None, "quarter": None,
        "decision_posture": None, "decision_priority": None,
        "decision_confidence": None, "financial_health": None,
        "financial_risk_signal": None, "financial_trajectory": None,
        "peer_position": "UNKNOWN", "calibrated_overall_rank": None,
        "calibrated_display_score": None, "revenue_value": None,
        "revenue_yoy_growth_pct": None, "operating_margin": None,
        "net_margin": None, "ttm_revenue": None,
        "ttm_operating_margin": None, "ttm_net_margin": None,
        "feature_data_quality": "NO_DATA", "decision_version": None,
        "profile_version": PROFILE_VERSION,
    }])
    signals = pd.DataFrame(columns=[
        "cik", "fiscal_year", "quarter", "signal_type", "signal_code",
        "signal_text", "signal_direction", "signal_priority", "profile_version",
    ])
    return profile, history, signals


def build_company_profile(
    decisions: pd.DataFrame,
    features: pd.DataFrame,
    company_master: pd.DataFrame,
    features_root: Path,
    decisions_root: Path,
    cik: str,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:

    latest_decision = latest_row(decisions)
    latest_feature = latest_row(features)

    strengths = build_structured_strengths(
        latest_decision,
        latest_feature,
    )

    concerns = build_structured_concerns(
        latest_decision,
        latest_feature,
    )

    archetype = derive_company_archetype(
        latest_decision,
        latest_feature,
    )

    quality = derive_profile_quality(
        latest_decision,
        latest_feature,
    )

    company_name = normalize_text(cik)

    if not company_master.empty:
        match = company_master[
            company_master["cik"] == normalize_cik(cik)
        ]

        if not match.empty:
            company_name_value = match.iloc[0].get(
                "company_name"
            )

            if normalize_text(company_name_value):
                company_name = normalize_text(
                    company_name_value
                )

    numeric = extract_numeric_profile(
        latest_feature
    )

    history = build_history_summary(
        decisions
    )

    profile = {
        "cik": normalize_cik(cik),
        "company_name": company_name,

        "latest_fiscal_year": safe_int(
            latest_decision.get("fiscal_year")
        ),
        "latest_quarter": normalize_upper(
            latest_decision.get("quarter")
        ),
        "profile_as_of_period": (
            f"{safe_int(latest_decision.get('fiscal_year'))}-"
            f"{normalize_upper(latest_decision.get('quarter'))}"
        ),

        "financial_health": normalize_upper(
            latest_decision.get("financial_health")
        ),
        "financial_risk_signal": normalize_upper(
            latest_decision.get("financial_risk_signal")
        ),
        "financial_trajectory": normalize_upper(
            latest_decision.get("financial_trajectory")
        ),

        "calibrated_overall_rank": safe_int(
            latest_decision.get(
                "calibrated_overall_rank"
            )
        ),
        "calibrated_display_score": safe_float(
            latest_decision.get(
                "calibrated_display_score"
            )
        ),
        "peer_position": normalize_upper(
            latest_decision.get("peer_position")
        ),
        "peer_population_class": normalize_upper(
            latest_decision.get(
                "peer_population_class"
            )
        ),
        "peer_sample_size": safe_int(
            latest_decision.get(
                "peer_sample_size"
            )
        ),

        "decision_posture": normalize_upper(
            latest_decision.get("decision_posture")
        ),
        "decision_priority": DECISION_PRIORITY_MAP.get(
            normalize_upper(
                latest_decision.get(
                    "decision_posture"
                )
            )
        ),
        "decision_reason_code": normalize_upper(
            latest_decision.get(
                "decision_reason_code"
            )
        ),
        "decision_confidence": normalize_upper(
            latest_decision.get(
                "decision_confidence"
            )
        ),

        "revenue_growth_signal": normalize_upper(
            latest_decision.get(
                "revenue_growth_signal"
            )
        ),
        "profitability_signal": normalize_upper(
            latest_decision.get(
                "profitability_signal"
            )
        ),
        "growth_consistency_signal": normalize_upper(
            latest_decision.get(
                "growth_consistency_signal"
            )
        ),
        "momentum_signal": normalize_upper(
            latest_decision.get(
                "momentum_signal"
            )
        ),

        "company_archetype": archetype,

        "primary_strengths": unique_join(
            strengths
        ),
        "primary_considerations": unique_join(
            concerns
        ),

        "profile_quality": quality,
        "profile_quality_reason": derive_profile_quality_reason(
            latest_decision,
            latest_feature,
        ),

        "profile_narrative": build_profile_narrative(
            {
                "financial_health": latest_decision.get(
                    "financial_health"
                ),
                "financial_risk_signal": latest_decision.get(
                    "financial_risk_signal"
                ),
                "financial_trajectory": latest_decision.get(
                    "financial_trajectory"
                ),
                "decision_posture": latest_decision.get(
                    "decision_posture"
                ),
                "peer_position": latest_decision.get(
                    "peer_position"
                ),
                "company_archetype": archetype,
            },
            strengths,
            concerns,
        ),

        "source_feature_version": normalize_text(
            latest_feature.get("feature_version")
        ),
        "source_decision_version": normalize_text(
            latest_decision.get("decision_version")
        ),

        "profile_version": PROFILE_VERSION,
        "profile_generated_at_utc": utc_now_iso(),
        "profile_data_as_of_utc": upstream_data_as_of_utc(
            features_root,
            decisions_root,
            normalize_cik(cik),
        ),
    }

    profile.update(numeric)
    profile.update(history)

    profile_generated = pd.Timestamp(
        profile["profile_generated_at_utc"],
        tz="UTC",
    )
    profile_data_as_of = (
        pd.Timestamp(profile["profile_data_as_of_utc"], tz="UTC")
        if profile["profile_data_as_of_utc"]
        else None
    )
    profile_age_days = (
        max(
            0.0,
            (
                profile_generated - profile_data_as_of
            ).total_seconds() / 86400.0,
        )
        if profile_data_as_of is not None
        else None
    )
    profile["profile_age_days"] = profile_age_days
    profile["profile_freshness_status"] = (
        "UNKNOWN"
        if profile_age_days is None
        else (
            "FRESH"
            if profile_age_days <= 30.0
            else (
                "STALE"
                if profile_age_days <= 90.0
                else "VERY_STALE"
            )
        )
    )

    # -------------------------------------------------------------------------
    # Historical profile rows
    # -------------------------------------------------------------------------

    history_rows = []

    feature_index = features.set_index(
        ["fiscal_year", "quarter"],
        drop=False,
    )

    for _, decision in decisions.iterrows():

        key = (
            safe_int(decision.get("fiscal_year")),
            normalize_upper(decision.get("quarter")),
        )

        feature = (
            feature_index.loc[key]
            if key in feature_index.index
            else pd.Series(dtype=object)
        )

        history_rows.append(
            {
                "cik": normalize_cik(cik),
                "fiscal_year": key[0],
                "quarter": key[1],
                "decision_posture": normalize_upper(
                    decision.get("decision_posture")
                ),
                "decision_priority": DECISION_PRIORITY_MAP.get(
                    normalize_upper(
                        decision.get(
                            "decision_posture"
                        )
                    )
                ),
                "decision_confidence": normalize_upper(
                    decision.get(
                        "decision_confidence"
                    )
                ),
                "financial_health": normalize_upper(
                    decision.get(
                        "financial_health"
                    )
                ),
                "financial_risk_signal": normalize_upper(
                    decision.get(
                        "financial_risk_signal"
                    )
                ),
                "financial_trajectory": normalize_upper(
                    decision.get(
                        "financial_trajectory"
                    )
                ),
                "peer_position": normalize_upper(
                    decision.get(
                        "peer_position"
                    )
                ),
                "calibrated_overall_rank": safe_int(
                    decision.get(
                        "calibrated_overall_rank"
                    )
                ),
                "calibrated_display_score": safe_float(
                    decision.get(
                        "calibrated_display_score"
                    )
                ),
                "revenue_value": safe_float(
                    feature.get("revenue_value")
                ),
                "revenue_yoy_growth_pct": safe_float(
                    feature.get(
                        "revenue_yoy_growth_pct"
                    )
                ),
                "operating_margin": safe_float(
                    feature.get(
                        "operating_margin"
                    )
                ),
                "net_margin": safe_float(
                    feature.get(
                        "net_margin"
                    )
                ),
                "ttm_revenue": safe_float(
                    feature.get(
                        "ttm_revenue"
                    )
                ),
                "ttm_operating_margin": safe_float(
                    feature.get(
                        "ttm_operating_margin"
                    )
                ),
                "ttm_net_margin": safe_float(
                    feature.get(
                        "ttm_net_margin"
                    )
                ),
                "feature_data_quality": normalize_upper(
                    feature.get(
                        "feature_data_quality"
                    )
                ),
                "decision_version": normalize_text(
                    decision.get(
                        "decision_version"
                    )
                ),
                "profile_version": PROFILE_VERSION,
            }
        )

    history_df = pd.DataFrame(history_rows)

    # -------------------------------------------------------------------------
    # Structured signal rows
    # -------------------------------------------------------------------------

    signal_rows = []

    for strength in strengths:
        signal_rows.append(
            {
                "cik": normalize_cik(cik),
                "fiscal_year": safe_int(
                    latest_decision.get("fiscal_year")
                ),
                "quarter": normalize_upper(
                    latest_decision.get("quarter")
                ),
                "signal_type": "STRENGTH",
                "signal_code": strength.upper()
                .replace(" ", "_"),
                "signal_text": strength,
                "signal_direction": "POSITIVE",
                "signal_priority": 1,
                "profile_version": PROFILE_VERSION,
            }
        )

    for concern in concerns:
        signal_rows.append(
            {
                "cik": normalize_cik(cik),
                "fiscal_year": safe_int(
                    latest_decision.get("fiscal_year")
                ),
                "quarter": normalize_upper(
                    latest_decision.get("quarter")
                ),
                "signal_type": "CONCERN",
                "signal_code": concern.upper()
                .replace(" ", "_"),
                "signal_text": concern,
                "signal_direction": "NEGATIVE",
                "signal_priority": 1,
                "profile_version": PROFILE_VERSION,
            }
        )

    signal_df = pd.DataFrame(signal_rows)

    return (
        profile,
        history_df,
        signal_df,
    )


# =============================================================================
# VALIDATION
# =============================================================================

def validate_profile(
    profile_df: pd.DataFrame,
) -> None:

    required = [
        "cik",
        "company_name",
        "latest_fiscal_year",
        "latest_quarter",
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "peer_population_class",
        "decision_posture",
        "decision_priority",
        "decision_confidence",
        "decision_reason_code",
        "company_archetype",
        "primary_strengths",
        "primary_considerations",
        "profile_quality",
        "profile_version",
    ]

    missing = [
        column
        for column in required
        if column not in profile_df.columns
    ]

    if missing:
        raise ValueError(
            f"Company profile missing required columns: {missing}"
        )

    if profile_df["cik"].duplicated().any():
        raise ValueError(
            "Step 16 profile grain violation: "
            "expected exactly one profile row per company."
        )

    health = set(
        profile_df["financial_health"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = health - VALID_HEALTH

    # region agent log
    try:
        import json
        import time
        payload = {
            "sessionId": "63fc53",
            "runId": "post-fix",
            "hypothesisId": "E",
            "location": "step16_company_intelligence_profile.py:validate_profile",
            "message": "profile health vocabulary",
            "data": {
                "health": sorted(health),
                "invalid": sorted(invalid),
            },
            "timestamp": int(time.time() * 1000),
        }
        with open(Path(__file__).resolve().parents[2] / "debug-63fc53.log", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion

    if invalid:
        raise ValueError(
            f"Invalid financial health values: {sorted(invalid)}"
        )

    risk = set(
        profile_df["financial_risk_signal"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = risk - VALID_RISK

    if invalid:
        raise ValueError(
            f"Invalid financial risk values: {sorted(invalid)}"
        )

    trajectory = set(
        profile_df["financial_trajectory"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = trajectory - VALID_TRAJECTORY

    if invalid:
        raise ValueError(
            f"Invalid financial trajectory values: {sorted(invalid)}"
        )

    decisions = set(
        profile_df["decision_posture"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = decisions - VALID_DECISIONS

    if invalid:
        raise ValueError(
            f"Invalid decision posture values: {sorted(invalid)}"
        )

    confidence = set(
        profile_df["decision_confidence"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = confidence - VALID_CONFIDENCE

    if invalid:
        raise ValueError(
            f"Invalid decision confidence values: {sorted(invalid)}"
        )

    peers = set(
        profile_df["peer_position"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = peers - VALID_PEER_POSITION

    if invalid:
        raise ValueError(
            f"Invalid peer positions: {sorted(invalid)}"
        )

    populations = set(
        profile_df["peer_population_class"]
        .dropna()
        .map(normalize_upper)
    )

    invalid = populations - VALID_PEER_POPULATION

    if invalid:
        raise ValueError(
            f"Invalid peer population classes: {sorted(invalid)}"
        )

    for _, row in profile_df.iterrows():

        decision = normalize_upper(
            row["decision_posture"]
        )

        priority = safe_int(
            row["decision_priority"]
        )

        expected_priority = DECISION_PRIORITY_MAP.get(
            decision
        )

        if priority != expected_priority:
            raise ValueError(
                f"Decision priority mismatch for {row['cik']}: "
                f"{decision} -> {priority}; "
                f"expected {expected_priority}"
            )

        population = normalize_upper(
            row["peer_population_class"]
        )

        confidence = normalize_upper(
            row["decision_confidence"]
        )

        if (
            population == "LIMITED"
            and confidence == "HIGH"
        ):
            raise ValueError(
                f"Step 16 confidence contract violation for "
                f"{row['cik']}: LIMITED peer population cannot "
                f"have HIGH confidence."
            )


def validate_history(
    history_df: pd.DataFrame,
) -> None:

    if history_df.empty:
        raise ValueError(
            "Step 16 historical intelligence is empty."
        )

    duplicate_mask = history_df.duplicated(
        subset=[
            "cik",
            "fiscal_year",
            "quarter",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            "Step 16 historical profile grain violation."
        )


def validate_signals(
    signals_df: pd.DataFrame,
) -> None:

    if signals_df.empty:
        return

    allowed = {
        "STRENGTH",
        "CONCERN",
    }

    invalid = set(
        signals_df["signal_type"]
        .dropna()
        .map(normalize_upper)
    ) - allowed

    if invalid:
        raise ValueError(
            f"Invalid Step 16 signal types: {sorted(invalid)}"
        )


# =============================================================================
# LINEAGE
# =============================================================================

def build_lineage() -> pd.DataFrame:

    lineage = [
        (
            "company_name",
            "Optional company master.cik/company_name",
            "Company master lookup with CIK fallback",
        ),
        (
            "latest financial metrics",
            "Step 12 financial_features",
            "Latest fiscal quarter selection",
        ),
        (
            "financial_health",
            "Step 15 decision_signals.financial_health",
            "Pass-through of authoritative Step 15 interpretation",
        ),
        (
            "financial_risk_signal",
            "Step 15 decision_signals.financial_risk_signal",
            "Pass-through of authoritative Step 15 interpretation",
        ),
        (
            "financial_trajectory",
            "Step 15 decision_signals.financial_trajectory",
            "Pass-through of authoritative Step 15 interpretation",
        ),
        (
            "peer_position",
            "Step 15 decision_signals.peer_position",
            "Pass-through of calibrated peer interpretation",
        ),
        (
            "decision_posture",
            "Step 15 decision_signals.decision_posture",
            "Pass-through of deterministic Step 15 decision",
        ),
        (
            "decision_priority",
            "Step 15 decision_posture",
            "Deterministic priority mapping; no score created",
        ),
        (
            "decision_reason_code",
            "Step 15 decision_signals.decision_reason_code",
            "Pass-through of machine-readable Step 15 reason",
        ),
        (
            "decision_confidence",
            "Step 15 decision_signals.decision_confidence",
            "Pass-through of conservative Step 15 confidence",
        ),
        (
            "company_archetype",
            "Step 15 signals + deterministic Step 16 rules",
            "Structured company profile classification",
        ),
        (
            "primary_strengths",
            "Step 15 signals + Step 12 latest features",
            "Deterministic signal interpretation",
        ),
        (
            "primary_considerations",
            "Step 15 signals + Step 12 latest features",
            "Deterministic risk/trend interpretation",
        ),
        (
            "profile_narrative",
            "Step 16 structured profile fields",
            "Deterministic textual composition; no LLM",
        ),
        (
            "historical_decision_statistics",
            "Step 15 decision_signals",
            "Historical aggregation across company quarters",
        ),
    ]

    return pd.DataFrame(
        [
            {
                "output_field": output,
                "source": source,
                "transformation": transformation,
                "profile_version": PROFILE_VERSION,
                "new_financial_score_created": False,
            }
            for output, source, transformation in lineage
        ]
    )


# =============================================================================
# SUMMARY
# =============================================================================

def build_summary(
    profile_df: pd.DataFrame,
    history_df: pd.DataFrame,
    signal_df: pd.DataFrame,
) -> pd.DataFrame:

    rows = [
        {
            "profile_version": PROFILE_VERSION,
            "total_companies": int(
                profile_df["cik"].nunique()
            ),
            "total_historical_rows": int(
                len(history_df)
            ),
            "total_structured_signals": int(
                len(signal_df)
            ),
            "positive_companies": int(
                (
                    profile_df["decision_posture"]
                    .map(normalize_upper)
                    == "POSITIVE"
                ).sum()
            ),
            "neutral_companies": int(
                (
                    profile_df["decision_posture"]
                    .map(normalize_upper)
                    == "NEUTRAL"
                ).sum()
            ),
            "watch_companies": int(
                (
                    profile_df["decision_posture"]
                    .map(normalize_upper)
                    == "WATCH"
                ).sum()
            ),
            "caution_companies": int(
                (
                    profile_df["decision_posture"]
                    .map(normalize_upper)
                    == "CAUTION"
                ).sum()
            ),
            "improving_companies": int(
                (
                    profile_df["financial_trajectory"]
                    .map(normalize_upper)
                    == "IMPROVING"
                ).sum()
            ),
            "mixed_companies": int(
                (
                    profile_df["financial_trajectory"]
                    .map(normalize_upper)
                    == "MIXED"
                ).sum()
            ),
            "deteriorating_companies": int(
                (
                    profile_df["financial_trajectory"]
                    .map(normalize_upper)
                    == "DETERIORATING"
                ).sum()
            ),
            "complete_profiles": int(
                (
                    profile_df["profile_quality"]
                    .map(normalize_upper)
                    == QUALITY_COMPLETE
                ).sum()
            ),
            "partial_profiles": int(
                (
                    profile_df["profile_quality"]
                    .map(normalize_upper)
                    == QUALITY_PARTIAL
                ).sum()
            ),
            "limited_peer_companies": int(
                (
                    profile_df["peer_population_class"]
                    .map(normalize_upper)
                    == "LIMITED"
                ).sum()
            ),
            "new_financial_score_created": False,
            "generated_at_utc": utc_now_iso(),
        }
    ]

    return pd.DataFrame(rows)


# =============================================================================
# OUTPUT PATHS
# =============================================================================

def output_paths(
    output_root: Path,
) -> dict[str, Path]:

    return {
        "profile": (
            output_root
            / "company_intelligence_profile"
            / "company_intelligence_profile.parquet"
        ),
        "history": (
            output_root
            / "company_intelligence_history"
            / "company_intelligence_history.parquet"
        ),
        "signals": (
            output_root
            / "company_intelligence_signals"
            / "company_intelligence_signals.parquet"
        ),
        "summary": (
            output_root
            / "company_intelligence_summary"
            / "company_intelligence_summary.parquet"
        ),
        "lineage": (
            output_root
            / "company_intelligence_lineage"
            / "company_intelligence_lineage.parquet"
        ),
    }


# =============================================================================
# MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Step 16 - Company Intelligence Profile"
    )

    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES,
    )

    parser.add_argument(
        "--decisions",
        type=Path,
        default=DEFAULT_DECISIONS,
    )

    parser.add_argument(
        "--company-master",
        type=Path,
        default=DEFAULT_COMPANY_MASTER,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    return parser.parse_args()


def main() -> None:

    args = parse_args()

    section(
        "STEP 16 - COMPANY INTELLIGENCE PROFILE"
    )

    log(
        f"Features            : {args.features}"
    )
    log(
        f"Decision Intelligence: {args.decisions}"
    )
    log(
        f"Company Master      : {args.company_master}"
    )
    log(
        f"Output              : {args.output}"
    )

    log(
        f"Profile version     : {PROFILE_VERSION}"
    )

    log(
        f"Supported features  : "
        f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    log(
        f"Supported decisions : "
        f"{sorted(SUPPORTED_DECISION_VERSIONS)}"
    )

    log(
        "Contract            : NO NEW FINANCIAL SCORE"
    )

    log(
        "Contract            : ONE LATEST PROFILE PER COMPANY"
    )

    log(
        "Contract            : DETERMINISTIC INTELLIGENCE"
    )

    log(
        "Contract            : STEP 15 DECISION IS AUTHORITATIVE"
    )

    log("")

    company_master = load_company_master(
        args.company_master
    )

    companies = discover_companies(
        args.decisions
    )

    log(
        f"Companies discovered: {len(companies)}"
    )

    all_profiles = []
    all_history = []
    all_signals = []

    # Load the shared Step 15 artifact once. Company scope is controlled by
    # the registry; this avoids re-reading the full decision table per CIK.
    decision_path = (
        args.decisions
        / "decision_signals"
        / "financial_decision_signals.parquet"
    )
    decisions_all = read_parquet(
        decision_path,
        "Step 15 decision signals",
    )
    decisions_all["cik"] = decisions_all["cik"].map(normalize_cik)

    failures = []

    for cik in companies:

        section(
            f"PROCESSING CIK: {cik}"
        )

        try:

            decisions = decisions_all[
                decisions_all["cik"] == normalize_cik(cik)
            ].copy()

            feature_path = (
                args.features
                / f"cik={normalize_cik(cik)}"
                / "financial_features.parquet"
            )
            if decisions.empty or not feature_path.exists():
                profile, history, signals = build_no_data_company_profile(
                    company_master=company_master,
                    cik=cik,
                )
                features = pd.DataFrame()
            else:
                features = load_features(args.features, cik)
                profile, history, signals = build_company_profile(
                    decisions=decisions,
                    features=features,
                    company_master=company_master,
                    features_root=args.features,
                    decisions_root=args.decisions,
                    cik=cik,
                )

            all_profiles.append(
                profile
            )

            all_history.append(
                history
            )

            if not signals.empty:
                all_signals.append(
                    signals
                )

            log(
                f"Decision rows      : {len(decisions)}"
            )

            log(
                f"Feature rows       : {len(features)}"
            )

            log(
                f"Latest quarter     : "
                f"{profile['latest_fiscal_year']} "
                f"{profile['latest_quarter']}"
            )

            log(
                f"Financial health   : "
                f"{profile['financial_health']}"
            )

            log(
                f"Financial risk     : "
                f"{profile['financial_risk_signal']}"
            )

            log(
                f"Trajectory         : "
                f"{profile['financial_trajectory']}"
            )

            log(
                f"Decision           : "
                f"{profile['decision_posture']}"
            )

            log(
                f"Decision priority  : "
                f"{profile['decision_priority']}"
            )

            log(
                f"Peer position      : "
                f"{profile['peer_position']}"
            )

            log(
                f"Peer population    : "
                f"{profile['peer_population_class']}"
            )

            log(
                f"Archetype          : "
                f"{profile['company_archetype']}"
            )

            log(
                f"Profile quality    : "
                f"{profile['profile_quality']}"
            )

            log(
                f"Strengths          : "
                f"{profile['primary_strengths']}"
            )

            log(
                f"Considerations     : "
                f"{profile['primary_considerations']}"
            )

        except Exception as exc:

            failures.append(
                (cik, str(exc))
            )

            log(
                f"ERROR processing {cik}: {exc}"
            )

            traceback.print_exc()

    if failures:

        section(
            "STEP 16 FAILED"
        )

        for cik, error in failures:
            log(
                f"{cik}: {error}"
            )

        raise RuntimeError(
            f"Step 16 failed for {len(failures)} company/companies."
        )

    profile_df = pd.DataFrame(
        all_profiles
    )

    history_df = pd.concat(
        all_history,
        ignore_index=True,
    )

    if all_signals:
        signal_df = pd.concat(
            all_signals,
            ignore_index=True,
        )
    else:
        signal_df = pd.DataFrame(
            columns=[
                "cik",
                "fiscal_year",
                "quarter",
                "signal_type",
                "signal_code",
                "signal_text",
                "signal_direction",
                "signal_priority",
                "profile_version",
            ]
        )

    profile_df = profile_df.sort_values(
        ["cik"]
    ).reset_index(drop=True)

    history_df["quarter_num"] = (
        history_df["quarter"].map(quarter_num)
    )

    history_df = history_df.sort_values(
        [
            "cik",
            "fiscal_year",
            "quarter_num",
        ]
    ).drop(
        columns=["quarter_num"]
    ).reset_index(drop=True)

    signal_df = signal_df.sort_values(
        [
            "cik",
            "signal_type",
            "signal_priority",
            "signal_code",
        ]
    ).reset_index(drop=True)

    validate_profile(
        profile_df
    )

    validate_history(
        history_df
    )

    validate_signals(
        signal_df
    )

    summary_df = build_summary(
        profile_df=profile_df,
        history_df=history_df,
        signal_df=signal_df,
    )

    lineage_df = build_lineage()

    paths = output_paths(
        args.output
    )

    write_parquet(
        profile_df,
        paths["profile"],
    )

    write_parquet(
        history_df,
        paths["history"],
    )

    write_parquet(
        signal_df,
        paths["signals"],
    )

    write_parquet(
        summary_df,
        paths["summary"],
    )

    write_parquet(
        lineage_df,
        paths["lineage"],
    )

    # -------------------------------------------------------------------------
    # Final terminal summary
    # -------------------------------------------------------------------------

    section(
        "STEP 16 SUMMARY"
    )

    log(
        f"Total companies       : "
        f"{len(profile_df)}"
    )

    log(
        f"Historical rows       : "
        f"{len(history_df)}"
    )

    log(
        f"Structured signals    : "
        f"{len(signal_df)}"
    )

    log("")

    log(
        "Latest decision posture:"
    )

    log(
        profile_df[
            "decision_posture"
        ]
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")

    log(
        "Latest trajectory:"
    )

    log(
        profile_df[
            "financial_trajectory"
        ]
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")

    log(
        "Profile quality:"
    )

    log(
        profile_df[
            "profile_quality"
        ]
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")

    section(
        "COMPANY INTELLIGENCE PROFILE"
    )

    display_columns = [
        "cik",
        "company_name",
        "latest_fiscal_year",
        "latest_quarter",
        "financial_health",
        "financial_risk_signal",
        "financial_trajectory",
        "peer_position",
        "decision_confidence",
        "decision_posture",
        "decision_priority",
        "company_archetype",
        "profile_quality",
    ]

    existing = [
        column
        for column in display_columns
        if column in profile_df.columns
    ]

    log(
        profile_df[
            existing
        ].to_string(index=False)
    )

    log("")

    section(
        "OUTPUTS"
    )

    log(
        f"Company profile     : "
        f"{paths['profile']}"
    )

    log(
        f"Company history     : "
        f"{paths['history']}"
    )

    log(
        f"Company signals     : "
        f"{paths['signals']}"
    )

    log(
        f"Company summary     : "
        f"{paths['summary']}"
    )

    log(
        f"Company lineage     : "
        f"{paths['lineage']}"
    )

    log("")

    section(
        "STEP 16 COMPLETE"
    )

    log(
        f"Company Intelligence version: "
        f"{PROFILE_VERSION}"
    )

    log(
        "No new financial score was created."
    )

    log(
        "Step 15 decision posture remained authoritative."
    )


if __name__ == "__main__":
    main()
