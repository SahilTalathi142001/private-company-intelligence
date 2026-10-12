"""
===============================================================================
STEP 14 - FINANCIAL RANKING
===============================================================================

Purpose
-------
Rank companies using the validated Step 13 Financial Intelligence layer.

Upstream contracts
------------------
Step 12.1:
    Financial feature quality gate must PASS.

Step 13:
    Intelligence version must be supported.

Step 13.1:
    Financial intelligence quality gate must PASS.

Ranking philosophy
------------------
The ranking must NOT simply copy financial_score.

Instead it evaluates independent dimensions:

    1. Financial strength
    2. Growth
    3. Profitability
    4. Momentum
    5. Consistency
    6. Risk

The final ranking score is a weighted combination of these dimensions.

Output
------
data/gold/financial_ranking/
    company_financial_ranking/
        company_financial_ranking.parquet

    company_peer_comparison/
        company_peer_comparison.parquet

    company_financial_trends/
        company_financial_trends.parquet

    financial_watchlist/
        financial_watchlist.parquet

    financial_ranking_summary/
        financial_ranking_summary.parquet

Version
-------
Ranking version: 1.0.0
Supported intelligence versions: 1.0.0
Supported feature versions: 1.2.0
"""

from __future__ import annotations

import shutil
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FEATURES_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_features"
)

FEATURE_VALIDATION_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financials_feature_validation"
)

INTELLIGENCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence"
)

INTELLIGENCE_VALIDATION_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_intelligence_validation"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "financial_ranking"
)

RANKING_DIR = OUTPUT_DIR / "company_financial_ranking"
PEER_DIR = OUTPUT_DIR / "company_peer_comparison"
TREND_DIR = OUTPUT_DIR / "company_financial_trends"
WATCHLIST_DIR = OUTPUT_DIR / "financial_watchlist"
SUMMARY_DIR = OUTPUT_DIR / "financial_ranking_summary"


RANKING_VERSION = "1.0.0"

SUPPORTED_INTELLIGENCE_VERSIONS = {
    "1.0.0",
}

SUPPORTED_FEATURE_VERSIONS = {
    "1.2.0",
}


# =============================================================================
# RANKING WEIGHTS
# =============================================================================

# IMPORTANT:
# These weights are explicit and versioned.
#
# They do NOT simply reproduce financial_score.
#
# Total = 100%.

RANKING_WEIGHTS = {
    "financial_strength": 0.20,
    "growth": 0.20,
    "profitability": 0.20,
    "momentum": 0.15,
    "consistency": 0.15,
    "risk": 0.10,
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


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def ensure_columns(
    df: pd.DataFrame,
    required: List[str],
    dataset_name: str,
) -> None:

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
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

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        path,
        index=False,
    )


def safe_numeric(
    series: pd.Series,
) -> pd.Series:

    return pd.to_numeric(
        series,
        errors="coerce",
    )


def clamp(
    value: float,
    low: float = 0.0,
    high: float = 100.0,
) -> float:

    if pd.isna(value):
        return np.nan

    return max(
        low,
        min(high, float(value)),
    )


# =============================================================================
# INPUT PATHS
# =============================================================================

def intelligence_signal_path() -> Path:
    return (
        INTELLIGENCE_DIR
        / "financial_signals"
        / "financial_signals.parquet"
    )


def intelligence_snapshot_path() -> Path:
    return (
        INTELLIGENCE_DIR
        / "company_financial_snapshot"
        / "company_financial_snapshot.parquet"
    )


def intelligence_summary_path() -> Path:
    return (
        INTELLIGENCE_DIR
        / "financial_intelligence_summary"
        / "financial_intelligence_summary.parquet"
    )


def intelligence_validation_summary_path() -> Path:
    return (
        INTELLIGENCE_VALIDATION_DIR
        / "intelligence_validation_summary"
        / "intelligence_validation_summary.parquet"
    )


def intelligence_company_validation_path() -> Path:
    return (
        INTELLIGENCE_VALIDATION_DIR
        / "company_intelligence_summary"
        / "company_intelligence_summary.parquet"
    )


def feature_validation_summary_path() -> Path:
    return (
        FEATURE_VALIDATION_DIR
        / "feature_validation_summary"
        / "feature_validation_summary.parquet"
    )


# =============================================================================
# VERSION / UPSTREAM GATES
# =============================================================================

def validate_weights() -> None:

    total = sum(
        RANKING_WEIGHTS.values()
    )

    if not np.isclose(
        total,
        1.0,
        atol=1e-12,
    ):
        raise ValueError(
            f"Ranking weights must sum to 1.0. "
            f"Current total={total}"
        )


def load_step12_1_gate() -> pd.DataFrame:

    path = feature_validation_summary_path()

    df = read_parquet(
        path,
        "Step 12.1 validation summary",
    )

    status_column = (
        "audit_status"
        if "audit_status" in df.columns
        else "validation_status"
    )
    ensure_columns(
        df,
        [status_column],
        "Step 12.1 validation summary",
    )

    statuses = (
        df[status_column]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    if not statuses:
        raise ValueError(
            "Step 12.1 validation summary contains no audit status."
        )

    if any(
        status != "PASS"
        for status in statuses
    ):
        raise RuntimeError(
            "Step 12.1 feature-quality gate failed."
        )

    return df


def load_step13_1_gate() -> pd.DataFrame:

    path = intelligence_validation_summary_path()

    df = read_parquet(
        path,
        "Step 13.1 validation summary",
    )

    ensure_columns(
        df,
        [
            "audit_status",
        ],
        "Step 13.1 validation summary",
    )

    statuses = (
        df["audit_status"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    if not statuses:
        raise ValueError(
            "Step 13.1 validation summary contains no audit status."
        )

    # PASS and WARNING are acceptable when there are no failed rows
    # or fatal exceptions. Step 13.1 uses WARNING for non-fatal fixture
    # row-count mismatches (for example, running one company instead
    # of the full multi-company fixture).
    if any(
        status not in {"PASS", "WARNING"}
        for status in statuses
    ):
        raise RuntimeError(
            "Step 13.1 intelligence-quality gate failed: "
            f"unsupported audit_status values={statuses}"
        )

    for column in ("fail_rows", "fatal_exceptions"):
        if column in df.columns:
            counts = pd.to_numeric(df[column], errors="coerce")
            if counts.isna().any() or (counts > 0).any():
                raise RuntimeError(
                    "Step 13.1 intelligence-quality gate failed: "
                    f"{column} is missing or greater than zero."
                )

    return df


# =============================================================================
# COMPANY DISCOVERY
# =============================================================================

def discover_companies() -> List[str]:

    companies = set()

    if INTELLIGENCE_DIR.exists():

        for path in INTELLIGENCE_DIR.glob(
            "financial_signals/financial_signals.parquet"
        ):
            pass

    # Prefer Step 13 snapshot.
    snapshot_path = intelligence_snapshot_path()

    if snapshot_path.exists():

        snapshot = pd.read_parquet(
            snapshot_path
        )

        if "cik" in snapshot.columns:

            companies.update(
                snapshot["cik"]
                .dropna()
                .astype(str)
                .str.zfill(10)
                .tolist()
            )

    # Fallback to feature partitions.
    if not companies:

        for path in FEATURES_DIR.glob(
            "cik=*/financial_features.parquet"
        ):

            cik = path.parent.name

            if cik.startswith("cik="):

                companies.add(
                    cik.split("=", 1)[1]
                )

    return sorted(companies)


# =============================================================================
# NORMALIZATION
# =============================================================================

def normalize_signal_data(
    df: pd.DataFrame,
) -> pd.DataFrame:

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "financial_score",
        "financial_health",
        "financial_risk_signal",
        "financial_signal_confidence",
        "revenue_growth_signal",
        "profitability_signal",
        "growth_consistency_signal",
        "momentum_signal",
    ]

    ensure_columns(
        df,
        required,
        "Financial signals",
    )

    df = df.copy()

    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.zfill(10)
    )

    df["fiscal_year"] = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    )

    df["quarter"] = (
        df["quarter"]
        .astype(str)
        .str.upper()
    )

    df["financial_score"] = safe_numeric(
        df["financial_score"]
    )

    quarter_order = {
        "Q1": 1,
        "Q2": 2,
        "Q3": 3,
        "Q4": 4,
    }

    df["_quarter_order"] = (
        df["quarter"]
        .map(quarter_order)
    )

    df = df.sort_values(
        [
            "cik",
            "fiscal_year",
            "_quarter_order",
        ]
    ).reset_index(drop=True)

    return df


# =============================================================================
# LATEST SNAPSHOT
# =============================================================================

def validate_snapshot(
    snapshot: pd.DataFrame,
) -> pd.DataFrame:

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "financial_score",
        "financial_health",
        "financial_risk_signal",
        "financial_signal_confidence",
    ]

    ensure_columns(
        snapshot,
        required,
        "Company financial snapshot",
    )

    snapshot = snapshot.copy()

    snapshot["cik"] = (
        snapshot["cik"]
        .astype(str)
        .str.zfill(10)
    )

    quarter_order = {
        "Q1": 1,
        "Q2": 2,
        "Q3": 3,
        "Q4": 4,
    }

    snapshot["_quarter_order"] = (
        snapshot["quarter"]
        .astype(str)
        .str.upper()
        .map(quarter_order)
    )

    snapshot["fiscal_year"] = pd.to_numeric(
        snapshot["fiscal_year"],
        errors="coerce",
    )

    snapshot = snapshot.sort_values(
        [
            "cik",
            "fiscal_year",
            "_quarter_order",
        ]
    )

    duplicates = (
        snapshot.groupby(
            ["cik"],
            dropna=False,
        )
        .size()
    )

    if (duplicates > 1).any():

        # Step 13 contract normally gives one snapshot/company.
        # If multiple rows exist, select the latest only if they
        # represent the same company and are unambiguous.

        latest = (
            snapshot
            .sort_values(
                [
                    "cik",
                    "fiscal_year",
                    "_quarter_order",
                ]
            )
            .groupby(
                "cik",
                as_index=False,
            )
            .tail(1)
        )

        if latest["cik"].nunique() != snapshot["cik"].nunique():
            raise ValueError(
                "Unable to establish one deterministic snapshot per CIK."
            )

        snapshot = latest

    return snapshot.reset_index(
        drop=True
    )


# =============================================================================
# SIGNAL MAPPINGS
# =============================================================================

GROWTH_SIGNAL_SCORE = {
    "STRONG_ACCELERATION": 100,
    "ACCELERATING": 85,
    "EXPANDING": 70,
    "STABLE": 55,
    "DECELERATING": 40,
    "CONTRACTING": 25,
    "STRONG_CONTRACTION": 10,
    "SOURCE_DATA_MISSING": np.nan,
    "INSUFFICIENT_HISTORY": np.nan,
}


PROFITABILITY_SIGNAL_SCORE = {
    "STRONG_EXPANSION": 100,
    "EXPANDING": 85,
    "STABLE": 65,
    "CONTRACTING": 35,
    "STRONG_CONTRACTION": 10,
    "SOURCE_DATA_MISSING": np.nan,
    "INSUFFICIENT_HISTORY": np.nan,
}


CONSISTENCY_SIGNAL_SCORE = {
    "CONSISTENT": 100,
    "MIXED": 50,
    "INCONSISTENT": 20,
    "SOURCE_DATA_MISSING": np.nan,
    "INSUFFICIENT_HISTORY": np.nan,
}


MOMENTUM_SIGNAL_SCORE = {
    "STRONG_POSITIVE": 100,
    "POSITIVE": 80,
    "NEUTRAL": 50,
    "NEGATIVE": 25,
    "STRONG_NEGATIVE": 0,
    "SOURCE_DATA_MISSING": np.nan,
    "INSUFFICIENT_HISTORY": np.nan,
}


RISK_SIGNAL_SCORE = {
    "LOW": 100,
    "MODERATE": 70,
    "ELEVATED": 40,
    "HIGH": 15,
    "CRITICAL": 0,
    "SOURCE_DATA_MISSING": np.nan,
    "INSUFFICIENT_HISTORY": np.nan,
}


HEALTH_SCORE = {
    "EXCELLENT": 100,
    "STRONG": 85,
    "HEALTHY": 70,
    "MIXED": 50,
    "WEAK": 30,
    "DISTRESSED": 10,
}


CONFIDENCE_SCORE = {
    "HIGH": 100,
    "MEDIUM": 65,
    "LOW": 30,
}


# =============================================================================
# DIMENSION SCORES
# =============================================================================

def calculate_dimension_scores(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    df["growth_score"] = (
        df["revenue_growth_signal"]
        .map(GROWTH_SIGNAL_SCORE)
    )

    df["profitability_score"] = (
        df["profitability_signal"]
        .map(PROFITABILITY_SIGNAL_SCORE)
    )

    df["consistency_score"] = (
        df["growth_consistency_signal"]
        .map(CONSISTENCY_SIGNAL_SCORE)
    )

    df["momentum_score"] = (
        df["momentum_signal"]
        .map(MOMENTUM_SIGNAL_SCORE)
    )

    df["risk_score"] = (
        df["financial_risk_signal"]
        .map(RISK_SIGNAL_SCORE)
    )

    df["financial_strength_score"] = (
        df["financial_score"]
        .clip(
            lower=0,
            upper=100,
        )
    )

    df["confidence_score"] = (
        df["financial_signal_confidence"]
        .map(CONFIDENCE_SCORE)
    )

    return df


# =============================================================================
# OVERALL RANKING
# =============================================================================

def weighted_score(
    row: pd.Series,
) -> float:

    components = {
        "financial_strength": row.get(
            "financial_strength_score"
        ),
        "growth": row.get(
            "growth_score"
        ),
        "profitability": row.get(
            "profitability_score"
        ),
        "momentum": row.get(
            "momentum_score"
        ),
        "consistency": row.get(
            "consistency_score"
        ),
        "risk": row.get(
            "risk_score"
        ),
    }

    weighted_values = []
    weighted_weights = []

    for name, value in components.items():

        if pd.isna(value):
            continue

        weight = RANKING_WEIGHTS[name]

        weighted_values.append(
            float(value) * weight
        )

        weighted_weights.append(
            weight
        )

    if not weighted_values:
        return np.nan

    # Renormalize when a dimension is legitimately unavailable.
    return (
        sum(weighted_values)
        / sum(weighted_weights)
    )


def calculate_overall_ranking_score(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    df["overall_ranking_score"] = (
        df.apply(
            weighted_score,
            axis=1,
        )
    )

    df["overall_ranking_score"] = (
        df["overall_ranking_score"]
        .round(4)
    )

    return df


# =============================================================================
# RANKING CONFIDENCE
# =============================================================================

def calculate_ranking_confidence(
    row: pd.Series,
) -> str:

    source_confidence = str(
        row.get(
            "financial_signal_confidence",
            "",
        )
    ).upper()

    health = str(
        row.get(
            "financial_health",
            "",
        )
    ).upper()

    score = row.get(
        "overall_ranking_score"
    )

    if pd.isna(score):
        return "LOW"

    if source_confidence == "HIGH":
        return "HIGH"

    if source_confidence == "MEDIUM":
        return "MEDIUM"

    if health in {
        "EXCELLENT",
        "STRONG",
    }:
        return "MEDIUM"

    return "LOW"


# =============================================================================
# PERCENTILE / RANK
# =============================================================================

def assign_ranks(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.copy()

    scored = df["overall_ranking_score"].notna()
    ordered = df.loc[scored].sort_values(
        ["overall_ranking_score", "cik"],
        ascending=[False, True],
        kind="mergesort",
    )
    df["overall_rank"] = pd.Series(
        pd.NA,
        index=df.index,
        dtype="Int64",
    )
    df.loc[ordered.index, "overall_rank"] = pd.Series(
        range(1, len(ordered) + 1),
        index=ordered.index,
        dtype="Int64",
    )

    # region agent log
    try:
        import json
        import time
        ties = (
            ordered.groupby("overall_ranking_score")["cik"]
            .apply(lambda values: [str(v).zfill(10) for v in values])
        )
        tied = {
            str(score): ciks
            for score, ciks in ties.items()
            if len(ciks) > 1
        }
        payload = {
            "sessionId": "63fc53",
            "runId": "post-fix",
            "hypothesisId": "G",
            "location": "step14_financial_ranking.py:assign_ranks",
            "message": "ranking tie break",
            "data": {
                "companies": int(scored.sum()),
                "tied_scores": tied,
                "unique_ranks": int(df["overall_rank"].nunique(dropna=True)),
            },
            "timestamp": int(time.time() * 1000),
        }
        with open(Path(__file__).resolve().parents[2] / "debug-63fc53.log", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion

    company_count = (
        df["overall_ranking_score"]
        .notna()
        .sum()
    )

    if company_count <= 1:

        df["ranking_percentile"] = np.where(
            df["overall_ranking_score"].notna(),
            100.0,
            np.nan,
        )

    else:

        df["ranking_percentile"] = (
            df["overall_ranking_score"]
            .rank(
                ascending=True,
                pct=True,
                method="average",
            )
            * 100.0
        )

    df["ranking_percentile"] = (
        df["ranking_percentile"]
        .round(2)
    )

    return df


# =============================================================================
# ARCHETYPE
# =============================================================================

def classify_archetype(
    row: pd.Series,
) -> str:

    confidence = str(
        row.get(
            "financial_signal_confidence",
            "",
        )
    ).upper()

    if confidence == "LOW":
        return "INSUFFICIENT_CONFIDENCE"

    growth = str(
        row.get(
            "revenue_growth_signal",
            "",
        )
    ).upper()

    profitability = str(
        row.get(
            "profitability_signal",
            "",
        )
    ).upper()

    momentum = str(
        row.get(
            "momentum_signal",
            "",
        )
    ).upper()

    risk = str(
        row.get(
            "financial_risk_signal",
            "",
        )
    ).upper()

    if (
        growth in {
            "STRONG_ACCELERATION",
            "ACCELERATING",
        }
        and profitability in {
            "STRONG_EXPANSION",
            "EXPANDING",
        }
    ):
        return "HIGH_GROWTH_HIGH_PROFITABILITY"

    if (
        growth in {
            "STRONG_ACCELERATION",
            "ACCELERATING",
            "EXPANDING",
        }
        and profitability in {
            "CONTRACTING",
            "STRONG_CONTRACTION",
        }
    ):
        return "GROWTH_WITH_MARGIN_PRESSURE"

    if (
        profitability in {
            "STRONG_EXPANSION",
            "EXPANDING",
        }
        and growth in {
            "STABLE",
            "DECELERATING",
            "CONTRACTING",
        }
    ):
        return "MARGIN_EXPANSION_WITH_MODERATE_GROWTH"

    if (
        growth in {
            "EXPANDING",
            "STABLE",
        }
        and profitability == "STABLE"
        and risk in {
            "LOW",
            "MODERATE",
        }
    ):
        return "STABLE_PROFITABLE"

    if (
        growth in {
            "DECELERATING",
            "CONTRACTING",
            "STRONG_CONTRACTION",
        }
        and profitability in {
            "CONTRACTING",
            "STRONG_CONTRACTION",
        }
    ):
        return "DETERIORATING"

    if (
        growth in {
            "STRONG_ACCELERATION",
            "ACCELERATING",
        }
        and momentum in {
            "POSITIVE",
            "STRONG_POSITIVE",
        }
    ):
        return "HIGH_GROWTH_MOMENTUM"

    return "MIXED"


# =============================================================================
# WATCHLIST
# =============================================================================

def classify_watchlist(
    row: pd.Series,
) -> Tuple[str, str]:

    risk = str(
        row.get(
            "financial_risk_signal",
            "",
        )
    ).upper()

    growth = str(
        row.get(
            "revenue_growth_signal",
            "",
        )
    ).upper()

    profitability = str(
        row.get(
            "profitability_signal",
            "",
        )
    ).upper()

    momentum = str(
        row.get(
            "momentum_signal",
            "",
        )
    ).upper()

    health = str(
        row.get(
            "financial_health",
            "",
        )
    ).upper()

    if risk in {
        "HIGH",
        "CRITICAL",
    }:
        return (
            "HIGH_RISK",
            "Elevated financial risk signal.",
        )

    if (
        growth in {
            "CONTRACTING",
            "STRONG_CONTRACTION",
        }
        and profitability in {
            "CONTRACTING",
            "STRONG_CONTRACTION",
        }
    ):
        return (
            "DETERIORATING",
            "Growth and profitability are both contracting.",
        )

    if (
        momentum in {
            "NEGATIVE",
            "STRONG_NEGATIVE",
        }
        and profitability == "CONTRACTING"
    ):
        return (
            "MOMENTUM_RISK",
            "Negative momentum combined with profitability pressure.",
        )

    if health in {
        "WEAK",
        "DISTRESSED",
    }:
        return (
            "FINANCIAL_HEALTH_RISK",
            "Overall financial health is weak or distressed.",
        )

    if (
        growth in {
            "STRONG_ACCELERATION",
            "ACCELERATING",
        }
        and profitability in {
            "STRONG_EXPANSION",
            "EXPANDING",
        }
    ):
        return (
            "POSITIVE_MOMENTUM",
            "Growth and profitability are both improving.",
        )

    return (
        "NO_ACTION",
        "No material watchlist condition detected.",
    )


# =============================================================================
# TREND ANALYSIS
# =============================================================================

def build_trends(
    signals: pd.DataFrame,
) -> pd.DataFrame:

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "financial_score",
        "overall_ranking_score",
        "financial_health",
        "financial_risk_signal",
    ]

    ensure_columns(
        signals,
        required,
        "Ranking signals",
    )

    df = signals.copy()

    df = df.sort_values(
        [
            "cik",
            "fiscal_year",
            "_quarter_order",
        ]
    )

    df["ranking_score_qoq_change"] = (
        df.groupby("cik")[
            "overall_ranking_score"
        ].diff()
    )

    df["financial_score_qoq_change"] = (
        df.groupby("cik")[
            "financial_score"
        ].diff()
    )

    df["ranking_score_trend"] = np.select(
        [
            df["ranking_score_qoq_change"] > 5,
            df["ranking_score_qoq_change"] < -5,
        ],
        [
            "IMPROVING",
            "DETERIORATING",
        ],
        default="STABLE",
    )

    df["financial_score_trend"] = np.select(
        [
            df["financial_score_qoq_change"] > 5,
            df["financial_score_qoq_change"] < -5,
        ],
        [
            "IMPROVING",
            "DETERIORATING",
        ],
        default="STABLE",
    )

    return df[
        [
            "cik",
            "fiscal_year",
            "quarter",
            "overall_ranking_score",
            "financial_score",
            "ranking_score_qoq_change",
            "financial_score_qoq_change",
            "ranking_score_trend",
            "financial_score_trend",
            "financial_health",
            "financial_risk_signal",
        ]
    ].reset_index(drop=True)


# =============================================================================
# PEER COMPARISON
# =============================================================================

def build_peer_comparison(
    ranking: pd.DataFrame,
) -> pd.DataFrame:

    df = ranking.copy()

    dimensions = [
        "overall_ranking_score",
        "growth_score",
        "profitability_score",
        "momentum_score",
        "consistency_score",
        "risk_score",
    ]

    for column in dimensions:

        if column not in df.columns:
            continue

        mean_value = df[column].mean()
        median_value = df[column].median()

        df[f"{column}_vs_mean"] = (
            df[column] - mean_value
        ).round(4)

        df[f"{column}_vs_median"] = (
            df[column] - median_value
        ).round(4)

    df["peer_position"] = np.select(
        [
            df["ranking_percentile"] >= 80,
            df["ranking_percentile"] >= 60,
            df["ranking_percentile"] >= 40,
            df["ranking_percentile"] >= 20,
        ],
        [
            "TOP_TIER",
            "ABOVE_MEDIAN",
            "MID_TIER",
            "BELOW_MEDIAN",
        ],
        default="BOTTOM_TIER",
    )

    return df


# =============================================================================
# SUMMARY
# =============================================================================

def build_summary(
    ranking: pd.DataFrame,
    trends: pd.DataFrame,
) -> pd.DataFrame:

    summary = pd.DataFrame(
        {
            "ranking_version": [
                RANKING_VERSION
            ],
            "company_count": [
                ranking["cik"].nunique()
            ],
            "ranked_company_count": [
                ranking[
                    "overall_ranking_score"
                ].notna().sum()
            ],
            "high_confidence_count": [
                (
                    ranking[
                        "ranking_confidence"
                    ]
                    == "HIGH"
                ).sum()
            ],
            "medium_confidence_count": [
                (
                    ranking[
                        "ranking_confidence"
                    ]
                    == "MEDIUM"
                ).sum()
            ],
            "low_confidence_count": [
                (
                    ranking[
                        "ranking_confidence"
                    ]
                    == "LOW"
                ).sum()
            ],
            "positive_watchlist_count": [
                (
                    ranking[
                        "watchlist_class"
                    ]
                    != "NO_ACTION"
                ).sum()
            ],
            "average_ranking_score": [
                ranking[
                    "overall_ranking_score"
                ].mean()
            ],
            "median_ranking_score": [
                ranking[
                    "overall_ranking_score"
                ].median()
            ],
            "max_ranking_score": [
                ranking[
                    "overall_ranking_score"
                ].max()
            ],
            "min_ranking_score": [
                ranking[
                    "overall_ranking_score"
                ].min()
            ],
        }
    )

    summary["average_ranking_score"] = (
        summary[
            "average_ranking_score"
        ].round(4)
    )

    summary["median_ranking_score"] = (
        summary[
            "median_ranking_score"
        ].round(4)
    )

    summary["max_ranking_score"] = (
        summary[
            "max_ranking_score"
        ].round(4)
    )

    summary["min_ranking_score"] = (
        summary[
            "min_ranking_score"
        ].round(4)
    )

    return summary


# =============================================================================
# CONTRACT VALIDATION
# =============================================================================

def validate_ranking_contract(
    df: pd.DataFrame,
) -> None:

    required = [
        "cik",
        "fiscal_year",
        "quarter",
        "overall_ranking_score",
        "overall_rank",
        "ranking_percentile",
        "ranking_confidence",
        "company_archetype",
        "watchlist_class",
    ]

    ensure_columns(
        df,
        required,
        "Financial ranking",
    )

    key = [
        "cik",
    ]

    duplicates = (
        df.groupby(
            key,
            dropna=False,
        )
        .size()
    )

    if (duplicates > 1).any():

        duplicate_ciks = (
            duplicates[
                duplicates > 1
            ]
            .index
            .tolist()
        )

        raise ValueError(
            "Financial ranking grain violation. "
            f"Duplicate CIKs: {duplicate_ciks}"
        )

    if df["cik"].isna().any():
        raise ValueError(
            "Financial ranking contains NULL CIK."
        )

    if (
        df["overall_ranking_score"]
        .dropna()
        .lt(0)
        .any()
        or
        df["overall_ranking_score"]
        .dropna()
        .gt(100)
        .any()
    ):
        raise ValueError(
            "Overall ranking score outside [0, 100]."
        )

    if (
        df["ranking_percentile"]
        .dropna()
        .lt(0)
        .any()
        or
        df["ranking_percentile"]
        .dropna()
        .gt(100)
        .any()
    ):
        raise ValueError(
            "Ranking percentile outside [0, 100]."
        )

    expected_ranks = (
        df[
            "overall_rank"
        ]
        .dropna()
        .astype(int)
        .tolist()
    )

    if len(expected_ranks) != len(
        set(expected_ranks)
    ):
        raise ValueError(
            "Financial ranking contains duplicate ranks."
        )


# =============================================================================
# COMPANY PROCESSING
# =============================================================================

def process_company_snapshots(
    signals: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    signals = normalize_signal_data(
        signals
    )

    snapshots = validate_snapshot(
        snapshots
    )

    companies = []

    for _, row in snapshots.iterrows():

        cik = str(row["cik"]).zfill(10)

        company_signals = signals[
            signals["cik"] == cik
        ].copy()

        if company_signals.empty:
            raise ValueError(
                f"No Step 13 signal rows found for {cik}."
            )

        latest = (
            company_signals
            .sort_values(
                [
                    "fiscal_year",
                    "_quarter_order",
                ]
            )
            .tail(1)
            .iloc[0]
        )

        companies.append(
            latest.to_dict()
        )

    ranking = pd.DataFrame(
        companies
    )

    return (
        ranking.reset_index(drop=True),
        signals,
    )


# =============================================================================
# MAIN RANKING PIPELINE
# =============================================================================

def build_ranking(
    snapshot_df: pd.DataFrame,
) -> pd.DataFrame:

    df = calculate_dimension_scores(
        snapshot_df
    )

    df = calculate_overall_ranking_score(
        df
    )

    df["ranking_confidence"] = (
        df.apply(
            calculate_ranking_confidence,
            axis=1,
        )
    )

    df = assign_ranks(
        df
    )

    df["company_archetype"] = (
        df.apply(
            classify_archetype,
            axis=1,
        )
    )

    watchlist = (
        df.apply(
            classify_watchlist,
            axis=1,
        )
    )

    df["watchlist_class"] = [
        item[0]
        for item in watchlist
    ]

    df["watchlist_reason"] = [
        item[1]
        for item in watchlist
    ]

    df["ranking_version"] = (
        RANKING_VERSION
    )

    df["supported_intelligence_version"] = (
        "1.0.0"
    )

    df["supported_feature_version"] = (
        "1.2.0"
    )

    df = df.sort_values(
        [
            "overall_rank",
            "cik",
        ],
        na_position="last",
    ).reset_index(
        drop=True
    )

    return df


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:

    section(
        "STEP 14 - FINANCIAL RANKING"
    )

    log(
        f"Features            : {FEATURES_DIR}"
    )

    log(
        f"Feature Validation  : {FEATURE_VALIDATION_DIR}"
    )

    log(
        f"Intelligence        : {INTELLIGENCE_DIR}"
    )

    log(
        f"Intelligence Valid. : {INTELLIGENCE_VALIDATION_DIR}"
    )

    log(
        f"Output              : {OUTPUT_DIR}"
    )

    log(
        f"Ranking version     : {RANKING_VERSION}"
    )

    log(
        "Supported intelligence: "
        f"{sorted(SUPPORTED_INTELLIGENCE_VERSIONS)}"
    )

    log(
        "Supported features    : "
        f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    log(
        f"Project root        : {PROJECT_ROOT}"
    )

    # -------------------------------------------------------------------------
    # Configuration validation
    # -------------------------------------------------------------------------

    validate_weights()

    # -------------------------------------------------------------------------
    # Step 12.1 gate
    # -------------------------------------------------------------------------

    log(
        "Checking Step 12.1 feature-quality gate..."
    )

    feature_gate = load_step12_1_gate()

    log(
        "Step 12.1 gate       : PASS"
    )

    # -------------------------------------------------------------------------
    # Step 13.1 gate
    # -------------------------------------------------------------------------

    log(
        "Checking Step 13.1 intelligence-quality gate..."
    )

    intelligence_gate = load_step13_1_gate()

    log(
        "Step 13.1 gate       : PASS"
    )

    # -------------------------------------------------------------------------
    # Load Step 13
    # -------------------------------------------------------------------------

    signals = read_parquet(
        intelligence_signal_path(),
        "Financial intelligence signals",
    )

    snapshots = read_parquet(
        intelligence_snapshot_path(),
        "Company financial snapshots",
    )
    for published_column, canonical_column in [
        ("latest_fiscal_year", "fiscal_year"),
        ("latest_quarter", "quarter"),
    ]:
        if canonical_column not in snapshots.columns:
            if published_column not in snapshots.columns:
                continue
            snapshots[canonical_column] = snapshots[published_column]

    # -------------------------------------------------------------------------
    # Version validation
    # -------------------------------------------------------------------------

    if "intelligence_version" in signals.columns:

        versions = (
            signals[
                "intelligence_version"
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        unsupported = [
            version
            for version in versions
            if version not in
            SUPPORTED_INTELLIGENCE_VERSIONS
        ]

        if unsupported:
            raise RuntimeError(
                "Unsupported intelligence versions found: "
                f"{unsupported}"
            )

    if "feature_version" in signals.columns:

        versions = (
            signals[
                "feature_version"
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        unsupported = [
            version
            for version in versions
            if version not in
            SUPPORTED_FEATURE_VERSIONS
        ]

        if unsupported:
            raise RuntimeError(
                "Unsupported feature versions found: "
                f"{unsupported}"
            )

    # -------------------------------------------------------------------------
    # Company discovery
    # -------------------------------------------------------------------------

    companies = discover_companies()

    if not companies:
        raise RuntimeError(
            "No companies discovered."
        )

    log(
        f"Companies discovered: {len(companies)}"
    )

    # -------------------------------------------------------------------------
    # Normalize
    # -------------------------------------------------------------------------

    signals = normalize_signal_data(
        signals
    )

    snapshots = validate_snapshot(
        snapshots
    )

    discovered_set = set(
        companies
    )

    signal_set = set(
        signals["cik"]
        .dropna()
        .astype(str)
        .str.zfill(10)
    )

    snapshot_set = set(
        snapshots["cik"]
        .dropna()
        .astype(str)
        .str.zfill(10)
    )

    if discovered_set != snapshot_set:
        raise RuntimeError(
            "Company discovery does not match Step 13 snapshots.\n"
            f"Discovered={sorted(discovered_set)}\n"
            f"Snapshots={sorted(snapshot_set)}"
        )

    if discovered_set != signal_set:
        raise RuntimeError(
            "Company discovery does not match Step 13 signals.\n"
            f"Discovered={sorted(discovered_set)}\n"
            f"Signals={sorted(signal_set)}"
        )

    # -------------------------------------------------------------------------
    # Build latest-company snapshot
    # -------------------------------------------------------------------------

    latest_rows = []

    for cik in companies:

        company_signals = signals[
            signals["cik"] == cik
        ].copy()

        if company_signals.empty:
            raise RuntimeError(
                f"No signal rows for {cik}."
            )

        latest = (
            company_signals
            .sort_values(
                [
                    "fiscal_year",
                    "_quarter_order",
                ]
            )
            .tail(1)
            .iloc[0]
            .copy()
        )

        latest_rows.append(
            latest
        )

    ranking = pd.DataFrame(
        latest_rows
    ).reset_index(
        drop=True
    )

    # -------------------------------------------------------------------------
    # Calculate ranking
    # -------------------------------------------------------------------------

    ranking = build_ranking(
        ranking
    )

    # -------------------------------------------------------------------------
    # Trends
    # -------------------------------------------------------------------------

    signals_scored = calculate_dimension_scores(
        signals
    )

    signals_scored = calculate_overall_ranking_score(
        signals_scored
    )

    trends = build_trends(
        signals_scored
    )

    # -------------------------------------------------------------------------
    # Peer comparison
    # -------------------------------------------------------------------------

    peer = build_peer_comparison(
        ranking
    )

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------

    summary = build_summary(
        ranking,
        trends,
    )

    # -------------------------------------------------------------------------
    # Watchlist
    # -------------------------------------------------------------------------

    watchlist = ranking[
        ranking["watchlist_class"]
        != "NO_ACTION"
    ].copy()

    # -------------------------------------------------------------------------
    # Contract validation
    # -------------------------------------------------------------------------

    validate_ranking_contract(
        ranking
    )

    if ranking["cik"].nunique() != len(
        companies
    ):
        raise ValueError(
            "Ranking does not contain exactly one row per company."
        )

    if trends.empty:
        raise ValueError(
            "Financial trends output is empty."
        )

    # -------------------------------------------------------------------------
    # Output staging
    # -------------------------------------------------------------------------

    staging_dir = (
        OUTPUT_DIR.parent
        / f".financial_ranking_staging_{RANKING_VERSION.replace('.', '_')}"
    )

    if staging_dir.exists():
        shutil.rmtree(
            staging_dir
        )

    staging_ranking = (
        staging_dir
        / "company_financial_ranking"
        / "company_financial_ranking.parquet"
    )

    staging_peer = (
        staging_dir
        / "company_peer_comparison"
        / "company_peer_comparison.parquet"
    )

    staging_trends = (
        staging_dir
        / "company_financial_trends"
        / "company_financial_trends.parquet"
    )

    staging_watchlist = (
        staging_dir
        / "financial_watchlist"
        / "financial_watchlist.parquet"
    )

    staging_summary = (
        staging_dir
        / "financial_ranking_summary"
        / "financial_ranking_summary.parquet"
    )

    write_parquet(
        ranking,
        staging_ranking,
    )

    write_parquet(
        peer,
        staging_peer,
    )

    write_parquet(
        trends,
        staging_trends,
    )

    write_parquet(
        watchlist,
        staging_watchlist,
    )

    write_parquet(
        summary,
        staging_summary,
    )

    # -------------------------------------------------------------------------
    # Replace output atomically at directory level
    # -------------------------------------------------------------------------

    if OUTPUT_DIR.exists():
        shutil.rmtree(
            OUTPUT_DIR
        )

    staging_dir.rename(
        OUTPUT_DIR
    )

    # -------------------------------------------------------------------------
    # Console reporting
    # -------------------------------------------------------------------------

    section(
        "STEP 14 SUMMARY"
    )

    log(
        f"Total companies       : {len(ranking)}"
    )

    log(
        f"Ranked companies      : "
        f"{ranking['overall_ranking_score'].notna().sum()}"
    )

    log(
        f"High confidence       : "
        f"{(ranking['ranking_confidence'] == 'HIGH').sum()}"
    )

    log(
        f"Medium confidence     : "
        f"{(ranking['ranking_confidence'] == 'MEDIUM').sum()}"
    )

    log(
        f"Low confidence        : "
        f"{(ranking['ranking_confidence'] == 'LOW').sum()}"
    )

    log(
        f"Watchlist companies   : "
        f"{len(watchlist)}"
    )

    log("")

    log("Financial health:")
    log(
        ranking[
            "financial_health"
        ]
        .value_counts()
        .sort_index()
        .to_string()
    )

    log("")

    log("Company ranking:")

    display_columns = [
        "cik",
        "fiscal_year",
        "quarter",
        "overall_rank",
        "overall_ranking_score",
        "ranking_percentile",
        "financial_health",
        "financial_risk_signal",
        "ranking_confidence",
        "company_archetype",
        "watchlist_class",
    ]

    log(
        ranking[
            display_columns
        ].to_string(
            index=False
        )
    )

    section(
        "OUTPUTS"
    )

    log(
        f"Financial ranking : "
        f"{RANKING_DIR / 'company_financial_ranking.parquet'}"
    )

    log(
        f"Peer comparison    : "
        f"{PEER_DIR / 'company_peer_comparison.parquet'}"
    )

    log(
        f"Financial trends   : "
        f"{TREND_DIR / 'company_financial_trends.parquet'}"
    )

    log(
        f"Financial watchlist: "
        f"{WATCHLIST_DIR / 'financial_watchlist.parquet'}"
    )

    log(
        f"Ranking summary    : "
        f"{SUMMARY_DIR / 'financial_ranking_summary.parquet'}"
    )

    section(
        "STEP 14 COMPLETE"
    )

    log(
        f"Ranking version: {RANKING_VERSION}"
    )

    log(
        "All companies successfully ranked."
    )

    log(
        "Step 12.1 and Step 13.1 remained upstream quality gates."
    )

    log(
        "Steps 10, 11, 12, 12.1, 13 and 13.1 were not modified."
    )


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    try:

        main()

    except Exception as exc:

        section(
            "STEP 14 FAILED"
        )

        log(
            str(exc)
        )

        traceback.print_exc()

        raise