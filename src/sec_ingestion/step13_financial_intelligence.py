#!/usr/bin/env python3
"""
STEP 13 - FINANCIAL INTELLIGENCE
================================

Production financial-intelligence layer for the Private Company Intelligence
pipeline.

Input contract
--------------
1. Step 12 financial features:
   data/gold/financials_features/cik=<CIK>/financial_features.parquet

2. Step 12.1 feature-quality audit:
    data/gold/financials_feature_validation/company_feature_summary/
       company_feature_summary.parquet
    data/gold/financials_feature_validation/feature_validation_summary/
       feature_validation_summary.parquet

Output contract
---------------
data/gold/financial_intelligence/
    financial_signals/
        financial_signals.parquet
    company_financial_snapshot/
        company_financial_snapshot.parquet
    financial_intelligence_summary/
        financial_intelligence_summary.parquet

Design principles
-----------------
* Read-only against Steps 10/11/12/12.1.
* Never silently repair source/features.
* Step 12.1 is the hard input gate.
* Legitimate Step-12 data-quality states are preserved as confidence,
  not converted into financial risk.
* Quarter-level grain: CIK + fiscal_year + quarter.
* Snapshot grain: CIK.
* Deterministic, explainable signals.
* Configuration is centralized near the top.
* No market-price or valuation logic belongs here.
"""

from __future__ import annotations

import argparse
import math
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_FINAL_GOLD = PROJECT_ROOT / "data" / "gold" / "financials_quarterly_final"
DEFAULT_ANALYTICS = PROJECT_ROOT / "data" / "gold" / "financials_analytics"
DEFAULT_FEATURES = PROJECT_ROOT / "data" / "gold" / "financials_features"
DEFAULT_FEATURE_VALIDATION = (
    PROJECT_ROOT / "data" / "gold" / "financials_feature_validation"
)
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "gold" / "financial_intelligence"

INTELLIGENCE_VERSION = "1.0.0"
SUPPORTED_FEATURE_VERSIONS = {"1.2.0"}
SUPPORTED_AUDIT_VERSIONS = {"1.0.0"}

# Feature-quality states are defined by Step 12.
QUALITY_COMPLETE = "COMPLETE"
QUALITY_PARTIAL = "PARTIAL"
QUALITY_SOURCE_INCOMPLETE = "SOURCE_DATA_INCOMPLETE"
QUALITY_NON_CONSECUTIVE = "NON_CONSECUTIVE_DATA"
QUALITY_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"

# Growth thresholds.
GROWTH_STRONG_THRESHOLD = 0.10
GROWTH_POSITIVE_THRESHOLD = 0.00
ACCELERATION_STABLE_TOLERANCE = 0.005

# Margin thresholds.
STRONG_MARGIN_EXPANSION = 0.02
MARGIN_EXPANSION_THRESHOLD = 0.005
MARGIN_CONTRACTION_THRESHOLD = -0.005

# Scoring weights. Must sum to 1.
SCORE_WEIGHTS = {
    "growth": 0.30,
    "profitability": 0.30,
    "momentum": 0.25,
    "consistency": 0.15,
}

# Health bands.
HEALTH_BANDS = (
    (90.0, "EXCELLENT"),
    (75.0, "STRONG"),
    (60.0, "HEALTHY"),
    (45.0, "MIXED"),
    (0.0, "WEAK"),
)

REQUIRED_FEATURE_COLUMNS = {
    "cik",
    "fiscal_year",
    "quarter",
    "revenue_value",
    "revenue_yoy_growth_pct",
    "revenue_growth_acceleration",
    "operating_margin",
    "operating_margin_qoq_change",
    "net_margin",
    "ttm_revenue",
    "ttm_operating_income",
    "ttm_net_income",
    "ttm_operating_margin",
    "ttm_net_margin",
    "revenue_growth_trend",
    "operating_margin_trend",
    "net_margin_trend",
    "revenue_growth_consistent_4q_flag",
    "revenue_positive_growth_quarters_4q",
    "revenue_growth_volatility_4q",
    "operating_margin_expansion_quarters_4q",
    "net_margin_expansion_quarters_4q",
    "feature_data_quality",
    "feature_version",
}

OPTIONAL_FEATURE_COLUMNS = {
    "revenue_growth_yoy_change",
    "operating_margin_yoy_change",
    "net_margin_qoq_change",
    "net_margin_yoy_change",
    "ttm_revenue_status",
    "ttm_operating_income_status",
    "ttm_net_income_status",
    "ttm_operating_margin_status",
    "ttm_net_margin_status",
    "feature_generated_at_utc",
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CompanyResult:
    cik: str
    feature_rows: int
    signal_rows: int
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
# LOGGING
# =============================================================================

def log(message: str = "") -> None:
    print(message, flush=True)


def section(title: str) -> None:
    log("=" * 80)
    log(title)
    log("=" * 80)


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def is_missing(value) -> bool:
    return value is None or pd.isna(value)


def safe_float(value) -> Optional[float]:
    if is_missing(value):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def normalize_cik(value) -> str:
    if is_missing(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(10)


def quarter_number(q) -> Optional[int]:
    if is_missing(q):
        return None
    text = str(q).strip().upper()
    if text.startswith("Q"):
        text = text[1:]
    try:
        n = int(text)
    except ValueError:
        return None
    return n if n in (1, 2, 3, 4) else None


def quarter_key(year, quarter) -> Optional[tuple[int, int]]:
    if is_missing(year):
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        return None
    q = quarter_number(quarter)
    if q is None:
        return None
    return y, q


def previous_quarter(year: int, q: int) -> tuple[int, int]:
    return (year - 1, 4) if q == 1 else (year, q - 1)


def is_consecutive_quarter(a_year, a_q, b_year, b_q) -> bool:
    a = quarter_key(a_year, a_q)
    b = quarter_key(b_year, b_q)
    if a is None or b is None:
        return False
    return previous_quarter(b[0], b[1]) == a


def require_columns(df: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"{label} is missing required columns: {missing}"
        )


def read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return pd.read_parquet(path)


def discover_ciks(features_root: Path) -> list[str]:
    ciks = []
    for path in features_root.glob("cik=*/financial_features.parquet"):
        cik = path.parent.name.split("=", 1)[-1]
        cik = normalize_cik(cik)
        if cik:
            ciks.append(cik)
    return sorted(set(ciks))


def feature_path(features_root: Path, cik: str) -> Path:
    return features_root / f"cik={cik}" / "financial_features.parquet"


# =============================================================================
# STEP 12.1 INPUT GATE
# =============================================================================

def load_validation_gate(validation_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    company_path = (
        validation_root
        / "company_feature_summary"
        / "company_feature_summary.parquet"
    )
    summary_path = (
        validation_root
        / "feature_validation_summary"
        / "feature_validation_summary.parquet"
    )

    company_df = read_parquet(company_path, "Step 12.1 company audit summary")
    summary_df = read_parquet(summary_path, "Step 12.1 validation summary")

    require_columns(
        company_df,
        {
            "cik",
            "feature_rows",
            "fatal_exception_rows",
            "audit_status",
        },
        "Step 12.1 company audit summary",
    )

    require_columns(
        summary_df,
        {"feature_version"},
        "Step 12.1 validation summary",
    )

    company_df["cik"] = company_df["cik"].map(normalize_cik)

    fatal_column = (
        "fatal_exception_rows"
        if "fatal_exception_rows" in summary_df.columns
        else "fatal_exception_count"
    )
    if fatal_column not in summary_df.columns:
        raise ValueError(
            "Step 12.1 validation summary is missing a fatal exception count "
            "column: expected 'fatal_exception_rows' or "
            "'fatal_exception_count'."
        )

    fatal_total = int(
        pd.to_numeric(summary_df[fatal_column], errors="coerce")
        .fillna(0)
        .sum()
    )

    bad_companies = company_df[
        (company_df["audit_status"].astype(str).str.upper() != "PASS")
        | (
            pd.to_numeric(
                company_df["fatal_exception_rows"], errors="coerce"
            ).fillna(0)
            > 0
        )
    ]

    if fatal_total > 0 or not bad_companies.empty:
        raise RuntimeError(
            "Step 12.1 gate failed. Step 13 will not process financial "
            f"features. fatal_total={fatal_total}, "
            f"bad_companies={bad_companies['cik'].tolist()}"
        )

    audit_versions = set(
        summary_df["audit_version"].dropna().astype(str).unique()
    ) if "audit_version" in summary_df.columns else set()

    unsupported_audits = audit_versions - SUPPORTED_AUDIT_VERSIONS
    if unsupported_audits:
        raise RuntimeError(
            f"Unsupported Step 12.1 audit versions: {sorted(unsupported_audits)}"
        )

    feature_versions = set(
        summary_df["feature_version"].dropna().astype(str).unique()
    )

    unsupported_features = feature_versions - SUPPORTED_FEATURE_VERSIONS
    if unsupported_features:
        raise RuntimeError(
            "Step 12.1 contains unsupported feature versions: "
            f"{sorted(unsupported_features)}; supported="
            f"{sorted(SUPPORTED_FEATURE_VERSIONS)}"
        )

    return company_df, summary_df


# =============================================================================
# FEATURE INPUT NORMALIZATION
# =============================================================================

def normalize_features(df: pd.DataFrame, cik: str) -> pd.DataFrame:
    require_columns(df, REQUIRED_FEATURE_COLUMNS, f"Step 12 features for {cik}")

    out = df.copy()
    out["cik"] = out["cik"].map(normalize_cik)

    if set(out["cik"].dropna().unique()) != {cik}:
        raise ValueError(
            f"Feature file CIK mismatch for {cik}: "
            f"{sorted(out['cik'].dropna().unique().tolist())}"
        )

    out["fiscal_year"] = pd.to_numeric(
        out["fiscal_year"], errors="coerce"
    ).astype("Int64")
    out["quarter"] = out["quarter"].astype("string").str.upper().str.strip()

    out["quarter_num"] = out["quarter"].map(quarter_number).astype("Int64")

    if out[["fiscal_year", "quarter_num"]].isna().any().any():
        raise ValueError(f"Invalid fiscal year/quarter in Step 12 features for {cik}.")

    duplicate_mask = out.duplicated(
        subset=["cik", "fiscal_year", "quarter"], keep=False
    )

    if duplicate_mask.any():
        dupes = (
            out.loc[
                duplicate_mask,
                ["cik", "fiscal_year", "quarter"]
            ]
            .drop_duplicates()
            .head(20)
        )
        raise ValueError(
            f"Step 12 feature grain violation for {cik}:\n{dupes.to_string(index=False)}"
        )

    feature_versions = set(
        out["feature_version"].dropna().astype(str).unique()
    )
    unsupported = feature_versions - SUPPORTED_FEATURE_VERSIONS
    if unsupported:
        raise ValueError(
            f"Unsupported feature versions for {cik}: {sorted(unsupported)}"
        )

    for col in REQUIRED_FEATURE_COLUMNS:
        if col in {
            "cik",
            "quarter",
            "feature_data_quality",
            "feature_version",
            "revenue_growth_trend",
            "operating_margin_trend",
            "net_margin_trend",
        }:
            continue
        if pd.api.types.is_bool_dtype(out[col]):
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values(
        ["fiscal_year", "quarter_num"]
    ).reset_index(drop=True)

    return out


# =============================================================================
# SIGNAL LOGIC
# =============================================================================

def growth_signal(row: pd.Series) -> str:
    yoy = safe_float(row.get("revenue_yoy_growth_pct"))
    accel = safe_float(row.get("revenue_growth_acceleration"))

    if yoy is None:
        return "NO_SIGNAL"

    if yoy > GROWTH_STRONG_THRESHOLD and accel is not None and accel > 0:
        return "STRONG_ACCELERATION"

    if yoy > GROWTH_POSITIVE_THRESHOLD and accel is not None:
        if accel > ACCELERATION_STABLE_TOLERANCE:
            return "ACCELERATING"
        if accel < -ACCELERATION_STABLE_TOLERANCE:
            return "DECELERATING"
        return "STABLE"

    if yoy > GROWTH_POSITIVE_THRESHOLD:
        return "STABLE"

    if yoy <= GROWTH_POSITIVE_THRESHOLD:
        return "CONTRACTING"

    return "NO_SIGNAL"


def profitability_signal(row: pd.Series) -> str:
    margin = safe_float(row.get("ttm_operating_margin"))
    change = safe_float(row.get("operating_margin_yoy_change"))

    if change is None:
        change = safe_float(row.get("operating_margin_qoq_change"))

    if margin is None:
        return "NO_SIGNAL"

    if change is None:
        return "NO_SIGNAL"

    if change >= STRONG_MARGIN_EXPANSION:
        return "STRONG_EXPANSION"

    if change >= MARGIN_EXPANSION_THRESHOLD:
        return "EXPANDING"

    if change <= MARGIN_CONTRACTION_THRESHOLD:
        return "CONTRACTING"

    if margin < 0:
        return "WEAK"

    return "STABLE"


def consistency_signal(row: pd.Series) -> str:
    history = safe_float(row.get("revenue_positive_growth_quarters_4q"))
    volatility = safe_float(row.get("revenue_growth_volatility_4q"))
    consistent = row.get("revenue_growth_consistent_4q_flag")

    if history is None or volatility is None:
        return "INSUFFICIENT_HISTORY"

    if bool(consistent) and history >= 4:
        return "CONSISTENT"

    if volatility >= 0.15:
        return "VOLATILE"

    return "MIXED"


def momentum_signal(row: pd.Series) -> str:
    growth = str(row.get("revenue_growth_trend", "")).upper()
    operating = str(row.get("operating_margin_trend", "")).upper()
    net = str(row.get("net_margin_trend", "")).upper()

    expanding = {"EXPANDING"}
    contracting = {"CONTRACTING"}

    positive = int(growth in expanding) + int(operating in expanding) + int(net in expanding)
    negative = int(growth in contracting) + int(operating in contracting) + int(net in contracting)

    if positive == 3:
        return "STRONG_POSITIVE"
    if negative == 3:
        return "STRONG_NEGATIVE"
    if positive >= 2 and negative == 0:
        return "POSITIVE"
    if negative >= 2 and positive == 0:
        return "NEGATIVE"
    return "NEUTRAL"


def financial_risk_signal(row: pd.Series) -> str:
    growth = str(row.get("revenue_growth_trend", "")).upper()
    operating = str(row.get("operating_margin_trend", "")).upper()
    net = str(row.get("net_margin_trend", "")).upper()

    yoy = safe_float(row.get("revenue_yoy_growth_pct"))
    volatility = safe_float(row.get("revenue_growth_volatility_4q"))

    score = 0

    if growth == "CONTRACTING":
        score += 2
    if operating == "CONTRACTING":
        score += 2
    if net == "CONTRACTING":
        score += 1
    if yoy is not None and yoy < 0:
        score += 2
    if volatility is not None and volatility >= 0.15:
        score += 1

    if score >= 6:
        return "HIGH"
    if score >= 4:
        return "ELEVATED"
    if score >= 2:
        return "MODERATE"
    return "LOW"



def signal_confidence(row: pd.Series) -> str:
    """
    Determine confidence from the core financial evidence required for
    financial intelligence.

    HIGH:
        Complete feature quality and all core evidence available.

    MEDIUM:
        Partial/non-consecutive source data with sufficient core evidence
        to support meaningful intelligence.

    LOW:
        Insufficient history or insufficient evidence for meaningful
        intelligence.
    """
    quality = str(row.get("feature_data_quality", "")).upper()

    core_cols = [
        "revenue_value",
        "operating_margin",
        "ttm_revenue",
    ]

    available = sum(
        safe_float(row.get(column)) is not None
        for column in core_cols
    )

    # Complete evidence supports high confidence.
    if quality == QUALITY_COMPLETE and available == len(core_cols):
        return "HIGH"

    # Partial/non-consecutive data can support medium confidence only
    # when at least two core evidence fields are actually available.
    if quality in {
        QUALITY_SOURCE_INCOMPLETE,
        QUALITY_NON_CONSECUTIVE,
    } and available >= 2:
        return "MEDIUM"

    # Insufficient history is always low confidence.
    if quality == QUALITY_INSUFFICIENT_HISTORY:
        return "LOW"

    # One or zero core evidence fields are not enough for medium confidence.
    if available < 2:
        return "LOW"

    # Do not promote otherwise unsupported rows to MEDIUM merely because
    # two fields happen to be populated.
    return "LOW"
# =============================================================================
# SCORE LOGIC
# =============================================================================

def score_growth(row: pd.Series) -> Optional[float]:
    yoy = safe_float(row.get("revenue_yoy_growth_pct"))
    accel = safe_float(row.get("revenue_growth_acceleration"))

    if yoy is None:
        return None

    # Growth level: 0 at -10%, 50 at 0%, 100 at +20%.
    level_score = clamp((yoy + 0.10) / 0.30 * 100.0)

    # Acceleration contribution: centered at 50.
    accel_score = 50.0 if accel is None else clamp(50.0 + accel * 500.0)

    return round(0.70 * level_score + 0.30 * accel_score, 4)


def score_profitability(row: pd.Series) -> Optional[float]:
    margin = safe_float(row.get("ttm_operating_margin"))
    change = safe_float(row.get("operating_margin_yoy_change"))

    if change is None:
        change = safe_float(row.get("operating_margin_qoq_change"))

    if margin is None:
        return None

    # 0% margin -> 40; 20% -> 70; 40%+ -> 100.
    margin_score = clamp(40.0 + margin * 150.0)

    change_score = 50.0 if change is None else clamp(50.0 + change * 1000.0)

    return round(0.75 * margin_score + 0.25 * change_score, 4)


def score_momentum(row: pd.Series) -> Optional[float]:
    mapping = {
        "STRONG_POSITIVE": 100.0,
        "POSITIVE": 80.0,
        "NEUTRAL": 50.0,
        "NEGATIVE": 25.0,
        "STRONG_NEGATIVE": 0.0,
    }
    return mapping.get(momentum_signal(row))


def score_consistency(row: pd.Series) -> Optional[float]:
    mapping = {
        "CONSISTENT": 100.0,
        "MIXED": 60.0,
        "VOLATILE": 25.0,
        "INSUFFICIENT_HISTORY": None,
    }
    return mapping.get(consistency_signal(row))


def composite_financial_score(row: pd.Series) -> tuple[Optional[float], str]:
    components = {
        "growth": score_growth(row),
        "profitability": score_profitability(row),
        "momentum": score_momentum(row),
        "consistency": score_consistency(row),
    }

    valid = {
        k: v for k, v in components.items()
        if v is not None
    }

    if len(valid) < 2:
        return None, "INSUFFICIENT_DATA"

    total_weight = sum(SCORE_WEIGHTS[k] for k in valid)

    score = sum(
        valid[k] * SCORE_WEIGHTS[k]
        for k in valid
    ) / total_weight

    confidence = signal_confidence(row)

    # Don't allow incomplete data to masquerade as a high-confidence score.
    if confidence == "LOW":
        return round(score, 4), "INSUFFICIENT_DATA"

    return round(clamp(score), 4), "VALID"


def health_classification(score: Optional[float], confidence: str) -> str:
    numeric_score = safe_float(score)

    if numeric_score is None or confidence == "LOW":
        return "INSUFFICIENT_DATA"

    for lower, label in HEALTH_BANDS:
        if numeric_score >= lower:
            return label

    return "WEAK"


# =============================================================================
# BUILD SIGNALS
# =============================================================================

def build_signals(features: pd.DataFrame, cik: str) -> pd.DataFrame:
    out = features.copy()

    out["revenue_growth_signal"] = out.apply(growth_signal, axis=1)
    out["profitability_signal"] = out.apply(profitability_signal, axis=1)
    out["growth_consistency_signal"] = out.apply(consistency_signal, axis=1)
    out["momentum_signal"] = out.apply(momentum_signal, axis=1)
    out["financial_risk_signal"] = out.apply(financial_risk_signal, axis=1)
    out["financial_signal_confidence"] = out.apply(signal_confidence, axis=1)

    scores = out.apply(composite_financial_score, axis=1)
    out["financial_score"] = [x[0] for x in scores]
    out["financial_score_status"] = [x[1] for x in scores]

    out["financial_health"] = [
        health_classification(score, confidence)
        for score, confidence in zip(
            out["financial_score"],
            out["financial_signal_confidence"],
        )
    ]

    out["intelligence_version"] = INTELLIGENCE_VERSION
    out["intelligence_generated_at_utc"] = utc_now_iso()

    out["cik"] = cik

    return out


# =============================================================================
# SNAPSHOT
# =============================================================================

def build_snapshot(signals: pd.DataFrame, cik: str) -> pd.DataFrame:
    if signals.empty:
        raise ValueError(f"No signals available for snapshot: {cik}")

    latest = (
        signals.sort_values(["fiscal_year", "quarter_num"])
        .iloc[-1]
        .copy()
    )

    row = {
        "cik": cik,
        "latest_fiscal_year": int(latest["fiscal_year"]),
        "latest_quarter": str(latest["quarter"]),
        "latest_revenue_value": safe_float(latest.get("revenue_value")),
        "latest_revenue_yoy_growth_pct": safe_float(
            latest.get("revenue_yoy_growth_pct")
        ),
        "latest_revenue_growth_acceleration": safe_float(
            latest.get("revenue_growth_acceleration")
        ),
        "latest_operating_margin": safe_float(
            latest.get("operating_margin")
        ),
        "latest_net_margin": safe_float(
            latest.get("net_margin")
        ),
        "ttm_revenue": safe_float(latest.get("ttm_revenue")),
        "ttm_operating_income": safe_float(
            latest.get("ttm_operating_income")
        ),
        "ttm_net_income": safe_float(latest.get("ttm_net_income")),
        "ttm_operating_margin": safe_float(
            latest.get("ttm_operating_margin")
        ),
        "ttm_net_margin": safe_float(
            latest.get("ttm_net_margin")
        ),
        "revenue_growth_signal": latest["revenue_growth_signal"],
        "profitability_signal": latest["profitability_signal"],
        "growth_consistency_signal": latest["growth_consistency_signal"],
        "momentum_signal": latest["momentum_signal"],
        "financial_risk_signal": latest["financial_risk_signal"],
        "financial_signal_confidence": latest[
            "financial_signal_confidence"
        ],
        "financial_score": safe_float(latest.get("financial_score")),
        "financial_score_status": latest["financial_score_status"],
        "financial_health": latest["financial_health"],
        "feature_data_quality": latest["feature_data_quality"],
        "feature_version": str(latest["feature_version"]),
        "intelligence_version": INTELLIGENCE_VERSION,
        "intelligence_generated_at_utc": utc_now_iso(),
    }

    return pd.DataFrame([row])


# =============================================================================
# COMPANY SUMMARY
# =============================================================================

def build_company_summary(
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
    cik: str,
) -> dict:
    quality = signals["feature_data_quality"].astype(str).str.upper()
    confidence = signals["financial_signal_confidence"].astype(str).str.upper()

    return {
        "cik": cik,
        "feature_rows": len(signals),
        "complete_rows": int((quality == QUALITY_COMPLETE).sum()),
        "partial_rows": int((quality == QUALITY_PARTIAL).sum()),
        "source_incomplete_rows": int(
            (quality == QUALITY_SOURCE_INCOMPLETE).sum()
        ),
        "non_consecutive_rows": int(
            (quality == QUALITY_NON_CONSECUTIVE).sum()
        ),
        "insufficient_history_rows": int(
            (quality == QUALITY_INSUFFICIENT_HISTORY).sum()
        ),
        "high_confidence_rows": int((confidence == "HIGH").sum()),
        "medium_confidence_rows": int((confidence == "MEDIUM").sum()),
        "low_confidence_rows": int((confidence == "LOW").sum()),
        "latest_fiscal_year": int(snapshot.iloc[0]["latest_fiscal_year"]),
        "latest_quarter": snapshot.iloc[0]["latest_quarter"],
        "latest_financial_score": snapshot.iloc[0]["financial_score"],
        "latest_financial_health": snapshot.iloc[0]["financial_health"],
        "latest_financial_risk": snapshot.iloc[0]["financial_risk_signal"],
        "latest_signal_confidence": snapshot.iloc[0][
            "financial_signal_confidence"
        ],
        "feature_version": str(snapshot.iloc[0]["feature_version"]),
        "intelligence_version": INTELLIGENCE_VERSION,
        "intelligence_generated_at_utc": utc_now_iso(),
    }


# =============================================================================
# OUTPUT WRITERS
# =============================================================================

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def write_company_signal(
    signals: pd.DataFrame,
    output_root: Path,
    cik: str,
) -> Path:
    path = (
        output_root
        / "financial_signals"
        / f"cik={cik}"
        / "financial_signals.parquet"
    )
    write_parquet(signals, path)
    return path


# =============================================================================
# COMPANY PROCESSING
# =============================================================================

def process_company(
    cik: str,
    features_root: Path,
    output_root: Path,
) -> tuple[CompanyResult, pd.DataFrame, pd.DataFrame]:
    path = feature_path(features_root, cik)

    features_raw = read_parquet(path, f"Step 12 feature file for {cik}")
    features = normalize_features(features_raw, cik)

    signals = build_signals(features, cik)
    snapshot = build_snapshot(signals, cik)

    signal_path = write_company_signal(signals, output_root, cik)

    quality = signals["feature_data_quality"].astype(str).str.upper()
    confidence = signals["financial_signal_confidence"].astype(str).str.upper()

    result = CompanyResult(
        cik=cik,
        feature_rows=len(features),
        signal_rows=len(signals),
        snapshot_rows=len(snapshot),
        complete_rows=int((quality == QUALITY_COMPLETE).sum()),
        partial_rows=int((quality == QUALITY_PARTIAL).sum()),
        source_incomplete_rows=int(
            (quality == QUALITY_SOURCE_INCOMPLETE).sum()
        ),
        non_consecutive_rows=int(
            (quality == QUALITY_NON_CONSECUTIVE).sum()
        ),
        insufficient_history_rows=int(
            (quality == QUALITY_INSUFFICIENT_HISTORY).sum()
        ),
        high_confidence_rows=int((confidence == "HIGH").sum()),
        medium_confidence_rows=int((confidence == "MEDIUM").sum()),
        low_confidence_rows=int((confidence == "LOW").sum()),
    )

    return result, signals, snapshot


# =============================================================================
# FINAL CONTRACT VALIDATION
# =============================================================================

def validate_final_outputs(
    signals: pd.DataFrame,
    snapshot: pd.DataFrame,
) -> None:
    require_columns(
        signals,
        {
            "cik",
            "fiscal_year",
            "quarter",
            "revenue_growth_signal",
            "profitability_signal",
            "growth_consistency_signal",
            "momentum_signal",
            "financial_risk_signal",
            "financial_signal_confidence",
            "financial_score",
            "financial_health",
            "intelligence_version",
        },
        "financial_signals",
    )

    require_columns(
        snapshot,
        {
            "cik",
            "latest_fiscal_year",
            "latest_quarter",
            "financial_score",
            "financial_health",
            "financial_risk_signal",
            "financial_signal_confidence",
            "intelligence_version",
        },
        "company_financial_snapshot",
    )

    if signals.duplicated(
        subset=["cik", "fiscal_year", "quarter"]
    ).any():
        raise ValueError("Final financial_signals grain violation.")

    if snapshot.duplicated(subset=["cik"]).any():
        raise ValueError("Final company_financial_snapshot grain violation.")

    if not set(signals["intelligence_version"].unique()) == {
        INTELLIGENCE_VERSION
    }:
        raise ValueError("Unexpected intelligence version in signals.")

    if not set(snapshot["intelligence_version"].unique()) == {
        INTELLIGENCE_VERSION
    }:
        raise ValueError("Unexpected intelligence version in snapshot.")

    valid_confidence = {"HIGH", "MEDIUM", "LOW"}
    invalid_confidence = (
        set(signals["financial_signal_confidence"].dropna().astype(str))
        - valid_confidence
    )
    if invalid_confidence:
        raise ValueError(
            f"Invalid financial confidence values: {sorted(invalid_confidence)}"
        )


# =============================================================================
# ARGUMENTS
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Step 13 - Financial Intelligence"
    )

    parser.add_argument(
        "--features",
        type=Path,
        default=DEFAULT_FEATURES,
        help="Step 12 financial features root.",
    )

    parser.add_argument(
        "--validation",
        type=Path,
        default=DEFAULT_FEATURE_VALIDATION,
        help="Step 12.1 validation root.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Step 13 output root.",
    )

    parser.add_argument(
        "--cik",
        action="append",
        default=None,
        help="Process only the supplied CIK. Can be repeated.",
    )

    parser.add_argument(
        "--allow-missing-validation",
        action="store_true",
        help=(
            "NOT recommended for production. Allows running without the "
            "Step 12.1 gate."
        ),
    )

    return parser.parse_args()


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    args = parse_args()

    section("STEP 13 - FINANCIAL INTELLIGENCE")

    log(f"Features            : {args.features}")
    log(f"Validation          : {args.validation}")
    log(f"Output              : {args.output}")
    log(f"Intelligence version: {INTELLIGENCE_VERSION}")
    log(
        f"Supported features : {sorted(SUPPORTED_FEATURE_VERSIONS)}"
    )

    # The other roots are printed intentionally because they document the
    # upstream lineage without being read/recalculated by Step 13.
    log(f"Project root        : {PROJECT_ROOT}")

    # -------------------------------------------------------------------------
    # Step 12.1 hard gate
    # -------------------------------------------------------------------------
    if not args.allow_missing_validation:
        log("Checking Step 12.1 feature-quality gate...")
        _, validation_summary = load_validation_gate(args.validation)
        log("Step 12.1 gate       : PASS")
    else:
        log("WARNING: Step 12.1 validation gate explicitly bypassed.")
        validation_summary = pd.DataFrame()

    # -------------------------------------------------------------------------
    # Discover companies
    # -------------------------------------------------------------------------
    discovered = discover_ciks(args.features)

    if args.cik:
        requested = {normalize_cik(c) for c in args.cik}
        discovered = [c for c in discovered if c in requested]

    if not discovered:
        raise RuntimeError("No Step 12 feature companies discovered.")

    log(f"Companies discovered: {len(discovered)}")
    log("")

    all_signals = []
    all_snapshots = []
    results = []

    for cik in discovered:
        section(f"PROCESSING CIK: {cik}")

        try:
            result, signals, snapshot = process_company(
                cik=cik,
                features_root=args.features,
                output_root=args.output,
            )

            log(f"Feature rows        : {result.feature_rows}")
            log(f"Signal rows         : {result.signal_rows}")
            log(f"Snapshot rows       : {result.snapshot_rows}")
            log(f"COMPLETE             : {result.complete_rows}")
            log(f"PARTIAL              : {result.partial_rows}")
            log(f"SOURCE_DATA_INCOMPLETE: {result.source_incomplete_rows}")
            log(f"NON_CONSECUTIVE      : {result.non_consecutive_rows}")
            log(f"INSUFFICIENT_HISTORY : {result.insufficient_history_rows}")
            log(f"HIGH confidence      : {result.high_confidence_rows}")
            log(f"MEDIUM confidence    : {result.medium_confidence_rows}")
            log(f"LOW confidence       : {result.low_confidence_rows}")

            log("")
            log("Latest financial intelligence:")
            display_cols = [
                "cik",
                "fiscal_year",
                "quarter",
                "revenue_yoy_growth_pct",
                "operating_margin",
                "ttm_revenue",
                "ttm_operating_margin",
                "revenue_growth_signal",
                "profitability_signal",
                "growth_consistency_signal",
                "momentum_signal",
                "financial_risk_signal",
                "financial_signal_confidence",
                "financial_score",
                "financial_health",
            ]

            existing = [c for c in display_cols if c in signals.columns]
            log(
                signals[existing]
                .tail(4)
                .to_string(index=False)
            )

            results.append(result)
            all_signals.append(signals)
            all_snapshots.append(snapshot)

        except Exception as exc:
            result = CompanyResult(
                cik=cik,
                feature_rows=0,
                signal_rows=0,
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
            results.append(result)

            log(f"ERROR processing {cik}: {exc}")
            traceback.print_exc()

    failed = [r for r in results if r.failed]

    if failed:
        section("STEP 13 FAILED")
        for result in failed:
            log(f"{result.cik}: {result.error}")

        raise RuntimeError(
            f"Step 13 failed for {len(failed)} company/companies."
        )

    # -------------------------------------------------------------------------
    # Combine
    # -------------------------------------------------------------------------
    signals_df = pd.concat(all_signals, ignore_index=True)
    all_snapshots = [
        df for df in all_snapshots
        if df is not None and not df.empty
        ]

    if not all_snapshots:
        raise RuntimeError("No company snapshots were generated.")

    snapshot_df = pd.concat(
        all_snapshots,
        ignore_index=True,
    )

    signals_df = signals_df.sort_values(
        ["cik", "fiscal_year", "quarter_num"]
    ).reset_index(drop=True)

    snapshot_df = snapshot_df.sort_values(
        ["cik"]
    ).reset_index(drop=True)

    validate_final_outputs(signals_df, snapshot_df)

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    result_rows = [r.__dict__ for r in results]
    company_summary_df = pd.DataFrame(result_rows)

    latest_summary = snapshot_df[
        [
            "cik",
            "latest_fiscal_year",
            "latest_quarter",
            "financial_score",
            "financial_health",
            "financial_risk_signal",
            "financial_signal_confidence",
        ]
    ].copy()

    financial_summary_df = company_summary_df.merge(
        latest_summary,
        on="cik",
        how="left",
        validate="one_to_one",
    )

    financial_summary_df["intelligence_version"] = INTELLIGENCE_VERSION
    financial_summary_df["generated_at_utc"] = utc_now_iso()

    # -------------------------------------------------------------------------
    # Write outputs
    # -------------------------------------------------------------------------
    section("OUTPUTS")

    signals_path = args.output / "financial_signals" / "financial_signals.parquet"
    snapshot_path = (
        args.output
        / "company_financial_snapshot"
        / "company_financial_snapshot.parquet"
    )
    summary_path = (
        args.output
        / "financial_intelligence_summary"
        / "financial_intelligence_summary.parquet"
    )

    write_parquet(signals_df, signals_path)
    write_parquet(snapshot_df, snapshot_path)
    write_parquet(financial_summary_df, summary_path)

    log(f"Financial signals   : {signals_path}")
    log(f"Company snapshot    : {snapshot_path}")
    log(f"Intelligence summary: {summary_path}")

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    section("STEP 13 SUMMARY")

    log(
        f"Total signal rows        : {len(signals_df)}"
    )
    log(
        f"Total company snapshots  : {len(snapshot_df)}"
    )
    log(
        f"Companies processed      : {len(results)}"
    )

    log("")
    log("Financial health:")
    log(
        snapshot_df["financial_health"]
        .value_counts(dropna=False)
        .sort_index()
        .to_string()
    )

    log("")
    log("Financial risk:")
    log(
        snapshot_df["financial_risk_signal"]
        .value_counts(dropna=False)
        .sort_index()
        .to_string()
    )

    log("")
    log("Signal confidence:")
    log(
        snapshot_df["financial_signal_confidence"]
        .value_counts(dropna=False)
        .sort_index()
        .to_string()
    )

    log("")
    log("Company snapshot:")
    snapshot_display = [
        "cik",
        "latest_fiscal_year",
        "latest_quarter",
        "financial_score",
        "financial_health",
        "financial_risk_signal",
        "financial_signal_confidence",
        "revenue_growth_signal",
        "profitability_signal",
        "momentum_signal",
    ]
    # These signal columns are not in the snapshot yet, so enrich only for
    # console display from the latest signal rows.
    latest_signals = (
        signals_df
        .sort_values(["cik", "fiscal_year", "quarter_num"])
        .groupby("cik", as_index=False)
        .tail(1)
    )

    console_snapshot = latest_signals[
        [
            "cik",
            "fiscal_year",
            "quarter",
            "financial_score",
            "financial_health",
            "financial_risk_signal",
            "financial_signal_confidence",
            "revenue_growth_signal",
            "profitability_signal",
            "momentum_signal",
        ]
    ].copy()

    log(console_snapshot.to_string(index=False))

    log("")
    log("=" * 80)
    log("STEP 13 COMPLETE")
    log("=" * 80)
    log(f"Intelligence version: {INTELLIGENCE_VERSION}")
    log("All companies successfully processed.")
    log("Step 12.1 remained the upstream quality gate.")
    log("Steps 10, 11, 12 and 12.1 were not modified.")


if __name__ == "__main__":
    main()
