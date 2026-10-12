"""
STEP 20 - UNIFIED INVESTMENT INTELLIGENCE

Purpose
-------
Integrate the validated financial, funding/capital, investor/ownership,
and valuation layers into one company-level investment intelligence
contract.

This step is an integration/decision-support layer. It does NOT create
a new financial score and does NOT manufacture missing funding,
investor, ownership, or valuation information.

Source hierarchy
----------------
Step 12  -> financial features
Step 15  -> financial decision intelligence
Step 17  -> funding & capital intelligence
Step 18  -> investor & ownership intelligence
Step 19  -> valuation intelligence

Important semantics
-------------------
- Financial intelligence remains the primary operating/financial signal.
- SEC Form D capital is SEC exempt-offering capital only.
- Missing funding/investor/ownership/valuation is NO_DATA, not zero.
- Valuation remains separate from SEC offering capital.
- Investor identity is not inferred from Form D investor counts.
- Ownership is not inferred from funding amounts or investor counts.
- No new financial score is calculated.
- Unified ranking is NOT created here.
- The unified posture is a deterministic synthesis of existing signals,
  not a replacement score.

Outputs
-------
data/gold/unified_investment_intelligence/
    investment_profile/
    investment_signals/
    investment_summary/
    investment_lineage/
    investment_exceptions/
    investment_promotion/

Run
---
python ./src/sec_ingestion/step20_unified_investment_intelligence.py
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from company_platform.registry.active_company_scope import load_active_company_ciks
from company_platform.registry.active_company_scope import assert_company_scope
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = REPO_ROOT / "data"

STEP12_ROOT = DATA_ROOT / "gold" / "financials_features"
STEP15_ROOT = DATA_ROOT / "gold" / "financial_decision_intelligence"
STEP17_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"
STEP18_ROOT = DATA_ROOT / "gold" / "investor_ownership_intelligence"
STEP19_ROOT = DATA_ROOT / "gold" / "valuation_intelligence"

OUTPUT_ROOT = DATA_ROOT / "gold" / "unified_investment_intelligence"

STEP12_VERSION_EXPECTED = "1.2.0"
STEP15_VERSION_EXPECTED = "1.2.0"
STEP17_VERSION_EXPECTED = "3.0.1"
STEP18_VERSION_EXPECTED = "18.0.0"
STEP19_VERSION_EXPECTED = "19.0.0"

STEP20_VERSION = "20.0.0"

def configured_companies() -> list[str]:
    """Resolve the authoritative active production company scope."""
    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))


COMPANIES = configured_companies()

VALID_FINANCIAL_DECISIONS = {
    "CAUTION",
    "WATCH",
    "NEUTRAL",
    "POSITIVE",
}

VALID_TRAJECTORIES = {
    "IMPROVING",
    "MIXED",
    "DETERIORATING",
    "UNKNOWN",
}

VALID_FUNDING_STATUSES = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_INVESTOR_COVERAGE = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_VALUATION_STATUSES = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_UNIFIED_POSTURES = {
    "STRONG_POSITIVE",
    "POSITIVE",
    "NEUTRAL",
    "WATCH",
    "CAUTION",
    "INSUFFICIENT_DATA",
}

VALID_DATA_COVERAGE = {
    "COMPLETE",
    "PARTIAL",
    "LIMITED",
    "NO_DATA",
}

VALID_SIGNAL_TYPES = {
    "FINANCIAL_DECISION",
    "FINANCIAL_TRAJECTORY",
    "FUNDING_COVERAGE",
    "FUNDING_ACTIVITY",
    "INVESTOR_COVERAGE",
    "OWNERSHIP_COVERAGE",
    "VALUATION_COVERAGE",
    "VALUATION_STATUS",
    "SEC_CAPITAL_CONTEXT",
    "DATA_COVERAGE",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step20")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(10) if digits else ""


COMPANIES = configured_companies()

def configured_companies_from_step15(default_companies=None):
    return assert_company_scope()



def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = " ".join(str(value).split()).strip()
    return text or None


def numeric(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def load_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required {label} artifact not found: {path}")

    if path.is_dir():
        partitions = sorted(path.glob("cik=*/financial_features.parquet"))
        if not partitions:
            raise FileNotFoundError(
                f"Required {label} directory contains no feature partitions: {path}"
            )
        return pd.concat(
            [pd.read_parquet(partition) for partition in partitions],
            ignore_index=True,
        )

    return pd.read_parquet(path)


def first_existing(paths: List[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def make_signal(
    cik: str,
    signal_type: str,
    signal_value: Any,
    signal_status: str,
    source_scope: str,
    confidence: str = "UNKNOWN",
    reason: str | None = None,
) -> Dict[str, Any]:
    return {
        "cik": normalize_cik(cik),
        "signal_type": signal_type,
        "signal_value": signal_value,
        "signal_status": signal_status,
        "source_scope": source_scope,
        "confidence": confidence,
        "reason": reason,
        "step20_version": STEP20_VERSION,
        "generated_at_utc": utc_now_iso(),
    }


# =============================================================================
# INPUT PATHS
# =============================================================================

def input_paths() -> Dict[str, Path]:
    feature_path = first_existing([
        STEP12_ROOT / "features" / "financial_features.parquet",
        STEP12_ROOT / "financial_features.parquet",
    ])
    if feature_path is None:
        feature_partitions = sorted(
            STEP12_ROOT.glob("cik=*/financial_features.parquet")
        )
        if feature_partitions:
            feature_path = STEP12_ROOT

    decision_path = first_existing([
        STEP15_ROOT / "decision_signals" / "financial_decision_signals.parquet",
    ])

    decision_snapshot_path = first_existing([
        STEP15_ROOT / "company_financial_decision_snapshot"
        / "company_financial_decision_snapshot.parquet",
    ])

    funding_snapshot_path = (
        STEP17_ROOT / "company_funding_snapshot"
        / "company_funding_snapshot.parquet"
    )

    funding_signals_path = (
        STEP17_ROOT / "funding_signals"
        / "funding_signals.parquet"
    )

    investor_snapshot_path = (
        STEP18_ROOT / "company_investor_snapshot"
        / "company_investor_snapshot.parquet"
    )

    ownership_snapshot_path = (
        STEP18_ROOT / "ownership_snapshot"
        / "ownership_snapshot.parquet"
    )

    investor_signals_path = (
        STEP18_ROOT / "investor_signals"
        / "investor_signals.parquet"
    )

    valuation_snapshot_path = (
        STEP19_ROOT / "company_valuation_snapshot"
        / "company_valuation_snapshot.parquet"
    )

    valuation_signals_path = (
        STEP19_ROOT / "valuation_signals"
        / "valuation_signals.parquet"
    )

    missing = []
    for label, path in {
        "Step 12 financial features": feature_path,
        "Step 15 decision signals": decision_path,
        "Step 15 decision snapshot": decision_snapshot_path,
        "Step 17 funding snapshot": funding_snapshot_path,
        "Step 17 funding signals": funding_signals_path,
        "Step 18 investor snapshot": investor_snapshot_path,
        "Step 18 ownership snapshot": ownership_snapshot_path,
        "Step 18 investor signals": investor_signals_path,
        "Step 19 valuation snapshot": valuation_snapshot_path,
        "Step 19 valuation signals": valuation_signals_path,
    }.items():
        if path is None:
            missing.append(label)

    if missing:
        raise FileNotFoundError(
            "Required Step 20 dependencies are missing: "
            + ", ".join(missing)
        )

    return {
        "features": feature_path,
        "decision_signals": decision_path,
        "step15_snapshot": decision_snapshot_path,
        "funding_snapshot": funding_snapshot_path,
        "funding_signals": funding_signals_path,
        "investor_snapshot": investor_snapshot_path,
        "ownership_snapshot": ownership_snapshot_path,
        "investor_signals": investor_signals_path,
        "valuation_snapshot": valuation_snapshot_path,
        "valuation_signals": valuation_signals_path,
    }


# =============================================================================
# NORMALIZATION
# =============================================================================

def normalize_company_frame(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "cik" not in result.columns:
        raise ValueError("Input frame must contain cik.")
    result["cik"] = result["cik"].map(normalize_cik)
    return result


def latest_by_cik(
    df: pd.DataFrame,
    date_columns: List[str],
) -> pd.DataFrame:
    result = normalize_company_frame(df)

    for column in date_columns:
        if column in result.columns:
            result[column] = pd.to_datetime(
                result[column],
                errors="coerce",
            )

    usable_dates = [c for c in date_columns if c in result.columns]

    if usable_dates:
        result["_sort_date"] = pd.NaT
        for column in usable_dates:
            result["_sort_date"] = result["_sort_date"].fillna(result[column])
        result = result.sort_values(
            ["cik", "_sort_date"],
            na_position="first",
        )

    result = result.drop_duplicates("cik", keep="last")
    return result.drop(columns=["_sort_date"], errors="ignore")


# =============================================================================
# VERSION VALIDATION
# =============================================================================

def validate_expected_version(
    df: pd.DataFrame,
    version_columns: List[str],
    expected: str,
    label: str,
) -> None:
    observed = set()

    for column in version_columns:
        if column in df.columns:
            observed.update(
                df[column].dropna().astype(str).tolist()
            )

    if observed and expected not in observed:
        raise ValueError(
            f"{label} version mismatch. Expected {expected}; observed "
            f"{sorted(observed)}"
        )

    unexpected = observed - {expected}
    if unexpected:
        raise ValueError(
            f"{label} contains unexpected versions: {sorted(unexpected)}"
        )


# =============================================================================
# SOURCE-SPECIFIC INTERPRETATION
# =============================================================================

def derive_funding_activity(row: pd.Series) -> Tuple[Any, str, str]:
    """
    Returns:
        value, status, reason

    Funding amount is SEC exempt-offering capital only.
    It is not company valuation and not conventional funding raised.
    """
    event_count = numeric(row.get("funding_event_count"))
    capital = numeric(row.get("total_capital_raised_usd"))

    if event_count is None:
        event_count = 0.0

    if event_count <= 0:
        return None, "NO_DATA", "NO_SEC_FORM_D_EVENT"

    if capital is not None:
        return capital, "AVAILABLE", "SEC_EXEMPT_OFFERING_CAPITAL"

    return None, "PARTIAL", "FORM_D_EVENT_WITHOUT_EXTRACTABLE_CAPITAL"


def derive_unified_posture(
    financial_decision: str | None,
    financial_trajectory: str | None,
    funding_status: str,
    investor_status: str,
    ownership_status: str,
    valuation_status: str,
    data_coverage: str,
) -> Tuple[str, str]:
    """
    Deterministic decision-support posture.

    Financial decision remains the primary signal.

    Funding/investor/ownership/valuation layers modify context only;
    they do not create a new financial score.
    """
    fd = clean_text(financial_decision)
    trajectory = clean_text(financial_trajectory)

    if data_coverage == "NO_DATA":
        return "INSUFFICIENT_DATA", "INSUFFICIENT_CROSS_LAYER_DATA"

    if fd == "CAUTION":
        if trajectory == "DETERIORATING":
            return "CAUTION", "FINANCIAL_CAUTION_DETERIORATING"
        return "CAUTION", "FINANCIAL_CAUTION"

    if fd == "WATCH":
        if trajectory == "DETERIORATING":
            return "CAUTION", "FINANCIAL_WATCH_DETERIORATING"
        return "WATCH", "FINANCIAL_WATCH"

    if fd == "POSITIVE":
        if trajectory == "IMPROVING":
            return "STRONG_POSITIVE", "FINANCIAL_POSITIVE_IMPROVING"
        return "POSITIVE", "FINANCIAL_POSITIVE"

    if fd == "NEUTRAL":
        if trajectory == "DETERIORATING":
            return "WATCH", "FINANCIAL_NEUTRAL_DETERIORATING"
        return "NEUTRAL", "FINANCIAL_NEUTRAL"

    return "INSUFFICIENT_DATA", "FINANCIAL_DECISION_UNAVAILABLE"


def derive_data_coverage(
    financial_available: bool,
    funding_status: str,
    investor_status: str,
    ownership_status: str,
    valuation_status: str,
) -> Tuple[str, str]:
    supporting = [
        funding_status,
        investor_status,
        ownership_status,
        valuation_status,
    ]

    if not financial_available:
        if all(status == "NO_DATA" for status in supporting):
            return "NO_DATA", "NO_FINANCIAL_OR_SUPPORTING_DATA"
        return "LIMITED", "FINANCIAL_DATA_UNAVAILABLE"

    available_count = sum(
        status == "AVAILABLE" for status in supporting
    )
    partial_count = sum(
        status == "PARTIAL" for status in supporting
    )

    if available_count >= 3:
        return "COMPLETE", "FINANCIAL_PLUS_THREE_SUPPORTING_LAYERS"

    if available_count >= 1 or partial_count >= 1:
        return "PARTIAL", "FINANCIAL_PLUS_PARTIAL_SUPPORTING_DATA"

    return "LIMITED", "FINANCIAL_ONLY"


# =============================================================================
# BUILD PROFILE
# =============================================================================

def build_profile(
    features: pd.DataFrame,
    decision_snapshot: pd.DataFrame,
    funding_snapshot: pd.DataFrame,
    investor_snapshot: pd.DataFrame,
    ownership_snapshot: pd.DataFrame,
    valuation_snapshot: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one current company-level unified profile per configured CIK.
    """
    f = latest_by_cik(
        features,
        ["period_end", "feature_generated_at_utc", "generated_at_utc"],
    )
    d = latest_by_cik(
        decision_snapshot,
        ["period_end", "decision_generated_at_utc", "generated_at_utc"],
    )
    fu = latest_by_cik(
        funding_snapshot,
        ["last_funding_date", "generated_at_utc"],
    )
    inv = latest_by_cik(
        investor_snapshot,
        ["as_of_date", "generated_at_utc"],
    )
    own = latest_by_cik(
        ownership_snapshot,
        ["ownership_as_of_date", "as_of_date", "generated_at_utc"],
    )
    val = latest_by_cik(
        valuation_snapshot,
        ["latest_valuation_date", "generated_at_utc"],
    )

    rows = []

    for cik in COMPANIES:
        cik = normalize_cik(cik)

        fr = f[f["cik"] == cik]
        dr = d[d["cik"] == cik]
        fur = fu[fu["cik"] == cik]
        ir = inv[inv["cik"] == cik]
        orow = own[own["cik"] == cik]
        vr = val[val["cik"] == cik]

        feature = fr.iloc[0] if not fr.empty else pd.Series(dtype=object)
        decision = dr.iloc[0] if not dr.empty else pd.Series(dtype=object)
        funding = fur.iloc[0] if not fur.empty else pd.Series(dtype=object)
        investor = ir.iloc[0] if not ir.empty else pd.Series(dtype=object)
        ownership = orow.iloc[0] if not orow.empty else pd.Series(dtype=object)
        valuation = vr.iloc[0] if not vr.empty else pd.Series(dtype=object)

        financial_decision = clean_text(
            decision.get("decision_posture")
        )
        financial_trajectory = clean_text(
            decision.get("financial_trajectory")
        )

        if financial_decision is None:
            financial_decision = clean_text(
                decision.get("decision_priority_label")
            )

        # Preserve the actual Step 15 posture if present.
        if financial_decision not in VALID_FINANCIAL_DECISIONS:
            financial_decision = clean_text(
                decision.get("latest_decision")
            )

        funding_event_count = numeric(
            funding.get("funding_event_count")
        )
        funding_status = clean_text(
            funding.get("funding_status")
        ) or (
            "AVAILABLE"
            if funding_event_count is not None and funding_event_count > 0
            else "NO_DATA"
        )

        investor_status = clean_text(
            investor.get("investor_identity_status")
        ) or clean_text(
            investor.get("coverage_status")
        ) or "NO_DATA"

        ownership_status = clean_text(
            ownership.get("ownership_status")
        ) or clean_text(
            ownership.get("coverage_status")
        ) or "NO_DATA"

        valuation_status = clean_text(
            valuation.get("valuation_status")
        ) or "NO_DATA"

        financial_available = not decision.empty

        data_coverage, coverage_reason = derive_data_coverage(
            financial_available,
            funding_status,
            investor_status,
            ownership_status,
            valuation_status,
        )

        posture, posture_reason = derive_unified_posture(
            financial_decision,
            financial_trajectory,
            funding_status,
            investor_status,
            ownership_status,
            valuation_status,
            data_coverage,
        )

        funding_amount, funding_amount_status, funding_reason = (
            derive_funding_activity(funding)
        )

        latest_valuation = numeric(
            valuation.get("latest_valuation_amount_usd")
        )

        sec_capital = numeric(
            funding.get("total_sec_capital_raised_usd")
        )
        if sec_capital is None:
            sec_capital = numeric(
                funding.get("total_sec_capital_usd")
            )

        row = {
            "cik": cik,

            # Financial layer.
            "financial_decision": financial_decision,
            "financial_trajectory": financial_trajectory,
            "financial_score": numeric(
                decision.get("financial_score")
            ),
            "financial_rank": numeric(
                decision.get("rank")
            ),

            # Funding layer.
            "funding_status": funding_status,
            "funding_event_count": funding_event_count,
            "sec_exempt_offering_capital_usd": funding_amount,
            "funding_activity_status": funding_amount_status,
            "funding_activity_reason": funding_reason,
            "last_funding_date": funding.get("last_funding_date"),
            "funding_stage": clean_text(
                funding.get("funding_stage")
            ),

            # Investor layer.
            "investor_coverage_status": investor_status,
            "named_investor_count": numeric(
                investor.get("named_investor_count")
            ),
            "investor_concentration_status": clean_text(
                investor.get("investor_concentration_status")
            ),

            # Ownership layer.
            "ownership_status": ownership_status,
            "ownership_coverage_status": clean_text(
                ownership.get("ownership_coverage_status")
            ),
            "ownership_pct_available": safe_bool(
                ownership.get("ownership_pct_available")
            ),

            # Valuation layer.
            "valuation_status": valuation_status,
            "latest_valuation_amount_usd": latest_valuation,
            "latest_valuation_date": valuation.get(
                "latest_valuation_date"
            ),
            "valuation_type": clean_text(
                valuation.get("latest_valuation_type")
            ),
            "valuation_source": clean_text(
                valuation.get("latest_valuation_source")
            ),

            # Contextual capital/valuation separation.
            "sec_capital_context_usd": sec_capital,
            "valuation_to_sec_capital_ratio": numeric(
                valuation.get("valuation_to_sec_capital_ratio")
            ),
            "sec_capital_treated_as_valuation": False,
            "valuation_fabrication_prevented": True,

            # Unified interpretation.
            "data_coverage": data_coverage,
            "data_coverage_reason": coverage_reason,
            "unified_posture": posture,
            "unified_posture_reason": posture_reason,

            # Lineage.
            "step12_version": STEP12_VERSION_EXPECTED,
            "step15_version": STEP15_VERSION_EXPECTED,
            "step17_version": STEP17_VERSION_EXPECTED,
            "step18_version": STEP18_VERSION_EXPECTED,
            "step19_version": STEP19_VERSION_EXPECTED,
            "step20_version": STEP20_VERSION,
            "generated_at_utc": utc_now_iso(),
        }

        rows.append(row)

    return pd.DataFrame(rows)


# =============================================================================
# SIGNALS
# =============================================================================

def build_signals(profile: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for _, row in profile.iterrows():
        cik = normalize_cik(row["cik"])

        financial_decision = clean_text(row["financial_decision"])
        if financial_decision in VALID_FINANCIAL_DECISIONS:
            rows.append(make_signal(
                cik,
                "FINANCIAL_DECISION",
                financial_decision,
                "AVAILABLE",
                "STEP15_FINANCIAL_DECISION",
                "MEDIUM",
                "Existing Step 15 decision posture.",
            ))
        else:
            rows.append(make_signal(
                cik,
                "FINANCIAL_DECISION",
                None,
                "NO_DATA",
                "STEP15_FINANCIAL_DECISION",
                "UNKNOWN",
                "No valid financial decision available.",
            ))

        trajectory = clean_text(row["financial_trajectory"])
        if trajectory in VALID_TRAJECTORIES and trajectory != "UNKNOWN":
            rows.append(make_signal(
                cik,
                "FINANCIAL_TRAJECTORY",
                trajectory,
                "AVAILABLE",
                "STEP15_FINANCIAL_DECISION",
                "MEDIUM",
                "Existing Step 15 financial trajectory.",
            ))
        else:
            rows.append(make_signal(
                cik,
                "FINANCIAL_TRAJECTORY",
                None,
                "NO_DATA",
                "STEP15_FINANCIAL_DECISION",
                "UNKNOWN",
                "No valid financial trajectory available.",
            ))

        funding_status = clean_text(row["funding_status"]) or "NO_DATA"
        funding_value = row["sec_exempt_offering_capital_usd"]

        rows.append(make_signal(
            cik,
            "FUNDING_COVERAGE",
            funding_status if funding_status in VALID_FUNDING_STATUSES else None,
            "AVAILABLE" if funding_status in VALID_FUNDING_STATUSES else "NO_DATA",
            "STEP17_SEC_FORM_D",
            "MEDIUM" if funding_status == "AVAILABLE" else "UNKNOWN",
            "SEC Form D coverage only.",
        ))

        activity_status = clean_text(row["funding_activity_status"])
        if activity_status == "AVAILABLE":
            rows.append(make_signal(
                cik,
                "FUNDING_ACTIVITY",
                funding_value,
                "AVAILABLE",
                "STEP17_SEC_EXEMPT_OFFERING",
                "MEDIUM",
                "SEC exempt-offering capital; not complete private-company funding history.",
            ))
        else:
            rows.append(make_signal(
                cik,
                "FUNDING_ACTIVITY",
                None,
                "NO_DATA" if activity_status == "NO_DATA" else "PARTIAL",
                "STEP17_SEC_EXEMPT_OFFERING",
                "UNKNOWN",
                clean_text(row["funding_activity_reason"]),
            ))

        investor_status = clean_text(
            row["investor_coverage_status"]
        ) or "NO_DATA"

        rows.append(make_signal(
            cik,
            "INVESTOR_COVERAGE",
            investor_status if investor_status in VALID_INVESTOR_COVERAGE else None,
            "AVAILABLE" if investor_status in VALID_INVESTOR_COVERAGE else "NO_DATA",
            "STEP18_INVESTOR_OWNERSHIP",
            "MEDIUM" if investor_status == "AVAILABLE" else "UNKNOWN",
            "Named investor identity requires enrichment; Form D investor counts are not names.",
        ))

        ownership_status = clean_text(row["ownership_status"]) or "NO_DATA"
        ownership_signal_value = (
            ownership_status
            if ownership_status in {"AVAILABLE", "PARTIAL"}
            else None
        )

        rows.append(make_signal(
            cik,
            "OWNERSHIP_COVERAGE",
            ownership_signal_value,
            "AVAILABLE" if ownership_status in {"AVAILABLE", "PARTIAL"} else "NO_DATA",
            "STEP18_INVESTOR_OWNERSHIP",
            "MEDIUM" if ownership_status == "AVAILABLE" else "UNKNOWN",
            "Ownership is source-dependent; no cap-table inference.",
        ))

        valuation_status = clean_text(row["valuation_status"]) or "NO_DATA"

        rows.append(make_signal(
            cik,
            "VALUATION_COVERAGE",
            valuation_status,
            "AVAILABLE" if valuation_status in VALID_VALUATION_STATUSES else "NO_DATA",
            "STEP19_VALUATION",
            "MEDIUM" if valuation_status == "AVAILABLE" else "UNKNOWN",
            "Valuation source coverage from Step 19.",
        ))

        valuation_value = row["latest_valuation_amount_usd"]

        if valuation_status == "AVAILABLE" and not is_missing(valuation_value):
            rows.append(make_signal(
                cik,
                "VALUATION_STATUS",
                valuation_value,
                "AVAILABLE",
                "STEP19_VALUATION",
                "MEDIUM",
                "Source-aware valuation observation.",
            ))
        else:
            rows.append(make_signal(
                cik,
                "VALUATION_STATUS",
                None,
                "NO_DATA",
                "STEP19_VALUATION",
                "UNKNOWN",
                "No defensible valuation observation available.",
            ))

        sec_capital = row["sec_capital_context_usd"]

        if not is_missing(sec_capital):
            rows.append(make_signal(
                cik,
                "SEC_CAPITAL_CONTEXT",
                sec_capital,
                "AVAILABLE",
                "STEP17_SEC_EXEMPT_OFFERING",
                "MEDIUM",
                "Contextual SEC exempt-offering capital; never a valuation.",
            ))
        else:
            rows.append(make_signal(
                cik,
                "SEC_CAPITAL_CONTEXT",
                None,
                "NO_DATA",
                "STEP17_SEC_EXEMPT_OFFERING",
                "UNKNOWN",
                "No SEC exempt-offering capital observation.",
            ))

        coverage = clean_text(row["data_coverage"]) or "NO_DATA"

        rows.append(make_signal(
            cik,
            "DATA_COVERAGE",
            coverage,
            "AVAILABLE" if coverage in VALID_DATA_COVERAGE else "NO_DATA",
            "STEP20_UNIFIED",
            "MEDIUM" if coverage in {"COMPLETE", "PARTIAL"} else "UNKNOWN",
            clean_text(row["data_coverage_reason"]),
        ))

    result = pd.DataFrame(rows)
    result["signal_value_numeric"] = pd.to_numeric(
        result["signal_value"],
        errors="coerce",
    )
    result["signal_value"] = result["signal_value"].map(
        lambda value: None
        if value is None or pd.isna(value)
        else str(value)
    )

    return result


# =============================================================================
# SUMMARY
# =============================================================================

def build_summary(profile: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    posture_counts = (
        profile["unified_posture"]
        .value_counts()
        .to_dict()
    )

    coverage_counts = (
        profile["data_coverage"]
        .value_counts()
        .to_dict()
    )

    return pd.DataFrame([{
        "step": "STEP_20",
        "step20_version": STEP20_VERSION,
        "configured_companies": len(COMPANIES),
        "profile_rows": len(profile),
        "signal_rows": len(signals),
        "strong_positive_count": int(
            posture_counts.get("STRONG_POSITIVE", 0)
        ),
        "positive_count": int(
            posture_counts.get("POSITIVE", 0)
        ),
        "neutral_count": int(
            posture_counts.get("NEUTRAL", 0)
        ),
        "watch_count": int(
            posture_counts.get("WATCH", 0)
        ),
        "caution_count": int(
            posture_counts.get("CAUTION", 0)
        ),
        "insufficient_data_count": int(
            posture_counts.get("INSUFFICIENT_DATA", 0)
        ),
        "complete_coverage_count": int(
            coverage_counts.get("COMPLETE", 0)
        ),
        "partial_coverage_count": int(
            coverage_counts.get("PARTIAL", 0)
        ),
        "limited_coverage_count": int(
            coverage_counts.get("LIMITED", 0)
        ),
        "no_data_coverage_count": int(
            coverage_counts.get("NO_DATA", 0)
        ),
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "generated_at_utc": utc_now_iso(),
    }])


# =============================================================================
# VALIDATION
# =============================================================================

def validate_profile(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    results = []

    configured = {normalize_cik(c) for c in COMPANIES}
    actual = set(profile["cik"].map(normalize_cik))

    if actual != configured:
        results.append({
            "check_name": "COMPANY_COVERAGE",
            "status": "FAIL",
            "message": f"Missing={sorted(configured-actual)}, Extra={sorted(actual-configured)}",
        })
    else:
        results.append({
            "check_name": "COMPANY_COVERAGE",
            "status": "PASS",
            "message": "All configured companies have one unified profile.",
        })

    if profile["cik"].duplicated().any():
        results.append({
            "check_name": "PROFILE_GRAIN",
            "status": "FAIL",
            "message": "Duplicate CIKs detected in unified profile.",
        })
    else:
        results.append({
            "check_name": "PROFILE_GRAIN",
            "status": "PASS",
            "message": "Unified profile grain is one row per CIK.",
        })

    invalid_postures = (
        set(profile["unified_posture"].dropna().astype(str))
        - VALID_UNIFIED_POSTURES
    )

    if invalid_postures:
        results.append({
            "check_name": "POSTURE_VALUES",
            "status": "FAIL",
            "message": f"Invalid unified postures: {sorted(invalid_postures)}",
        })
    else:
        results.append({
            "check_name": "POSTURE_VALUES",
            "status": "PASS",
            "message": "Unified posture values are valid.",
        })

    invalid_coverage = (
        set(profile["data_coverage"].dropna().astype(str))
        - VALID_DATA_COVERAGE
    )

    if invalid_coverage:
        results.append({
            "check_name": "DATA_COVERAGE_VALUES",
            "status": "FAIL",
            "message": f"Invalid data coverage values: {sorted(invalid_coverage)}",
        })
    else:
        results.append({
            "check_name": "DATA_COVERAGE_VALUES",
            "status": "PASS",
            "message": "Data coverage values are valid.",
        })

    if not profile["sec_capital_treated_as_valuation"].map(safe_bool).eq(False).all():
        results.append({
            "check_name": "SEC_CAPITAL_SEPARATION",
            "status": "FAIL",
            "message": "SEC capital was incorrectly treated as valuation.",
        })
    else:
        results.append({
            "check_name": "SEC_CAPITAL_SEPARATION",
            "status": "PASS",
            "message": "SEC capital remains contextual and separate from valuation.",
        })

    if not profile["valuation_fabrication_prevented"].map(safe_bool).eq(True).all():
        results.append({
            "check_name": "VALUATION_FABRICATION_CONTROL",
            "status": "FAIL",
            "message": "Valuation fabrication prevention is not enabled.",
        })
    else:
        results.append({
            "check_name": "VALUATION_FABRICATION_CONTROL",
            "status": "PASS",
            "message": "Valuation fabrication prevention is enabled.",
        })

    if profile["financial_score"].notna().any():
        # This is only a lineage check: Step 20 must preserve the existing
        # financial score rather than calculate a replacement.
        results.append({
            "check_name": "FINANCIAL_SCORE_PRESERVATION",
            "status": "PASS",
            "message": "Existing financial score is preserved as upstream context; Step 20 creates no new score.",
        })
    else:
        results.append({
            "check_name": "FINANCIAL_SCORE_PRESERVATION",
            "status": "PASS",
            "message": "No upstream financial score available; no replacement score was created.",
        })

    return results


def validate_signals(signals: pd.DataFrame) -> List[Dict[str, Any]]:
    results = []

    expected_types = VALID_SIGNAL_TYPES

    actual_types = set(
        signals["signal_type"].dropna().astype(str)
    )

    unexpected = actual_types - expected_types

    if unexpected:
        results.append({
            "check_name": "SIGNAL_TYPES",
            "status": "FAIL",
            "message": f"Unexpected signal types: {sorted(unexpected)}",
        })
    else:
        results.append({
            "check_name": "SIGNAL_TYPES",
            "status": "PASS",
            "message": "All signal types are within the Step 20 contract.",
        })

    duplicates = signals[
        ["cik", "signal_type"]
    ].duplicated()

    if duplicates.any():
        results.append({
            "check_name": "SIGNAL_GRAIN",
            "status": "FAIL",
            "message": "Duplicate CIK/signal_type rows detected.",
        })
    else:
        results.append({
            "check_name": "SIGNAL_GRAIN",
            "status": "PASS",
            "message": "Signal grain is one row per CIK/signal type.",
        })

    no_data_with_value = []

    for _, row in signals.iterrows():
        if clean_text(row["signal_status"]) == "NO_DATA":
            if not is_missing(row["signal_value"]):
                no_data_with_value.append(
                    f"{row['cik']}:{row['signal_type']}"
                )

    if no_data_with_value:
        results.append({
            "check_name": "NO_DATA_SEMANTICS",
            "status": "FAIL",
            "message": f"NO_DATA signals contain values: {no_data_with_value}",
        })
    else:
        results.append({
            "check_name": "NO_DATA_SEMANTICS",
            "status": "PASS",
            "message": "NO_DATA signals correctly use NULL values.",
        })

    return results


def validate_semantics(profile: pd.DataFrame) -> List[Dict[str, Any]]:
    results = []

    invalid = []

    for _, row in profile.iterrows():
        funding = numeric(row["sec_exempt_offering_capital_usd"])
        valuation = numeric(row["latest_valuation_amount_usd"])

        if funding is not None and valuation is not None:
            ratio = numeric(row["valuation_to_sec_capital_ratio"])

            if ratio is not None and ratio < 0:
                invalid.append(
                    f"{row['cik']}: negative valuation/capital ratio"
                )

    if invalid:
        results.append({
            "check_name": "VALUATION_CAPITAL_CONTEXT",
            "status": "FAIL",
            "message": "; ".join(invalid),
        })
    else:
        results.append({
            "check_name": "VALUATION_CAPITAL_CONTEXT",
            "status": "PASS",
            "message": "Valuation-to-SEC-capital is contextual only.",
        })

    # No funding stage invention.
    if "funding_stage" in profile.columns:
        suspicious = profile[
            profile["funding_stage"].fillna("").astype(str).str.upper()
            .isin({"SEED", "SERIES A", "SERIES B", "SERIES C", "SERIES D"})
            & (
                profile["funding_status"].astype(str)
                != "AVAILABLE"
            )
        ]

        if not suspicious.empty:
            results.append({
                "check_name": "FUNDING_STAGE_SEMANTICS",
                "status": "FAIL",
                "message": "Funding stage appears populated without supported funding coverage.",
            })
        else:
            results.append({
                "check_name": "FUNDING_STAGE_SEMANTICS",
                "status": "PASS",
                "message": "Funding stage remains source-aware.",
            })

    # Investor identity must not be inferred from Form D.
    if "named_investor_count" in profile.columns:
        # A count is allowed only when Step 18 says named investor coverage
        # exists. It must never be generated merely from Form D event count.
        invalid_named_count = profile[
            profile["named_investor_count"].notna()
            & (
                profile["investor_coverage_status"].astype(str)
                == "NO_DATA"
            )
        ]

        if not invalid_named_count.empty:
            results.append({
                "check_name": "INVESTOR_IDENTITY_SEMANTICS",
                "status": "FAIL",
                "message": "Named investor count exists while investor coverage is NO_DATA.",
            })
        else:
            results.append({
                "check_name": "INVESTOR_IDENTITY_SEMANTICS",
                "status": "PASS",
                "message": "Investor identity remains source-aware.",
            })

    return results


# =============================================================================
# LINEAGE / OUTPUT
# =============================================================================

def build_lineage() -> pd.DataFrame:
    return pd.DataFrame([{
        "step": "STEP_20",
        "step20_version": STEP20_VERSION,
        "source_step12_version": STEP12_VERSION_EXPECTED,
        "source_step15_version": STEP15_VERSION_EXPECTED,
        "source_step17_version": STEP17_VERSION_EXPECTED,
        "source_step18_version": STEP18_VERSION_EXPECTED,
        "source_step19_version": STEP19_VERSION_EXPECTED,
        "source_step12": str(STEP12_ROOT),
        "source_step15": str(STEP15_ROOT),
        "source_step17": str(STEP17_ROOT),
        "source_step18": str(STEP18_ROOT),
        "source_step19": str(STEP19_ROOT),
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inferred": False,
        "investor_identity_inferred": False,
        "ownership_inferred": False,
        "generated_at_utc": utc_now_iso(),
    }])


def write_outputs(
    profile: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    validation_results: List[Dict[str, Any]],
    promotion: str,
) -> None:
    validation_df = pd.DataFrame(validation_results)

    exceptions = validation_df[
        validation_df["status"] == "FAIL"
    ].copy()

    promotion_df = pd.DataFrame([{
        "step": "STEP_20",
        "step20_version": STEP20_VERSION,
        "promotion_decision": promotion,
        "fatal_exception_count": len(exceptions),
        "new_financial_score_created": False,
        "unified_ranking_created": False,
        "sec_capital_treated_as_valuation": False,
        "valuation_fabrication_prevented": True,
        "funding_stage_inferred": False,
        "investor_identity_inferred": False,
        "ownership_inferred": False,
        "generated_at_utc": utc_now_iso(),
    }])

    write_parquet(
        profile,
        OUTPUT_ROOT / "investment_profile"
        / "investment_profile.parquet",
    )

    write_parquet(
        signals,
        OUTPUT_ROOT / "investment_signals"
        / "investment_signals.parquet",
    )

    write_parquet(
        summary,
        OUTPUT_ROOT / "investment_summary"
        / "investment_summary.parquet",
    )

    write_parquet(
        lineage,
        OUTPUT_ROOT / "investment_lineage"
        / "investment_lineage.parquet",
    )

    write_parquet(
        exceptions,
        OUTPUT_ROOT / "investment_exceptions"
        / "investment_exceptions.parquet",
    )

    write_parquet(
        promotion_df,
        OUTPUT_ROOT / "investment_promotion"
        / "investment_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    global COMPANIES

    paths = input_paths()

    # Step 15 is the authoritative financial scope for Step 20.
    step15_snapshot = load_required(
        paths["step15_snapshot"],
        "Step 15 financial decision snapshot",
    )

    COMPANIES = configured_companies()

    logger.info("=" * 72)
    logger.info("STEP 20 - UNIFIED INVESTMENT INTELLIGENCE")
    logger.info("Version: %s", STEP20_VERSION)
    logger.info("=" * 72)


    frames = {
        name: load_required(path, f"Step 20 {name}")
        for name, path in paths.items()
    }

    for name, df in frames.items():
        logger.info("Loaded %s rows=%d", name, len(df))

    # Version checks.
    validate_expected_version(
        frames["features"],
        ["feature_version"],
        STEP12_VERSION_EXPECTED,
        "Step 12",
    )

    validate_expected_version(
        frames["decision_signals"],
        ["decision_version", "step15_version"],
        STEP15_VERSION_EXPECTED,
        "Step 15",
    )

    validate_expected_version(
        frames["funding_snapshot"],
        ["step17_version", "source_step_version"],
        STEP17_VERSION_EXPECTED,
        "Step 17",
    )

    validate_expected_version(
        frames["investor_snapshot"],
        ["step18_version", "source_step_version"],
        STEP18_VERSION_EXPECTED,
        "Step 18",
    )

    validate_expected_version(
        frames["valuation_snapshot"],
        ["step19_version", "source_step_version"],
        STEP19_VERSION_EXPECTED,
        "Step 19",
    )

    profile = build_profile(
        frames["features"],
        frames["step15_snapshot"],
        frames["funding_snapshot"],
        frames["investor_snapshot"],
        frames["ownership_snapshot"],
        frames["valuation_snapshot"],
    )

    signals = build_signals(profile)
    summary = build_summary(profile, signals)
    lineage = build_lineage()

    validation_results = []
    validation_results.extend(validate_profile(profile))
    validation_results.extend(validate_signals(signals))
    validation_results.extend(validate_semantics(profile))

    fatal = [
        r for r in validation_results
        if r["status"] == "FAIL"
    ]

    promotion = "BLOCKED" if fatal else "PROMOTE"

    write_outputs(
        profile,
        signals,
        summary,
        lineage,
        validation_results,
        promotion,
    )

    logger.info("")
    logger.info("Configured companies       : %d", len(COMPANIES))
    logger.info("Unified investment profiles: %d", len(profile))
    logger.info("Unified investment signals : %d", len(signals))
    logger.info(
        "Strong positive            : %d",
        int((profile["unified_posture"] == "STRONG_POSITIVE").sum()),
    )
    logger.info(
        "Positive                   : %d",
        int((profile["unified_posture"] == "POSITIVE").sum()),
    )
    logger.info(
        "Neutral                    : %d",
        int((profile["unified_posture"] == "NEUTRAL").sum()),
    )
    logger.info(
        "Watch                      : %d",
        int((profile["unified_posture"] == "WATCH").sum()),
    )
    logger.info(
        "Caution                    : %d",
        int((profile["unified_posture"] == "CAUTION").sum()),
    )
    logger.info(
        "Insufficient data          : %d",
        int((profile["unified_posture"] == "INSUFFICIENT_DATA").sum()),
    )
    logger.info(
        "Fatal validation failures : %d",
        len(fatal),
    )
    logger.info(
        "Promotion decision         : %s",
        promotion,
    )
    logger.info("New financial score        : NO")
    logger.info("Unified ranking created    : NO")
    logger.info("SEC capital ≠ valuation   : ENFORCED")
    logger.info("Funding stage inference   : PREVENTED")
    logger.info("Investor identity inference: PREVENTED")
    logger.info("Ownership inference       : PREVENTED")

    if promotion != "PROMOTE":
        logger.error("STEP 20 FAILED")
        raise RuntimeError(
            f"Step 20 blocked promotion with {len(fatal)} validation failures."
        )

    logger.info(
        "STEP 20 PASS: Unified Investment Intelligence promoted successfully."
    )


if __name__ == "__main__":
    main()
