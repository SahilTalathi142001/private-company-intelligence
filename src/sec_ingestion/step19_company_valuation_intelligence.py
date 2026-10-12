"""
STEP 19 - COMPANY VALUATION INTELLIGENCE

Purpose
-------
Create a source-aware valuation layer without inventing private-company
valuations.

Valuation hierarchy
-------------------
1. REPORTED_VALUATION
   Explicit valuation supplied by a trusted source/enrichment dataset.

2. TRANSACTION_IMPLIED_VALUATION
   Explicit transaction amount plus ownership/equity percentage from an
   enrichment source, where the implied valuation is mathematically derived.

3. SEC_EXEMPT_OFFERING_CAPITAL
   Step 17 capital raised / amount sold. This is NOT a valuation and is kept
   as contextual capital information only.

4. NO_DATA
   No defensible valuation observation is available.

The pipeline never treats:
- total offering amount as company valuation
- amount sold as company valuation
- investor count as valuation
- funding stage as valuation
- missing valuation as zero

Optional enrichment
-------------------
data/silver/valuation/company_valuations.parquet

Expected enrichment fields:
    cik
    valuation_amount_usd

Optional fields:
    valuation_date
    valuation_type
    valuation_source
    funding_event_id
    offering_id
    equity_pct
    transaction_amount_usd
    confidence
    currency

No optional enrichment source is a valid state. In that case the Gold layer
is promoted with NO_DATA valuation status and explicit source limitations.

Outputs
-------
data/gold/valuation_intelligence/
    valuation_events/
    valuation_event_history/
    company_valuation_snapshot/
    valuation_signals/
    valuation_summary/
    valuation_lineage/
    valuation_exceptions/
    valuation_promotion/

Run
---
python ./src/sec_ingestion/step19_company_valuation_intelligence.py
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
COMPANY_MASTER_PATH = (
    DATA_ROOT / "silver" / "company" / "company.parquet"
)

STEP17_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"
STEP18_ROOT = DATA_ROOT / "gold" / "investor_ownership_intelligence"

VALUATION_SILVER_ROOT = DATA_ROOT / "silver" / "valuation"
VALUATION_ENRICHMENT_PATH = (
    VALUATION_SILVER_ROOT / "company_valuations.parquet"
)

OUTPUT_ROOT = DATA_ROOT / "gold" / "valuation_intelligence"

STEP17_VERSION_EXPECTED = "3.0.1"
STEP18_VERSION_EXPECTED = "18.0.0"
STEP19_VERSION = "19.0.0"

def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(10) if digits else ""

def configured_companies() -> list[str]:
    """Resolve the authoritative active production company scope."""
    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))


COMPANIES = configured_companies()

VALID_VALUATION_TYPES = {
    "REPORTED_VALUATION",
    "TRANSACTION_IMPLIED_VALUATION",
}

VALID_VALUATION_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_CONFIDENCE = {
    "HIGH",
    "MEDIUM",
    "LOW",
    "UNKNOWN",
}

VALID_SIGNAL_TYPES = {
    "VALUATION_COVERAGE",
    "VALUATION_FRESHNESS",
    "VALUATION_TO_CAPITAL",
    "VALUATION_DATA_LIMITED",
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step19")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = " ".join(str(value).split()).strip()
    return text or None


def numeric(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_bool(value: Any) -> Optional[bool]:
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


def parse_date(value: Any) -> Optional[pd.Timestamp]:
    try:
        result = pd.to_datetime(value, errors="coerce")
        return result if pd.notna(result) else None
    except Exception:
        return None


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def file_sha256(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_mtime_iso(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return datetime.fromtimestamp(
        path.stat().st_mtime,
        tz=timezone.utc,
    ).isoformat()


# =============================================================================
# INPUT PATHS
# =============================================================================

def step17_paths() -> Dict[str, Path]:
    return {
        "events": STEP17_ROOT / "funding_events" / "funding_events.parquet",
        "history": STEP17_ROOT / "funding_event_history" / "funding_event_history.parquet",
        "snapshot": STEP17_ROOT / "company_funding_snapshot" / "company_funding_snapshot.parquet",
    }


def step18_paths() -> Dict[str, Path]:
    return {
        "snapshot": STEP18_ROOT / "company_investor_snapshot" / "company_investor_snapshot.parquet",
        "ownership": STEP18_ROOT / "ownership_snapshot" / "ownership_snapshot.parquet",
    }


def load_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required {label} artifact not found: {path}"
        )
    return pd.read_parquet(path)


# =============================================================================
# OPTIONAL VALUATION ENRICHMENT
# =============================================================================

def load_valuation_enrichment() -> Tuple[pd.DataFrame, str]:
    if not VALUATION_ENRICHMENT_PATH.exists():
        logger.info(
            "No optional valuation enrichment source found. "
            "Valuation will remain NO_DATA."
        )
        return pd.DataFrame(), "NONE"

    df = pd.read_parquet(VALUATION_ENRICHMENT_PATH)

    required = {"cik", "valuation_amount_usd"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Valuation enrichment missing required columns: {sorted(missing)}"
        )

    out = df.copy()
    out["cik"] = out["cik"].map(normalize_cik)
    out["valuation_amount_usd"] = pd.to_numeric(
        out["valuation_amount_usd"],
        errors="coerce",
    )

    allowed = {normalize_cik(c) for c in COMPANIES}
    out = out[
        out["cik"].isin(allowed)
        & out["valuation_amount_usd"].notna()
    ].copy()

    if "valuation_date" in out.columns:
        out["valuation_date"] = pd.to_datetime(
            out["valuation_date"],
            errors="coerce",
        )

    if "valuation_type" not in out.columns:
        out["valuation_type"] = "REPORTED_VALUATION"

    out["valuation_type"] = (
        out["valuation_type"]
        .map(clean_text)
        .fillna("REPORTED_VALUATION")
        .str.upper()
    )

    invalid_types = (
        set(out["valuation_type"]) - VALID_VALUATION_TYPES
    )
    if invalid_types:
        raise ValueError(
            f"Invalid valuation_type values: {sorted(invalid_types)}"
        )

    if "valuation_source" not in out.columns:
        out["valuation_source"] = "EXTERNAL_ENRICHMENT"

    out["valuation_source"] = (
        out["valuation_source"]
        .map(clean_text)
        .fillna("EXTERNAL_ENRICHMENT")
    )

    if "confidence" not in out.columns:
        out["confidence"] = "UNKNOWN"

    out["confidence"] = (
        out["confidence"]
        .map(clean_text)
        .fillna("UNKNOWN")
        .str.upper()
    )

    invalid_confidence = (
        set(out["confidence"]) - VALID_CONFIDENCE
    )
    if invalid_confidence:
        raise ValueError(
            f"Invalid valuation confidence values: {sorted(invalid_confidence)}"
        )

    for col in ["equity_pct", "transaction_amount_usd"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "equity_pct" in out.columns:
        bad = out[
            out["equity_pct"].notna()
            & (
                (out["equity_pct"] < 0)
                | (out["equity_pct"] > 100)
            )
        ]
        if not bad.empty:
            raise ValueError(
                "Valuation enrichment contains equity_pct outside 0-100."
            )

    logger.info(
        "Valuation enrichment loaded: %s rows from %s",
        len(out),
        VALUATION_ENRICHMENT_PATH,
    )

    return out.reset_index(drop=True), "EXTERNAL_ENRICHMENT"


# =============================================================================
# TRANSACTION-IMPLIED VALUATION
# =============================================================================

def derive_transaction_implied_valuation(
    df: pd.DataFrame,
) -> pd.DataFrame:
    out = df.copy()

    if "valuation_type" not in out.columns:
        out["valuation_type"] = "REPORTED_VALUATION"

    transaction = (
        pd.to_numeric(
            out.get(
                "transaction_amount_usd",
                pd.Series(index=out.index, dtype=float),
            ),
            errors="coerce",
        )
    )
    equity_pct = (
        pd.to_numeric(
            out.get(
                "equity_pct",
                pd.Series(index=out.index, dtype=float),
            ),
            errors="coerce",
        )
    )

    eligible = (
        transaction.notna()
        & transaction.gt(0)
        & equity_pct.notna()
        & equity_pct.gt(0)
        & equity_pct.le(100)
    )

    out.loc[eligible, "valuation_amount_usd"] = (
        transaction[eligible]
        / (equity_pct[eligible] / 100.0)
    )
    out.loc[eligible, "valuation_type"] = (
        "TRANSACTION_IMPLIED_VALUATION"
    )
    out.loc[eligible, "valuation_derivation_method"] = (
        "TRANSACTION_AMOUNT_DIVIDED_BY_EQUITY_PERCENT"
    )

    out.loc[
        ~eligible & out["valuation_amount_usd"].notna(),
        "valuation_derivation_method",
    ] = out.loc[
        ~eligible & out["valuation_amount_usd"].notna(),
        "valuation_derivation_method",
    ].fillna("SOURCE_REPORTED")

    return out


# =============================================================================
# VALUATION EVENTS
# =============================================================================

def build_valuation_events(
    enrichment: pd.DataFrame,
    source: str,
) -> pd.DataFrame:
    columns = [
        "valuation_event_id",
        "cik",
        "funding_event_id",
        "offering_id",
        "valuation_date",
        "valuation_amount_usd",
        "valuation_type",
        "valuation_source",
        "confidence",
        "transaction_amount_usd",
        "equity_pct",
        "valuation_derivation_method",
        "valuation_status",
        "source_scope",
        "step_version",
        "generated_at_utc",
    ]

    if enrichment.empty:
        return pd.DataFrame(columns=columns)

    df = derive_transaction_implied_valuation(enrichment.copy())

    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        cik = normalize_cik(row.get("cik"))
        amount = numeric(row.get("valuation_amount_usd"))

        if not cik or amount is None or amount <= 0:
            continue

        date = parse_date(row.get("valuation_date"))
        funding_event_id = clean_text(row.get("funding_event_id"))
        offering_id = clean_text(row.get("offering_id"))

        identity = "|".join([
            cik,
            clean_text(row.get("valuation_source")) or source,
            clean_text(row.get("valuation_type")) or "REPORTED_VALUATION",
            str(date.date()) if date is not None else "NO_DATE",
            str(amount),
            funding_event_id or "",
            offering_id or "",
        ])

        event_id = hashlib.sha256(
            identity.encode("utf-8")
        ).hexdigest()[:32]

        rows.append({
            "valuation_event_id": event_id,
            "cik": cik,
            "funding_event_id": funding_event_id,
            "offering_id": offering_id,
            "valuation_date": date,
            "valuation_amount_usd": amount,
            "valuation_type": clean_text(
                row.get("valuation_type")
            ) or "REPORTED_VALUATION",
            "valuation_source": clean_text(
                row.get("valuation_source")
            ) or source,
            "confidence": clean_text(
                row.get("confidence")
            ) or "UNKNOWN",
            "transaction_amount_usd": numeric(
                row.get("transaction_amount_usd")
            ),
            "equity_pct": numeric(row.get("equity_pct")),
            "valuation_derivation_method": clean_text(
                row.get("valuation_derivation_method")
            ) or "SOURCE_REPORTED",
            "valuation_status": "AVAILABLE",
            "source_scope": "EXTERNAL_ENRICHMENT",
            "step_version": STEP19_VERSION,
            "generated_at_utc": utc_now_iso(),
        })

    return pd.DataFrame(rows, columns=columns).drop_duplicates(
        subset=["valuation_event_id"],
        keep="last",
    )


# =============================================================================
# EVENT HISTORY
# =============================================================================

def build_valuation_event_history(
    valuation_events: pd.DataFrame,
) -> pd.DataFrame:
    if valuation_events.empty:
        return pd.DataFrame(
            columns=valuation_events.columns.tolist()
        )

    history = valuation_events.copy()
    history["history_status"] = "OBSERVATION"
    history["history_recorded_at_utc"] = utc_now_iso()
    return history


# =============================================================================
# COMPANY SNAPSHOT
# =============================================================================

def build_company_snapshot(
    enrichment: pd.DataFrame,
    valuation_events: pd.DataFrame,
    funding_snapshot: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    configured = [normalize_cik(c) for c in COMPANIES]

    for cik in configured:
        events = (
            valuation_events[
                valuation_events["cik"].map(normalize_cik) == cik
            ].copy()
            if not valuation_events.empty
            else pd.DataFrame()
        )

        funding = funding_snapshot[
            funding_snapshot["cik"].map(normalize_cik) == cik
        ] if not funding_snapshot.empty else pd.DataFrame()

        if events.empty:
            latest_amount = None
            latest_date = None
            latest_type = None
            latest_source = None
            latest_event_id = None
            valuation_status = "NO_DATA"
            quality = "NO_DATA"
            confidence = "UNKNOWN"
        else:
            sort_cols = []
            if "valuation_date" in events.columns:
                sort_cols.append("valuation_date")
            if sort_cols:
                events = events.sort_values(
                    sort_cols,
                    na_position="first",
                )

            latest = events.iloc[-1]
            latest_amount = numeric(
                latest.get("valuation_amount_usd")
            )
            latest_date = parse_date(
                latest.get("valuation_date")
            )
            latest_type = clean_text(
                latest.get("valuation_type")
            )
            latest_source = clean_text(
                latest.get("valuation_source")
            )
            latest_event_id = clean_text(
                latest.get("valuation_event_id")
            )
            valuation_status = "AVAILABLE"
            quality = "COMPLETE"
            confidence = clean_text(
                latest.get("confidence")
            ) or "UNKNOWN"

        capital_raised = None
        if not funding.empty and "total_sec_capital_raised_usd" in funding.columns:
            capital_raised = numeric(
                funding.iloc[0]["total_sec_capital_raised_usd"]
            )

        valuation_to_capital = None
        if (
            latest_amount is not None
            and capital_raised is not None
            and capital_raised > 0
        ):
            valuation_to_capital = (
                latest_amount / capital_raised
            )

        rows.append({
            "cik": cik,
            "valuation_event_count": int(len(events)),
            "latest_valuation_event_id": latest_event_id,
            "latest_valuation_amount_usd": latest_amount,
            "latest_valuation_date": latest_date,
            "latest_valuation_type": latest_type,
            "latest_valuation_source": latest_source,
            "valuation_status": valuation_status,
            "valuation_data_quality": quality,
            "valuation_confidence": confidence,
            "sec_capital_context_usd": capital_raised,
            "valuation_to_sec_capital_ratio": valuation_to_capital,
            "valuation_data_scope": (
                "EXTERNAL_VALUATION_ENRICHMENT"
                if valuation_status == "AVAILABLE"
                else "NO_VALUATION_SOURCE"
            ),
            "valuation_fabrication_prevented": True,
            "step_version": STEP19_VERSION,
            "generated_at_utc": utc_now_iso(),
        })

    return pd.DataFrame(rows)


# =============================================================================
# SIGNALS
# =============================================================================

def build_signals(snapshot: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for _, row in snapshot.iterrows():
        cik = normalize_cik(row["cik"])
        status = clean_text(row["valuation_status"])

        coverage_value = (
            1.0 if status == "AVAILABLE" else None
        )

        ratio = numeric(
            row.get("valuation_to_sec_capital_ratio")
        )

        rows.append({
            "cik": cik,
            "signal_type": "VALUATION_COVERAGE",
            "signal_value": coverage_value,
            "signal_unit": "BOOLEAN_NUMERIC",
            "signal_status": (
                "AVAILABLE" if coverage_value is not None else "NO_DATA"
            ),
            "signal_data_quality": (
                "COMPLETE" if coverage_value is not None else "NO_DATA"
            ),
            "source_scope": "EXTERNAL_VALUATION_ENRICHMENT",
            "step_version": STEP19_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "VALUATION_FRESHNESS",
            "signal_value": (
                row.get("latest_valuation_date")
                if status == "AVAILABLE"
                else None
            ),
            "signal_unit": "DATE",
            "signal_status": (
                "AVAILABLE"
                if status == "AVAILABLE"
                and pd.notna(row.get("latest_valuation_date"))
                else "NO_DATA"
            ),
            "signal_data_quality": (
                "COMPLETE"
                if status == "AVAILABLE"
                and pd.notna(row.get("latest_valuation_date"))
                else "NO_DATA"
            ),
            "source_scope": "EXTERNAL_VALUATION_ENRICHMENT",
            "step_version": STEP19_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "VALUATION_TO_CAPITAL",
            "signal_value": ratio,
            "signal_unit": "RATIO",
            "signal_status": (
                "AVAILABLE" if ratio is not None else "NO_DATA"
            ),
            "signal_data_quality": (
                "COMPLETE" if ratio is not None else "NO_DATA"
            ),
            "source_scope": "VALUATION_PLUS_SEC_CAPITAL_CONTEXT",
            "step_version": STEP19_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "VALUATION_DATA_LIMITED",
            "signal_value": (
                False if status == "AVAILABLE" else True
            ),
            "signal_unit": "BOOLEAN",
            "signal_status": "AVAILABLE",
            "signal_data_quality": "COMPLETE",
            "source_scope": "VALUATION_SOURCE_COVERAGE",
            "step_version": STEP19_VERSION,
        })

    return pd.DataFrame(rows)


# =============================================================================
# SUMMARY / LINEAGE / EXCEPTIONS
# =============================================================================

def build_summary(
    valuation_events: pd.DataFrame,
    snapshot: pd.DataFrame,
    source: str,
) -> pd.DataFrame:
    available = int(
        snapshot["valuation_status"].eq("AVAILABLE").sum()
    ) if not snapshot.empty else 0

    return pd.DataFrame([{
        "step": "STEP_19",
        "step_name": "Company Valuation Intelligence",
        "step_version": STEP19_VERSION,
        "step17_version": STEP17_VERSION_EXPECTED,
        "step18_version": STEP18_VERSION_EXPECTED,
        "configured_companies": len(COMPANIES),
        "valuation_event_rows": len(valuation_events),
        "companies_with_valuation": available,
        "valuation_source_available": source != "NONE",
        "valuation_source": source,
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "source_completeness": (
            "ENRICHMENT_AVAILABLE"
            if source != "NONE"
            else "LIMITED"
        ),
        "generated_at_utc": utc_now_iso(),
    }])


def build_lineage(
    valuation_events: pd.DataFrame,
    source: str,
) -> pd.DataFrame:
    rows = [{
        "step": "STEP_19",
        "step_version": STEP19_VERSION,
        "source_step": "STEP_17",
        "source_step_version": STEP17_VERSION_EXPECTED,
        "source_step18_version": STEP18_VERSION_EXPECTED,
        "source_scope": "SEC_EXEMPT_OFFERING_PLUS_OPTIONAL_VALUATION",
        "valuation_enrichment_source": source,
        "valuation_event_rows": len(valuation_events),
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "generated_at_utc": utc_now_iso(),
    }]

    rows.append({
        "step": "STEP_19",
        "step_version": STEP19_VERSION,
        "lineage_type": "VALUATION_SEMANTICS",
        "description": (
            "SEC Form D capital amounts are contextual capital observations "
            "and are never promoted to company valuation."
        ),
        "generated_at_utc": utc_now_iso(),
    })

    rows.append({
        "step": "STEP_19",
        "step_version": STEP19_VERSION,
        "lineage_type": "VALUATION_DERIVATION",
        "description": (
            "Transaction-implied valuation is derived only when an explicit "
            "transaction amount and equity percentage are supplied."
        ),
        "generated_at_utc": utc_now_iso(),
    })

    return pd.DataFrame(rows)


def build_exceptions(source: str) -> pd.DataFrame:
    if source == "NONE":
        return pd.DataFrame([{
            "severity": "WARNING",
            "exception_code": "VALUATION_SOURCE_UNAVAILABLE",
            "cik": None,
            "message": (
                "No optional valuation enrichment source is available. "
                "Company valuation remains NO_DATA."
            ),
            "step_version": STEP19_VERSION,
            "created_at_utc": utc_now_iso(),
        }])

    return pd.DataFrame(columns=[
        "severity",
        "exception_code",
        "cik",
        "message",
        "step_version",
        "created_at_utc",
    ])


# =============================================================================
# VALIDATION / PROMOTION
# =============================================================================

def validate_and_promote(
    valuation_events: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
) -> Tuple[str, List[str]]:
    failures: List[str] = []
    configured = {normalize_cik(c) for c in COMPANIES}

    if set(snapshot["cik"].map(normalize_cik)) != configured:
        failures.append("SNAPSHOT_CIK_COVERAGE_MISMATCH")

    if snapshot["cik"].map(normalize_cik).duplicated().any():
        failures.append("DUPLICATE_SNAPSHOT_CIK")

    if not valuation_events.empty:
        required = {
            "valuation_event_id",
            "cik",
            "valuation_amount_usd",
            "valuation_type",
            "valuation_source",
            "valuation_status",
        }
        missing = required - set(valuation_events.columns)
        if missing:
            failures.append(
                f"VALUATION_EVENT_SCHEMA_MISSING:{sorted(missing)}"
            )

        if valuation_events["valuation_event_id"].duplicated().any():
            failures.append("DUPLICATE_VALUATION_EVENT_ID")

        amounts = pd.to_numeric(
            valuation_events["valuation_amount_usd"],
            errors="coerce",
        )
        if amounts.isna().any() or (amounts <= 0).any():
            failures.append("INVALID_VALUATION_AMOUNT")

        invalid_types = (
            set(valuation_events["valuation_type"].dropna().astype(str))
            - VALID_VALUATION_TYPES
        )
        if invalid_types:
            failures.append(
                f"INVALID_VALUATION_TYPES:{sorted(invalid_types)}"
            )

    if signals[["cik", "signal_type"]].duplicated().any():
        failures.append("DUPLICATE_SIGNAL_KEY")

    invalid_signal_types = (
        set(signals["signal_type"].dropna().astype(str))
        - VALID_SIGNAL_TYPES
    )
    if invalid_signal_types:
        failures.append(
            f"INVALID_SIGNAL_TYPES:{sorted(invalid_signal_types)}"
        )

    # Critical NO_DATA rule.
    for _, row in signals.iterrows():
        if clean_text(row["signal_status"]) == "NO_DATA":
            value = row.get("signal_value")
            if value is not None:
                try:
                    if not pd.isna(value):
                        failures.append(
                            f"NO_DATA_WITH_SIGNAL_VALUE:{row['cik']}:{row['signal_type']}"
                        )
                except Exception:
                    failures.append(
                        f"NO_DATA_WITH_SIGNAL_VALUE:{row['cik']}:{row['signal_type']}"
                    )

    # Valuation must never be derived from SEC capital alone.
    for _, row in snapshot.iterrows():
        if safe_bool(row["valuation_fabrication_prevented"]) is not True:
            failures.append(
                f"VALUATION_FABRICATION_CONTROL_DISABLED:{row['cik']}"
            )

    return (
        ("BLOCKED", failures)
        if failures
        else ("PROMOTE", [])
    )


def write_outputs(
    valuation_events: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    exceptions: pd.DataFrame,
    promotion: str,
) -> None:
    write_parquet(
        valuation_events,
        OUTPUT_ROOT / "valuation_events" / "valuation_events.parquet",
    )
    write_parquet(
        history,
        OUTPUT_ROOT / "valuation_event_history" / "valuation_event_history.parquet",
    )
    write_parquet(
        snapshot,
        OUTPUT_ROOT / "company_valuation_snapshot" / "company_valuation_snapshot.parquet",
    )
    write_parquet(
        signals,
        OUTPUT_ROOT / "valuation_signals" / "valuation_signals.parquet",
    )
    write_parquet(
        summary,
        OUTPUT_ROOT / "valuation_summary" / "valuation_summary.parquet",
    )
    write_parquet(
        lineage,
        OUTPUT_ROOT / "valuation_lineage" / "valuation_lineage.parquet",
    )
    write_parquet(
        exceptions,
        OUTPUT_ROOT / "valuation_exceptions" / "valuation_exceptions.parquet",
    )

    promotion_df = pd.DataFrame([{
        "step": "STEP_19",
        "step_version": STEP19_VERSION,
        "promotion_decision": promotion,
        "valuation_fabrication_prevented": True,
        "sec_capital_treated_as_valuation": False,
        "generated_at_utc": utc_now_iso(),
    }])

    write_parquet(
        promotion_df,
        OUTPUT_ROOT / "valuation_promotion" / "valuation_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 19 - COMPANY VALUATION INTELLIGENCE")
    logger.info("Version: %s", STEP19_VERSION)
    logger.info("=" * 72)
    logger.info("Configured companies: %s", len(COMPANIES))
    logger.info("Step 17 dependency: %s", STEP17_VERSION_EXPECTED)
    logger.info("Step 18 dependency: %s", STEP18_VERSION_EXPECTED)
    logger.info("Output: %s", OUTPUT_ROOT)

    step17 = {
        name: load_required(path, f"Step 17 {name}")
        for name, path in step17_paths().items()
    }

    # Step 18 is a semantic dependency for investor/ownership context and
    # version lineage. The valuation calculation itself does not infer
    # valuation from investor data.
    step18 = {
        name: load_required(path, f"Step 18 {name}")
        for name, path in step18_paths().items()
    }

    logger.info(
        "Loaded Step 17 funding events=%d",
        len(step17["events"]),
    )
    logger.info(
        "Loaded Step 17 funding history=%d",
        len(step17["history"]),
    )
    logger.info(
        "Loaded Step 17 funding snapshot=%d",
        len(step17["snapshot"]),
    )
    logger.info(
        "Loaded Step 18 investor snapshot=%d",
        len(step18["snapshot"]),
    )
    logger.info(
        "Loaded Step 18 ownership snapshot=%d",
        len(step18["ownership"]),
    )

    enrichment, source = load_valuation_enrichment()

    valuation_events = build_valuation_events(
        enrichment,
        source,
    )
    history = build_valuation_event_history(
        valuation_events,
    )
    snapshot = build_company_snapshot(
        enrichment,
        valuation_events,
        step17["snapshot"],
    )
    signals = build_signals(snapshot)
    summary = build_summary(
        valuation_events,
        snapshot,
        source,
    )
    lineage = build_lineage(
        valuation_events,
        source,
    )
    exceptions = build_exceptions(source)

    promotion, failures = validate_and_promote(
        valuation_events,
        snapshot,
        signals,
    )

    if failures:
        for failure in failures:
            logger.error("FATAL: %s", failure)

    write_outputs(
        valuation_events,
        history,
        snapshot,
        signals,
        summary,
        lineage,
        exceptions,
        promotion,
    )

    companies_with_valuation = int(
        snapshot["valuation_status"].eq("AVAILABLE").sum()
    )

    logger.info("")
    logger.info("Configured companies       : %d", len(COMPANIES))
    logger.info("Valuation event rows       : %d", len(valuation_events))
    logger.info(
        "Companies with valuation   : %d",
        companies_with_valuation,
    )
    logger.info(
        "Valuation source           : %s",
        (
            "EXTERNAL ENRICHMENT"
            if source != "NONE"
            else "NONE (expected until valuation enrichment is added)"
        ),
    )
    logger.info(
        "SEC capital used as valuation: NO"
    )
    logger.info(
        "Valuation fabrication      : PREVENTED"
    )
    logger.info("Fatal exceptions           : %d", len(failures))
    logger.info("Promotion decision         : %s", promotion)

    if source == "NONE":
        logger.warning(
            "No optional valuation enrichment source is available."
        )

    if promotion == "PROMOTE":
        logger.info(
            "STEP 19 PASS: Valuation Gold promoted with explicit "
            "source-aware semantics."
        )
    else:
        logger.error("STEP 19 FAILED")
        raise RuntimeError(
            f"Step 19 promotion blocked: {failures}"
        )


if __name__ == "__main__":
    main()
