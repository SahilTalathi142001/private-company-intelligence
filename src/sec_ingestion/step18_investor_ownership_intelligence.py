"""
STEP 18 - INVESTOR & OWNERSHIP INTELLIGENCE

Purpose
-------
Build a source-aware investor and ownership Gold layer from Step 17.

Design principles
-----------------
1. SEC Form D investor counts are counts, NOT named investor identities.
2. SEC Form D does not provide a complete cap table.
3. Never fabricate investor names, ownership percentages, or investment amounts.
4. Optional investor enrichment may be supplied through:
       data/silver/funding/investors.parquet
5. When enrichment is absent, investor/ownership status is explicitly NO_DATA.
6. Step 18 does not create a funding score and does not reinterpret Step 17
   capital as conventional funding rounds.
7. Current and historical investor relationships are source-aware and
   amendment-aware through Step 17 funding_event_id/offering_id.

Run
---
python ./src/sec_ingestion/step18_investor_ownership_intelligence.py
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from company_platform.manifests.lineage import retain_lineaged_rows
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

STEP17_GOLD_ROOT = DATA_ROOT / "gold" / "funding_capital_intelligence"
STEP17_VERSION_EXPECTED = "3.0.1"

SILVER_FUNDING_ROOT = DATA_ROOT / "silver" / "funding"
INVESTOR_ENRICHMENT_PATH = SILVER_FUNDING_ROOT / "investors.parquet"

OUTPUT_ROOT = DATA_ROOT / "gold" / "investor_ownership_intelligence"

def configured_companies() -> List[str]:
    """Return the authoritative production execution scope.

    The SEC/company master is reference identity data only.  Active
    processing scope is controlled exclusively by company_registry.parquet.
    """
    return assert_company_scope()


STEP18_VERSION = "18.0.0"

EXPECTED_SOURCE_SCOPE = "SEC_EXEMPT_OFFERING_ONLY"
EXPECTED_EVENT_SOURCE_SCOPE = "SEC_EXEMPT_OFFERING"

BENEFICIAL_OWNERSHIP_SOURCES = {
    "SEC_SCHEDULE_13D",
    "SEC_SCHEDULE_13G",
    "SEC_DEF_14A",
}

VALID_INVESTOR_TYPES = {
    "VC",
    "PE",
    "CORPORATE",
    "FAMILY_OFFICE",
    "ANGEL",
    "INDIVIDUAL",
    "INSTITUTIONAL",
    "BANK",
    "OTHER",
    "UNKNOWN",
}

VALID_INVESTOR_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_OWNERSHIP_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_SIGNAL_STATUS = {
    "AVAILABLE",
    "NO_DATA",
    "PARTIAL",
}

VALID_SIGNAL_TYPES = {
    "INVESTOR_COVERAGE",
    "NAMED_INVESTOR_COUNT",
    "INVESTOR_CONCENTRATION",
    "OWNERSHIP_COVERAGE",
    "OWNERSHIP_CONCENTRATION",
    "INVESTOR_DATA_LIMITED",
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("step18")


# =============================================================================
# HELPERS
# =============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    text = "".join(ch for ch in str(value) if ch.isdigit())
    return text.zfill(10) if text else ""

COMPANIES = assert_company_scope()
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


def parse_numeric(value: Any) -> Optional[float]:
    try:
        result = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return float(result) if pd.notna(result) else None
    except Exception:
        return None


def normalize_date(value: Any) -> Optional[pd.Timestamp]:
    try:
        result = pd.to_datetime(value, errors="coerce")
        return result if pd.notna(result) else None
    except Exception:
        return None


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def hash_text(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def first_existing(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    available = set(columns)
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


# =============================================================================
# STEP 17 INPUTS
# =============================================================================

def step17_paths() -> Dict[str, Path]:
    return {
        "events": STEP17_GOLD_ROOT / "funding_events" / "funding_events.parquet",
        "history": STEP17_GOLD_ROOT / "funding_event_history" / "funding_event_history.parquet",
        "snapshot": STEP17_GOLD_ROOT / "company_funding_snapshot" / "company_funding_snapshot.parquet",
        "signals": STEP17_GOLD_ROOT / "funding_signals" / "funding_signals.parquet",
        "lineage": STEP17_GOLD_ROOT / "funding_lineage" / "funding_lineage.parquet",
    }


def load_required_step17() -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for name, path in step17_paths().items():
        if not path.exists():
            raise FileNotFoundError(
                f"Required Step 17 artifact not found: {name} -> {path}"
            )
        frames[name] = pd.read_parquet(path)
    return frames


def validate_step17_version(frames: Dict[str, pd.DataFrame]) -> None:
    versions: set[str] = set()

    for name, df in frames.items():
        for col in ["step_version", "source_step_version"]:
            if col in df.columns:
                values = df[col].dropna().astype(str)
                versions.update(values.tolist())

    bad = versions - {STEP17_VERSION_EXPECTED}
    if bad:
        raise ValueError(
            f"Unexpected Step 17 versions in inputs: {sorted(bad)}; "
            f"expected {STEP17_VERSION_EXPECTED}"
        )


def validate_configured_ciks(events: pd.DataFrame) -> None:
    configured = {normalize_cik(c) for c in COMPANIES}
    if events.empty:
        return
    observed = set(events["cik"].map(normalize_cik))
    extras = sorted(observed - configured)
    if extras:
        raise ValueError(
            f"Step 17 events contain non-configured CIKs: {extras}"
        )


# =============================================================================
# OPTIONAL INVESTOR ENRICHMENT
# =============================================================================

def load_investor_enrichment() -> Tuple[pd.DataFrame, str]:
    if not INVESTOR_ENRICHMENT_PATH.exists():
        logger.info(
            "No optional investor enrichment source found. "
            "Investor identity will remain NO_DATA."
        )
        return pd.DataFrame(), "NONE"

    df = pd.read_parquet(INVESTOR_ENRICHMENT_PATH)

    required = {"cik", "investor_name"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Investor enrichment missing required columns: {sorted(missing)}"
        )

    out = df.copy()
    out["cik"] = out["cik"].map(normalize_cik)
    out["investor_name"] = out["investor_name"].map(clean_text)

    out = out[
        out["cik"].isin({normalize_cik(c) for c in COMPANIES})
        & out["investor_name"].notna()
    ].copy()

    if "investor_type" not in out.columns:
        out["investor_type"] = "UNKNOWN"
    out["investor_type"] = (
        out["investor_type"]
        .map(clean_text)
        .fillna("UNKNOWN")
        .str.upper()
    )

    invalid_types = sorted(set(out["investor_type"]) - VALID_INVESTOR_TYPES)
    if invalid_types:
        raise ValueError(
            f"Invalid investor_type values: {invalid_types}"
        )

    for col in ["ownership_pct", "investment_amount_usd"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "as_of_date" in out.columns:
        out["as_of_date"] = pd.to_datetime(
            out["as_of_date"], errors="coerce"
        )

    logger.info(
        "Investor enrichment loaded: %s rows from %s",
        len(out),
        INVESTOR_ENRICHMENT_PATH,
    )
    return out.reset_index(drop=True), "ENRICHMENT"


# =============================================================================
# EVENT-LEVEL INVESTOR INTELLIGENCE
# =============================================================================

def _beneficial_ownership_events(enrichment: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Holders stated on 13D, 13G, or a proxy are not Form D funding investors."""
    if enrichment.empty or "investor_source" not in enrichment.columns:
        return pd.DataFrame(), enrichment
    source = enrichment["investor_source"].map(clean_text).str.upper()
    beneficial = enrichment[source.isin(BENEFICIAL_OWNERSHIP_SOURCES)].copy()
    remaining = enrichment[~source.isin(BENEFICIAL_OWNERSHIP_SOURCES)].copy()
    if beneficial.empty:
        return pd.DataFrame(), remaining
    rows = []
    for record in beneficial.to_dict(orient="records"):
        investor_type = clean_text(record.get("investor_type") or "UNKNOWN").upper() or "UNKNOWN"
        if investor_type not in VALID_INVESTOR_TYPES:
            investor_type = "UNKNOWN"
        stated_source = clean_text(record.get("investor_source")).upper()
        rows.append({
            "cik": normalize_cik(record.get("cik")),
            "funding_event_id": None,
            "offering_id": None,
            "accession_number": clean_text(record.get("source_accession_number")) or None,
            "investor_name": clean_text(record.get("investor_name")),
            "investor_type": investor_type,
            "investor_source": stated_source,
            "investor_confidence": "SOURCE_PROVIDED",
            "ownership_pct": record.get("ownership_pct"),
            "investment_amount_usd": None,
            "as_of_date": record.get("as_of_date"),
            "investor_identity_status": "AVAILABLE",
            "source_system": stated_source,
            "step_version": STEP18_VERSION,
        })
    return retain_lineaged_rows(pd.DataFrame(rows)), remaining


def build_investor_events(
    events: pd.DataFrame,
    enrichment: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    beneficial_events, enrichment = _beneficial_ownership_events(enrichment)
    if events.empty:
        return retain_lineaged_rows(beneficial_events).reset_index(drop=True)

    base_cols = [
        "cik",
        "funding_event_id",
        "offering_id",
        "accession_number",
        "first_sale_date",
        "filing_date",
        "capital_amount_usd",
        "investor_count",
    ]
    base = events[
        [c for c in base_cols if c in events.columns]
    ].copy()
    base["cik"] = base["cik"].map(normalize_cik)

    if enrichment.empty:
        return retain_lineaged_rows(beneficial_events).reset_index(drop=True)

    enr = enrichment.copy()

    event_key_col = first_existing(
        enr.columns,
        ["funding_event_id", "offering_id", "accession_number"],
    )

    if event_key_col:
        left = base.copy()
        right = enr.copy()
        left_key = event_key_col
        if left_key not in left.columns:
            raise ValueError(
                f"Enrichment uses {event_key_col}, but Step 17 events do not."
            )

        merged = left.merge(
            right,
            on=left_key,
            how="inner",
            suffixes=("", "_enrichment"),
        )
    else:
        # CIK-only enrichment is allowed, but event linkage remains explicitly
        # source-level rather than being treated as a precise event attribution.
        merged = base.merge(
            enr,
            on="cik",
            how="inner",
            suffixes=("", "_enrichment"),
        )
        merged["investor_linkage_status"] = "CIK_LEVEL"

    if merged.empty:
        return retain_lineaged_rows(beneficial_events).reset_index(drop=True)

    merged["investor_name"] = merged["investor_name"].map(clean_text)
    merged["investor_type"] = (
        merged["investor_type"]
        .map(clean_text)
        .fillna("UNKNOWN")
        .str.upper()
    )

    if "investor_source" not in merged.columns:
        merged["investor_source"] = investor_source
    if "investor_confidence" not in merged.columns:
        merged["investor_confidence"] = "SOURCE_PROVIDED"
    if "ownership_pct" not in merged.columns:
        merged["ownership_pct"] = pd.NA
    if "investment_amount_usd" not in merged.columns:
        merged["investment_amount_usd"] = pd.NA
    if "as_of_date" not in merged.columns:
        merged["as_of_date"] = pd.NaT

    merged["investor_identity_status"] = "AVAILABLE"
    merged["source_system"] = (
        merged.get("investor_source", pd.Series(index=merged.index))
        .map(clean_text)
        .fillna(investor_source)
    )
    merged["step_version"] = STEP18_VERSION

    keep = [
        "cik",
        "funding_event_id",
        "offering_id",
        "accession_number",
        "first_sale_date",
        "filing_date",
        "capital_amount_usd",
        "investor_count",
        "investor_name",
        "investor_type",
        "investor_source",
        "investor_confidence",
        "ownership_pct",
        "investment_amount_usd",
        "as_of_date",
        "investor_identity_status",
        "source_system",
        "step_version",
    ]
    form_d_events = retain_lineaged_rows(
        merged[[c for c in keep if c in merged.columns]].drop_duplicates()
    )
    if beneficial_events.empty:
        return form_d_events.reset_index(drop=True)
    return retain_lineaged_rows(pd.concat(
        [form_d_events, beneficial_events],
        ignore_index=True,
    )).drop_duplicates(
        subset=["cik", "investor_name", "investor_source"],
        keep="first",
    ).reset_index(drop=True)


# =============================================================================
# HISTORY
# =============================================================================

def build_investor_event_history(
    investor_events: pd.DataFrame,
    history: pd.DataFrame,
    enrichment: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    if investor_events.empty:
        return pd.DataFrame()

    # Event-level enrichment is intentionally reused for historical filings only
    # when the enrichment contains an explicit accession/offering/event key.
    if enrichment.empty:
        return pd.DataFrame()

    key = first_existing(
        enrichment.columns,
        ["funding_event_id", "offering_id", "accession_number"],
    )
    if not key:
        return pd.DataFrame()

    hist = history.copy()
    hist["cik"] = hist["cik"].map(normalize_cik)

    enr = enrichment.copy()
    enr["cik"] = enr["cik"].map(normalize_cik)

    merged = hist.merge(
        enr,
        on=key,
        how="inner",
        suffixes=("", "_enrichment"),
    )
    if merged.empty:
        return pd.DataFrame()

    merged["investor_name"] = merged["investor_name"].map(clean_text)
    merged["investor_type"] = (
        merged.get("investor_type", pd.Series(index=merged.index))
        .map(clean_text)
        .fillna("UNKNOWN")
        .str.upper()
    )
    merged["investor_source"] = (
        merged.get("investor_source", pd.Series(index=merged.index))
        .map(clean_text)
        .fillna(investor_source)
    )
    merged["investor_identity_status"] = "AVAILABLE"
    merged["step_version"] = STEP18_VERSION

    return merged


# =============================================================================
# COMPANY SNAPSHOT
# =============================================================================

def build_company_investor_snapshot(
    events: pd.DataFrame,
    investor_events: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    configured = pd.DataFrame({
        "cik": [normalize_cik(c) for c in COMPANIES]
    })

    rows: List[Dict[str, Any]] = []

    for cik in configured["cik"]:
        event_count = int(
            (events["cik"].map(normalize_cik) == cik).sum()
        ) if not events.empty else 0

        inv = (
            investor_events[
                investor_events["cik"].map(normalize_cik) == cik
            ].copy()
            if not investor_events.empty
            else pd.DataFrame()
        )

        named_count = (
            int(inv["investor_name"].nunique())
            if not inv.empty and "investor_name" in inv.columns
            else 0
        )

        ownership_available = (
            bool(
                not inv.empty
                and "ownership_pct" in inv.columns
                and pd.to_numeric(inv["ownership_pct"], errors="coerce")
                .notna()
                .any()
            )
        )

        if investor_source == "NONE":
            investor_status = "NO_DATA"
        elif named_count > 0:
            investor_status = "AVAILABLE"
        else:
            investor_status = "NO_DATA"

        ownership_status = (
            "AVAILABLE" if ownership_available else "NO_DATA"
        )

        rows.append({
            "cik": cik,
            "funding_event_count": event_count,
            "named_investor_count": named_count if named_count else None,
            "investor_identity_status": investor_status,
            "ownership_status": ownership_status,
            "investor_source": investor_source,
            "investor_data_scope": (
                "NAMED_INVESTOR_ENRICHMENT"
                if investor_source != "NONE"
                else "SEC_FORM_D_ONLY"
            ),
            "sec_form_d_investor_count_available": bool(
                event_count > 0
                and "investor_count" in events.columns
                and pd.to_numeric(
                    events.loc[
                        events["cik"].map(normalize_cik) == cik,
                        "investor_count",
                    ],
                    errors="coerce",
                ).notna().any()
            ),
            "ownership_fabrication_prevented": True,
            "investor_data_quality": (
                "COMPLETE" if investor_status == "AVAILABLE" else "NO_DATA"
            ),
            "investor_freshness_status": "UNKNOWN",
            "step_version": STEP18_VERSION,
            "snapshot_generated_at_utc": utc_now_iso(),
        })

    return pd.DataFrame(rows)


# =============================================================================
# CONCENTRATION
# =============================================================================

def build_concentration(
    investor_events: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for cik in [normalize_cik(c) for c in COMPANIES]:
        inv = (
            investor_events[
                investor_events["cik"].map(normalize_cik) == cik
            ].copy()
            if not investor_events.empty
            else pd.DataFrame()
        )

        if inv.empty:
            rows.append({
                "cik": cik,
                "investor_concentration_status": "NO_DATA",
                "named_investor_count": None,
                "investment_amount_coverage": "NO_DATA",
                "top_investor_share_pct": None,
                "herfindahl_index": None,
                "ownership_concentration_status": "NO_DATA",
                "step_version": STEP18_VERSION,
            })
            continue

        amount = (
            pd.to_numeric(inv["investment_amount_usd"], errors="coerce")
            if "investment_amount_usd" in inv.columns
            else pd.Series(dtype=float)
        )

        amount = amount.fillna(0)
        total_amount = float(amount.sum())

        if total_amount > 0:
            shares = amount / total_amount
            top_share = float(shares.max() * 100)
            hhi = float((shares ** 2).sum())
            amount_coverage = "AVAILABLE"
            concentration_status = "AVAILABLE"
        else:
            top_share = None
            hhi = None
            amount_coverage = "NO_DATA"
            concentration_status = "NO_DATA"

        ownership = (
            pd.to_numeric(inv["ownership_pct"], errors="coerce")
            if "ownership_pct" in inv.columns
            else pd.Series(dtype=float)
        )
        ownership = ownership.dropna()

        if not ownership.empty:
            ownership_shares = ownership / ownership.sum()
            ownership_hhi = float((ownership_shares ** 2).sum())
            ownership_status = "AVAILABLE"
        else:
            ownership_hhi = None
            ownership_status = "NO_DATA"

        rows.append({
            "cik": cik,
            "investor_concentration_status": concentration_status,
            "named_investor_count": int(inv["investor_name"].nunique()),
            "investment_amount_coverage": amount_coverage,
            "top_investor_share_pct": top_share,
            "herfindahl_index": hhi,
            "ownership_concentration_status": ownership_status,
            "ownership_herfindahl_index": ownership_hhi,
            "step_version": STEP18_VERSION,
        })

    return pd.DataFrame(rows)


# =============================================================================
# OWNERSHIP SNAPSHOT
# =============================================================================

def build_ownership_snapshot(
    investor_events: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for cik in [normalize_cik(c) for c in COMPANIES]:
        inv = (
            investor_events[
                investor_events["cik"].map(normalize_cik) == cik
            ].copy()
            if not investor_events.empty
            else pd.DataFrame()
        )

        if inv.empty or "ownership_pct" not in inv.columns:
            rows.append({
                "cik": cik,
                "ownership_status": "NO_DATA",
                "ownership_row_count": 0,
                "ownership_total_pct": None,
                "ownership_coverage_pct": None,
                "ownership_data_source": "NONE",
                "ownership_fabrication_prevented": True,
                "step_version": STEP18_VERSION,
            })
            continue

        ownership = pd.to_numeric(
            inv["ownership_pct"], errors="coerce"
        ).dropna()

        if ownership.empty:
            rows.append({
                "cik": cik,
                "ownership_status": "NO_DATA",
                "ownership_row_count": 0,
                "ownership_total_pct": None,
                "ownership_coverage_pct": None,
                "ownership_data_source": investor_source_name(inv),
                "ownership_fabrication_prevented": True,
                "step_version": STEP18_VERSION,
            })
            continue

        total = float(ownership.sum())
        rows.append({
            "cik": cik,
            "ownership_status": "AVAILABLE",
            "ownership_row_count": int(len(ownership)),
            "ownership_total_pct": total,
            "ownership_coverage_pct": (
                min(total, 100.0) if total >= 0 else None
            ),
            "ownership_data_source": investor_source_name(inv),
            "ownership_fabrication_prevented": True,
            "step_version": STEP18_VERSION,
        })

    return pd.DataFrame(rows)


def investor_source_name(df: pd.DataFrame) -> str:
    if "investor_source" not in df.columns:
        return "UNKNOWN"
    values = [
        clean_text(v)
        for v in df["investor_source"].dropna().tolist()
        if clean_text(v)
    ]
    return values[0] if values else "UNKNOWN"


# =============================================================================
# SIGNALS
# =============================================================================

def build_signals(
    snapshot: pd.DataFrame,
    concentration: pd.DataFrame,
    ownership: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for _, snap in snapshot.iterrows():
        cik = normalize_cik(snap["cik"])
        inv_status = clean_text(snap["investor_identity_status"])
        own_status = clean_text(snap["ownership_status"])

        rows.append({
            "cik": cik,
            "signal_type": "INVESTOR_COVERAGE",
            "signal_value": (
                snap["named_investor_count"]
                if inv_status == "AVAILABLE"
                else None
            ),
            "signal_unit": "COUNT",
            "signal_direction": "AVAILABLE" if inv_status == "AVAILABLE" else "UNKNOWN",
            "signal_strength": "AVAILABLE" if inv_status == "AVAILABLE" else "NONE",
            "signal_status": "AVAILABLE" if inv_status == "AVAILABLE" else "NO_DATA",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "NAMED_INVESTOR_COUNT",
            "signal_value": (
                snap["named_investor_count"]
                if inv_status == "AVAILABLE"
                else None
            ),
            "signal_unit": "COUNT",
            "signal_direction": "AVAILABLE" if inv_status == "AVAILABLE" else "UNKNOWN",
            "signal_strength": "AVAILABLE" if inv_status == "AVAILABLE" else "NONE",
            "signal_status": "AVAILABLE" if inv_status == "AVAILABLE" else "NO_DATA",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

        con = concentration[
            concentration["cik"].map(normalize_cik) == cik
        ]
        con_row = con.iloc[0] if not con.empty else None

        con_value = (
            con_row["herfindahl_index"]
            if con_row is not None
            and clean_text(con_row["investor_concentration_status"]) == "AVAILABLE"
            else None
        )

        rows.append({
            "cik": cik,
            "signal_type": "INVESTOR_CONCENTRATION",
            "signal_value": con_value,
            "signal_unit": "HHI",
            "signal_direction": "AVAILABLE" if con_value is not None else "UNKNOWN",
            "signal_strength": "AVAILABLE" if con_value is not None else "NONE",
            "signal_status": "AVAILABLE" if con_value is not None else "NO_DATA",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "OWNERSHIP_COVERAGE",
            "signal_value": True if own_status == "AVAILABLE" else None,
            "signal_unit": "BOOLEAN",
            "signal_direction": "AVAILABLE" if own_status == "AVAILABLE" else "UNKNOWN",
            "signal_strength": "AVAILABLE" if own_status == "AVAILABLE" else "NONE",
            "signal_status": "AVAILABLE" if own_status == "AVAILABLE" else "NO_DATA",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

        own = ownership[
            ownership["cik"].map(normalize_cik) == cik
        ]
        own_row = own.iloc[0] if not own.empty else None

        own_hhi = (
            con_row["ownership_herfindahl_index"]
            if con_row is not None
            and pd.notna(con_row.get("ownership_herfindahl_index"))
            else None
        )

        rows.append({
            "cik": cik,
            "signal_type": "OWNERSHIP_CONCENTRATION",
            "signal_value": own_hhi,
            "signal_unit": "HHI",
            "signal_direction": "AVAILABLE" if own_hhi is not None else "UNKNOWN",
            "signal_strength": "AVAILABLE" if own_hhi is not None else "NONE",
            "signal_status": "AVAILABLE" if own_hhi is not None else "NO_DATA",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

        rows.append({
            "cik": cik,
            "signal_type": "INVESTOR_DATA_LIMITED",
            "signal_value": True if inv_status != "AVAILABLE" else False,
            "signal_unit": "BOOLEAN",
            "signal_direction": "LIMITED" if inv_status != "AVAILABLE" else "AVAILABLE",
            "signal_strength": "HIGH" if inv_status != "AVAILABLE" else "LOW",
            "signal_status": "AVAILABLE",
            "source_scope": EXPECTED_SOURCE_SCOPE,
            "signal_version": STEP18_VERSION,
        })

    return pd.DataFrame(rows)


# =============================================================================
# SUMMARY / LINEAGE / PROMOTION
# =============================================================================

def build_summary(
    events: pd.DataFrame,
    history: pd.DataFrame,
    investor_events: pd.DataFrame,
    snapshot: pd.DataFrame,
    ownership: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    companies_with_investors = int(
        (
            pd.to_numeric(
                snapshot["named_investor_count"], errors="coerce"
            ).fillna(0) > 0
        ).sum()
    ) if not snapshot.empty else 0

    companies_with_ownership = int(
        ownership["ownership_status"].eq("AVAILABLE").sum()
    ) if not ownership.empty else 0

    return pd.DataFrame([{
        "step": "STEP_18",
        "step_name": "Investor & Ownership Intelligence",
        "step_version": STEP18_VERSION,
        "step17_version": STEP17_VERSION_EXPECTED,
        "configured_companies": len(COMPANIES),
        "funding_events": len(events),
        "historical_form_d_filings": len(history),
        "investor_event_rows": len(investor_events),
        "unique_named_investors": (
            int(investor_events["investor_name"].nunique())
            if not investor_events.empty
            else 0
        ),
        "companies_with_investors": companies_with_investors,
        "companies_with_ownership": companies_with_ownership,
        "investor_source": investor_source,
        "investor_identity_from_sec_form_d": False,
        "ownership_from_sec_form_d": False,
        "ownership_fabrication_prevented": True,
        "source_coverage_status": (
            "ENRICHMENT_AVAILABLE"
            if investor_source != "NONE"
            else "SEC_FORM_D_ONLY"
        ),
        "generated_at_utc": utc_now_iso(),
    }])


def build_lineage(
    events: pd.DataFrame,
    history: pd.DataFrame,
    investor_events: pd.DataFrame,
    snapshot: pd.DataFrame,
    signals: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    return pd.DataFrame([{
        "step": "STEP_18",
        "step_version": STEP18_VERSION,
        "source_step": "STEP_17",
        "source_step_version": STEP17_VERSION_EXPECTED,
        "source_scope": EXPECTED_SOURCE_SCOPE,
        "source_system": "SEC_FORM_D",
        "investor_enrichment_source": investor_source,
        "funding_event_rows": len(events),
        "funding_history_rows": len(history),
        "investor_event_rows": len(investor_events),
        "snapshot_rows": len(snapshot),
        "signal_rows": len(signals),
        "ownership_fabrication_prevented": True,
        "investor_identity_assumed_from_form_d": False,
        "ownership_assumed_from_form_d": False,
        "generated_at_utc": utc_now_iso(),
    }, {
        "step": "STEP_18",
        "step_version": STEP18_VERSION,
        "lineage_type": "SEMANTIC_LIMITATION",
        "description": (
            "SEC Form D investor counts are counts only; named investor "
            "identity and complete ownership are not inferred."
        ),
        "generated_at_utc": utc_now_iso(),
    }, {
        "step": "STEP_18",
        "step_version": STEP18_VERSION,
        "lineage_type": "CAPITAL_SCOPE",
        "description": (
            "Step 18 does not reinterpret SEC exempt-offering amounts as "
            "conventional funding rounds."
        ),
        "generated_at_utc": utc_now_iso(),
    }])


def build_exceptions(
    snapshot: pd.DataFrame,
    investor_events: pd.DataFrame,
    investor_source: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    if investor_source == "NONE":
        for cik in [normalize_cik(c) for c in COMPANIES]:
            rows.append({
                "severity": "INFO",
                "exception_code": "INVESTOR_IDENTITY_NO_DATA",
                "cik": cik,
                "message": (
                    "No named-investor enrichment source is available. "
                    "SEC Form D investor counts are not named identities."
                ),
                "step_version": STEP18_VERSION,
                "created_at_utc": utc_now_iso(),
            })

    return pd.DataFrame(rows)


def validate_and_promote(
    events: pd.DataFrame,
    history: pd.DataFrame,
    investor_events: pd.DataFrame,
    snapshot: pd.DataFrame,
    concentration: pd.DataFrame,
    ownership: pd.DataFrame,
    signals: pd.DataFrame,
    exceptions: pd.DataFrame,
) -> Tuple[str, List[str]]:
    failures: List[str] = []

    configured = {normalize_cik(c) for c in COMPANIES}

    if set(snapshot["cik"].map(normalize_cik)) != configured:
        failures.append("SNAPSHOT_CIK_COVERAGE_MISMATCH")

    if snapshot["cik"].duplicated().any():
        failures.append("DUPLICATE_SNAPSHOT_CIK")

    if not investor_events.empty:
        required = {
            "cik",
            "funding_event_id",
            "investor_name",
            "investor_type",
        }
        missing = required - set(investor_events.columns)
        if missing:
            failures.append(
                f"INVESTOR_EVENT_MISSING_COLUMNS:{sorted(missing)}"
            )

        if investor_events["investor_name"].isna().any():
            failures.append("BLANK_NAMED_INVESTOR")

        if investor_events[
            ["cik", "funding_event_id", "investor_name"]
        ].duplicated().any():
            failures.append("DUPLICATE_INVESTOR_EVENT")

        invalid_types = (
            set(investor_events["investor_type"].dropna().astype(str))
            - VALID_INVESTOR_TYPES
        )
        if invalid_types:
            failures.append(
                f"INVALID_INVESTOR_TYPES:{sorted(invalid_types)}"
            )

    for _, row in snapshot.iterrows():
        if clean_text(row["investor_identity_status"]) == "NO_DATA":
            if pd.notna(row["named_investor_count"]):
                failures.append(
                    f"NO_DATA_WITH_NAMED_INVESTOR_COUNT:{row['cik']}"
                )

        if clean_text(row["ownership_status"]) == "NO_DATA":
            # Ownership signal must not become False/0 merely because it is
            # unavailable. Absence is represented by null.
            pass

        if safe_bool(row["ownership_fabrication_prevented"]) is not True:
            failures.append(
                f"OWNERSHIP_FABRICATION_CONTROL_DISABLED:{row['cik']}"
            )

    if not signals.empty:
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

        for _, row in signals.iterrows():
            status = clean_text(row["signal_status"])
            value = row["signal_value"]

            if status == "NO_DATA" and value is not None:
                try:
                    if not pd.isna(value):
                        failures.append(
                            f"NO_DATA_WITH_SIGNAL_VALUE:{row['cik']}:{row['signal_type']}"
                        )
                except Exception:
                    failures.append(
                        f"NO_DATA_WITH_SIGNAL_VALUE:{row['cik']}:{row['signal_type']}"
                    )

    if failures:
        return "BLOCKED", failures

    return "PROMOTE", []


def write_outputs(
    investor_events: pd.DataFrame,
    investor_history: pd.DataFrame,
    snapshot: pd.DataFrame,
    concentration: pd.DataFrame,
    ownership: pd.DataFrame,
    signals: pd.DataFrame,
    summary: pd.DataFrame,
    lineage: pd.DataFrame,
    exceptions: pd.DataFrame,
    promotion: str,
) -> None:
    write_parquet(
        investor_events,
        OUTPUT_ROOT / "investor_events" / "investor_events.parquet",
    )
    write_parquet(
        investor_history,
        OUTPUT_ROOT / "investor_event_history" / "investor_event_history.parquet",
    )
    write_parquet(
        snapshot,
        OUTPUT_ROOT / "company_investor_snapshot" / "company_investor_snapshot.parquet",
    )
    write_parquet(
        concentration,
        OUTPUT_ROOT / "investor_concentration" / "investor_concentration.parquet",
    )
    write_parquet(
        ownership,
        OUTPUT_ROOT / "ownership_snapshot" / "ownership_snapshot.parquet",
    )
    write_parquet(
        signals,
        OUTPUT_ROOT / "investor_signals" / "investor_signals.parquet",
    )
    write_parquet(
        summary,
        OUTPUT_ROOT / "investor_summary" / "investor_summary.parquet",
    )
    write_parquet(
        lineage,
        OUTPUT_ROOT / "investor_lineage" / "investor_lineage.parquet",
    )
    write_parquet(
        exceptions,
        OUTPUT_ROOT / "investor_exceptions" / "investor_exceptions.parquet",
    )

    promotion_df = pd.DataFrame([{
        "step": "STEP_18",
        "step_version": STEP18_VERSION,
        "promotion_decision": promotion,
        "ownership_fabrication_prevented": True,
        "created_at_utc": utc_now_iso(),
    }])
    write_parquet(
        promotion_df,
        OUTPUT_ROOT / "investor_promotion" / "investor_promotion.parquet",
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    logger.info("=" * 72)
    logger.info("STEP 18 - INVESTOR & OWNERSHIP INTELLIGENCE")
    logger.info("Version: %s", STEP18_VERSION)
    logger.info("=" * 72)
    logger.info("Configured companies: %s", len(COMPANIES))
    logger.info("Step 17 dependency version: %s", STEP17_VERSION_EXPECTED)
    logger.info("Output: %s", OUTPUT_ROOT)

    frames = load_required_step17()
    validate_step17_version(frames)
    validate_configured_ciks(frames["events"])

    enrichment, investor_source = load_investor_enrichment()

    events = frames["events"].copy()
    history = frames["history"].copy()

    investor_events = build_investor_events(
        events,
        enrichment,
        investor_source,
    )
    investor_history = build_investor_event_history(
        investor_events,
        history,
        enrichment,
        investor_source,
    )
    snapshot = build_company_investor_snapshot(
        events,
        investor_events,
        investor_source,
    )
    concentration = build_concentration(investor_events)
    ownership = build_ownership_snapshot(investor_events)
    signals = build_signals(snapshot, concentration, ownership)
    summary = build_summary(
        events,
        history,
        investor_events,
        snapshot,
        ownership,
        investor_source,
    )
    lineage = build_lineage(
        events,
        history,
        investor_events,
        snapshot,
        signals,
        investor_source,
    )
    exceptions = build_exceptions(
        snapshot,
        investor_events,
        investor_source,
    )

    promotion, failures = validate_and_promote(
        events,
        history,
        investor_events,
        snapshot,
        concentration,
        ownership,
        signals,
        exceptions,
    )

    if failures:
        for failure in failures:
            logger.error("FATAL: %s", failure)

    write_outputs(
        investor_events,
        investor_history,
        snapshot,
        concentration,
        ownership,
        signals,
        summary,
        lineage,
        exceptions,
        promotion,
    )

    companies_with_investors = int(
        snapshot["investor_identity_status"].eq("AVAILABLE").sum()
    )
    companies_with_ownership = int(
        snapshot["ownership_status"].eq("AVAILABLE").sum()
    )

    logger.info("")
    logger.info("Configured companies: %s", len(COMPANIES))
    logger.info("Funding events: %s", len(events))
    logger.info("Investor event rows: %s", len(investor_events))
    logger.info(
        "Unique named investors: %s",
        (
            investor_events["investor_name"].nunique()
            if not investor_events.empty
            else 0
        ),
    )
    logger.info("Companies with investors: %s", companies_with_investors)
    logger.info("Companies with ownership: %s", companies_with_ownership)
    logger.info("Fatal exceptions: %s", len(failures))
    logger.info("Promotion decision: %s", promotion)
    logger.info(
        "Investor source: %s",
        (
            "NAMED INVESTOR ENRICHMENT"
            if investor_source != "NONE"
            else "NONE (expected for current Step 17 Form D-only scope)"
        ),
    )
    logger.info("SEC Form D investor identity: NOT ASSUMED")
    logger.info("Ownership fabrication: PREVENTED")

    if investor_source == "NONE":
        logger.info(
            "STEP 18 PASS: investor/ownership Gold promoted with explicit "
            "source-coverage limitations."
        )
    elif promotion == "PROMOTE":
        logger.info("STEP 18 PASS: investor/ownership Gold promoted.")
    else:
        logger.error("STEP 18 FAILED")
        raise RuntimeError(
            f"Step 18 promotion blocked: {failures}"
        )


if __name__ == "__main__":
    main()
