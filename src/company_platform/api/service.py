"""Read-only CIDM API data access layer.

Reads the project's existing Silver/Gold artifacts and SQLite stores. It does not
seed data, synthesize financial values, or treat artifact presence as completeness.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[3]
DATA = Path(os.environ.get("CIDM_DATA_DIR", ROOT / "data")).resolve()
API_VERSION = "1.0.0"
READ_ERRORS: dict[str, str] = {}
FRAME_CACHE: dict[str, tuple[int, int, pd.DataFrame]] = {}
READ_ERROR_CACHE: dict[str, tuple[int, int, str]] = {}

ARTIFACTS: dict[str, tuple[str, ...]] = {
    "company_profile": ("gold/unified_company_intelligence/unified_company_view/unified_company_view.parquet",),
    "financials": ("gold/financials/cik={cik}/financials.parquet", "silver/financial_facts/cik={cik}/financial_facts.parquet"),
    "funding": ("gold/funding_capital_intelligence/company_funding_snapshot/company_funding_snapshot.parquet", "gold/funding_capital_intelligence/funding_events/funding_events.parquet"),
    "ownership": ("gold/investor_ownership_intelligence/ownership_snapshot/ownership_snapshot.parquet", "gold/investor_ownership_intelligence/company_investor_snapshot/company_investor_snapshot.parquet"),
    "valuation": ("gold/valuation_intelligence/company_valuation_snapshot/company_valuation_snapshot.parquet",),
    "investment_profile": ("gold/unified_investment_intelligence/investment_profile/investment_profile.parquet", "gold/unified_investment_intelligence/investment_summary/investment_summary.parquet"),
    "investment_signals": ("gold/unified_investment_intelligence/investment_signals/investment_signals.parquet",),
    "coverage_matrix": ("gold/step34_1_event_intelligence_coverage/coverage_matrix.parquet",),
}
REGISTRY_CANDIDATES = ("silver/platform/company_registry.parquet", "silver/company/company.parquet")
UNIFIED_PATH = "gold/unified_company_intelligence/unified_company_view/unified_company_view.parquet"
# Compatibility path constants retained for the repository's existing Step 32 tests.
CONTROL_DB = DATA / "silver/platform/control.db"
STRUCTURED_DB = DATA / "silver/platform/structured_events.db"
REGISTRY = DATA / "silver/platform/company_registry.parquet"
UNIFIED = DATA / UNIFIED_PATH


def normalize_cik(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    text = text.removeprefix("SEC_")
    return text.zfill(10) if text.isdigit() else text


def clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [clean(v) for v in value]
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        missing = pd.isna(value)
        if bool(missing):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in ("{", "["):
            try:
                return clean(json.loads(stripped))
            except Exception:
                return value
    return value


def _read_frame(path: Path) -> pd.DataFrame:
    """Read an artifact without disguising corrupt/unreadable files as missing."""
    if not path.is_file():
        return pd.DataFrame()
    key = str(path.resolve())
    try:
        stat = path.stat()
        cached = FRAME_CACHE.get(key)
        if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
            return cached[2].copy()
        cached_error = READ_ERROR_CACHE.get(key)
        if cached_error and cached_error[0] == stat.st_mtime_ns and cached_error[1] == stat.st_size:
            return pd.DataFrame()
        if path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            frame = pd.read_csv(path)
        elif path.suffix.lower() == ".json":
            frame = pd.read_json(path)
        else:
            return pd.DataFrame()
        READ_ERRORS.pop(key, None)
        READ_ERROR_CACHE.pop(key, None)
        FRAME_CACHE[key] = (stat.st_mtime_ns, stat.st_size, frame.copy())
        return frame
    except Exception as exc:
        FRAME_CACHE.pop(key, None)
        READ_ERRORS[key] = type(exc).__name__
        try:
            error_stat = path.stat()
            READ_ERROR_CACHE[key] = (error_stat.st_mtime_ns, error_stat.st_size, type(exc).__name__)
        except OSError:
            READ_ERROR_CACHE.pop(key, None)
        LOG.exception("Unable to read CIDM artifact %s", path)
        return pd.DataFrame()


def _with_cik(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    columns = {str(c).lower(): c for c in df.columns}
    cik_col = columns.get("cik") or columns.get("company_cik")
    if cik_col:
        df = df.copy()
        df["__cidm_cik"] = df[cik_col].map(normalize_cik)
    return df


def _for_company(df: pd.DataFrame, cik: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = _with_cik(df)
    if "__cidm_cik" not in df:
        return pd.DataFrame()
    return df[df["__cidm_cik"] == normalize_cik(cik)].drop(columns=["__cidm_cik"], errors="ignore")


def _frame_records(df: pd.DataFrame, limit: int = 500) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return [{str(k): clean(v) for k, v in row.items() if not str(k).startswith("__cidm_")} for row in df.head(limit).to_dict(orient="records")]


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(DATA))
    except ValueError:
        return str(path)


def _first_frame(paths: tuple[str, ...], cik: str | None = None) -> tuple[pd.DataFrame, str | None]:
    last_existing: str | None = None
    for template in paths:
        rendered = Path(template.format(cik=normalize_cik(cik) if cik else ""))
        path = rendered if rendered.is_absolute() else DATA / rendered
        if not path.is_file():
            continue
        last_existing = _relative(path)
        frame = _read_frame(path)
        if frame.empty:
            if str(path.resolve()) in READ_ERRORS:
                # Do not hide a broken preferred Gold artifact by silently falling back.
                return pd.DataFrame(), last_existing
            continue
        if cik:
            scoped = _for_company(frame, cik)
            # A partitioned artifact is already company-scoped if it has no CIK column.
            cik_columns = {str(c).lower() for c in frame.columns}
            if scoped.empty and f"cik={normalize_cik(cik)}" in template and not (cik_columns & {"cik", "company_cik"}):
                scoped = frame
            frame = scoped
        if not frame.empty:
            return frame, last_existing
    return pd.DataFrame(), last_existing


def _registry() -> tuple[pd.DataFrame, str | None]:
    candidates = [(REGISTRY, _relative(REGISTRY)), (DATA / "silver/company/company.parquet", "silver/company/company.parquet")]
    for path, rel in candidates:
        if not path.is_file():
            continue
        df = _read_frame(path)
        if str(path.resolve()) in READ_ERRORS:
            return pd.DataFrame(), rel
        if df.empty:
            continue
        df = _with_cik(df)
        if "__cidm_cik" in df:
            df["cik"] = df["__cidm_cik"]
            df = df.drop(columns=["__cidm_cik"], errors="ignore")
        if "cik" not in df.columns:
            LOG.error("Registry artifact %s has no CIK column", rel)
            return pd.DataFrame(), rel
        if "status" in df.columns:
            status = df["status"].astype(str).str.strip().str.upper()
            df = df[status.isin({"ACTIVE", "ENABLED", "PRODUCTION"})]
        if "ingestion_enabled" in df.columns:
            enabled = df["ingestion_enabled"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})
            df = df[enabled]
        # Preserve duplicates. Identity conflicts must be visible, not silently deduplicated.
        return df, rel
    return pd.DataFrame(), None


def _record_by_cik(df: pd.DataFrame, cik: str) -> dict[str, Any] | None:
    matched = _for_company(df, cik)
    if matched.empty:
        return None
    if len(matched) > 1:
        return {"_data_quality_warning": "MULTIPLE_ROWS_FOR_COMPANY", "_matching_rows": len(matched)}
    return clean(matched.iloc[0].to_dict())


def _sqlite_rows(db: Path, table: str) -> tuple[list[dict[str, Any]], str | None]:
    if not db.is_file():
        return [], None
    try:
        with sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True) as con:
            tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if table not in tables:
                return [], _relative(db)
            cur = con.execute(f'SELECT * FROM "{table}"')
            names = [d[0] for d in cur.description or []]
            rows = [dict(zip(names, row)) for row in cur.fetchall()]
        # This project stores run/task status inside the JSON `data` column.
        expanded = []
        for row in rows:
            data = row.get("data")
            if isinstance(data, str):
                try:
                    parsed = json.loads(data)
                    if isinstance(parsed, dict):
                        row = {**row, **parsed}
                except (json.JSONDecodeError, TypeError):
                    pass
            expanded.append(row)
        return expanded, _relative(db)
    except Exception as exc:
        LOG.exception("Unable to read CIDM SQLite table %s from %s", table, db)
        return [], f"{_relative(db)} ({type(exc).__name__})"


def _structured_events(cik: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
    db = STRUCTURED_DB
    rows, path = _sqlite_rows(db, "structured_events")
    result = []
    for row in rows:
        row_cik = normalize_cik(row.get("cik"))
        if cik and row_cik != normalize_cik(cik):
            continue
        doc_id = str(row.get("document_id") or "")
        # The current production schema uses source_document_path, not source_path.
        source = str(row.get("source_document_path") or row.get("source_path") or row.get("document_path") or row.get("source_uri") or "")
        upper_doc = doc_id.upper()
        normalized_source = source.replace("\\", "/").lower()
        data_relative = normalized_source.split("/data/", 1)[1] if "/data/" in normalized_source else normalized_source
        data_parts = [part for part in data_relative.split("/") if part]
        test_path_component = any(part in {"test", "tests", "fixtures", "fixture"} for part in data_parts)
        if upper_doc.startswith(("TEST-", "TEST_", "E2E-")) or test_path_component:
            environment = "TEST"
        elif "/data/bronze/sec/documents/" in normalized_source:
            environment = "PRODUCTION"
        else:
            # Stored source_environment is a secondary hint only; explicit path/doc rules win.
            stored = str(row.get("source_environment") or "").upper()
            environment = stored if stored in {"PRODUCTION", "TEST"} and stored == "TEST" else "UNKNOWN"
        facts = clean(row.get("facts_json"))
        row["facts"] = facts
        row["source_environment"] = environment
        row["is_production"] = environment == "PRODUCTION" and str(row.get("status", "")).upper() == "PERSISTED"
        result.append(clean(row))
    return result, path



_EVENT_DERIVED_FIELDS = (
    "investment_signal_count", "investment_signal_types", "capital_activity_detected",
    "recurring_capital_activity", "capital_raise_execution", "event_decision_posture",
    "event_decision_confidence", "event_decision_id", "company_state_status",
    "company_state_version", "observed_funding_event_count", "latest_funding_date",
    "latest_total_offering_amount_usd", "latest_amount_sold_usd",
    "cumulative_offering_amount_usd", "cumulative_amount_sold_usd",
)


def _production_document_map(events: list[dict[str, Any]]) -> dict[tuple[str, str], bool]:
    result: dict[tuple[str, str], bool] = {}
    for event in events:
        document_id = str(event.get("document_id") or "")
        if not document_id:
            continue
        key = (normalize_cik(event.get("cik")), document_id)
        # Fail closed when the same document is associated with mixed environments.
        result[key] = result.get(key, True) and bool(event.get("is_production"))
    return result


def _sanitize_event_derived_fields(record: dict[str, Any] | None, cik: str,
                                   production_documents: dict[tuple[str, str], bool]) -> dict[str, Any] | None:
    """Do not expose state/event signals whose source cannot be tied to a persisted production event."""
    if record is None or record.get("_data_quality_warning"):
        return record
    document_id = str(record.get("source_document_id") or "").strip()
    key = (normalize_cik(cik), document_id)
    is_test_id = document_id.upper().startswith(("TEST-", "TEST_", "E2E-"))
    verified_production = bool(document_id) and production_documents.get(key) is True and not is_test_id
    if verified_production:
        record["event_intelligence_status"] = "SOURCE_VERIFIED_PRODUCTION"
        return record
    if not document_id:
        reason = "SOURCE_DOCUMENT_ID_MISSING"
    elif is_test_id or key in production_documents and production_documents.get(key) is False:
        reason = "NON_PRODUCTION_SOURCE"
    else:
        reason = "SOURCE_NOT_VERIFIED_AS_PRODUCTION"
    for field in _EVENT_DERIVED_FIELDS:
        if field in record:
            record[field] = None
    record["event_intelligence_status"] = "EXCLUDED_UNVERIFIED_SOURCE"
    record["event_intelligence_warning"] = reason
    return record


INSTANT_METRICS = {"assets", "liabilities", "cash", "equity"}


def _coverage_contract(
    *,
    artifact_availability: str,
    business_data_availability: str,
    completeness: str,
    freshness: str,
    audit_outcome: str,
    row_count: int | None,
    source_artifact: str | None,
    note: str | None,
    freshness_as_of: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Keep status as artifact availability for existing clients.

    The other fields are independent. An artifact can exist while the
    business data is empty, incomplete, undated, or failed by audit.
    """
    record = {
        "status": artifact_availability,
        "artifact_availability": artifact_availability,
        "business_data_availability": business_data_availability,
        "completeness": completeness,
        "freshness": freshness,
        "freshness_as_of": freshness_as_of,
        "audit_outcome": audit_outcome,
        "row_count": row_count,
        "source_artifact": source_artifact,
        "note": note,
    }
    if extra:
        record.update(extra)
    return record


def _dated(series: pd.Series | None) -> str | None:
    if series is None:
        return None
    stamps = pd.to_datetime(series, errors="coerce").dropna()
    if stamps.empty:
        return None
    latest = stamps.max()
    return latest.date().isoformat() if hasattr(latest, "date") else str(latest)[:10]


def _coverage_matrix_row(cik: str) -> dict[str, Any] | None:
    path = DATA / "gold/step34_1_event_intelligence_coverage/coverage_matrix.parquet"
    frame = _for_company(_read_frame(path), cik)
    if frame.empty:
        return None
    return clean(frame.iloc[0].to_dict())


def _financial_assessment(frame: pd.DataFrame) -> tuple[str, str, str, str, str | None]:
    if frame.empty or "value" not in frame.columns:
        return "EMPTY", "UNAVAILABLE", "NOT_AUDITED", "UNKNOWN", None
    values = pd.to_numeric(frame["value"], errors="coerce")
    business = "AVAILABLE" if values.notna().any() else "EMPTY"
    years = pd.to_numeric(frame["fiscal_year"], errors="coerce") if "fiscal_year" in frame.columns else None
    end = pd.to_datetime(frame["period_end"], errors="coerce") if "period_end" in frame.columns else None
    start = pd.to_datetime(frame["period_start"], errors="coerce") if "period_start" in frame.columns else None
    metric = frame["metric"].astype(str).str.lower() if "metric" in frame.columns else pd.Series("", index=frame.index)
    duration = ~metric.isin(INSTANT_METRICS)
    invalid_year = False
    if years is not None and end is not None:
        comparable = end.notna()
        invalid_year = bool((comparable & years.isna()).any() or (comparable & years.notna() & end.dt.year.ne(years)).any())
    missing_duration_start = False
    if start is not None and end is not None:
        missing_duration_start = bool((duration & end.notna() & start.isna()).any())
    completeness = "COMPLETE" if business == "AVAILABLE" and not invalid_year and not missing_duration_start else "PARTIAL" if business == "AVAILABLE" else "UNAVAILABLE"
    audit = "PASSED" if completeness == "COMPLETE" else "FAILED" if business == "AVAILABLE" else "NOT_AUDITED"
    as_of = _dated(frame["period_end"] if "period_end" in frame.columns else None)
    return business, completeness, audit, "DATED" if as_of else "UNKNOWN", as_of


def _column_max(frame: pd.DataFrame, names: tuple[str, ...]) -> float | None:
    for name in names:
        if name not in frame.columns:
            continue
        numbers = pd.to_numeric(frame[name], errors="coerce").dropna()
        if not numbers.empty:
            return float(numbers.max())
    return None


def _domain_status(domain: str, frame: pd.DataFrame, path: str | None, cik: str | None = None) -> dict[str, Any]:
    read_error = str((DATA / path).resolve()) if path else ""
    if read_error and read_error in READ_ERRORS:
        return _coverage_contract(
            artifact_availability="ERROR",
            business_data_availability="UNKNOWN",
            completeness="UNKNOWN",
            freshness="UNKNOWN",
            audit_outcome="NOT_AUDITED",
            row_count=None,
            source_artifact=path,
            note=f"Artifact exists but could not be read ({READ_ERRORS[read_error]}). Check the application log.",
        )
    if frame.empty:
        return _coverage_contract(
            artifact_availability="MISSING",
            business_data_availability="EMPTY",
            completeness="UNAVAILABLE",
            freshness="UNKNOWN",
            audit_outcome="NOT_AUDITED",
            row_count=None,
            source_artifact=path,
            note="No matching rows were found in the expected artifact. This is not proof that the information does not exist.",
        )

    business = "AVAILABLE"
    completeness = "UNKNOWN"
    audit = "NOT_AUDITED"
    freshness = "UNKNOWN"
    as_of = _dated(frame["generated_at_utc"] if "generated_at_utc" in frame.columns else None)
    note = None
    if domain == "financials":
        business, completeness, audit, freshness, as_of = _financial_assessment(frame)
        if audit == "FAILED":
            note = "Fiscal year does not match the measurement date, or a duration period is missing its start date."
    elif domain == "funding":
        observed = _column_max(frame, ("funding_event_count", "observed_funding_event_count", "form_d_event_count"))
        if observed is None:
            observed = float(len(frame)) if "funding_event_id" in frame.columns else 0.0
        business = "AVAILABLE" if observed > 0 else "EMPTY"
        completeness = "PARTIAL" if business == "AVAILABLE" else "UNAVAILABLE"
        note = "SEC Form D observations are not a complete fundraising history."
        as_of = _dated(frame["filing_date"] if "filing_date" in frame.columns else frame["latest_funding_date"] if "latest_funding_date" in frame.columns else None) or as_of
        freshness = "DATED" if as_of else "UNKNOWN"
    elif domain == "coverage_matrix":
        matrix = _coverage_matrix_row(cik) if cik else clean(frame.iloc[0].to_dict())
        audit_status = str((matrix or {}).get("audit_status") or "").upper()
        coverage_status = str((matrix or {}).get("coverage_status") or "").upper()
        audit = "PASSED" if audit_status == "PASSED" else "FAILED" if audit_status else "NOT_AUDITED"
        if coverage_status == "COMPLETE":
            completeness = "COMPLETE"
        elif coverage_status == "PARTIAL":
            completeness = "PARTIAL"
        else:
            completeness = "UNAVAILABLE"
        business = "AVAILABLE"
        downloaded = (matrix or {}).get("downloaded_document_count")
        note = (
            "Document processing has not covered the expected latest filings."
            if completeness == "UNAVAILABLE"
            else "Processed documents cover only part of the available filing window."
            if completeness == "PARTIAL"
            else None
        )
        if downloaded == 0:
            note = "No SEC filing documents have been downloaded for this company. Financial facts do not depend on those documents."
    elif domain == "ownership":
        identity = (
            frame["investor_identity_status"].astype(str).str.upper()
            if "investor_identity_status" in frame.columns
            else pd.Series(dtype=str)
        )
        ownership = (
            frame["ownership_status"].astype(str).str.upper()
            if "ownership_status" in frame.columns
            else pd.Series(dtype=str)
        )
        named = bool(len(identity) and identity.eq("AVAILABLE").any())
        owned = bool(len(ownership) and ownership.eq("AVAILABLE").any())
        business = "AVAILABLE" if named or owned else "EMPTY"
        completeness = "PARTIAL" if business == "AVAILABLE" else "UNAVAILABLE"
        note = (
            "Named investors or ownership percentages come from an enrichment file."
            if business == "AVAILABLE"
            else "Investor names and ownership percentages are unknown. SEC Form D investor counts are not identities or a cap table."
        )
    elif domain == "valuation":
        status = (
            frame["valuation_status"].astype(str).str.upper()
            if "valuation_status" in frame.columns
            else pd.Series(dtype=str)
        )
        amounts = (
            pd.to_numeric(frame["latest_valuation_amount_usd"], errors="coerce")
            if "latest_valuation_amount_usd" in frame.columns
            else pd.Series(dtype=float)
        )
        has_valuation = bool(len(frame) and status.eq("AVAILABLE").any() and amounts.notna().any())
        business = "AVAILABLE" if has_valuation else "EMPTY"
        completeness = "PARTIAL" if business == "AVAILABLE" else "UNAVAILABLE"
        note = (
            "Valuation comes from a reported enrichment source or a transaction-implied calculation."
            if business == "AVAILABLE"
            else "No reported valuation. SEC offering amounts and amounts sold are funding context, not company valuation."
        )
    elif domain == "events":
        business = "AVAILABLE"
    if freshness == "UNKNOWN" and as_of:
        freshness = "DATED"
    return _coverage_contract(
        artifact_availability="AVAILABLE",
        business_data_availability=business,
        completeness=completeness,
        freshness=freshness,
        freshness_as_of=as_of,
        audit_outcome=audit,
        row_count=int(len(frame)),
        source_artifact=path,
        note=note,
    )


def get_health() -> dict[str, Any]:
    """Legacy health contract retained for existing Step 32 integrations."""
    return {"status": "ok", "api_version": API_VERSION}


def get_health_detail() -> dict[str, Any]:
    registry, reg_path = _registry()
    available = not registry.empty
    return {"status": "ok" if available else "degraded", "api_version": API_VERSION,
            "data_root_configured": DATA.exists(), "company_registry": "AVAILABLE" if available else ("ERROR" if reg_path and str(Path(REGISTRY).resolve()) in READ_ERRORS else "MISSING"),
            "registry_artifact": reg_path, "artifact_read_errors": len(READ_ERRORS)}


def list_companies(query: str | None = None, limit: int | None = None, offset: int = 0, sort: str = "name", direction: str = "asc", status: str = "") -> dict[str, Any] | list[dict[str, Any]]:
    legacy_list = query is None and limit is None and offset == 0 and sort == "name" and direction == "asc" and status == ""
    query = query or ""
    limit = 50 if limit is None else limit
    registry, registry_path = _registry()
    if registry.empty:
        read_error = str((DATA / registry_path).resolve()) if registry_path else ""
        state = "ERROR" if read_error and read_error in READ_ERRORS else "NO_REGISTRY"
        message = "The registry artifact exists but could not be read; no substitute company records were created." if state == "ERROR" else "Company registry artifact is unavailable. No placeholder companies were created."
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "data_status": state,
                "source_artifact": registry_path, "message": message}

    name_col = next((c for c in ("company_name", "name", "legal_name", "entity_name") if c in registry.columns), None)
    if name_col is None:
        registry = registry.copy()
        registry["name"] = registry.get("company_id", registry["cik"])
        name_col = "name"
    registry = registry.copy()
    registry[name_col] = registry[name_col].fillna("").astype(str)
    if query.strip():
        q = query.strip().casefold()
        searchable = registry[name_col].str.casefold().str.contains(q, na=False) | registry["cik"].astype(str).str.contains(q, na=False)
        if "ticker" in registry.columns:
            searchable |= registry["ticker"].fillna("").astype(str).str.casefold().str.contains(q, na=False)
        registry = registry[searchable]
    if status:
        status_col = next((c for c in ("status", "onboarding_status") if c in registry.columns), None)
        if status_col:
            registry = registry[registry[status_col].astype(str).str.casefold() == status.casefold()]

    cik_counts = registry["cik"].map(normalize_cik).value_counts().to_dict()
    unified_path = _relative(UNIFIED)
    unified = _read_frame(UNIFIED)
    unified_by_cik: dict[str, dict[str, Any]] = {}
    unified_conflicts: set[str] = set()
    if not unified.empty:
        scoped = _with_cik(unified)
        if "__cidm_cik" in scoped:
            scoped["cik"] = scoped["__cidm_cik"]
            scoped = scoped.drop(columns=["__cidm_cik"], errors="ignore")
            if scoped["cik"].duplicated().any():
                unified_conflicts = set(scoped.loc[scoped["cik"].duplicated(keep=False), "cik"].astype(str))
            for _, row in scoped.iterrows():
                c = normalize_cik(row.get("cik"))
                if c and c not in unified_conflicts:
                    unified_by_cik[c] = clean(row.to_dict())
        else:
            unified_path = None
    else:
        err = str(UNIFIED.resolve())
        if err in READ_ERRORS:
            unified_path = UNIFIED_PATH
        else:
            unified_path = None

    source_events, _ = _structured_events()
    production_documents = _production_document_map(source_events)
    # Sanitize the unified row before it is merged into company discovery.
    unified_by_cik = {cik: _sanitize_event_derived_fields(row, cik, production_documents) for cik, row in unified_by_cik.items()}
    records = _frame_records(registry, limit=100000)
    for item in records:
        cik = normalize_cik(item.get("cik"))
        item["cik"] = cik
        if cik_counts.get(cik, 0) > 1:
            item["identity_status"] = "CONFLICT"
            item["identity_conflict_count"] = int(cik_counts[cik])
            item["intelligence"] = {}
        else:
            item["identity_status"] = "RESOLVED"
            if cik in unified_conflicts:
                item["intelligence_status"] = "CONFLICT"
                item["intelligence"] = {}
            elif cik in unified_by_cik:
                intelligence = unified_by_cik[cik]
                # Registry identity fields remain authoritative; intelligence fills other fields only.
                nested = {key: value for key, value in intelligence.items() if key != "cik"}
                for key, value in intelligence.items():
                    if key in {"cik", "company_id", "company_name", "name", "ticker"}:
                        continue
                    if key not in item or item[key] is None or key not in registry.columns:
                        item[key] = value
                item["intelligence"] = nested
                item["intelligence_status"] = "AVAILABLE"
            else:
                item["intelligence_status"] = "MISSING"
                item["intelligence"] = {}
        item["coverage"] = {domain: _artifact_coverage(domain, cik) for domain in ARTIFACTS}

    sort_key = sort if records and any(sort in row for row in records) else name_col
    def sort_value(row: dict[str, Any]):
        value = row.get(sort_key)
        if value is None:
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, value)
        try:
            numeric = float(value)
            return (0, numeric)
        except (TypeError, ValueError):
            return (0, str(value).casefold())
    records.sort(key=sort_value, reverse=direction.lower() == "desc")
    total = len(records)
    if legacy_list:
        return records
    return {"items": records[offset:offset + limit], "total": total, "limit": limit, "offset": offset,
            "data_status": "AVAILABLE", "source_artifact": registry_path, "intelligence_artifact": unified_path,
            "identity_conflict_count": sum(1 for r in records if r.get("identity_status") == "CONFLICT")}


def _artifact_coverage(domain: str, cik: str) -> dict[str, Any]:
    paths = ARTIFACTS.get(domain, ())
    if not paths:
        return _coverage_contract(
            artifact_availability="NOT_CONFIGURED",
            business_data_availability="UNKNOWN",
            completeness="UNKNOWN",
            freshness="UNKNOWN",
            audit_outcome="NOT_AUDITED",
            row_count=None,
            source_artifact=None,
            note="No artifact mapping is configured for this domain.",
        )
    df, path = _first_frame(paths, cik)
    return _domain_status(domain, df, path, cik)


def get_company(cik: str) -> dict[str, Any] | None:
    cik = normalize_cik(cik)
    registry, registry_path = _registry()
    base = _record_by_cik(registry, cik)
    if base is None:
        return None
    if base.get("_data_quality_warning"):
        return {"api_version": API_VERSION, "cik": cik, "identity_status": "CONFLICT", "identity": base,
                "sections": {}, "coverage": {}, "message": "Multiple active registry rows exist for this CIK. Details are withheld until identity is reconciled."}

    sections: dict[str, Any] = {}
    coverage: dict[str, Any] = {}
    events, events_path = _structured_events(cik)
    production_events = [e for e in events if e.get("is_production")]
    production_documents = _production_document_map(events)
    for domain, paths in ARTIFACTS.items():
        if domain == "company_profile":
            paths = (str(UNIFIED),)
        df, path = _first_frame(paths, cik)
        coverage[domain] = _domain_status(domain, df, path, cik)
        if domain in ("company_profile", "investment_profile", "valuation"):
            record = _record_by_cik(df, cik)
            if domain == "company_profile":
                record = _sanitize_event_derived_fields(record, cik, production_documents)
            if domain == "valuation" and isinstance(record, dict):
                if str(record.get("valuation_status") or "").upper() != "AVAILABLE":
                    record["latest_valuation_amount_usd"] = None
                    record["sec_capital_context_usd"] = None
                    record["valuation_to_sec_capital_ratio"] = None
            sections[domain] = record
        else:
            sections[domain] = _frame_records(df)

    unknown_events = [e for e in events if e.get("source_environment") == "UNKNOWN"]
    test_events = [e for e in events if e.get("source_environment") == "TEST"]
    non_persisted = [e for e in events if str(e.get("status", "")).upper() != "PERSISTED"]
    sections["events"] = production_events
    event_dates = [event.get("updated_at_utc") or event.get("created_at_utc") for event in production_events]
    event_as_of = _dated(pd.Series(event_dates)) if event_dates else None
    matrix = _coverage_matrix_row(cik)
    matrix_audit = str((matrix or {}).get("audit_status") or "").upper()
    matrix_coverage = str((matrix or {}).get("coverage_status") or "").upper()
    if production_events and matrix_coverage == "COMPLETE":
        event_completeness = "COMPLETE"
    elif production_events:
        event_completeness = "PARTIAL"
    else:
        event_completeness = "UNAVAILABLE"
    coverage["events"] = _coverage_contract(
        artifact_availability="AVAILABLE" if events_path else "MISSING",
        business_data_availability="AVAILABLE" if production_events else "EMPTY",
        completeness=event_completeness,
        freshness="DATED" if event_as_of else "UNKNOWN",
        freshness_as_of=event_as_of,
        audit_outcome="PASSED" if matrix_audit == "PASSED" else "FAILED" if matrix_audit else "NOT_AUDITED",
        row_count=len(production_events) if production_events else None,
        source_artifact=events_path,
        note="Only persisted events with a recognized SEC bronze source path are exposed as production. Downloaded filing documents are required for event coverage; they are not required for XBRL financial facts.",
        extra={
            "unknown_environment_events": len(unknown_events),
            "test_events_excluded": len(test_events),
            "non_persisted_events_excluded": len(non_persisted),
        },
    )
    formd_events = [e for e in production_events if str(e.get("event_type", "")).upper() == "SEC_FORM_D_FILING"]
    funding_rows = sections.get("funding") or []
    formd_observations: int | None = None
    for funding_row in funding_rows:
        for count_key in ("funding_event_count", "observed_funding_event_count", "form_d_event_count"):
            value = funding_row.get(count_key)
            if value is not None:
                try:
                    formd_observations = int(float(value))
                except (TypeError, ValueError):
                    formd_observations = None
                break
        if formd_observations is not None:
            break
    if formd_observations is None and formd_events:
        formd_observations = len(formd_events)
    if formd_observations is None and "funding_events.parquet" in str(coverage.get("funding", {}).get("source_artifact") or ""):
        formd_observations = len(funding_rows)
    intelligence = sections.get("company_profile") or {}
    return {"api_version": API_VERSION, "cik": cik, "company_id": base.get("company_id"),
            "company_name": base.get("company_name") or base.get("name"),
            "identity_status": "RESOLVED", "identity": clean(base), "intelligence": intelligence,
            "sections": sections, "coverage": coverage, "events": production_events,
            "event_count": len(production_events), "form_d_event_count": formd_observations,
            "coverage_summary": {"production_structured_events": len(production_events), "form_d_observations": formd_observations,
                                 "test_structured_events_excluded": len(test_events), "unknown_environment_events_excluded": len(unknown_events)},
            "data_policy": {"missing_values": "Not inferred or replaced with zero", "freshness": "Unknown unless a reliable source timestamp is available",
                            "funding_scope": "SEC Form D observations are not a complete fundraising history",
                            "ownership_scope": "Investor names and ownership percentages stay unknown unless an enrichment file provides them. Form D investor counts are not identities.",
                            "valuation_scope": "No reported valuation is invented from SEC offering amounts or amounts sold.",
                            "events": "Only persisted events from recognized SEC bronze paths are production; test and unknown-source events are excluded",
                            "event_intelligence": "Event-derived state fields are withheld unless source_document_id matches a persisted production event for the same CIK"},
            "registry_artifact": registry_path}


def get_operations_summary() -> dict[str, Any]:
    control = CONTROL_DB
    events, _ = _sqlite_rows(control, "events")
    runs, _ = _sqlite_rows(control, "pipeline_runs")
    tasks, _ = _sqlite_rows(control, "task_runs")
    company_listing = list_companies(limit=100000)
    registry_available = company_listing.get("data_status") == "AVAILABLE"
    companies = company_listing.get("items", []) if registry_available else []
    def status_count(rows: list[dict[str, Any]], wanted: str) -> int:
        return sum(str(row.get("status", "")).upper() == wanted for row in rows)
    run_summary = {"total": len(runs), "failed": status_count(runs, "FAILED"), "running": status_count(runs, "RUNNING"), "succeeded": status_count(runs, "SUCCESS")}
    task_summary = {"total": len(tasks), "failed": status_count(tasks, "FAILED"), "blocked": status_count(tasks, "BLOCKED"), "running": status_count(tasks, "RUNNING"), "succeeded": status_count(tasks, "SUCCESS")}
    event_summary = {"total": len(events), "failed": status_count(events, "FAILED"), "pending": status_count(events, "PENDING"), "processing": status_count(events, "PROCESSING"), "quarantined": status_count(events, "QUARANTINED")}
    return {"api_version": API_VERSION, "company_count": len(companies) if registry_available else None,
            "company_registry_status": company_listing.get("data_status"), "pipeline_runs": run_summary, "runs": run_summary,
            "tasks": task_summary, "events": event_summary, "companies": companies,
            "data_root_available": DATA.exists(), "artifact_read_errors": len(READ_ERRORS)}


def get_readiness() -> dict[str, Any]:
    """Retain the existing Step 32 readiness integration."""
    from company_platform.production_readiness.readiness import evaluate_readiness
    summary, _ = evaluate_readiness()
    if summary.empty:
        raise RuntimeError("Production readiness returned no summary")
    return {key: clean(value) for key, value in summary.iloc[0].items()}


def get_event(event_id: str) -> dict[str, Any] | None:
    rows, _ = _structured_events()
    for row in rows:
        if str(row.get("event_id")) == event_id:
            return row
    rows, _ = _sqlite_rows(CONTROL_DB, "events")
    for row in rows:
        if str(row.get("event_id")) == event_id or str(row.get("id")) == event_id:
            payload = row.get("payload")
            if isinstance(payload, str):
                row["payload"] = clean(payload)
            return clean(row)
    return None


def get_run(run_id: str) -> dict[str, Any] | None:
    rows, _ = _sqlite_rows(CONTROL_DB, "pipeline_runs")
    for row in rows:
        if str(row.get("run_id") or row.get("id")) == run_id:
            return clean(row)
    return None
