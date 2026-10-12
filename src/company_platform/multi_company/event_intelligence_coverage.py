from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.events.structured_event_store import (
    StructuredEventStore,
    classify_source_environment,
)
from company_platform.multi_company.hardening import active_target_companies
from company_platform.company_intelligence.unified_company_view import (
    OUTPUT_ROOT as UNIFIED_OUTPUT_ROOT,
)


ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"
FILINGS_ROOT = DATA / "silver" / "filings"
BRONZE_DOCUMENTS_ROOT = DATA / "bronze" / "sec" / "documents"
DOCUMENTS_ROOT = DATA / "silver" / "documents"
CONTROL_DB = DATA / "silver" / "platform" / "control.db"
STRUCTURED_EVENTS_DB = DATA / "silver" / "platform" / "structured_events.db"
COMPANY_STATE_DB = DATA / "silver" / "platform" / "company_intelligence_state.db"
INVESTMENT_SIGNALS_DB = DATA / "silver" / "platform" / "investment_signals.db"
DECISION_SIGNALS_DB = DATA / "silver" / "platform" / "decision_signals.db"
COMPANY_REGISTRY = DATA / "silver" / "platform" / "company_registry.parquet"
OUTPUT_ROOT = DATA / "gold" / "step34_1_event_intelligence_coverage"
FUNDING_EVENTS_PATH = (
    DATA
    / "gold"
    / "funding_capital_intelligence"
    / "funding_events"
    / "funding_events.parquet"
)
UNIFIED_VIEW_PATH = (
    UNIFIED_OUTPUT_ROOT
    / "unified_company_view"
    / "unified_company_view.parquet"
)

DOCUMENT_ARTIFACTS = (
    "document_metadata.parquet",
    "document_text.parquet",
    "document_classification.parquet",
    "document_understanding.parquet",
)
STATE_EVENT_TYPES = {"SEC_FORM_D_FILING"}
BACKFILL_DOCUMENT_LIMIT = 10


def _normalize_cik(value: Any) -> str:
    text = str(value).strip()
    if text.upper().startswith("SEC_"):
        text = text[4:]
    return text.removesuffix(".0").zfill(10)


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.is_file() else pd.DataFrame()


def _document_ids(root: Path, artifact_name: str) -> set[str]:
    ids: set[str] = set()
    if not root.is_dir():
        return ids
    for path in root.glob(f"*/{artifact_name}"):
        frame = _read_parquet(path)
        if not frame.empty:
            if "document_id" not in frame.columns:
                raise ValueError(f"Missing document_id column in {path}")
            ids.update(frame["document_id"].dropna().astype(str))
    return ids


def _sqlite_rows(path: Path, table: str) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with sqlite3.connect(path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if table not in tables:
            return []
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(f"SELECT * FROM {table}")
        ]


def _downloaded_document_ids(cik: str, metadata_ids: set[str]) -> set[str]:
    manifest_path = (
        BRONZE_DOCUMENTS_ROOT
        / f"cik={cik}"
        / "document_manifest.parquet"
    )
    manifest = _read_parquet(manifest_path)
    if manifest.empty:
        return metadata_ids
    if not {"document_id", "status", "local_path"}.issubset(manifest.columns):
        raise ValueError(f"Invalid SEC document manifest schema: {manifest_path}")
    downloaded = manifest[
        manifest["status"].astype(str).str.upper().eq("DOWNLOADED")
    ]
    return {
        str(row.document_id)
        for row in downloaded.itertuples()
        if str(row.local_path).strip()
        and Path(str(row.local_path)).is_file()
    }


def _document_catalog(cik: str) -> dict[str, dict[str, Any]]:
    root = DOCUMENTS_ROOT / f"cik={cik}"
    catalog: dict[str, dict[str, Any]] = {}
    if not root.is_dir():
        return catalog
    for path in root.glob("*/document_metadata.parquet"):
        frame = _read_parquet(path)
        if frame.empty:
            continue
        for record in frame.to_dict(orient="records"):
            document_id = str(record.get("document_id", path.parent.name))
            catalog[document_id] = {
                **record,
                "source_environment": classify_source_environment(
                    document_id,
                    record.get("source_path"),
                ),
            }
    return catalog


def _filing_forms(filings: pd.DataFrame) -> pd.Series:
    if "form" not in filings.columns:
        return pd.Series("", index=filings.index, dtype="string")
    return (
        filings["form"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )


def _step17_form_d_rows(cik: str | None = None) -> pd.DataFrame:
    frame = _read_parquet(FUNDING_EVENTS_PATH)
    if frame.empty:
        return frame
    required = {
        "cik",
        "accession_number",
        "filing_type",
        "source_form",
        "source_url",
        "source_system",
        "event_status",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            f"Step 17 funding events missing required columns: {sorted(missing)}"
        )
    frame["cik"] = frame["cik"].map(_normalize_cik)
    frame["source_form"] = (
        frame["source_form"].fillna(frame["filing_type"]).astype(str).str.strip().str.upper()
    )
    frame["source_url"] = frame["source_url"].fillna("").astype(str).str.strip()
    frame["accession_number"] = (
        frame["accession_number"].fillna("").astype(str).str.strip()
    )
    frame = frame[
        frame["source_system"].astype(str).str.upper().eq("SEC_FORM_D")
        & frame["event_status"].astype(str).str.upper().eq(
            "CURRENT_LATEST_FILING"
        )
        & frame["source_form"].isin({"D", "D/A"})
        & frame["source_url"].str.startswith(
            "https://www.sec.gov/Archives/edgar/data/"
        )
        & frame["accession_number"].str.fullmatch(r"\d{10}-\d{2}-\d{6}")
    ].copy()
    if cik is not None:
        frame = frame[frame["cik"].eq(_normalize_cik(cik))]
    return frame.drop_duplicates(
        ["cik", "accession_number"], keep="last"
    ).reset_index(drop=True)


def _form_d_document_id(row: dict[str, Any]) -> str:
    cik = _normalize_cik(row["cik"])
    accession_number = str(row["accession_number"]).strip()
    primary_document = Path(urlparse(str(row["source_url"])).path).name
    return hashlib.sha256(
        f"{cik}|{accession_number}|{primary_document}".encode("utf-8")
    ).hexdigest()


def _form_d_filing_record(row: dict[str, Any]) -> dict[str, Any]:
    source_url = str(row["source_url"])
    primary_document = Path(urlparse(source_url).path).name
    filing_date = row.get("filing_date")
    first_sale_date = row.get("first_sale_date")
    return {
        "cik": _normalize_cik(row["cik"]),
        "company_name": str(row.get("issuer_name_source") or ""),
        "accession_number": str(row["accession_number"]),
        "filing_date": str(filing_date),
        "report_date": str(first_sale_date),
        "form": str(row["source_form"]),
        "primary_document": primary_document,
    }


def _form_d_facts(row: dict[str, Any]) -> dict[str, Any]:
    facts = {
        "issuer_name": row.get("issuer_name_source"),
        "filing_type": row.get("source_form"),
        "filing_date": row.get("filing_date"),
        "first_sale_date": row.get("first_sale_date"),
        "accession_number": row.get("accession_number"),
        "source_url": row.get("source_url"),
        "source_system": row.get("source_system"),
        "total_offering_amount": row.get("total_offering_amount_usd"),
        "total_amount_sold": row.get("total_amount_sold_usd"),
        "remaining_amount": row.get("remaining_amount_usd"),
        "number_of_investors": row.get("total_number_of_investors"),
    }
    normalized = {}
    for key, value in facts.items():
        if value is None or pd.isna(value):
            continue
        if isinstance(value, pd.Timestamp):
            value = value.isoformat()
        elif hasattr(value, "item"):
            value = value.item()
        normalized[key] = value
    return normalized


def _expected_event_documents(cik: str) -> set[str]:
    expected: set[str] = set()
    root = DOCUMENTS_ROOT / f"cik={cik}"
    if not root.is_dir():
        return expected
    for path in root.glob("*/document_understanding.parquet"):
        frame = _read_parquet(path)
        if frame.empty or "event_type" not in frame.columns:
            continue
        event_rows = frame[
            frame["event_type"].notna()
            & frame["event_type"].astype(str).str.strip().ne("")
        ]
        if not event_rows.empty:
            expected.update(event_rows["document_id"].astype(str))
    return expected


def build_coverage_matrix(
    target_companies: dict[str, str] | None = None,
) -> pd.DataFrame:
    targets = target_companies or active_target_companies()
    StructuredEventStore()
    inbound_events = _sqlite_rows(CONTROL_DB, "events")
    structured_events = _sqlite_rows(STRUCTURED_EVENTS_DB, "structured_events")
    states = {
        str(row["company_id"]): row
        for row in _sqlite_rows(COMPANY_STATE_DB, "company_state")
    }
    signals = _sqlite_rows(INVESTMENT_SIGNALS_DB, "investment_signals")
    decisions = _sqlite_rows(DECISION_SIGNALS_DB, "decision_signals")
    registry = _read_parquet(COMPANY_REGISTRY)
    registered_ciks = (
        set(registry["cik"].map(_normalize_cik))
        if not registry.empty and "cik" in registry.columns
        else set()
    )
    unified_view = _read_parquet(UNIFIED_VIEW_PATH)
    unified_ciks = (
        set(unified_view["cik"].map(_normalize_cik))
        if not unified_view.empty and "cik" in unified_view.columns
        else set()
    )

    rows = []
    for raw_cik, company_name in targets.items():
        cik = _normalize_cik(raw_cik)
        company_id = f"SEC_{cik}"
        filings_path = FILINGS_ROOT / f"cik={cik}" / "filings.parquet"
        filings = _read_parquet(filings_path)
        if not filings.empty and "cik" in filings.columns:
            filings = filings[filings["cik"].map(_normalize_cik).eq(cik)]
        forms = _filing_forms(filings)
        submissions_form_d = filings[
            forms.isin({"D", "D/A"})
        ] if not filings.empty else filings
        step17_form_d_rows = _step17_form_d_rows(cik).to_dict(
            orient="records"
        )
        form_d_accessions = {
            str(value).strip()
            for value in submissions_form_d.get(
                "accession_number", pd.Series(dtype="string")
            ).dropna()
        }
        form_d_accessions.update(
            str(row["accession_number"])
            for row in step17_form_d_rows
        )
        form_d_filings = len(form_d_accessions)

        artifact_ids = {
            name: _document_ids(DOCUMENTS_ROOT / f"cik={cik}", name)
            for name in DOCUMENT_ARTIFACTS
        }
        downloaded_ids = _downloaded_document_ids(
            cik,
            artifact_ids["document_metadata.parquet"],
        )
        complete_ids = set.intersection(
            *(artifact_ids[name] for name in DOCUMENT_ARTIFACTS)
        ) if DOCUMENT_ARTIFACTS else set()
        missing_document_ids = downloaded_ids - complete_ids
        document_catalog = _document_catalog(cik)
        production_document_ids = {
            document_id for document_id, row in document_catalog.items()
            if row["source_environment"] == "PRODUCTION"
        }
        test_document_ids = {
            document_id for document_id, row in document_catalog.items()
            if row["source_environment"] == "TEST"
        }
        production_downloaded_ids = downloaded_ids & production_document_ids
        production_processed_ids = complete_ids & production_document_ids
        expected_event_ids = (
            _expected_event_documents(cik) & production_document_ids
        )
        expected_step17_document_ids = {
            _form_d_document_id(row) for row in step17_form_d_rows
        }
        form_d_document_ids = {
            document_id for document_id, row in document_catalog.items()
            if row["source_environment"] == "PRODUCTION"
            and str(row.get("form", "")).strip().upper() in {"D", "D/A"}
        }

        company_events = [
            row for row in structured_events
            if str(row.get("company_id")) == company_id
        ]
        persisted_company_events = [
            row for row in company_events
            if str(row.get("status", "")).upper() in {"PERSISTED", "PROCESSED"}
        ]
        production_company_events = [
            row for row in persisted_company_events
            if str(row.get("source_environment", "UNKNOWN")).upper()
            == "PRODUCTION"
        ]
        test_company_events = [
            row for row in persisted_company_events
            if str(row.get("source_environment", "UNKNOWN")).upper() == "TEST"
        ]
        unknown_company_events = [
            row for row in persisted_company_events
            if str(row.get("source_environment", "UNKNOWN")).upper()
            == "UNKNOWN"
        ]
        persisted_event_ids = {
            str(row.get("document_id"))
            for row in production_company_events
            if row.get("document_id") is not None
        }
        missing_event_ids = expected_event_ids - persisted_event_ids
        persisted_step17_document_ids = (
            expected_step17_document_ids & persisted_event_ids
        )
        missing_step17_document_ids = (
            expected_step17_document_ids - persisted_event_ids
        )
        funding_event_count = sum(
            str(row.get("event_type")) in STATE_EVENT_TYPES
            for row in production_company_events
        )
        test_funding_event_count = sum(
            str(row.get("event_type")) in STATE_EVENT_TYPES
            for row in test_company_events
        )

        state = states.get(company_id)
        state_status = str(state.get("status")) if state else "NO_DATA"
        current_state_version = (
            int(state["state_version"])
            if state and state.get("state_version") is not None
            else None
        )
        signal_count = sum(
            str(row.get("company_id")) == company_id
            and current_state_version is not None
            and int(row.get("source_state_version") or -1)
            == current_state_version
            and not str(row.get("source_document_id", "")).upper().startswith(
                ("TEST-", "TEST_", "E2E-")
            )
            for row in signals
        )
        current_decision_count = sum(
            str(row.get("company_id")) == company_id
            and current_state_version is not None
            and int(row.get("source_state_version") or -1)
            == current_state_version
            and not str(row.get("source_document_id", "")).upper().startswith(
                ("TEST-", "TEST_", "E2E-")
            )
            for row in decisions
        )
        current_test_signal_count = sum(
            str(row.get("company_id")) == company_id
            and current_state_version is not None
            and int(row.get("source_state_version") or -1)
            == current_state_version
            and str(row.get("source_document_id", "")).upper().startswith(
                ("TEST-", "TEST_", "E2E-")
            )
            for row in signals
        )
        inbound_event_count = sum(
            str(row.get("company_id")) == company_id
            for row in inbound_events
        )
        test_inbound_event_count = sum(
            str(row.get("company_id")) == company_id
            and (
                str(row.get("source_id", "")).upper() == "TEST"
                or "TEST" in json.dumps(row.get("payload", {})).upper()
                or "E2E" in json.dumps(row.get("payload", {})).upper()
            )
            for row in inbound_events
        )
        company_registry_present = cik in registered_ciks
        unified_view_present = cik in unified_ciks
        if funding_event_count:
            state_signal_status = (
                "GENERATED"
                if state_status == "ACTIVE" and signal_count > 0
                else "INCOMPLETE"
            )
        elif step17_form_d_rows:
            state_signal_status = "INCOMPLETE"
        elif state_status == "NO_DATA" and signal_count == 0:
            state_signal_status = "NO_SUPPORTED_FUNDING_EVENTS"
        else:
            state_signal_status = "INCOMPLETE"
        latest_accession = None
        if not filings.empty and "filing_date" in filings.columns:
            latest = filings.sort_values("filing_date").iloc[-1]
            latest_accession = str(latest.get("accession_number", "")) or None

        expected_backfill_document_count = min(
            len(filings),
            BACKFILL_DOCUMENT_LIMIT,
        )
        document_backfill_scope_status = (
            "COMPLETE"
            if (
                len(downloaded_ids) >= expected_backfill_document_count
                and not missing_document_ids
            )
            else "INCOMPLETE"
        )
        filing_document_coverage_status = (
            "NO_COVERAGE"
            if not production_processed_ids
            else "COMPLETE"
            if len(production_processed_ids) >= len(filings)
            else "PARTIAL"
        )

        if filings.empty:
            audit_status = "FAILED"
            audit_failure_reason = "FILINGS_MISSING"
        elif not company_registry_present:
            audit_status = "FAILED"
            audit_failure_reason = "COMPANY_REGISTRY_MISSING"
        elif not unified_view_present:
            audit_status = "FAILED"
            audit_failure_reason = "UNIFIED_VIEW_MISSING"
        elif document_backfill_scope_status != "COMPLETE":
            audit_status = "FAILED"
            audit_failure_reason = "DOCUMENTS_INCOMPLETE"
        elif missing_event_ids:
            audit_status = "FAILED"
            audit_failure_reason = "STRUCTURED_EVENTS_INCOMPLETE"
        elif missing_step17_document_ids:
            audit_status = "FAILED"
            audit_failure_reason = "FORM_D_EVENTS_NOT_PERSISTED"
        elif state_signal_status == "INCOMPLETE":
            audit_status = "FAILED"
            audit_failure_reason = "STATE_SIGNAL_INCOMPLETE"
        else:
            audit_status = "PASSED"
            audit_failure_reason = None

        if audit_status == "FAILED":
            coverage_status = audit_failure_reason
        elif filing_document_coverage_status == "PARTIAL":
            coverage_status = "PARTIAL"
        elif filing_document_coverage_status == "NO_COVERAGE":
            coverage_status = "NO_COVERAGE"
        else:
            coverage_status = "COMPLETE"

        rows.append(
            {
                "cik": cik,
                "company_id": company_id,
                "company_name": company_name,
                "company_registry_present": company_registry_present,
                "sec_filing_count": len(filings),
                "available_sec_filing_scope": "LATEST_SUBMISSIONS_WINDOW",
                "form_d_filing_count": form_d_filings,
                "form_d_submissions_window_count": int(
                    forms.isin({"D", "D/A"}).sum()
                ),
                "step17_form_d_observation_count": len(step17_form_d_rows),
                "form_d_coverage_status": (
                    "EVENTS_FOUND"
                    if form_d_filings
                    else "NO_FORM_D_IN_AVAILABLE_FILINGS"
                    if not filings.empty
                    else "NO_COVERAGE"
                ),
                "latest_accession_number": latest_accession,
                "downloaded_document_count": len(downloaded_ids),
                "production_document_count": len(production_document_ids),
                "production_downloaded_document_count": len(production_downloaded_ids),
                "production_processed_document_count": len(production_processed_ids),
                "expected_backfill_document_count": expected_backfill_document_count,
                "document_backfill_scope_status": document_backfill_scope_status,
                "filing_document_coverage_status": filing_document_coverage_status,
                "test_document_count": len(test_document_ids),
                "form_d_document_count": len(form_d_document_ids),
                "metadata_document_count": len(artifact_ids["document_metadata.parquet"]),
                "text_document_count": len(artifact_ids["document_text.parquet"]),
                "classified_document_count": len(artifact_ids["document_classification.parquet"]),
                "understood_document_count": len(artifact_ids["document_understanding.parquet"]),
                "missing_document_artifact_count": len(missing_document_ids),
                "expected_structured_event_document_count": len(expected_event_ids),
                "persisted_structured_event_count": len(production_company_events),
                "test_structured_event_count": len(test_company_events),
                "unknown_environment_structured_event_count": len(
                    unknown_company_events
                ),
                "persisted_structured_event_document_count": len(
                    expected_event_ids & persisted_event_ids
                ),
                "missing_structured_event_document_count": len(missing_event_ids),
                "persisted_step17_form_d_document_count": len(
                    persisted_step17_document_ids
                ),
                "missing_step17_form_d_document_count": len(
                    missing_step17_document_ids
                ),
                "funding_event_count": funding_event_count,
                "test_funding_event_count": test_funding_event_count,
                "company_state_status": state_status,
                "investment_signal_count": signal_count,
                "test_current_investment_signal_count": current_test_signal_count,
                "decision_signal_count": current_decision_count,
                "state_signal_status": state_signal_status,
                "inbound_event_count": inbound_event_count,
                "test_inbound_event_count": test_inbound_event_count,
                "unified_company_view_present": unified_view_present,
                "audit_status": audit_status,
                "audit_failure_reason": audit_failure_reason,
                "coverage_status": coverage_status,
            }
        )

    return pd.DataFrame(rows).sort_values("cik").reset_index(drop=True)


def _backfill_company(
    cik: str,
    accession_number: str | None,
) -> dict[str, Any]:
    cik = _normalize_cik(cik)
    company_id = f"SEC_{cik}"
    event = PlatformEvent(
        event_type="DOCUMENT_RECEIVED",
        company_id=company_id,
        source_id="SEC",
        payload={
            "cik": cik,
            "accession_number": accession_number,
            "reason": "STEP34_1_EVENT_DOCUMENT_COVERAGE_BACKFILL",
        },
    )
    store = EventStore()
    store.append(event)
    from company_platform.orchestrator.event_consumer import EventConsumer

    result = EventConsumer(event_store=store).consume_event(event.event_id)
    if result.get("run_status") not in {"SUCCESS", "PARTIAL"}:
        raise RuntimeError(
            f"Document backfill failed for {company_id}: {result}"
        )
    return {
        "cik": cik,
        "company_id": company_id,
        "event_id": event.event_id,
        "run_id": result["run_id"],
        "run_status": result["run_status"],
    }


def _backfill_step17_form_d(row: dict[str, Any]) -> dict[str, Any]:
    cik = _normalize_cik(row["cik"])
    company_id = f"SEC_{cik}"
    document_id = _form_d_document_id(row)
    event = PlatformEvent(
        event_type="DOCUMENT_RECEIVED",
        company_id=company_id,
        source_id="SEC_FORM_D",
        payload={
            "cik": cik,
            "document_id": document_id,
            "filing_record": _form_d_filing_record(row),
            "funding_facts": _form_d_facts(row),
            "reason": "STEP34_1_STEP17_FORM_D_DOCUMENT_BACKFILL",
        },
    )
    store = EventStore()
    store.append(event)
    from company_platform.orchestrator.event_consumer import EventConsumer

    result = EventConsumer(event_store=store).consume_event(event.event_id)
    if result.get("run_status") != "SUCCESS":
        raise RuntimeError(
            f"SEC Form D backfill failed for {company_id} "
            f"({row['accession_number']}): {result}"
        )
    return {
        "cik": cik,
        "company_id": company_id,
        "event_id": event.event_id,
        "run_id": result["run_id"],
        "run_status": result["run_status"],
    }


def _rebuild_unified_company_view() -> pd.DataFrame:
    from company_platform.company_intelligence.unified_company_view import (
        UnifiedCompanyViewBuilder,
    )

    frame, _ = UnifiedCompanyViewBuilder().build_and_write()
    actual = set(frame["cik"].map(_normalize_cik))
    expected = {_normalize_cik(cik) for cik in active_target_companies()}
    if actual != expected:
        raise RuntimeError(
            "Unified Company View target mismatch: "
            f"missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )
    return frame


def _verify_website() -> None:
    from fastapi.testclient import TestClient
    from company_platform.api.app import app

    expected = {_normalize_cik(cik) for cik in active_target_companies()}
    with TestClient(app) as client:
        for path in ("/", "/web/app.js", "/web/styles.css"):
            response = client.get(path)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Website endpoint failed: {path} -> "
                    f"{response.status_code}"
                )
        companies = client.get("/api/companies")
        if companies.status_code != 200:
            raise RuntimeError(
                f"Company API failed: {companies.status_code}"
            )
        actual = {
            _normalize_cik(row["cik"])
            for row in companies.json().get("companies", [])
        }
        if actual != expected:
            raise RuntimeError(
                "Website company list mismatch: "
                f"missing={sorted(expected - actual)}, "
                f"unexpected={sorted(actual - expected)}"
            )
        for cik in sorted(expected):
            response = client.get(f"/api/companies/{cik}")
            if response.status_code != 200:
                raise RuntimeError(
                    f"Company detail page data failed for {cik}: "
                    f"{response.status_code}"
                )


def _write_coverage(matrix: pd.DataFrame, runs: list[dict[str, Any]]) -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    matrix.to_parquet(OUTPUT_ROOT / "coverage_matrix.parquet", index=False)
    pd.DataFrame(
        runs,
        columns=["cik", "company_id", "event_id", "run_id", "run_status"],
    ).to_parquet(
        OUTPUT_ROOT / "backfill_runs.parquet",
        index=False,
    )
    summary = {
        "step": "34.1",
        "target_company_count": len(active_target_companies()),
        "validated_company_count": int(
            matrix["audit_status"].eq("PASSED").sum()
        ),
        "failed_validation_company_count": int(
            matrix["audit_status"].ne("PASSED").sum()
        ),
        "companies_without_supported_funding_events": int(
            matrix["state_signal_status"]
            .eq("NO_SUPPORTED_FUNDING_EVENTS")
            .sum()
        ),
        "coverage_status": (
            "INCOMPLETE"
            if matrix["audit_status"].ne("PASSED").any()
            else "COMPLETE"
            if matrix["coverage_status"].eq("COMPLETE").all()
            else "PARTIAL"
        ),
        "audit_status": (
            "PASSED"
            if matrix["audit_status"].eq("PASSED").all()
            else "INCOMPLETE"
        ),
        "output": str(OUTPUT_ROOT.resolve()),
    }
    (OUTPUT_ROOT / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _load_backfill_runs() -> list[dict[str, Any]]:
    frame = _read_parquet(OUTPUT_ROOT / "backfill_runs.parquet")
    if frame.empty:
        return []
    return frame.to_dict(orient="records")


def form_d_download_targets(matrix: pd.DataFrame) -> set[str]:
    """Companies with Form D evidence and no downloaded SEC documents."""
    if matrix.empty:
        return set()
    selected = matrix[
        (
            matrix["step17_form_d_observation_count"].fillna(0).gt(0)
            | matrix["form_d_filing_count"].fillna(0).gt(0)
        )
        & matrix["downloaded_document_count"].fillna(0).eq(0)
    ]
    return {_normalize_cik(cik) for cik in selected["cik"]}


def latest_filing_download_targets(matrix: pd.DataFrame) -> set[str]:
    """Companies with SEC filings and no downloaded documents."""
    if matrix.empty:
        return set()
    selected = matrix[
        matrix["downloaded_document_count"].fillna(0).eq(0)
        & matrix["sec_filing_count"].fillna(0).gt(0)
    ]
    return {_normalize_cik(cik) for cik in selected["cik"]}


def document_limit_targets(matrix: pd.DataFrame) -> set[str]:
    """Companies still short of the 10-latest-filing document limit."""
    if matrix.empty:
        return set()
    selected = matrix[
        matrix["production_processed_document_count"].fillna(0)
        < matrix["expected_backfill_document_count"].fillna(0)
    ]
    return {_normalize_cik(cik) for cik in selected["cik"]}


def _downloaded_accessions(cik: str) -> set[str]:
    """Accessions whose primary document is already on disk."""
    manifest = _read_parquet(
        BRONZE_DOCUMENTS_ROOT
        / f"cik={_normalize_cik(cik)}"
        / "document_manifest.parquet"
    )
    if manifest.empty or not {"accession_number", "status"}.issubset(manifest.columns):
        return set()
    downloaded = manifest[
        manifest["status"].astype(str).str.upper().eq("DOWNLOADED")
    ]
    if "local_path" in downloaded.columns:
        downloaded = downloaded[
            downloaded["local_path"].fillna("").astype(str).str.strip().ne("")
            & downloaded["local_path"].map(lambda path: Path(str(path)).is_file())
        ]
    return {
        accession
        for accession in downloaded["accession_number"].dropna().astype(str).str.strip()
        if accession
    }


def missing_backfill_accessions(cik: str) -> list[str]:
    """Latest accessions still missing, capped at the document backfill limit."""
    cik = _normalize_cik(cik)
    filings = _read_parquet(FILINGS_ROOT / f"cik={cik}" / "filings.parquet")
    if filings.empty or "accession_number" not in filings.columns:
        return []
    filings = filings.dropna(subset=["accession_number"]).copy()
    filings["accession_number"] = filings["accession_number"].astype(str).str.strip()
    filings = filings[filings["accession_number"].ne("")]
    if filings.empty:
        return []
    if "filing_date" in filings.columns:
        filings = filings.sort_values(
            "filing_date",
            ascending=False,
            na_position="last",
        )
    latest: list[str] = []
    for accession in filings["accession_number"]:
        if accession in latest:
            continue
        latest.append(accession)
        if len(latest) >= BACKFILL_DOCUMENT_LIMIT:
            break
    downloaded = _downloaded_accessions(cik)
    return [accession for accession in latest if accession not in downloaded]


def _refresh_incomplete_funding_signals(matrix: pd.DataFrame) -> list[str]:
    """Regenerate signals from persisted Form D state.

    This does not create offering amounts. Companies with an active Form D
    state and no current-version signal are the only targets.
    """
    if matrix.empty or "state_signal_status" not in matrix.columns:
        return []
    selected = matrix[
        matrix["funding_event_count"].fillna(0).gt(0)
        & matrix["company_state_status"].fillna("").astype(str).str.upper().eq(
            "ACTIVE"
        )
        & matrix["state_signal_status"].eq("INCOMPLETE")
        & matrix["investment_signal_count"].fillna(0).eq(0)
    ]
    if selected.empty:
        return []
    from company_platform.investment_intelligence.unified_intelligence import (
        UnifiedInvestmentIntelligence,
    )

    intelligence = UnifiedInvestmentIntelligence()
    refreshed: list[str] = []
    for cik in selected["cik"]:
        normalized = _normalize_cik(cik)
        company_id = f"SEC_{normalized}"
        output = intelligence.write_company_artifact(company_id)
        if output is None:
            raise RuntimeError(
                "Funding signal refresh found no persisted Form D state "
                f"for {company_id}"
            )
        refreshed.append(normalized)
    return refreshed


def run(
    backfill: bool = False,
    form_d_only: bool = False,
    latest_filing_only: bool = False,
    fill_document_limit: bool = False,
) -> pd.DataFrame:
    initial = build_coverage_matrix()
    runs = _load_backfill_runs()
    accession_failures: list[dict[str, str]] = []
    if backfill or form_d_only or latest_filing_only or fill_document_limit:
        _write_coverage(initial, runs)
        if fill_document_limit:
            targets = document_limit_targets(initial)
            print(
                "FILL_DOCUMENT_LIMIT_TARGETS: "
                + ", ".join(sorted(targets or []))
            )
            pending = [
                (cik, accession)
                for cik in sorted(targets or [])
                for accession in missing_backfill_accessions(cik)
            ]
            print(f"FILL_DOCUMENT_LIMIT_ACCESSIONS: {len(pending)}")
            for cik, accession in pending:
                try:
                    runs.append(_backfill_company(cik, accession))
                except RuntimeError as exc:
                    accession_failures.append(
                        {
                            "cik": cik,
                            "accession_number": accession,
                            "error": str(exc),
                        }
                    )
                    print(
                        "FILL_DOCUMENT_ACCESSION_FAILED: "
                        f"{cik} {accession}"
                    )
                _write_coverage(build_coverage_matrix(), runs)
            _rebuild_unified_company_view()
            _verify_website()
        else:
            if latest_filing_only:
                refreshed = _refresh_incomplete_funding_signals(initial)
                if refreshed:
                    print(
                        "FUNDING_SIGNALS_REFRESHED: "
                        + ", ".join(sorted(refreshed))
                    )
                    initial = build_coverage_matrix()
                    _write_coverage(initial, runs)
                targets = latest_filing_download_targets(initial)
                print(
                    "LATEST_FILING_DOWNLOAD_TARGETS: "
                    + ", ".join(sorted(targets or []))
                )
            elif form_d_only:
                targets = form_d_download_targets(initial)
                print(
                    "FORM_D_DOWNLOAD_TARGETS: "
                    + ", ".join(sorted(targets or []))
                )
            else:
                targets = None
            missing = initial[
                (
                    initial["document_backfill_scope_status"].ne("COMPLETE")
                    | initial["missing_structured_event_document_count"].gt(0)
                )
                & initial["sec_filing_count"].gt(0)
            ]
            if targets is not None:
                missing = missing[
                    missing["cik"].map(_normalize_cik).isin(targets)
                ]
            for row in missing.itertuples():
                runs.append(
                    _backfill_company(
                        row.cik,
                        row.latest_accession_number,
                    )
                )
                _write_coverage(build_coverage_matrix(), runs)
            persisted_document_ids = set()
            structured_events = _sqlite_rows(
                STRUCTURED_EVENTS_DB,
                "structured_events",
            )
            for cik in active_target_companies():
                persisted_document_ids.update(
                    str(row.get("document_id"))
                    for row in structured_events
                    if str(row.get("company_id")) == f"SEC_{_normalize_cik(cik)}"
                    and str(row.get("source_environment", "")).upper()
                    == "PRODUCTION"
                    and row.get("document_id") is not None
                )
            form_d_records = _step17_form_d_rows().to_dict(orient="records")
            if targets is not None:
                form_d_records = [
                    record
                    for record in form_d_records
                    if _normalize_cik(record["cik"]) in targets
                ]
            for record in form_d_records:
                if _form_d_document_id(record) in persisted_document_ids:
                    continue
                runs.append(_backfill_step17_form_d(record))
                _write_coverage(build_coverage_matrix(), runs)
            _rebuild_unified_company_view()
            _verify_website()

    matrix = build_coverage_matrix()
    _write_coverage(matrix, runs)
    print(matrix.to_string(index=False))
    print(f"COVERAGE_MATRIX: {OUTPUT_ROOT / 'coverage_matrix.parquet'}")
    print(
        "AUDIT: "
        + (
            "PASSED"
            if matrix["audit_status"].eq("PASSED").all()
            else "FAILED"
        )
        + " | COVERAGE: "
        + (
            "COMPLETE"
            if matrix["coverage_status"].eq("COMPLETE").all()
            else "PARTIAL"
            if matrix["coverage_status"].eq("PARTIAL").all()
            else "INCOMPLETE"
        )
    )
    scoped_backfill = form_d_only or latest_filing_only or fill_document_limit
    if (
        backfill
        and not scoped_backfill
        and not matrix["audit_status"].eq("PASSED").all()
    ):
        raise RuntimeError(
            "Step 34.1 coverage remains incomplete; see coverage_matrix.parquet"
        )
    if form_d_only and not matrix["audit_status"].eq("PASSED").all():
        print(
            "FORM_D_BACKFILL_REPORTED: universe audit remains incomplete. "
            "Companies without Form D documents were not required to pass."
        )
    if latest_filing_only and not matrix["audit_status"].eq("PASSED").all():
        print(
            "LATEST_FILING_BACKFILL_REPORTED: universe audit remains incomplete. "
            "One latest filing was downloaded per company with no documents. "
            "The 10-document audit was not required to pass. "
            "Companies without Form D were not marked funding-complete."
        )
    if fill_document_limit:
        print(
            "FILL_DOCUMENT_LIMIT_REPORTED: "
            + (
                "universe audit remains incomplete. "
                if not matrix["audit_status"].eq("PASSED").all()
                else "document-limit backfill finished. "
            )
            + "Missing accessions among the 10 latest filings were downloaded. "
            "Companies without Form D were not marked funding-complete."
        )
        if accession_failures:
            print(
                "FILL_DOCUMENT_LIMIT_FAILURES: "
                + ", ".join(
                    f"{item['cik']}:{item['accession_number']}"
                    for item in accession_failures
                )
            )
            raise RuntimeError(
                "Document backfill left "
                f"{len(accession_failures)} accession(s) failed. "
                "Downloaded documents were kept. Rerun "
                "--fill-document-limit to retry the failed accessions."
            )
    return matrix


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit or backfill active-company SEC document intelligence coverage."
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill companies with missing document or structured-event artifacts.",
    )
    parser.add_argument(
        "--form-d-only",
        action="store_true",
        help=(
            "Backfill only companies that have Form D evidence and "
            "no downloaded SEC documents. Incomplete coverage for the "
            "rest of the universe is reported, not treated as a failure."
        ),
    )
    parser.add_argument(
        "--latest-filing-only",
        action="store_true",
        help=(
            "Download one latest SEC filing for companies that have "
            "filings and no documents. Incomplete 10-document coverage "
            "is reported, not treated as a failure."
        ),
    )
    parser.add_argument(
        "--fill-document-limit",
        action="store_true",
        help=(
            "Download missing accessions among the 10 latest filings "
            "for companies below that document limit. An incomplete "
            "universe audit is reported, not treated as a failure."
        ),
    )
    args = parser.parse_args()
    run(
        backfill=args.backfill,
        form_d_only=args.form_d_only,
        latest_filing_only=args.latest_filing_only,
        fill_document_limit=args.fill_document_limit,
    )


if __name__ == "__main__":
    main()
