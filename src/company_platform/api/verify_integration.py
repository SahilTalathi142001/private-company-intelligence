"""Read-only verification against the real CIDM data directory (no fixtures/seeding)."""
from __future__ import annotations

import json
import sys
from collections import Counter
from typing import Any

from . import service


def main() -> int:
    report: dict[str, Any] = {
        "data_root": str(service.DATA),
        "checks": [],
        "errors": [],
        "warnings": [],
        "company_count": 0,
        "domain_availability": {},
        "production_events": 0,
        "test_events_excluded": 0,
        "unknown_events_excluded": 0,
    }

    health = service.get_health_detail()
    report["health"] = health
    if health.get("company_registry") != "AVAILABLE":
        report["errors"].append("Company registry is not readable from the configured data root.")
        report["checks"].append({"name": "registry", "status": "FAIL"})
        print(json.dumps(report, indent=2, default=str))
        return 1
    report["checks"].append({"name": "registry", "status": "PASS", "artifact": health.get("registry_artifact")})

    first_page = service.list_companies(limit=200, offset=0)
    if first_page.get("data_status") != "AVAILABLE":
        report["errors"].append(f"Company listing failed: {first_page.get('data_status')}")
        print(json.dumps(report, indent=2, default=str))
        return 1
    companies = list(first_page.get("items", []))
    total = int(first_page.get("total", len(companies)) or 0)
    for offset in range(200, total, 200):
        page = service.list_companies(limit=200, offset=offset)
        if page.get("data_status") != "AVAILABLE":
            report["errors"].append(f"Company listing page at offset {offset} failed: {page.get('data_status')}")
            break
        companies.extend(page.get("items", []))
    report["company_count"] = total
    report["checks"].append({"name": "company_listing", "status": "PASS" if len(companies) == total else "FAIL", "count": len(companies), "expected": total})

    domain_counts: Counter[str] = Counter()
    domain_errors: list[dict[str, Any]] = []
    identity_conflicts = []
    for row in companies:
        cik = service.normalize_cik(row.get("cik"))
        if row.get("identity_status") == "CONFLICT":
            identity_conflicts.append(cik)
            continue
        company = service.get_company(cik)
        if not company:
            report["errors"].append(f"Registry CIK {cik} could not be resolved by company detail endpoint.")
            continue
        if service.normalize_cik(company.get("cik")) != cik:
            report["errors"].append(f"Company isolation mismatch for requested CIK {cik}.")
            continue
        for domain, status in (company.get("coverage") or {}).items():
            artifact = status.get("artifact_availability") or status.get("status", "UNKNOWN")
            domain_counts[f"{domain}:{artifact}"] += 1
            for facet in ("business_data_availability", "completeness", "freshness", "audit_outcome"):
                domain_counts[f"{domain}:{facet}:{status.get(facet, 'UNKNOWN')}"] += 1
            if status.get("status") == "ERROR":
                domain_errors.append({"cik": cik, "domain": domain, "artifact": status.get("source_artifact"), "note": status.get("note")})
        events = (company.get("sections") or {}).get("events", [])
        for event in events:
            if not event.get("is_production") or event.get("source_environment") != "PRODUCTION":
                report["errors"].append(f"Non-production event leaked into production event list for CIK {cik}.")
        event_coverage = (company.get("coverage") or {}).get("events", {})
        report["production_events"] += len(events)
        report["test_events_excluded"] += int(event_coverage.get("test_events_excluded", 0) or 0)
        report["unknown_events_excluded"] += int(event_coverage.get("unknown_environment_events", 0) or 0)

    report["domain_availability"] = dict(sorted(domain_counts.items()))
    if identity_conflicts:
        report["errors"].append(f"Registry identity conflicts found for CIK(s): {', '.join(sorted(set(identity_conflicts)))}")
    if domain_errors:
        report["errors"].append(f"{len(domain_errors)} artifact(s) exist but cannot be read.")
        report["artifact_read_errors"] = domain_errors
    if service.READ_ERRORS:
        report["warnings"].append("At least one artifact read failed; inspect artifact_read_errors and application logs.")
        report["artifact_read_errors"] = report.get("artifact_read_errors", []) + [
            {"path": path, "error_type": error_type} for path, error_type in service.READ_ERRORS.items()
        ]

    report["checks"].append({"name": "company_identity_and_isolation", "status": "FAIL" if identity_conflicts or any("isolation mismatch" in e for e in report["errors"]) else "PASS"})
    report["checks"].append({"name": "production_event_boundary", "status": "FAIL" if any("event leaked" in e for e in report["errors"]) else "PASS"})
    report["checks"].append({"name": "artifact_readability", "status": "FAIL" if domain_errors else "PASS"})
    print(json.dumps(report, indent=2, default=str))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
