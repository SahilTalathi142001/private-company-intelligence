"""CIDM Investor Intelligence API v1 — read-only presentation API."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import service

WEB_ROOT = Path(__file__).resolve().parents[1] / "web"
app = FastAPI(
    title="CIDM | Private Company Intelligence",
    description="Read-only, source-aware company intelligence API. Missing data is not inferred.",
    version=service.API_VERSION,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)
app.mount("/assets", StaticFiles(directory=WEB_ROOT / "assets"), name="assets")
# Read-only compatibility mount for the existing Step 34 website smoke check.
app.mount("/web", StaticFiles(directory=WEB_ROOT), name="web-compat")


class ErrorResponse(BaseModel):
    code: str
    message: str


@app.get("/", include_in_schema=False)
def home():
    return FileResponse(WEB_ROOT / "index.html")


@app.get("/api/v1/health", tags=["System"])
def health():
    return service.get_health_detail()


@app.get("/api/v1/companies", tags=["Companies"])
def companies(
    q: str = Query(default="", max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="name", max_length=80),
    direction: Literal["asc", "desc"] = "asc",
    status: str = Query(default="", max_length=40),
):
    result = service.list_companies(query=q, limit=limit, offset=offset, sort=sort, direction=direction, status=status)
    if result.get("data_status") != "AVAILABLE":
        raise HTTPException(status_code=503, detail={"code": "COMPANY_REGISTRY_UNAVAILABLE", "message": result.get("message", "Company registry is unavailable.")})
    return result


@app.get("/api/v1/companies/{cik}", tags=["Companies"])
def company(cik: str):
    if not cik.replace("SEC_", "").isdigit():
        raise HTTPException(status_code=422, detail={"code": "INVALID_CIK", "message": "CIK must be numeric."})
    item = service.get_company(cik)
    if item is None:
        health = service.get_health_detail()
        if health.get("company_registry") != "AVAILABLE":
            raise HTTPException(status_code=503, detail={"code": "COMPANY_REGISTRY_UNAVAILABLE", "message": "Company registry is missing or unreadable."})
        raise HTTPException(status_code=404, detail={"code": "COMPANY_NOT_FOUND", "message": "No matching company exists in the active registry."})
    return item


@app.get("/api/v1/companies/{cik}/domains/{domain}", tags=["Company data"])
def company_domain(cik: str, domain: str):
    if domain not in service.ARTIFACTS:
        raise HTTPException(status_code=404, detail={"code": "DOMAIN_NOT_SUPPORTED", "message": "Unsupported company data domain."})
    item = service.get_company(cik)
    if item is None:
        health = service.get_health_detail()
        if health.get("company_registry") != "AVAILABLE":
            raise HTTPException(status_code=503, detail={"code": "COMPANY_REGISTRY_UNAVAILABLE", "message": "Company registry is missing or unreadable."})
        raise HTTPException(status_code=404, detail={"code": "COMPANY_NOT_FOUND", "message": "No matching company exists in the active registry."})
    return {"cik": item["cik"], "domain": domain, "coverage": item["coverage"][domain], "data": item["sections"][domain]}


@app.get("/api/v1/coverage", tags=["Data quality"])
def coverage(q: str = Query(default="", max_length=200), limit: int = Query(default=100, ge=1, le=500), offset: int = Query(default=0, ge=0)):
    page = service.list_companies(query=q, limit=limit, offset=offset)
    if page.get("data_status") != "AVAILABLE":
        raise HTTPException(status_code=503, detail={"code": "COMPANY_REGISTRY_UNAVAILABLE", "message": page.get("message", "Company registry is unavailable.")})
    return {"items": [{"cik": c.get("cik"), "name": c.get("name") or c.get("company_name") or c.get("legal_name"), "coverage": c.get("coverage", {})} for c in page["items"]],
            "total": page["total"], "limit": limit, "offset": offset, "data_status": page["data_status"]}


@app.get("/api/v1/operations/summary", tags=["Operations"])
def operations_summary():
    return service.get_operations_summary()


# Thin compatibility adapters keep the existing Step 34 website verification working
# while the primary product contract remains /api/v1.
@app.get("/health", include_in_schema=False)
def legacy_health():
    return service.get_health()


@app.get("/readiness", include_in_schema=False)
def legacy_readiness():
    try:
        return service.get_readiness()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Production readiness is unavailable.") from exc


@app.get("/companies", include_in_schema=False)
def legacy_companies():
    result = service.list_companies(limit=100000, offset=0)
    if result.get("data_status") != "AVAILABLE":
        raise HTTPException(status_code=503, detail={"code": "COMPANY_REGISTRY_UNAVAILABLE", "message": result.get("message", "Company registry is unavailable.")})
    rows = result.get("items", [])
    return {"api_version": service.API_VERSION, "count": result.get("total", len(rows)), "companies": rows}


@app.get("/companies/{cik}", include_in_schema=False)
def legacy_company(cik: str):
    item = service.get_company(cik)
    if item is None:
        health = service.get_health_detail()
        if health.get("company_registry") != "AVAILABLE":
            raise HTTPException(status_code=503, detail="Company registry is missing or unreadable.")
        raise HTTPException(status_code=404, detail="Company not found.")
    return item


@app.get("/events/{event_id}", include_in_schema=False)
def legacy_event(event_id: str):
    item = service.get_event(event_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Event not found.")
    return item


@app.get("/runs/{run_id}", include_in_schema=False)
def legacy_run(run_id: str):
    item = service.get_run(run_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    return item


@app.get("/api/health", include_in_schema=False)
def legacy_api_health():
    return service.get_health_detail()


@app.get("/api/readiness", include_in_schema=False)
def legacy_api_readiness():
    return legacy_readiness()


@app.get("/api/companies", include_in_schema=False)
def legacy_api_companies():
    return legacy_companies()


@app.get("/api/companies/{cik}", include_in_schema=False)
def legacy_api_company(cik: str):
    return legacy_company(cik)


@app.get("/api/events/{event_id}", include_in_schema=False)
def legacy_api_event(event_id: str):
    return legacy_event(event_id)


@app.get("/api/runs/{run_id}", include_in_schema=False)
def legacy_api_run(run_id: str):
    return legacy_run(run_id)


@app.get("/api/operations/summary", include_in_schema=False)
def legacy_api_operations_summary():
    return service.get_operations_summary()


@app.exception_handler(Exception)
async def safe_exception_handler(request, exc):
    # Keep diagnostics in server logs without leaking local paths or stack traces to clients.
    logging.getLogger(__name__).exception("Unhandled CIDM API error for %s", request.url.path, exc_info=exc)
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=500, content={"error": {"code": "INTERNAL_ERROR", "message": "The request could not be completed. Check server logs for diagnostic details."}})
