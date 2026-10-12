from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

from .service import (
    API_VERSION,
    consume_event,
    get_company,
    get_coverage,
    get_event,
    get_health,
    get_operational_summary,
    get_readiness,
    get_run,
    list_companies,
    retry_event,
)

WEB_ROOT = Path(__file__).resolve().parents[1] / "web"


class RetryRequest(BaseModel):
    reason: str = Field(min_length=3)
    actor: str = Field(min_length=1)


def install_step33_routes(app: FastAPI) -> None:
    if WEB_ROOT.exists():
        app.mount("/web", StaticFiles(directory=WEB_ROOT), name="web")

    @app.get("/")
    def website():
        index_file = WEB_ROOT / "index.html"
        if not index_file.is_file():
            raise HTTPException(
                status_code=503,
                detail="Website assets are not installed.",
            )
        return FileResponse(index_file)

    @app.get("/companies")
    @app.get("/api/companies")
    def companies():
        company_list = list_companies()
        return {
            "api_version": API_VERSION,
            "count": len(company_list),
            "companies": company_list,
        }

    @app.get("/companies/{cik}")
    @app.get("/api/companies/{cik}")
    def company(cik: str):
        result = get_company(cik)
        if result is None:
            raise HTTPException(status_code=404, detail="Company not found")
        return result

    @app.get("/health")
    @app.get("/api/health")
    def health():
        return get_health()

    @app.get("/readiness")
    @app.get("/api/readiness")
    def readiness():
        return get_readiness()

    @app.get("/coverage")
    @app.get("/api/coverage")
    def coverage():
        return get_coverage()

    @app.get("/operations/summary")
    @app.get("/api/operations/summary")
    def operations_summary():
        return get_operational_summary()

    @app.get("/events/{event_id}")
    @app.get("/api/events/{event_id}")
    def event(event_id: str):
        result = get_event(event_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Event not found")
        return result

    @app.post("/events/{event_id}/consume")
    @app.post("/api/events/{event_id}/consume")
    def consume(event_id: str):
        try:
            return consume_event(event_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="Event not found")

    @app.post("/events/{event_id}/retry")
    @app.post("/api/events/{event_id}/retry")
    def retry(event_id: str, request: RetryRequest):
        try:
            return retry_event(event_id, request.reason, request.actor)
        except KeyError:
            raise HTTPException(status_code=404, detail="Event not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

    @app.get("/runs/{run_id}")
    @app.get("/api/runs/{run_id}")
    def run(run_id: str):
        result = get_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result
