from __future__ import annotations
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from .service import *

WEB_ROOT = Path(__file__).resolve().parents[1] / "web"
app = FastAPI(title="Private Company Intelligence Platform", version=API_VERSION)
app.mount("/web", StaticFiles(directory=WEB_ROOT), name="web")

class RetryRequest(BaseModel):
    reason: str = Field(min_length=3)
    actor: str = Field(min_length=1)

@app.get("/", include_in_schema=False)
def website(): return FileResponse(WEB_ROOT / "index.html")
@app.get("/health")
def health(): return get_health()
@app.get("/readiness")
def readiness(): return get_readiness()
@app.get("/companies")
def companies(): return {"api_version":API_VERSION,"count":len(list_companies()),"companies":list_companies()}
@app.get("/companies/{cik}")
def company(cik:str):
    x=get_company(cik)
    if x is None: raise HTTPException(404,"Company not found")
    return x
@app.get("/events/{event_id}")
def event(event_id:str):
    x=get_event(event_id)
    if x is None: raise HTTPException(404,"Event not found")
    return x
@app.get("/runs/{run_id}")
def run(run_id:str):
    x=get_run(run_id)
    if x is None: raise HTTPException(404,"Run not found")
    return x
@app.get("/operations/summary")
def operations_summary(): return get_operations_summary()
@app.post("/events/{event_id}/consume")
def consume(event_id:str):
    try: return consume_event(event_id)
    except KeyError: raise HTTPException(404,"Event not found")
@app.post("/events/{event_id}/retry")
def retry(event_id:str, request:RetryRequest):
    try: return retry_event(event_id,request.reason,request.actor)
    except KeyError: raise HTTPException(404,"Event not found")
    except ValueError as e: raise HTTPException(409,str(e))

for path, fn in [("/api/health",health),("/api/readiness",readiness),("/api/companies",companies),("/api/operations/summary",operations_summary)]: app.get(path)(fn)
app.get("/api/companies/{cik}")(company)
app.get("/api/events/{event_id}")(event)
app.get("/api/runs/{run_id}")(run)
