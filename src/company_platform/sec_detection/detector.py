"""Poll SEC submissions for newly observed filings and enqueue deterministic events.

This is polling, not a SEC push stream. A persistent cursor avoids relying on process memory.
"""
from __future__ import annotations
import argparse, hashlib, importlib, json, os, time
from urllib.request import Request, urlopen
from pathlib import Path
from datetime import datetime, timezone

try:
    load_dotenv = importlib.import_module("dotenv").load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    def load_dotenv(*args, **kwargs):
        return False

from company_platform.registry.company_registry import list_companies
from company_platform.events.models import PlatformEvent
from company_platform.events.event_store import EventStore

SEC_SUBMISSIONS="https://data.sec.gov/submissions/CIK{cik}.json"

class SECFilingDetector:
    def __init__(self,event_store=None, state_path=None, user_agent=None, timeout=30):
        root=Path(__file__).resolve().parents[3]
        load_dotenv(root / ".env")
        self.store=event_store or EventStore()
        self.state_path=Path(state_path) if state_path else root/"data"/"silver"/"platform"/"sec_filing_detector_state.json"
        self.state_path.parent.mkdir(parents=True,exist_ok=True)
        self.user_agent=user_agent or os.getenv("SEC_USER_AGENT")
        if not self.user_agent or "@" not in self.user_agent:
            raise ValueError("Set SEC_USER_AGENT to a descriptive app name and valid contact email")
        self.timeout=timeout
    def _state(self):
        return json.loads(self.state_path.read_text()) if self.state_path.exists() else {"seen_accessions":[]}
    def _save(self,state):
        tmp=self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state,indent=2,sort_keys=True),encoding="utf-8")
        tmp.replace(self.state_path)
    def _recent(self,cik):
        request=Request(SEC_SUBMISSIONS.format(cik=cik),headers={"User-Agent":self.user_agent})
        with urlopen(request,timeout=self.timeout) as response:
            data=json.loads(response.read().decode("utf-8"))
        recent=data.get("filings",{}).get("recent",{})
        n=len(recent.get("accessionNumber",[]))
        out=[]
        for i in range(n):
            acc=recent["accessionNumber"][i]
            out.append({k:recent.get(k,[None]*n)[i] for k in
                ["accessionNumber","filingDate","reportDate","form","primaryDocument","primaryDocDescription"]})
        return out
    def poll_once(self, forms=None, max_filings_per_company=20, sleep_seconds=0.12):
        state=self._state(); seen=set(state.get("seen_accessions",[])); emitted=[]
        companies=list_companies()
        if companies.empty:return {"checked":0,"emitted":0,"events":[]}
        for _,co in companies.iterrows():
            if str(co.get("status","ACTIVE"))!="ACTIVE" or not bool(co.get("ingestion_enabled",True)):continue
            cik=str(co["cik"]).zfill(10); company_id=str(co["company_id"])
            filings=self._recent(cik)
            filings=filings[:max_filings_per_company]
            for f in filings:
                acc=f.get("accessionNumber")
                if not acc:continue
                key=f"{cik}:{acc}"
                if key in seen:continue
                form=str(f.get("form") or "")
                if forms and form not in set(forms):continue
                event_key=hashlib.sha256(("SEC_NEW_FILING:"+key).encode()).hexdigest()
                event=PlatformEvent(event_type="NEW_SEC_FILING",company_id=company_id,source_id="SEC",
                    event_id=event_key,payload={"cik":cik,"accession_number":acc,"form":form,
                    "filing_date":f.get("filingDate"),"report_date":f.get("reportDate"),
                    "primary_document":f.get("primaryDocument"),"primary_doc_description":f.get("primaryDocDescription")})
                self.store.append(event); seen.add(key); emitted.append(event_key)
            time.sleep(sleep_seconds)
        state["seen_accessions"]=sorted(seen); state["last_poll_utc"]=datetime.now(timezone.utc).isoformat()
        self._save(state)
        return {"checked":len(companies),"emitted":len(emitted),"events":emitted}
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--forms",default="",help="Optional comma-separated SEC forms")
    p.add_argument("--once",action="store_true")
    p.add_argument("--interval-seconds",type=int,default=900)
    a=p.parse_args()
    d=SECFilingDetector()
    while True:
        print(json.dumps(d.poll_once(forms=[x.strip() for x in a.forms.split(",") if x.strip()] or None),indent=2))
        if a.once:break
        time.sleep(max(60,a.interval_seconds))
if __name__=="__main__":main()
