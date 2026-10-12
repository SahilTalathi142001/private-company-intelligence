
"""RunStore-compatible SQLite adapter; keeps pandas DataFrame query contract."""
import json, sqlite3, time
from dataclasses import fields
from pathlib import Path
import pandas as pd
from .models import PipelineRun, RUN_STATUSES, TASK_STATUSES

RUN_COLUMNS=["run_id","company_id","trigger_type","trigger_event_id","correlation_id","pipeline_version","status","started_at_utc","completed_at_utc","error_message","source_id"]
TASK_COLUMNS=["task_id","run_id","company_id","component_id","task_sequence","status","attempt_number","max_attempts","started_at_utc","completed_at_utc","error_message","dq_status","blocked_by"]

class SQLiteRunStore:
 def __init__(self, root=None, db_path=None):
  if db_path is None:
   root=Path(root) if root else Path(__file__).resolve().parents[3] / "data" / "silver" / "platform"
   root.mkdir(parents=True,exist_ok=True); db_path=root/"control.db"
  self.db_path=str(db_path); Path(self.db_path).parent.mkdir(parents=True,exist_ok=True)
  with self._db() as c:
   c.executescript("""
   CREATE TABLE IF NOT EXISTS pipeline_runs(run_id TEXT PRIMARY KEY,trigger_event_id TEXT UNIQUE,data TEXT NOT NULL);
   CREATE TABLE IF NOT EXISTS task_runs(task_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,component_id TEXT NOT NULL,data TEXT NOT NULL,UNIQUE(run_id,component_id),FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id));
   CREATE TABLE IF NOT EXISTS task_recovery_audit(audit_id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,from_status TEXT NOT NULL,to_status TEXT NOT NULL,reason TEXT NOT NULL,created_at_utc TEXT NOT NULL);
   """)
 def _db(self):
  c=sqlite3.connect(self.db_path,timeout=30); c.row_factory=sqlite3.Row
  c.execute("PRAGMA busy_timeout=30000"); c.execute("PRAGMA foreign_keys=ON"); return c
 def _clean(self,d):
  return {k:(v.item() if hasattr(v,"item") else v) for k,v in d.items()}
 def runs(self):
  with self._db() as c: rows=c.execute("SELECT data FROM pipeline_runs").fetchall()
  return pd.DataFrame([json.loads(r[0]) for r in rows],columns=RUN_COLUMNS)
 def tasks(self):
  with self._db() as c: rows=c.execute("SELECT data FROM task_runs").fetchall()
  return pd.DataFrame([json.loads(r[0]) for r in rows],columns=TASK_COLUMNS)
 def create_run(self,run):
  run.validate(); d=self._clean(run.to_dict())
  with self._db() as c:c.execute("INSERT INTO pipeline_runs VALUES(?,?,?)",(str(d["run_id"]),d.get("trigger_event_id"),json.dumps(d,default=str)))
 def create_task(self,task):
  task.validate(); d=self._clean(task.to_dict())
  with self._db() as c:c.execute("INSERT INTO task_runs VALUES(?,?,?,?)",(str(d["task_id"]),str(d["run_id"]),str(d["component_id"]),json.dumps(d,default=str)))
 def create_run_with_tasks(self,run,tasks):
  run.validate()
  for t in tasks:t.validate()
  rd=self._clean(run.to_dict()); tds=[self._clean(t.to_dict()) for t in tasks]
  with self._db() as c:
   c.execute("BEGIN IMMEDIATE")
   try:
    old=None
    if rd.get("trigger_event_id"):
     old=c.execute("SELECT run_id FROM pipeline_runs WHERE trigger_event_id=?",(rd["trigger_event_id"],)).fetchone()
    if old:c.rollback(); return old["run_id"],False
    c.execute("INSERT INTO pipeline_runs VALUES(?,?,?)",(str(rd["run_id"]),rd.get("trigger_event_id"),json.dumps(rd,default=str)))
    for d in tds:c.execute("INSERT INTO task_runs VALUES(?,?,?,?)",(str(d["task_id"]),str(d["run_id"]),str(d["component_id"]),json.dumps(d,default=str)))
    c.commit(); return rd["run_id"],True
   except: c.rollback(); raise
 def update_run(self,run_id,status,started_at_utc=None,completed_at_utc=None,error_message=None):
  if status not in RUN_STATUSES: raise ValueError(status)
  with self._db() as c:
   r=c.execute("SELECT data FROM pipeline_runs WHERE run_id=?",(str(run_id),)).fetchone()
   if not r:raise KeyError(run_id)
   d=json.loads(r[0]); d["status"]=status
   for k,v in [("started_at_utc",started_at_utc),("completed_at_utc",completed_at_utc),("error_message",error_message)]:
    if v is not None:d[k]=v
   c.execute("UPDATE pipeline_runs SET data=? WHERE run_id=?",(json.dumps(d,default=str),str(run_id)))
 def update_task(self,task_id,status,attempt_number=None,started_at_utc=None,completed_at_utc=None,error_message=None,dq_status=None,blocked_by=None):
  if status not in TASK_STATUSES:raise ValueError(status)
  with self._db() as c:
   r=c.execute("SELECT data FROM task_runs WHERE task_id=?",(str(task_id),)).fetchone()
   if not r:raise KeyError(task_id)
   d=json.loads(r[0]); d["status"]=status
   if status=="PENDING":
    d["started_at_utc"]=None; d["completed_at_utc"]=None
    d["error_message"]=None; d["dq_status"]=None; d["blocked_by"]=None
   for k,v in [("attempt_number",attempt_number),("started_at_utc",started_at_utc),("completed_at_utc",completed_at_utc),("error_message",error_message),("dq_status",dq_status),("blocked_by",blocked_by)]:
    if v is not None:d[k]=v
   c.execute("UPDATE task_runs SET data=? WHERE task_id=?",(json.dumps(d,default=str),str(task_id)))
 def get_run(self,run_id):
  with self._db() as c:r=c.execute("SELECT data FROM pipeline_runs WHERE run_id=?",(str(run_id),)).fetchone()
  if not r:
   return None
  data = json.loads(r[0])
  # Persist the full PipelineRun blob, then reconstruct the typed model.
  # Callers use both attribute access and dict-style .get()/[] (orchestrator,
  # ExecutionContext, and tests), so this must remain a PipelineRun rather
  # than a RUN_COLUMNS projection.
  allowed = {f.name for f in fields(PipelineRun)}
  return PipelineRun(**{k: v for k, v in data.items() if k in allowed})
 def get_runs_for_company(self,company_id):
  return self.runs().query("company_id == @company_id").reset_index(drop=True)
 def get_tasks_for_run(self,run_id):
  return self.tasks().query("run_id == @run_id").reset_index(drop=True)

 def reconcile_running_task(self,task_id,to_status,reason):
  """Explicitly reconcile a stranded RUNNING task; never auto-replays it."""
  from datetime import datetime, timezone
  if to_status not in {"SUCCESS","PENDING"}: raise ValueError("to_status must be SUCCESS or PENDING")
  if not isinstance(reason,str) or not reason.strip(): raise ValueError("A non-empty reconciliation reason is required")
  now=datetime.now(timezone.utc).isoformat()
  with self._db() as c:
   c.execute("BEGIN IMMEDIATE")
   row=c.execute("SELECT data FROM task_runs WHERE task_id=?",(str(task_id),)).fetchone()
   if not row: raise KeyError(task_id)
   d=json.loads(row[0])
   if d.get("status")!="RUNNING": raise ValueError("Only RUNNING tasks can be reconciled")
   old=d["status"]; d["status"]=to_status
   d["error_message"]=("Operator reconciliation: "+reason.strip()) if to_status=="PENDING" else None
   if to_status=="PENDING": d["started_at_utc"]=None; d["completed_at_utc"]=None
   else: d["completed_at_utc"]=now
   c.execute("UPDATE task_runs SET data=? WHERE task_id=?",(json.dumps(d,default=str),str(task_id)))
   c.execute("INSERT INTO task_recovery_audit(task_id,from_status,to_status,reason,created_at_utc) VALUES(?,?,?,?,?)",(str(task_id),old,to_status,reason.strip(),now))
  return d
