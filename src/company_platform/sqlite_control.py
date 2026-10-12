
"""SQLite transactional persistence for event/run idempotency and worker claims.

SQLite is suitable for workers sharing one local disk. For multi-host deployments use
a server database (e.g. PostgreSQL), not a network-mounted SQLite file.
"""
from __future__ import annotations
import json, sqlite3, time, uuid
from pathlib import Path
from contextlib import contextmanager

class PersistenceError(RuntimeError): pass
class LostClaim(PersistenceError): pass

class SQLiteControlStore:
    def __init__(self, path, lease_seconds=300):
        self.path=str(path); self.lease_seconds=int(lease_seconds)
        Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        with self._db() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS events(
              event_id TEXT PRIMARY KEY, event_json TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'PENDING', retry_count INTEGER NOT NULL DEFAULT 0,
              owner TEXT, token TEXT, lease_until REAL, updated REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS runs(
              run_id TEXT PRIMARY KEY, trigger_event_id TEXT UNIQUE,
              run_json TEXT NOT NULL, status TEXT NOT NULL, updated REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS tasks(
              task_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
              component_id TEXT NOT NULL, task_json TEXT NOT NULL,
              UNIQUE(run_id,component_id),
              FOREIGN KEY(run_id) REFERENCES runs(run_id));
            CREATE INDEX IF NOT EXISTS ix_events_claim ON events(status,lease_until);
            """)
    @contextmanager
    def _db(self):
        c=sqlite3.connect(self.path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA foreign_keys=ON")
        try: yield c
        finally: c.close()
    def put_event(self,event):
        d=event.to_dict() if hasattr(event,"to_dict") else dict(event)
        eid=str(d["event_id"]); now=time.time()
        with self._db() as c:
            c.execute("INSERT OR IGNORE INTO events(event_id,event_json,status,retry_count,updated) VALUES(?,?,?,?,?)",
                      (eid,json.dumps(d,default=str),d.get("status","PENDING"),int(d.get("retry_count",0) or 0),now))
        return self.get_event(eid)
    def get_event(self,eid):
        with self._db() as c:
            r=c.execute("SELECT * FROM events WHERE event_id=?",(str(eid),)).fetchone()
        if not r:return None
        d=json.loads(r["event_json"]); d.update(status=r["status"],retry_count=r["retry_count"])
        return d
    def claim(self,eid,worker_id):
        now=time.time(); token=str(uuid.uuid4())
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            r=c.execute("SELECT * FROM events WHERE event_id=?",(str(eid),)).fetchone()
            if not r: c.rollback(); raise KeyError(eid)
            claimable=r["status"]=="PENDING" or (r["status"]=="PROCESSING" and (r["lease_until"] or 0)<=now)
            if not claimable: c.rollback(); return None
            c.execute("UPDATE events SET status='PROCESSING',owner=?,token=?,lease_until=?,updated=? WHERE event_id=?",
                      (str(worker_id),token,now+self.lease_seconds,now,str(eid)))
            c.commit()
        return {"event":self.get_event(eid),"token":token}
    def claim_next(self,worker_id):
        with self._db() as c:
            r=c.execute("SELECT event_id FROM events WHERE status='PENDING' OR (status='PROCESSING' AND COALESCE(lease_until,0)<=?) ORDER BY updated,event_id LIMIT 1",(time.time(),)).fetchone()
        return None if not r else self.claim(r["event_id"],worker_id)
    def renew(self,eid,token):
        now=time.time()
        with self._db() as c:
            n=c.execute("UPDATE events SET lease_until=?,updated=? WHERE event_id=? AND status='PROCESSING' AND token=?",
                        (now+self.lease_seconds,now,str(eid),str(token))).rowcount
        if not n: raise LostClaim(eid)
    def finish_event(self,eid,token,status):
        if status not in {"COMPLETED","FAILED","QUARANTINED"}: raise ValueError(status)
        with self._db() as c:
            n=c.execute("UPDATE events SET status=?,owner=NULL,token=NULL,lease_until=NULL,updated=? WHERE event_id=? AND status='PROCESSING' AND token=?",
                        (status,time.time(),str(eid),str(token))).rowcount
        if not n: raise LostClaim(eid)
    def create_run_once(self,run,tasks):
        """Atomically insert run and all tasks; return (run_id, created)."""
        rd=run.to_dict() if hasattr(run,"to_dict") else dict(run)
        rid=str(rd["run_id"]); eid=rd.get("trigger_event_id")
        tds=[t.to_dict() if hasattr(t,"to_dict") else dict(t) for t in tasks]
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            if eid:
                old=c.execute("SELECT run_id FROM runs WHERE trigger_event_id=?",(str(eid),)).fetchone()
                if old:
                    c.rollback(); return old["run_id"],False
            try:
                c.execute("INSERT INTO runs VALUES(?,?,?,?,?)",(rid,eid,json.dumps(rd,default=str),rd.get("status","PENDING"),time.time()))
                for td in tds:
                    c.execute("INSERT INTO tasks VALUES(?,?,?,?)",(str(td["task_id"]),rid,str(td["component_id"]),json.dumps(td,default=str)))
                c.commit()
            except Exception:
                c.rollback(); raise
        return rid,True
    def run_for_event(self,eid):
        with self._db() as c:
            r=c.execute("SELECT run_json FROM runs WHERE trigger_event_id=?",(str(eid),)).fetchone()
        return None if not r else json.loads(r["run_json"])
    def task_count(self,rid):
        with self._db() as c:return c.execute("SELECT COUNT(*) FROM tasks WHERE run_id=?",(str(rid),)).fetchone()[0]
