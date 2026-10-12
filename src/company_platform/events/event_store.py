from pathlib import Path
import json, sqlite3, time, uuid
from contextlib import contextmanager
import pandas as pd
from .models import PlatformEvent, EVENT_STATUSES

COLUMNS=["event_id","event_type","event_version","occurred_at_utc","source_id","company_id","correlation_id","causation_id","status","payload","retry_count"]

class EventStore:
    """SQLite event queue with atomic claims, leases, fencing, and explicit retries."""
    def __init__(self, path=None, lease_seconds=300):
        if lease_seconds <= 0: raise ValueError("lease_seconds must be positive")
        root=Path(__file__).resolve().parents[3]
        self.path=Path(path) if path else root/"data"/"silver"/"platform"/"control.db"
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.lease_seconds=int(lease_seconds)
        with self._db() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS events(
              event_id TEXT PRIMARY KEY,event_type TEXT,event_version TEXT,occurred_at_utc TEXT,
              source_id TEXT,company_id TEXT,correlation_id TEXT,causation_id TEXT,
              status TEXT NOT NULL,payload TEXT,retry_count INTEGER NOT NULL DEFAULT 0,
              worker_id TEXT,claim_token TEXT,lease_until REAL,updated_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS ix_events_status ON events(status,lease_until,updated_at);
            """)
    @contextmanager
    def _db(self):
        c=sqlite3.connect(self.path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA busy_timeout=30000")
        c.execute("PRAGMA journal_mode=WAL")
        try:
            yield c
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()
    def append(self,event):
        d=event.to_dict()
        payload=json.dumps(d.get("payload",{}),sort_keys=True,separators=(",",":"))
        now=time.time()
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            old=c.execute("SELECT * FROM events WHERE event_id=?",(str(d["event_id"]),)).fetchone()
            if old:
                existing=self._row(old)
                same=(existing["event_type"]==d["event_type"] and
                      existing["company_id"]==d["company_id"] and
                      existing["source_id"]==d.get("source_id") and
                      existing["payload"]==d.get("payload",{}))
                c.commit()
                if not same: raise ValueError("event_id collision: existing event has different immutable content")
                return existing
            c.execute("""INSERT INTO events(event_id,event_type,event_version,occurred_at_utc,source_id,company_id,correlation_id,causation_id,status,payload,retry_count,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
              (d["event_id"],d["event_type"],d["event_version"],d["occurred_at_utc"],d.get("source_id"),
               d["company_id"],d["correlation_id"],d.get("causation_id"),"PENDING",payload,0,now))
            c.commit()
        return self.get(d["event_id"])
    def _row(self,r):
        if not r:return None
        d=dict(r); d["payload"]=json.loads(d["payload"] or "{}"); return d
    def get(self,event_id):
        with self._db() as c:return self._row(c.execute("SELECT * FROM events WHERE event_id=?",(str(event_id),)).fetchone())
    def list_events(self,status=None):
        with self._db() as c:
            rows=c.execute("SELECT * FROM events"+(" WHERE status=?" if status else "")+" ORDER BY updated_at,event_id",((status,) if status else ())).fetchall()
        return pd.DataFrame([self._row(r) for r in rows],columns=COLUMNS+["worker_id","claim_token","lease_until","updated_at"])
    def pending_events(self): return self.list_events("PENDING")
    def _claim_locked(self,c,event_id,worker_id,now):
        token=str(uuid.uuid4())
        cur=c.execute("""UPDATE events SET status='PROCESSING',worker_id=?,claim_token=?,lease_until=?,updated_at=?
          WHERE event_id=? AND (status='PENDING' OR (status='PROCESSING' AND lease_until<=?))""",
          (worker_id,token,now+self.lease_seconds,now,str(event_id),now))
        if cur.rowcount!=1:return None
        d=self._row(c.execute("SELECT * FROM events WHERE event_id=?",(str(event_id),)).fetchone())
        d["_claim_token"]=token; d["_worker_id"]=worker_id
        return d
    def claim(self,event_id,worker_id=None):
        worker_id=worker_id or "worker-"+str(uuid.uuid4()); now=time.time()
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            d=self._claim_locked(c,event_id,worker_id,now)
            if d is None:
                c.rollback(); raise ValueError(f"Event not claimable: {event_id}")
            c.commit(); return d
    def claim_next(self,worker_id=None):
        worker_id=worker_id or "worker-"+str(uuid.uuid4()); now=time.time()
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            r=c.execute("""SELECT event_id FROM events WHERE status='PENDING'
              OR (status='PROCESSING' AND lease_until<=?) ORDER BY updated_at,event_id LIMIT 1""",(now,)).fetchone()
            if not r:
                c.commit(); return None
            d=self._claim_locked(c,r["event_id"],worker_id,now)
            if d is None:
                c.rollback(); return None
            c.commit(); return d
    def _set(self,event_id,status,retry_count=None,claim_token=None):
        if status not in EVENT_STATUSES: raise ValueError(status)
        with self._db() as c:
            if claim_token:
                q="UPDATE events SET status=?,worker_id=NULL,claim_token=NULL,lease_until=NULL,updated_at=?"+(",retry_count=?" if retry_count is not None else "")+" WHERE event_id=? AND status='PROCESSING' AND claim_token=?"
                args=[status,time.time()]+([retry_count] if retry_count is not None else [])+[str(event_id),claim_token]
                if c.execute(q,args).rowcount!=1: raise RuntimeError("Stale claim token; event ownership lost")
            else:
                q="UPDATE events SET status=?,worker_id=NULL,claim_token=NULL,lease_until=NULL,updated_at=?"+(",retry_count=?" if retry_count is not None else "")+" WHERE event_id=?"
                args=[status,time.time()]+([retry_count] if retry_count is not None else [])+[str(event_id)]
                if c.execute(q,args).rowcount!=1: raise KeyError(event_id)
    def update_status(self,event_id,status,retry_count=None):
        if status not in EVENT_STATUSES: raise ValueError(status)
        self._set(event_id,status,retry_count)
    def complete(self,event_id,claim_token=None): self._set(event_id,"COMPLETED",claim_token=claim_token)
    def fail(self,event_id,retry_count=None,claim_token=None): self._set(event_id,"FAILED",retry_count,claim_token)
    def quarantine(self,event_id,retry_count=None,claim_token=None): self._set(event_id,"QUARANTINED",retry_count,claim_token)
    def retry(self,event_id,max_retries=3):
        if max_retries < 0: raise ValueError("max_retries must be >= 0")
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            r=c.execute("SELECT status,retry_count FROM events WHERE event_id=?",(str(event_id),)).fetchone()
            if not r: c.rollback(); raise KeyError(event_id)
            if r["status"]!="FAILED": c.rollback(); raise ValueError("Only FAILED events can be retried")
            if int(r["retry_count"])>=max_retries: c.rollback(); raise ValueError("Retry limit reached")
            c.execute("UPDATE events SET status='PENDING',retry_count=retry_count+1,updated_at=? WHERE event_id=? AND status='FAILED'",(time.time(),str(event_id)))
            c.commit()
        return self.get(event_id)
    def renew(self,event_id,claim_token):
        now=time.time()
        with self._db() as c:
            return c.execute("UPDATE events SET lease_until=?,updated_at=? WHERE event_id=? AND status='PROCESSING' AND claim_token=?",(now+self.lease_seconds,now,str(event_id),claim_token)).rowcount==1
