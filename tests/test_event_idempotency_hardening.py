import threading
import pytest
from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent

def test_duplicate_append_is_idempotent_and_collision_rejected(tmp_path):
    s=EventStore(path=tmp_path/"control.db")
    e=PlatformEvent(event_type="NEW_SEC_FILING",company_id="c1",event_id="e1",payload={"accession":"a"})
    a=s.append(e); b=s.append(e)
    assert a["event_id"]==b["event_id"] and b["status"]=="PENDING"
    other=PlatformEvent(event_type="NEW_SEC_FILING",company_id="c1",event_id="e1",payload={"accession":"b"})
    with pytest.raises(ValueError,match="collision"): s.append(other)

def test_only_one_worker_claims_event(tmp_path):
    s=EventStore(path=tmp_path/"control.db")
    e=PlatformEvent(event_type="NEW_SEC_FILING",company_id="c1"); s.append(e)
    barrier=threading.Barrier(2); got=[]; rejected=[]
    def w(n):
        barrier.wait()
        try: got.append(s.claim(e.event_id,worker_id=n))
        except ValueError: rejected.append(n)
    ts=[threading.Thread(target=w,args=(str(i),)) for i in range(2)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert len(got)==1 and len(rejected)==1

def test_expired_lease_reclaim_fences_old_worker(tmp_path):
    s=EventStore(path=tmp_path/"control.db",lease_seconds=1)
    e=PlatformEvent(event_type="NEW_SEC_FILING",company_id="c1"); s.append(e)
    old=s.claim(e.event_id,"old")
    with s._db() as c: c.execute("UPDATE events SET lease_until=0 WHERE event_id=?",(e.event_id,))
    new=s.claim(e.event_id,"new")
    assert new["_claim_token"]!=old["_claim_token"]
    with pytest.raises(RuntimeError,match="Stale claim token"): s.complete(e.event_id,old["_claim_token"])
    s.complete(e.event_id,new["_claim_token"])
    assert s.get(e.event_id)["status"]=="COMPLETED"

def test_retry_limit_counts_claim_attempts_not_double_increment(tmp_path):
    s=EventStore(path=tmp_path/"control.db")
    e=PlatformEvent(event_type="NEW_SEC_FILING",company_id="c1"); s.append(e)
    first=s.claim(e.event_id); assert s.get(e.event_id)["retry_count"]==0
    s.fail(e.event_id,claim_token=first["_claim_token"])
    s.retry(e.event_id,max_retries=3)
    second=s.claim(e.event_id); assert s.get(e.event_id)["retry_count"]==1
