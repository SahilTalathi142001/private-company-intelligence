import threading
from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent


def test_only_one_worker_claims_event(tmp_path):
    store = EventStore(path=tmp_path / "control.db")
    event = PlatformEvent(event_type="NEW_SEC_FILING", company_id="cmp-1", payload={})
    store.append(event)
    barrier = threading.Barrier(2)
    claimed, rejected = [], []
    def worker(name):
        barrier.wait()
        try:
            claimed.append(store.claim(event.event_id, worker_id=name))
        except ValueError:
            rejected.append(name)
    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(2)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(claimed) == 1
    assert len(rejected) == 1
    assert store.get(event.event_id)["status"] == "PROCESSING"
