from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent


def test_pending_events_returns_pending_rows(tmp_path):
    store = EventStore(path=tmp_path / "events.parquet")
    event = PlatformEvent(event_type="NEW_SEC_FILING", company_id="cmp-1", payload={"foo": "bar"})

    store.append(event)
    pending = store.pending_events()

    assert not pending.empty
    assert list(pending["event_id"]) == [event.event_id]
    assert pending.iloc[0]["status"] == "PENDING"
