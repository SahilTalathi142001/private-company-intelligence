import pytest
from company_platform.orchestrator.event_consumer import EventConsumer
from company_platform.events.models import PlatformEvent

class Store:
    def __init__(self): self.failed = []
    def fail(self, event_id, claim_token=None): self.failed.append((event_id, claim_token))

class Router:
    def route(self, event): raise RuntimeError('router unavailable')

def test_route_exception_marks_claimed_event_failed():
    store = Store()
    consumer = EventConsumer(event_store=store, router=Router(), orchestrator=object())
    event = PlatformEvent(
        event_id='evt-1', event_type='DATA_CHANGED', event_version='1',
        occurred_at_utc='2026-01-01T00:00:00Z', source_id=None,
        company_id='co-1', correlation_id='corr-1', causation_id=None,
        status='PROCESSING', payload={}, retry_count=1,
    )
    with pytest.raises(RuntimeError, match='router unavailable'):
        consumer._process(event, claim_token='token-1')
    assert store.failed == [('evt-1', 'token-1')]
