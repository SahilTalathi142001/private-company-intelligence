from __future__ import annotations

from company_platform.events.structured_event_store import (
    StructuredEventStore,
)


def persist_structured_events(
    *,
    cik: str,
    company_id: str,
    source_event_id: str | None,
    source_run_id: str | None,
    results: list[dict],
    additional_facts: dict[str, dict] | None = None,
) -> dict:
    store = StructuredEventStore()

    persisted = 0
    skipped = 0
    failed = 0
    event_results = []
    additional_facts = additional_facts or {}

    for result in results:
        for event in result.get("events") or []:
            event_type = event.get("event_type")

            # NO_SUPPORTED_FACTS does not create a business event.
            if not event_type:
                continue

            try:
                facts = {
                    **(result.get("facts") or {}),
                    **additional_facts.get(str(result["document_id"]), {}),
                }
                persisted_event = store.persist(
                    event_type=event_type,
                    company_id=company_id,
                    cik=cik,
                    document_id=str(result["document_id"]),
                    document_class=str(result["document_class"]),
                    source_document_path=result.get("source_path"),
                    source_document_sha256=str(result["sha256"]),
                    source_event_id=source_event_id,
                    source_run_id=source_run_id,
                    event_confidence=float(
                        event.get("event_confidence") or 0.0
                    ),
                    facts=facts,
                )

                if persisted_event["duplicate"]:
                    skipped += 1
                else:
                    persisted += 1

                event_results.append(persisted_event)

            except Exception:
                failed += 1
                raise

    return {
        "processed": persisted + skipped,
        "persisted": persisted,
        "skipped": skipped,
        "failed": failed,
        "events": event_results,
    }