import sqlite3
import json
from collections import Counter

db = r"data\silver\platform\control.db"
c = sqlite3.connect(db)

rows = c.execute("""
    SELECT
        e.event_id,
        json_extract(e.payload, '$.cik') AS cik,
        json_extract(e.payload, '$.form') AS form,
        json_extract(e.payload, '$.accession_number') AS accession,
        p.run_id,
        json_extract(p.data, '$.status') AS run_status,
        json_extract(p.data, '$.error_message') AS run_error
    FROM events e
    LEFT JOIN pipeline_runs p
        ON p.trigger_event_id = e.event_id
    WHERE e.event_type = 'NEW_SEC_FILING'
      AND e.status = 'FAILED'
      AND e.payload NOT LIKE '%TEST-DOCUMENT-INGESTION-001%'
    ORDER BY e.event_id
""").fetchall()

print("FAILED SEC EVENTS:", len(rows))

run_ids = [r[4] for r in rows if r[4]]

print("\nRUNS FOUND:", len(run_ids))
print("RUNS MISSING:", len(rows) - len(run_ids))

task_rows = []

for run_id in run_ids:
    task_rows.extend(c.execute("""
        SELECT
            run_id,
            component_id,
            json_extract(data, '$.status') AS status,
            json_extract(data, '$.error_message') AS error_message
        FROM task_runs
        WHERE run_id = ?
          AND json_extract(data, '$.status') IN ('FAILED', 'BLOCKED')
    """, (run_id,)).fetchall())

print("\nFAILED/BLOCKED TASK ROWS:", len(task_rows))

print("\nTASK FAILURE SUMMARY:")
summary = Counter(
    (r[1], r[2], r[3] or "<NO ERROR MESSAGE>")
    for r in task_rows
)

for (component, status, error), count in summary.most_common(20):
    print(f"\nCOUNT: {count}")
    print(f"COMPONENT: {component}")
    print(f"STATUS: {status}")
    print(f"ERROR: {error}")

print("\nFIRST 10 FAILED EVENTS:")
for r in rows[:10]:
    print("\n" + "=" * 90)
    print("EVENT:", r[0])
    print("CIK:", r[1])
    print("FORM:", r[2])
    print("ACCESSION:", r[3])
    print("RUN:", r[4])
    print("RUN STATUS:", r[5])
    print("RUN ERROR:", r[6])

c.close()
