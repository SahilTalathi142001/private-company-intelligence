import sqlite3
import json

db = r"data\silver\platform\control.db"

c = sqlite3.connect(db)

rows = c.execute("""
    SELECT event_id, payload
    FROM events
    WHERE event_type = 'NEW_SEC_FILING'
      AND status = 'FAILED'
      AND payload NOT LIKE '%TEST-DOCUMENT-INGESTION-001%'
""").fetchall()

print("FAILED:", len(rows))

for event_id, payload in rows[:10]:
    print("\n" + "=" * 80)
    print("EVENT:", event_id)
    print(json.dumps(json.loads(payload), indent=2))

c.close()
