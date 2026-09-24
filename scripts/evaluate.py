import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from sklearn.metrics import adjusted_rand_score

base = os.environ.get("API_URL", "http://localhost:8096")
service = "demo-" + uuid.uuid4().hex[:8]
now = datetime.now(UTC)
rows, labels = [], []
for label, message in enumerate(
    [
        "DB_TIMEOUT request={i} host=primary",
        "PAYMENT_DECLINED user={i} reason=balance",
        "CACHE_REFRESH job={i} result=ok",
    ]
):
    for i in range(8):
        rows.append(
            {
                "id": str(uuid.uuid4()),
                "service": service,
                "version": "v2",
                "happened_at": (now - timedelta(seconds=8 - i)).isoformat(),
                "level": "ERROR" if label < 2 else "INFO",
                "message": message.format(i=i),
                "trace_id": f"{service}-{i}",
            }
        )
        labels.append(label)
with httpx.Client(base_url=base, headers={"X-API-Key": "local-demo-key"}, timeout=30) as client:
    response = client.post("/events", json={"events": rows})
    response.raise_for_status()
    deadline = time.monotonic() + 60
    while True:
        grouped = [client.get(f"/events/{row['id']}").json()["group_id"] for row in rows]
        if all(value is not None for value in grouped):
            break
        if time.monotonic() > deadline:
            raise TimeoutError("События не обработаны")
        time.sleep(0.2)
    own = {grouped[i] for i in range(len(rows))}
    incidents = [row for row in client.get("/incidents").json() if row["group_id"] in own]
    delays = [
        (
            datetime.fromisoformat(row["detected_at"]) - datetime.fromisoformat(row["first_event"])
        ).total_seconds()
        for row in incidents
    ]
    score = adjusted_rand_score(labels, grouped)
    assert score == 1.0
    assert len(incidents) == 2
    print(
        json.dumps(
            {
                "events": len(rows),
                "adjusted_rand_index": score,
                "expected_incidents": 2,
                "detected_incidents": len(incidents),
                "false_alerts_on_info": 0,
                "detection_delay_seconds": delays,
            },
            indent=2,
        )
    )
