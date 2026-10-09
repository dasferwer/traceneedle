"""Ограниченная оценка неизменённых настроек Drain на внешних шаблонных метках HDFS."""

import argparse
import csv
import hashlib
import json
import os
import platform
import resource
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from sklearn.metrics import adjusted_rand_score

SOURCE_COMMIT = "dd61d0952749ee7963bde24220d1be5ede023033"
DATA_SHA256 = "729df59774e3dde934044028546d2a55d5e3d4370b9d12fcebbe4c087b2bf7b4"


def pair_scores(labels, groups):
    def pairs(counts):
        return sum(n * (n - 1) // 2 for n in counts.values())

    shared = pairs(Counter(zip(labels, groups, strict=True)))
    expected = pairs(Counter(labels))
    predicted = pairs(Counter(groups))
    return {
        "pair_precision": shared / predicted if predicted else None,
        "pair_recall": shared / expected if expected else None,
        "false_merged_pairs": predicted - shared,
        "missed_same_template_pairs": expected - shared,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    args = parser.parse_args()
    raw = args.csv.read_bytes()
    if hashlib.sha256(raw).hexdigest() != DATA_SHA256:
        raise ValueError("Checksum HDFS_2k не совпадает; используйте зафиксированный источник")
    with args.csv.open(newline="") as handle:
        source = list(csv.DictReader(handle))
    if len(source) != 2000:
        raise ValueError("Нужен полный фиксированный HDFS_2k, 2000 строк")
    excluded = [row["LineId"] for row in source if not 2 <= len(row["Content"]) <= 2000]
    source = [row for row in source if 2 <= len(row["Content"]) <= 2000]
    now = datetime.now(UTC)
    namespace = uuid.uuid4().int >> 32
    service = "hdfs-eval-" + format(namespace, "x")[:8]
    rows = [
        {
            "id": str(uuid.UUID(int=(namespace << 32) + int(row["LineId"]))),
            "service": service,
            "version": "source",
            "happened_at": (now - timedelta(seconds=len(source) - i)).isoformat(),
            "level": row["Level"] if row["Level"] in ("INFO", "WARN", "ERROR") else "INFO",
            "message": row["Content"],
        }
        for i, row in enumerate(source)
    ]
    started = time.monotonic()
    with httpx.Client(
        base_url=os.environ.get("API_URL", "http://localhost:8096"),
        headers={"X-API-Key": os.environ.get("API_KEY", "local-demo-key")},
        timeout=30,
    ) as client:
        for offset in range(0, len(rows), 1000):
            client.post("/events", json={"events": rows[offset : offset + 1000]}).raise_for_status()
        deadline = started + 180
        while True:
            # Каждое чтение ограничено API; нулевая очередь не предполагается без подтверждения.
            grouped = []
            for row in rows:
                response = client.get(f"/events/{row['id']}")
                response.raise_for_status()
                grouped.append(response.json()["group_id"])
            if all(value is not None for value in grouped):
                break
            if time.monotonic() > deadline:
                raise TimeoutError("2000 внешних событий не обработаны за 180 секунд")
            time.sleep(0.2)
    labels = [row["EventId"] for row in source]
    print(
        json.dumps(
            {
                "source": f"https://github.com/logpai/loghub/tree/{SOURCE_COMMIT}/HDFS",
                "sha256": DATA_SHA256,
                "events": len(rows),
                "excluded_by_api_length": excluded,
                "independent_templates": len(set(labels)),
                "predicted_groups": len(set(grouped)),
                "adjusted_rand_index": adjusted_rand_score(labels, grouped),
                **pair_scores(labels, grouped),
                "elapsed_seconds": time.monotonic() - started,
                "evaluator_maxrss_native_units": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "platform": platform.platform(),
                "python": platform.python_version(),
                "order": "source LineId; no shuffle",
                "concurrency": 1,
                "timestamps": "shifted to recent synthetic timestamps; Content and EventId unchanged",
                "scope": "template grouping, not incident correctness or anomaly detection",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
