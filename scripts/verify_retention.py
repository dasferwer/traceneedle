"""HTTP/PG проверка retention в отдельной локальной схеме runtime_p2*, без Docker управления."""

import json
import os
import platform
import random
import resource
import time
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx
from psycopg.conninfo import conninfo_to_dict

from traceneedle.db import connect


def main():
    api_url = os.environ["API_URL"]
    db_options = conninfo_to_dict(os.environ["DATABASE_URL"])
    if urlparse(api_url).hostname not in ("localhost", "127.0.0.1") or db_options.get(
        "host"
    ) not in ("localhost", "127.0.0.1"):
        raise ValueError("Нужны отдельные локальные API/PostgreSQL")
    with connect() as conn:
        schema = conn.execute("SELECT current_schema() AS name").fetchone()
        if not schema or not schema["name"].startswith("runtime_p2"):
            raise ValueError("Нужна изолированная схема runtime_p2*")
    service = "retention-check-" + uuid.uuid4().hex[:8]
    identity = str(uuid.uuid4())
    before = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=5)
    after = before + timedelta(minutes=2)
    with httpx.Client(
        base_url=api_url, headers={"X-API-Key": os.environ["API_KEY"]}, timeout=30
    ) as client:

        def call(method, route, body):
            response = client.request(method, route, json=body)
            response.raise_for_status()
            return response.json()

        def ingest(n, version, minute):
            body = {
                "events": [
                    {
                        "id": str(uuid.uuid4()),
                        "service": service,
                        "version": version,
                        "happened_at": (minute + timedelta(seconds=10)).isoformat(),
                        "level": "ERROR",
                        "message": f"RETENTION_TIMEOUT request={i} host=primary",
                    }
                    for i in range(n)
                ]
            }
            return call("POST", "/events", body)

        def wait_revision(revision):
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                result = client.get(f"/comparisons/{identity}")
                result.raise_for_status()
                state = result.json()
                if (
                    state["regressions"]
                    and state["regressions"][0]["revision"] == revision
                    and not state["dirty"]
                ):
                    return state
                time.sleep(0.1)
            raise TimeoutError(f"Воркер не записал ревизию {revision}")

        for version, minute in (("v1", before), ("v2", after)):
            call(
                "PUT",
                "/traffic",
                {
                    "service": service,
                    "version": version,
                    "minute": minute.isoformat(),
                    "requests": 100,
                },
            )
        call(
            "POST",
            "/comparisons",
            {
                "id": identity,
                "service": service,
                "before_version": "v1",
                "after_version": "v2",
                "before_start": before.isoformat(),
                "after_start": after.isoformat(),
                "minutes": 1,
            },
        )
        ingest(1, "v1", before)
        ingest(8, "v2", after)
        initial = wait_revision(1)
        assert initial["regressions"][0]["status"] == "active"
        # Ускоренное старение только собственного fixture вместо ожидания суток.
        with connect() as conn:
            assert conn.execute(
                "SELECT id FROM comparisons WHERE id=%s AND service=%s", (identity, service)
            ).fetchone(), "API и PG должны использовать одну изолированную схему"
            conn.execute("SELECT pg_advisory_xact_lock(360028)")
            conn.execute(
                "UPDATE events SET happened_at=happened_at-interval '1 day' WHERE service=%s",
                (service,),
            )
            conn.execute(
                "UPDATE traffic SET minute=minute-interval '1 day' WHERE service=%s", (service,)
            )
            conn.execute(
                "UPDATE comparisons SET before_start=before_start-interval '1 day',after_start=after_start-interval '1 day',dirty=true WHERE id=%s",
                (identity,),
            )
            group = conn.execute(
                "SELECT group_id FROM events WHERE service=%s LIMIT 1", (service,)
            ).fetchone()
            assert group is not None
        before -= timedelta(days=1)
        after -= timedelta(days=1)
        preview = call("POST", "/maintenance/cleanup", {"batch_size": 1000})
        assert (
            preview["enabled"]
            and not preview["items"]["events"]
            and not preview["items"]["traffic"]
        )
        call("POST", "/maintenance/cleanup", {"batch_size": 1000, "dry_run": False})
        assert wait_revision(1)["history"] == initial["history"]
        ingest(4, "v1", before)
        assert wait_revision(2)["regressions"][0]["status"] == "retracted"
        call(
            "PUT",
            "/traffic",
            {
                "service": service,
                "version": "v1",
                "minute": before.isoformat(),
                "requests": 1000,
                "expected_revision": 1,
            },
        )
        corrected = wait_revision(3)
        assert corrected["regressions"][0]["status"] == "active"
        # Зафиксированная синтетическая нагрузка на очистку, без изменения Drain.
        workload = 10000
        seed = 360
        generator = random.Random(seed)
        with connect() as conn, conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO events(id,service,version,happened_at,level,message,group_id) VALUES (%s,%s,'bulk',%s,'INFO','retention benchmark',%s)",
                [
                    (
                        uuid.UUID(int=generator.getrandbits(128)),
                        service + "-bulk",
                        before - timedelta(seconds=i),
                        group["group_id"],
                    )
                    for i in range(workload)
                ],
            )
        first = call("POST", "/maintenance/cleanup", {"batch_size": 1000})
        started = time.monotonic()
        batches = []
        removed = 0
        while True:
            batch_start = time.monotonic()
            result = call("POST", "/maintenance/cleanup", {"batch_size": 1000, "dry_run": False})
            batches.append(time.monotonic() - batch_start)
            if not any(result["items"].values()):
                break
            if removed == 0:
                assert result["items"] == first["items"]
            removed += len(result["items"]["events"])
            assert all(len(rows) <= 1000 for rows in result["items"].values())
        elapsed = time.monotonic() - started
        assert removed == workload
        assert client.get(f"/comparisons/{identity}").json()["history"] == corrected["history"]
        call("POST", f"/comparisons/{identity}/archive", {})
        expired = client.post(
            "/events",
            json={
                "events": [
                    {
                        "id": str(uuid.uuid4()),
                        "service": service,
                        "version": "v1",
                        "happened_at": before.isoformat(),
                        "level": "ERROR",
                        "message": "RETENTION_TIMEOUT request=99 host=primary",
                    }
                ]
            },
        )
        assert expired.status_code == 422
        print(
            json.dumps(
                {
                    "schema": schema["name"],
                    "seed": seed,
                    "workload": workload,
                    "batch_size": 1000,
                    "concurrency": 1,
                    "deleted_events": removed,
                    "cleanup_calls_including_empty": len(batches),
                    "elapsed_seconds": elapsed,
                    "max_batch_seconds": max(batches),
                    "pinned_history_revisions": [r["revision"] for r in corrected["history"]],
                    "statuses": [r["status"] for r in corrected["history"]],
                    "unsupported_after_archive_http_status": expired.status_code,
                    "driver_maxrss_native_units": resource.getrusage(
                        resource.RUSAGE_SELF
                    ).ru_maxrss,
                    "platform": platform.platform(),
                    "note": "synthetic aged workload; local result, not SLA",
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
