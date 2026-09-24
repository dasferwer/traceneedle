import uuid
from datetime import UTC, datetime, timedelta

import pytest

from traceneedle.db import connect
from traceneedle.mining import normalized
from traceneedle.worker import tick


def events(n=5, level="ERROR", version="v2", message="DB_TIMEOUT request={i} host=db", offset=0):
    now = datetime.now(UTC)
    return [
        {
            "id": str(uuid.uuid4()),
            "service": "orders",
            "version": version,
            "happened_at": (now - timedelta(seconds=n - i + offset)).isoformat(),
            "level": level,
            "message": message.format(i=i),
            "trace_id": "trace-demo",
        }
        for i in range(n)
    ]


def ingest(client, data):
    response = client.post("/events", json={"events": data})
    assert response.status_code == 200
    return response.json()


def test_grouping_deduplication_and_incident(client):
    data = events()
    assert ingest(client, data)["accepted"] == 5
    assert ingest(client, data)["duplicates"] == 5
    assert tick() == 5
    assert len(client.get("/groups").json()) == 1
    assert len(client.get("/incidents").json()) == 1
    assert len(client.get("/traces/trace-demo").json()["events"]) == 5
    assert tick() == 0


def test_restart_preserves_drain_identity(client):
    ingest(client, events(2))
    tick()
    group = client.get("/groups").json()[0]["id"]
    ingest(client, events(3))
    tick()
    assert client.get("/groups").json()[0]["id"] == group
    assert len(client.get("/groups").json()) == 1
    assert len(client.get("/incidents").json()) == 1


def test_late_arrival_inside_window_is_counted(client):
    data = events()
    ingest(client, list(reversed(data)))
    tick()
    assert len(client.get("/incidents").json()) == 1


def test_rare_normal_and_known_errors_do_not_trigger_new_incident(client):
    ingest(client, events(8, level="INFO"))
    tick()
    assert client.get("/incidents").json() == []
    ingest(client, events(1, version="v1"))
    tick()
    ingest(client, events(8, version="v2"))
    tick()
    assert client.get("/incidents").json() == []


def test_conflicting_duplicate_and_backpressure(client):
    data = events(1)
    ingest(client, data)
    assert (
        client.post(
            "/events", json={"events": [{**data[0], "message": "Другой текст"}]}
        ).status_code
        == 409
    )
    with connect() as conn:
        conn.execute("""INSERT INTO events(id,service,version,happened_at,level,message)
            SELECT gen_random_uuid(),'bulk','v1',now(),'INFO','bulk event' FROM generate_series(1,9999)""")
    response = client.post("/events", json={"events": events(1)})
    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"
    assert ingest(client, data)["duplicates"] == 1


def test_number_normalization_keeps_error_codes():
    assert "HTTP 500" in normalized("HTTP 500 request=123 duration 40ms")
    assert "request=<n>" in normalized("HTTP 500 request=123 duration 40ms")


def test_state_and_events_rollback_together(client, monkeypatch):
    from traceneedle import mining

    ingest(client, events(2))
    original = mining.State.save_state

    def fail(self, state):
        original(self, state)
        raise RuntimeError("Сбой сохранения состояния")

    with monkeypatch.context() as patch:
        patch.setattr(mining.State, "save_state", fail)
        with pytest.raises(RuntimeError):
            tick()
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM groups").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM miner_state").fetchone()["n"] == 0
    assert tick() == 2


def comparison_fixture(client, before_requests=100, after_requests=100, minutes=1):
    start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=5)
    later = start + timedelta(minutes=2)
    for version, minute, requests in [
        ("v1", start, before_requests),
        ("v2", later, after_requests),
    ]:
        assert (
            client.put(
                "/traffic",
                json={
                    "service": "orders",
                    "version": version,
                    "minute": minute.isoformat(),
                    "requests": requests,
                },
            ).status_code
            == 200
        )
    identity = str(uuid.uuid4())
    assert (
        client.post(
            "/comparisons",
            json={
                "id": identity,
                "service": "orders",
                "before_version": "v1",
                "after_version": "v2",
                "before_start": start.isoformat(),
                "after_start": later.isoformat(),
                "minutes": minutes,
            },
        ).status_code
        == 202
    )
    return identity, start, later


def at_minute(n, version, minute):
    return [
        {**r, "happened_at": (minute + timedelta(seconds=10)).isoformat()}
        for r in events(n, version=version)
    ]


def test_known_error_regression_retracted_by_late_baseline(client):
    identity, before, after = comparison_fixture(client)
    ingest(client, at_minute(1, "v1", before) + at_minute(8, "v2", after))
    tick()
    result = client.get(f"/comparisons/{identity}").json()
    assert result["regressions"][0]["status"] == "active"
    assert result["regressions"][0]["evidence"]["before_rate"] == 0.01
    ingest(client, at_minute(4, "v1", before))
    tick()
    result = client.get(f"/comparisons/{identity}").json()
    assert result["regressions"][0]["status"] == "retracted"
    assert [r["status"] for r in result["history"]] == ["active", "retracted"]
    tick()
    assert client.get(f"/comparisons/{identity}").json()["history"] == result["history"]


def test_traffic_growth_alone_is_not_regression(client):
    identity, before, after = comparison_fixture(client, 100, 1000)
    ingest(client, at_minute(1, "v1", before) + at_minute(10, "v2", after))
    tick()
    assert client.get(f"/comparisons/{identity}").json()["regressions"] == []


def test_incomplete_traffic_windows_do_not_raise_alert(client):
    identity, before, after = comparison_fixture(client, minutes=2)
    ingest(client, at_minute(1, "v1", before) + at_minute(10, "v2", after))
    tick()
    assert client.get(f"/comparisons/{identity}").json()["regressions"] == []


def test_traffic_correction_requires_current_revision(client):
    identity, before, _after = comparison_fixture(client)
    body = {"service": "orders", "version": "v1", "minute": before.isoformat(), "requests": 200}
    assert client.put("/traffic", json=body).status_code == 409
    assert client.put("/traffic", json={**body, "expected_revision": 1}).json()["revision"] == 2
    assert (
        client.get(
            "/traffic", params={"service": "orders", "version": "v1", "minute": before.isoformat()}
        ).json()["requests"]
        == 200
    )
    tick()
    assert not client.get(f"/comparisons/{identity}").json()["dirty"]
