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
