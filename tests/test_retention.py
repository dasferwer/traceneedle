import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.test_pipeline import at_minute, comparison_fixture, events, ingest
from traceneedle import db
from traceneedle.db import connect
from traceneedle.worker import tick


def enable(monkeypatch):
    monkeypatch.setenv("SUPPORTED_LATE_MINUTES", "60")
    monkeypatch.setenv("HISTORY_RETENTION_MINUTES", "120")


def cleanup(client, **kwargs):
    response = client.post("/maintenance/cleanup", json=kwargs)
    assert response.status_code == 200, response.text
    return response.json()


def test_cleanup_default_is_disabled_and_dry_run(monkeypatch, client):
    ingest(client, events(1, offset=10000))
    tick()
    result = cleanup(client)
    assert result["enabled"] is False
    assert result["dry_run"] is True
    assert client.get("/groups").json()


@pytest.mark.parametrize("late,history", [("0", "120"), ("60", "64"), ("-1", "0"), ("abc", "0")])
def test_invalid_retention_settings_fail_before_database(monkeypatch, late, history):
    monkeypatch.setenv("SUPPORTED_LATE_MINUTES", late)
    monkeypatch.setenv("HISTORY_RETENTION_MINUTES", history)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError, match="MINUTES"):
        db.init()


def test_cleanup_batches_are_deterministic_and_preserve_pending(monkeypatch, client):
    # Production break: deleting pending rows, using <= cutoff, or unstable/uncapped selection.
    ingest(client, events(3, offset=10000))
    tick()
    enable(monkeypatch)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    cutoff = now - timedelta(minutes=120)
    from traceneedle import retention

    monkeypatch.setattr(retention, "utcnow", lambda: now)
    with connect() as conn:
        old = conn.execute("SELECT id FROM events ORDER BY id").fetchall()
        conn.execute("UPDATE events SET happened_at=%s", (cutoff - timedelta(seconds=1),))
        boundary = uuid.uuid4()
        pending = uuid.uuid4()
        for identity, happened in [(boundary, cutoff), (pending, cutoff - timedelta(days=1))]:
            conn.execute(
                "INSERT INTO events(id,service,version,happened_at,level,message,group_id) VALUES (%s,'orders','v2',%s,'INFO','retained',%s)",
                (identity, happened, None if identity == pending else 1),
            )
    first = cleanup(client, batch_size=2)
    assert first == cleanup(client, batch_size=2)
    assert first["items"]["events"] == [str(r["id"]) for r in old[:2]]
    assert first["more"]["events"] is True
    assert client.get(f"/events/{old[0]['id']}").status_code == 200
    applied = cleanup(client, batch_size=2, dry_run=False)
    assert applied["items"] == first["items"]
    assert client.get(f"/events/{old[0]['id']}").status_code == 404
    assert cleanup(client, batch_size=2, dry_run=False)["items"]["events"] == [str(old[2]["id"])]
    assert client.get(f"/events/{boundary}").status_code == 200
    assert client.get(f"/events/{pending}").status_code == 200


def test_supported_lateness_boundary_and_cleanup_watermark(monkeypatch, client):
    enable(monkeypatch)
    from traceneedle import retention

    now = datetime.now(UTC).replace(second=0, microsecond=0)
    monkeypatch.setattr(retention, "utcnow", lambda: now)
    cutoff = now - timedelta(minutes=60)
    accepted = [{**events(1)[0], "happened_at": cutoff.isoformat()}]
    assert ingest(client, accepted)["accepted"] == 1
    rejected = [{**events(1)[0], "happened_at": (cutoff - timedelta(microseconds=1)).isoformat()}]
    assert client.post("/events", json={"events": rejected}).status_code == 422
    traffic = {"service": "orders", "version": "v1", "minute": cutoff.isoformat(), "requests": 100}
    assert client.put("/traffic", json=traffic).status_code == 200
    assert (
        client.put(
            "/traffic", json={**traffic, "minute": (cutoff - timedelta(minutes=1)).isoformat()}
        ).status_code
        == 422
    )
    comp = {
        "id": str(uuid.uuid4()),
        "service": "orders",
        "before_version": "v1",
        "after_version": "v2",
        "before_start": cutoff.isoformat(),
        "after_start": cutoff.isoformat(),
        "minutes": 1,
    }
    assert client.post("/comparisons", json=comp).status_code == 202
    assert (
        client.post(
            "/comparisons",
            json={
                **comp,
                "id": str(uuid.uuid4()),
                "before_start": (cutoff - timedelta(minutes=1)).isoformat(),
            },
        ).status_code
        == 422
    )
    cleanup(client, dry_run=False)
    monkeypatch.setenv("SUPPORTED_LATE_MINUTES", "0")
    monkeypatch.setenv("HISTORY_RETENTION_MINUTES", "0")
    ancient = [
        {**events(1)[0], "service": "other", "happened_at": (now - timedelta(days=1)).isoformat()}
    ]
    assert client.post("/events", json={"events": ancient}).status_code == 422
    assert ingest(client, accepted)["duplicates"] == 1


def test_active_windows_remain_reproducible_and_late_corrections_create_revisions(
    monkeypatch, client
):
    identity, before, after = comparison_fixture(client)
    ingest(client, at_minute(1, "v1", before) + at_minute(8, "v2", after))
    tick()
    initial = client.get(f"/comparisons/{identity}").json()
    enable(monkeypatch)
    from traceneedle import retention

    now = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(days=1)
    monkeypatch.setattr(retention, "utcnow", lambda: now)
    assert cleanup(client, dry_run=False)["items"]["events"] == []
    with connect() as conn:
        conn.execute("UPDATE comparisons SET dirty=true WHERE id=%s", (identity,))
    tick()
    assert client.get(f"/comparisons/{identity}").json()["history"] == initial["history"]
    ingest(client, at_minute(4, "v1", before))
    tick()
    second = client.get(f"/comparisons/{identity}").json()
    assert second["regressions"][0]["revision"] == 2
    assert second["regressions"][0]["status"] == "retracted"
    correction = {
        "service": "orders",
        "version": "v1",
        "minute": before.isoformat(),
        "requests": 1000,
        "expected_revision": 1,
    }
    assert client.put("/traffic", json=correction).status_code == 200
    tick()
    third = client.get(f"/comparisons/{identity}").json()
    assert third["regressions"][0]["revision"] == 3
    assert third["regressions"][0]["status"] == "active"
    assert [r["revision"] for r in third["history"]] == [1, 2, 3]


def test_archive_freezes_evidence_and_cleanup_purges_bounded_history(monkeypatch, client):
    identity, before, after = comparison_fixture(client)
    ingest(client, at_minute(1, "v1", before) + at_minute(8, "v2", after))
    tick()
    assert client.post(f"/comparisons/{identity}/archive").status_code == 200
    assert client.post(f"/comparisons/{identity}/archive").status_code == 200
    initial = client.get(f"/comparisons/{identity}").json()
    ingest(client, at_minute(4, "v1", before))
    tick()
    assert client.get(f"/comparisons/{identity}").json()["history"] == initial["history"]
    enable(monkeypatch)
    from traceneedle import retention

    monkeypatch.setattr(retention, "utcnow", lambda: datetime.now(UTC) + timedelta(days=1))
    preview = cleanup(client, batch_size=1)
    assert len(preview["items"]["history"]) == 1
    assert client.get(f"/comparisons/{identity}").status_code == 200
    for _ in range(20):
        result = cleanup(client, batch_size=1, dry_run=False)
        assert all(len(values) <= 1 for values in result["items"].values())
        if client.get(f"/comparisons/{identity}").status_code == 404:
            break
    assert client.get(f"/comparisons/{identity}").status_code == 404


def test_cleanup_retains_known_error_metadata(monkeypatch, client):
    ingest(client, events(1, version="v1", offset=10000))
    tick()
    enable(monkeypatch)
    cleanup(client, dry_run=False)
    ingest(client, events(8, version="v2"))
    tick()
    assert client.get("/incidents").json() == []


def test_dry_run_does_not_advance_watermark_and_rejected_batch_is_atomic(monkeypatch, client):
    enable(monkeypatch)
    cleanup(client)
    with connect() as conn:
        assert (
            conn.execute("SELECT retained_since FROM retention_state").fetchone()["retained_since"]
            is None
        )
    valid = events(1)
    stale = events(1, offset=10000)
    assert client.post("/events", json={"events": valid + stale}).status_code == 422
    assert client.get(f"/events/{valid[0]['id']}").status_code == 404
    assert client.post("/maintenance/cleanup", json={"batch_size": 0}).status_code == 422
    assert client.post("/maintenance/cleanup", json={"batch_size": 10001}).status_code == 422
    assert (
        client.post("/maintenance/cleanup", json={}, headers={"X-API-Key": "wrong"}).status_code
        == 401
    )


def test_existing_database_backfills_known_error_and_anchor(client):
    ingest(client, events(1, version="v1", offset=10000))
    tick()
    with connect() as conn:
        conn.execute("DROP TABLE group_versions")
    db.init()
    ingest(client, events(8, version="v2"))
    tick()
    assert client.get("/incidents").json() == []
    with connect() as conn:
        assert (
            conn.execute("SELECT first_error FROM group_versions WHERE version='v1'").fetchone()[
                "first_error"
            ]
            is not None
        )
