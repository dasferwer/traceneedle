"""Явные границы хранения; вызов очистки никогда не запускается автоматически."""

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, LiteralString, cast

from psycopg import Connection, sql


@dataclass(frozen=True)
class Policy:
    late_minutes: int
    history_minutes: int


def policy() -> Policy:
    try:
        late = int(os.environ.get("SUPPORTED_LATE_MINUTES", "0"))
        history = int(os.environ.get("HISTORY_RETENTION_MINUTES", "0"))
    except ValueError as exc:
        raise ValueError(
            "SUPPORTED_LATE_MINUTES/HISTORY_RETENTION_MINUTES: нужны целые минуты"
        ) from exc
    if late < 0 or history < 0 or (history and (not late or history < late + 5)):
        raise ValueError("MINUTES: значения >= 0; retention требует late > 0 и history >= late + 5")
    return Policy(late, history)


def utcnow() -> datetime:
    return datetime.now(UTC)


def supported_since(
    conn: Connection[dict[str, Any]], current: datetime | None = None
) -> datetime | None:
    settings = policy()
    row = conn.execute("SELECT retained_since FROM retention_state WHERE id=1").fetchone()
    assert row is not None
    watermark = row["retained_since"]
    late = (
        (current or utcnow()) - timedelta(minutes=settings.late_minutes)
        if settings.late_minutes
        else None
    )
    return (
        max(value for value in (watermark, late) if value is not None)
        if watermark or late
        else None
    )


# Полуоткрытые окна всех незавершённых сравнений защищают исходные строки.
def pinned(conn: Connection[dict[str, Any]], service: str, version: str, moment: datetime) -> bool:
    return (
        conn.execute(
            """SELECT 1 FROM comparisons WHERE archived_at IS NULL AND service=%s AND
        ((before_version=%s AND %s>=before_start AND %s<before_start+minutes*interval '1 minute') OR
         (after_version=%s AND %s>=after_start AND %s<after_start+minutes*interval '1 minute')) LIMIT 1""",
            (service, version, moment, moment, version, moment, moment),
        ).fetchone()
        is not None
    )


def supported_input(
    conn: Connection[dict[str, Any]],
    service: str,
    version: str,
    moment: datetime,
    floor: datetime | None,
) -> bool:
    return floor is None or moment >= floor or pinned(conn, service, version, moment)


PIN = """NOT EXISTS (SELECT 1 FROM comparisons c WHERE c.archived_at IS NULL
    AND c.service=t.service AND
    ((c.before_version=t.version AND {moment}>=c.before_start AND {moment}<c.before_start+c.minutes*interval '1 minute') OR
     (c.after_version=t.version AND {moment}>=c.after_start AND {moment}<c.after_start+c.minutes*interval '1 minute')))"""


def cleanup(
    conn: Connection[dict[str, Any]], *, dry_run: bool = True, batch_size: int = 1000
) -> dict[str, Any]:
    # Порядок совпадает с ingest; worker/traffic/archive используют второй lock.
    conn.execute("SELECT pg_advisory_xact_lock(360027)")
    conn.execute("SELECT pg_advisory_xact_lock(360028)")
    settings = policy()
    current = utcnow()
    cutoff = (
        current - timedelta(minutes=settings.history_minutes) if settings.history_minutes else None
    )
    keys = {
        "events": ("events", ["id"]),
        "traffic": ("traffic", ["service", "version", "minute"]),
        "history": ("regression_history", ["comparison_id", "group_id", "revision"]),
        "regressions": ("regressions", ["comparison_id", "group_id"]),
        "comparisons": ("comparisons", ["id"]),
    }
    result: dict[str, Any] = {
        "enabled": bool(cutoff),
        "dry_run": dry_run,
        "cutoff": cutoff,
        "supported_since": supported_since(conn, current),
        "items": {name: [] for name in keys},
        "more": {name: False for name in keys},
    }
    if cutoff is None:
        return result
    queries = {
        "events": "SELECT t.id FROM events t WHERE t.group_id IS NOT NULL AND t.happened_at<%s AND "
        + PIN.format(moment="t.happened_at")
        + " ORDER BY t.happened_at,t.id LIMIT %s",
        "traffic": "SELECT t.service,t.version,t.minute FROM traffic t WHERE t.minute<%s AND "
        + PIN.format(moment="t.minute")
        + " ORDER BY t.minute,t.service,t.version LIMIT %s",
        "history": """SELECT h.comparison_id,h.group_id,h.revision FROM regression_history h
            JOIN comparisons c ON c.id=h.comparison_id WHERE c.archived_at<%s
            ORDER BY c.archived_at,h.comparison_id,h.group_id,h.revision LIMIT %s""",
        "regressions": """SELECT r.comparison_id,r.group_id FROM regressions r
            JOIN comparisons c ON c.id=r.comparison_id WHERE c.archived_at<%s
            AND NOT EXISTS (SELECT 1 FROM regression_history h WHERE h.comparison_id=r.comparison_id AND h.group_id=r.group_id)
            ORDER BY c.archived_at,r.comparison_id,r.group_id LIMIT %s""",
        "comparisons": """SELECT c.id FROM comparisons c WHERE c.archived_at<%s
            AND NOT EXISTS (SELECT 1 FROM regressions r WHERE r.comparison_id=c.id)
            AND NOT EXISTS (SELECT 1 FROM regression_history h WHERE h.comparison_id=c.id)
            ORDER BY c.archived_at,c.id LIMIT %s""",
    }
    # Сначала строим полный план: dry-run и apply имеют одинаковый состав на одном snapshot.
    selected = {}
    for name, query in queries.items():
        rows = conn.execute(
            sql.SQL(cast(LiteralString, query)), (cutoff, batch_size + 1)
        ).fetchall()
        selected[name] = rows[:batch_size]
        result["more"][name] = len(rows) > batch_size
        columns = keys[name][1]
        result["items"][name] = [
            str(row[columns[0]]) if len(columns) == 1 else row for row in rows[:batch_size]
        ]
    if not dry_run:
        for name in ("events", "traffic", "history", "regressions", "comparisons"):
            table, columns = keys[name]
            query = sql.SQL("DELETE FROM {} WHERE {}").format(
                sql.Identifier(table),
                sql.SQL(" AND ").join(sql.SQL("{}=%s").format(sql.Identifier(c)) for c in columns),
            )
            for row in selected[name]:
                conn.execute(query, tuple(row[c] for c in columns))
        conn.execute(
            "UPDATE retention_state SET retained_since=GREATEST(retained_since,%s) WHERE id=1",
            (cutoff,),
        )
    return result
