from psycopg.types.json import Jsonb


def evaluate(conn, comparison):
    def measures(version, start):
        traffic = conn.execute(
            "SELECT coalesce(sum(requests),0) AS n,count(*) AS coverage FROM traffic WHERE service=%s AND version=%s AND minute>=%s AND minute<%s+%s*interval '1 minute'",
            (comparison["service"], version, start, start, comparison["minutes"]),
        ).fetchone()
        groups = conn.execute(
            "SELECT group_id,count(*) AS n FROM events WHERE service=%s AND version=%s AND happened_at>=%s AND happened_at<%s+%s*interval '1 minute' AND level='ERROR' AND group_id IS NOT NULL GROUP BY group_id",
            (comparison["service"], version, start, start, comparison["minutes"]),
        ).fetchall()
        return int(traffic["n"]), traffic["coverage"], {r["group_id"]: r["n"] for r in groups}

    before, before_coverage, before_groups = measures(
        comparison["before_version"], comparison["before_start"]
    )
    after, after_coverage, after_groups = measures(
        comparison["after_version"], comparison["after_start"]
    )
    existing = {
        r["group_id"]: r
        for r in conn.execute(
            "SELECT * FROM regressions WHERE comparison_id=%s", (comparison["id"],)
        ).fetchall()
    }
    for group in before_groups.keys() | after_groups.keys() | existing.keys():
        old, new = before_groups.get(group, 0), after_groups.get(group, 0)
        before_rate = old / before if before else None
        after_rate = new / after if after else None
        enough = (
            before >= 100
            and after >= 100
            and before_coverage == comparison["minutes"]
            and after_coverage == comparison["minutes"]
        )
        active = bool(enough and new >= 5 and after_rate >= max(0.01, 3 * before_rate))
        state = "active" if active else ("retracted" if group in existing else "quiet")
        evidence = {
            "before_errors": old,
            "after_errors": new,
            "before_requests": before,
            "after_requests": after,
            "before_rate": before_rate,
            "after_rate": after_rate,
            "sufficient_traffic": enough,
            "before_minutes": before_coverage,
            "after_minutes": after_coverage,
            "rule": {
                "minimum_requests": 100,
                "minimum_errors": 5,
                "minimum_rate": 0.01,
                "multiplier": 3,
            },
        }
        previous = existing.get(group)
        if previous and previous["evidence"] == evidence and previous["status"] == state:
            continue
        if not previous and not active:
            continue
        row = conn.execute(
            """INSERT INTO regressions(comparison_id,group_id,status,evidence)
            VALUES (%s,%s,%s,%s) ON CONFLICT(comparison_id,group_id) DO UPDATE SET
            status=EXCLUDED.status,evidence=EXCLUDED.evidence,revision=regressions.revision+1,updated_at=clock_timestamp()
            RETURNING revision""",
            (comparison["id"], group, state, Jsonb(evidence)),
        ).fetchone()
        conn.execute(
            "INSERT INTO regression_history(comparison_id,group_id,revision,status,evidence) VALUES (%s,%s,%s,%s,%s)",
            (comparison["id"], group, row["revision"], state, Jsonb(evidence)),
        )
    conn.execute(
        "UPDATE comparisons SET dirty=false,checked_at=clock_timestamp() WHERE id=%s",
        (comparison["id"],),
    )


def tick(conn):
    # Тот же порядок блокировок, что у обработки логов; late data не теряет dirty-флаг.
    queued = conn.execute(
        "SELECT * FROM comparisons WHERE dirty ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 20"
    ).fetchall()
    for comparison in queued:
        evaluate(conn, comparison)
    return len(queued)
