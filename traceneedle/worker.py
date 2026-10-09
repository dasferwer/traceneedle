import logging
import os
import time

import pika
from pika.exceptions import AMQPError

from traceneedle.db import connect, init
from traceneedle.mining import embedding, miner, normalized
from traceneedle.regression import tick as regression_tick

logger = logging.getLogger(__name__)


def tick():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(360028)")
        events = conn.execute(
            "SELECT * FROM events WHERE group_id IS NULL ORDER BY received_at,id LIMIT 100 FOR UPDATE"
        ).fetchall()
        if not events:
            return regression_tick(conn)
        model = miner(conn)
        for event in events:
            result = model.add_log_message(event["service"] + " " + normalized(event["message"]))
            group_row = conn.execute(
                """INSERT INTO groups(service,cluster_id,template,embedding,first_seen)
                VALUES (%s,%s,%s,%s::public.vector,%s) ON CONFLICT(service,cluster_id)
                DO UPDATE SET template=EXCLUDED.template,embedding=EXCLUDED.embedding,
                first_seen=LEAST(groups.first_seen,EXCLUDED.first_seen) RETURNING id""",
                (
                    event["service"],
                    result["cluster_id"],
                    result["template_mined"],
                    embedding(result["template_mined"]),
                    event["happened_at"],
                ),
            ).fetchone()
            assert group_row is not None
            group = group_row["id"]
            conn.execute("UPDATE events SET group_id=%s WHERE id=%s", (group, event["id"]))
            # Окно задано временем события, а обнаружение измеряется временем приёма.
            anchor_row = conn.execute(
                """INSERT INTO group_versions(group_id,version,last_seen,first_error)
                VALUES (%s,%s,%s,%s) ON CONFLICT(group_id,version) DO UPDATE SET
                last_seen=GREATEST(group_versions.last_seen,EXCLUDED.last_seen),
                first_error=LEAST(group_versions.first_error,EXCLUDED.first_error) RETURNING last_seen""",
                (
                    group,
                    event["version"],
                    event["happened_at"],
                    event["happened_at"] if event["level"] == "ERROR" else None,
                ),
            ).fetchone()
            assert anchor_row is not None
            anchor = anchor_row["last_seen"]
            recent = conn.execute(
                """SELECT count(*) AS n,min(happened_at) AS first FROM events
                WHERE group_id=%s AND version=%s AND level='ERROR'
                AND happened_at BETWEEN %s-interval '5 minutes' AND %s""",
                (group, event["version"], anchor, anchor),
            ).fetchone()
            assert recent is not None
            if event["level"] == "ERROR" and recent["n"] >= 5:
                known = conn.execute(
                    "SELECT 1 FROM group_versions WHERE group_id=%s AND version<>%s AND first_error IS NOT NULL LIMIT 1",
                    (group, event["version"]),
                ).fetchone()
                if not known:
                    conn.execute(
                        """INSERT INTO incidents VALUES (%s,%s,clock_timestamp(),%s) ON CONFLICT DO NOTHING""",
                        (group, event["version"], recent["first"]),
                    )
        conn.execute(
            "UPDATE comparisons SET dirty=true WHERE service=ANY(%s) AND archived_at IS NULL",
            (list({e["service"] for e in events}),),
        )
        regression_tick(conn)
        model.save_state("batch")
        return len(events)


def main():
    logging.basicConfig(level=logging.WARNING)
    init()
    while True:
        try:
            if tick():
                continue
            try:
                with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
                    channel = broker.channel()
                    channel.queue_declare(queue="logs", durable=True)
                    method, _, _ = channel.basic_get(queue="logs", auto_ack=False)
                    if method:
                        channel.basic_ack(method.delivery_tag)
            except (AMQPError, OSError):
                pass
        except Exception:
            logger.exception("Пакет логов не обработан; повтор после восстановления")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
