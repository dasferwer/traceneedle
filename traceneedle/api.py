import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Literal

import pika
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from traceneedle.db import connect, init


@asynccontextmanager
async def lifespan(app):
    init()
    yield


def authorize(x_api_key: str = Header(default="")):
    key = os.environ.get("API_KEY", "")
    if not key or not secrets.compare_digest(x_api_key, key):
        raise HTTPException(401, "Неверный API-ключ")


app = FastAPI(title="TraceNeedle", lifespan=lifespan, dependencies=[Depends(authorize)])


class Event(BaseModel):
    id: uuid.UUID
    service: str = Field(min_length=1, max_length=80)
    version: str = Field(min_length=1, max_length=80)
    happened_at: datetime
    level: Literal["INFO", "WARN", "ERROR"]
    message: str = Field(min_length=2, max_length=2000)
    trace_id: str | None = Field(default=None, max_length=120)

    @field_validator("happened_at")
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None:
            raise ValueError("Нужен часовой пояс")
        if (value - datetime.now(UTC)).total_seconds() > 60:
            raise ValueError("Событие слишком далеко в будущем")
        return value


class Batch(BaseModel):
    events: list[Event] = Field(min_length=1, max_length=1000)


@app.post("/events")
def ingest(body: Batch):
    accepted = 0
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(360027)")
        pending = conn.execute(
            "SELECT count(*) AS n FROM events WHERE group_id IS NULL"
        ).fetchone()["n"]
        existing = {
            r["id"]
            for r in conn.execute(
                "SELECT id FROM events WHERE id=ANY(%s)", ([e.id for e in body.events],)
            ).fetchall()
        }
        new_count = len({e.id for e in body.events} - existing)
        if pending + new_count > 10000:
            raise HTTPException(429, "Очередь заполнена", headers={"Retry-After": "1"})
        for event in body.events:
            record = event.model_dump()
            old = conn.execute(
                "SELECT id,service,version,happened_at,level,message,trace_id FROM events WHERE id=%s",
                (event.id,),
            ).fetchone()
            if old:
                if old != record:
                    raise HTTPException(409, "ID события уже связан с другими данными")
                continue
            conn.execute(
                "INSERT INTO events(id,service,version,happened_at,level,message,trace_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (
                    event.id,
                    event.service,
                    event.version,
                    event.happened_at,
                    event.level,
                    event.message,
                    event.trace_id,
                ),
            )
            accepted += 1
    try:
        with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
            channel = broker.channel()
            channel.queue_declare(queue="logs", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="logs",
                body="ready",
                properties=pika.BasicProperties(delivery_mode=2),
            )
    except (pika.exceptions.AMQPError, OSError):
        pass
    return {"accepted": accepted, "duplicates": len(body.events) - accepted}


@app.get("/groups")
def groups(after: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    with connect() as conn:
        return conn.execute(
            "SELECT id,service,template,first_seen FROM groups WHERE id>%s ORDER BY id LIMIT %s",
            (after, limit),
        ).fetchall()


@app.get("/groups/{identity}/similar")
def similar(identity: int):
    with connect() as conn:
        return conn.execute(
            """SELECT b.id,b.service,b.template,1-(a.embedding OPERATOR(public.<=>) b.embedding) AS similarity
            FROM groups a JOIN groups b ON a.id<>b.id WHERE a.id=%s
            ORDER BY a.embedding OPERATOR(public.<=>) b.embedding LIMIT 5""",
            (identity,),
        ).fetchall()


@app.get("/incidents")
def incidents():
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM incidents ORDER BY detected_at DESC LIMIT 1000"
        ).fetchall()


@app.get("/traces/{trace_id}")
def trace(trace_id: str):
    with connect() as conn:
        rows = conn.execute(
            "SELECT service,version,happened_at,group_id,level FROM events WHERE trace_id=%s ORDER BY happened_at,id LIMIT 1000",
            (trace_id,),
        ).fetchall()
    return {
        "events": rows,
        "interpretation": "Общий trace связывает события; порядок по времени сам по себе не доказывает причину.",
    }


@app.get("/deployments/{service}")
def deployment(service: str, before: str, after: str):
    with connect() as conn:
        rows = conn.execute(
            """SELECT version,count(*) AS total,
            count(*) FILTER (WHERE level='ERROR') AS errors,
            count(DISTINCT group_id) FILTER (WHERE level='ERROR') AS error_groups
            FROM events WHERE service=%s AND version=ANY(%s) GROUP BY version""",
            (service, [before, after]),
        ).fetchall()
    return {
        "versions": rows,
        "note": "Числа относятся ко всему загруженному набору каждой версии, без нормировки на время.",
    }


@app.get("/events/{identity}")
def event(identity: uuid.UUID):
    with connect() as conn:
        row = conn.execute("SELECT * FROM events WHERE id=%s", (identity,)).fetchone()
    if not row:
        raise HTTPException(404, "Событие не найдено")
    return row


@app.get("/health")
def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


class Traffic(BaseModel):
    service: str = Field(min_length=1, max_length=80)
    version: str = Field(min_length=1, max_length=80)
    minute: datetime
    requests: int = Field(ge=0, le=1000000000)
    expected_revision: int = Field(default=0, ge=0)

    @field_validator("minute")
    @classmethod
    def minute_boundary(cls, value):
        if value.tzinfo is None or value.second or value.microsecond:
            raise ValueError("Нужна граница минуты с часовым поясом")
        return value


@app.put("/traffic")
def traffic(body: Traffic):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(360028)")
        previous = conn.execute(
            "SELECT * FROM traffic WHERE service=%s AND version=%s AND minute=%s",
            (body.service, body.version, body.minute),
        ).fetchone()
        revision = previous["revision"] if previous else 0
        if revision != body.expected_revision:
            raise HTTPException(409, "Счётчик трафика уже изменён; прочитайте актуальную ревизию")
        conn.execute(
            "INSERT INTO traffic VALUES (%s,%s,%s,%s,%s) ON CONFLICT(service,version,minute) DO UPDATE SET requests=EXCLUDED.requests,revision=EXCLUDED.revision",
            (body.service, body.version, body.minute, body.requests, revision + 1),
        )
        conn.execute("UPDATE comparisons SET dirty=true WHERE service=%s", (body.service,))
        return {"revision": revision + 1}


@app.get("/traffic")
def traffic_state(service: str, version: str, minute: datetime):
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM traffic WHERE service=%s AND version=%s AND minute=%s",
            (service, version, minute),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "Счётчик не найден")
    return row


class Comparison(BaseModel):
    id: uuid.UUID
    service: str = Field(min_length=1, max_length=80)
    before_version: str = Field(min_length=1, max_length=80)
    after_version: str = Field(min_length=1, max_length=80)
    before_start: datetime
    after_start: datetime
    minutes: int = Field(default=5, ge=1, le=60)

    @field_validator("before_start", "after_start")
    @classmethod
    def boundaries(cls, value):
        return Traffic.minute_boundary(value)


@app.post("/comparisons", status_code=202)
def compare_deployment(body: Comparison):
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(360028)")
        old = conn.execute("SELECT * FROM comparisons WHERE id=%s", (body.id,)).fetchone()
        if old:
            if any(old[k] != v for k, v in body.model_dump().items()):
                raise HTTPException(409, "Идентификатор сравнения уже используется")
            return {"id": body.id}
        if conn.execute("SELECT count(*) AS n FROM comparisons").fetchone()["n"] >= 100:
            raise HTTPException(409, "Лимит стенда: 100 сравнений")
        conn.execute(
            "INSERT INTO comparisons(id,service,before_version,after_version,before_start,after_start,minutes) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                body.id,
                body.service,
                body.before_version,
                body.after_version,
                body.before_start,
                body.after_start,
                body.minutes,
            ),
        )
    return {"id": body.id}


@app.get("/comparisons/{identity}")
def comparison(identity: uuid.UUID):
    with connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        row = conn.execute("SELECT * FROM comparisons WHERE id=%s", (identity,)).fetchone()
        if row is None:
            raise HTTPException(404, "Сравнение не найдено")
        row["regressions"] = conn.execute(
            "SELECT * FROM regressions WHERE comparison_id=%s ORDER BY group_id", (identity,)
        ).fetchall()
        row["history"] = conn.execute(
            "SELECT * FROM regression_history WHERE comparison_id=%s ORDER BY group_id,revision",
            (identity,),
        ).fetchall()
        return row
