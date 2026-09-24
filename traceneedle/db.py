import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def init():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(360029)")
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS miner_state (id integer PRIMARY KEY CHECK(id=1), state bytea NOT NULL);
            CREATE TABLE IF NOT EXISTS groups (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                service text NOT NULL, cluster_id integer NOT NULL, template text NOT NULL,
                embedding public.vector(64) NOT NULL, first_seen timestamptz NOT NULL,
                UNIQUE(service,cluster_id));
            CREATE TABLE IF NOT EXISTS events (
                id uuid PRIMARY KEY, service text NOT NULL, version text NOT NULL,
                happened_at timestamptz NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
                level text NOT NULL, message text NOT NULL, trace_id text,
                group_id bigint REFERENCES groups(id));
            CREATE INDEX IF NOT EXISTS pending_events ON events(received_at) WHERE group_id IS NULL;
            CREATE INDEX IF NOT EXISTS group_events ON events(group_id,happened_at);
            CREATE TABLE IF NOT EXISTS incidents (
                group_id bigint REFERENCES groups(id), version text NOT NULL,
                detected_at timestamptz NOT NULL, first_event timestamptz NOT NULL,
                PRIMARY KEY(group_id,version));
            CREATE INDEX IF NOT EXISTS service_window ON events(service,version,happened_at);
            CREATE TABLE IF NOT EXISTS traffic (
                service text NOT NULL,version text NOT NULL,minute timestamptz NOT NULL,
                requests bigint NOT NULL CHECK(requests>=0),revision integer NOT NULL,
                PRIMARY KEY(service,version,minute));
            CREATE TABLE IF NOT EXISTS comparisons (
                id uuid PRIMARY KEY,service text NOT NULL,before_version text NOT NULL,after_version text NOT NULL,
                before_start timestamptz NOT NULL,after_start timestamptz NOT NULL,minutes integer NOT NULL,
                dirty boolean NOT NULL DEFAULT true,checked_at timestamptz);
            CREATE TABLE IF NOT EXISTS regressions (
                comparison_id uuid REFERENCES comparisons(id),group_id bigint REFERENCES groups(id),
                status text NOT NULL,revision integer NOT NULL DEFAULT 1,evidence jsonb NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(comparison_id,group_id));
            CREATE TABLE IF NOT EXISTS regression_history (
                comparison_id uuid NOT NULL,group_id bigint NOT NULL,revision integer NOT NULL,
                status text NOT NULL,evidence jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY(comparison_id,group_id,revision));
        """)
