from __future__ import annotations

import os

import psycopg2


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connect_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=env("POSTGRES_DB"),
        user=env("POSTGRES_USER"),
        password=env("POSTGRES_PASSWORD"),
    )


def ensure_schema(conn):
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "init_db.sql")
    with open(schema_path, "r", encoding="utf-8") as handle:
        sql = handle.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
