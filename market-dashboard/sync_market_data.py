#!/usr/bin/env python3
import csv
import io
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import psycopg2
import requests
from psycopg2.extras import Json


LOOKBACK_DAYS = int(os.getenv("MARKET_SYNC_LOOKBACK_DAYS", "370"))
FRED_TIMEOUT = 30

@dataclass(frozen=True)
class FredSeries:
    key: str
    name: str
    category: str
    series_id: str


MARKET_SERIES = [
    FredSeries("SPX", "S&P 500", "indices", "SP500"),
    FredSeries("NDX", "Nasdaq Composite", "indices", "NASDAQCOM"),
    FredSeries("DJI", "Dow Jones Industrial Average", "indices", "DJIA"),
    FredSeries("NKY", "Nikkei 225", "indices", "NIKKEI225"),
    FredSeries("WTI", "WTI Crude Oil", "commodities", "DCOILWTICO"),
    FredSeries("BRENT", "Brent Crude Oil", "commodities", "DCOILBRENTEU"),
    FredSeries("COPPER", "Copper", "commodities", "PCOPPUSDM"),
    FredSeries("DXY", "Broad Dollar Index", "fx", "DTWEXBGS"),
    FredSeries("EURUSD", "EUR/USD", "fx", "DEXUSEU"),
    FredSeries("USDJPY", "USD/JPY", "fx", "DEXJPUS"),
    FredSeries("USDCNY", "USD/CNY", "fx", "DEXCHUS"),
]

MACRO_SERIES = [
    FredSeries("UST2Y", "US Treasury 2Y", "rates", "DGS2"),
    FredSeries("UST10Y", "US Treasury 10Y", "rates", "DGS10"),
    FredSeries("UST30Y", "US Treasury 30Y", "rates", "DGS30"),
    FredSeries("CPI", "Consumer Price Index", "macro", "CPIAUCSL"),
    FredSeries("UNRATE", "Unemployment Rate", "macro", "UNRATE"),
    FredSeries("FEDFUNDS", "Federal Funds Rate", "macro", "FEDFUNDS"),
]


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
    with open(os.path.join(os.path.dirname(__file__), "init_db.sql"), "r", encoding="utf-8") as handle:
        sql = handle.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def fred_csv(series_id: str):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    response = requests.get(url, timeout=FRED_TIMEOUT)
    response.raise_for_status()
    rows = []
    reader = csv.DictReader(io.StringIO(response.text))
    date_key = "DATE" if "DATE" in reader.fieldnames else "observation_date"
    for row in reader:
        value = row.get(series_id)
        if not value or value == ".":
            continue
        ts = datetime.fromisoformat(row[date_key]).replace(tzinfo=timezone.utc)
        rows.append((ts, float(value)))
    if not rows:
        raise RuntimeError(f"No FRED data returned for {series_id}")
    return rows


def recent_rows(rows: Iterable[tuple[datetime, float]]):
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    filtered = [(ts, value) for ts, value in rows if ts >= cutoff]
    return filtered or list(rows)[-30:]


def upsert_points(conn, asset_key, asset_name, category, source, rows, meta_json):
    with conn.cursor() as cur:
        for ts, value in rows:
            cur.execute(
                """
                INSERT INTO market_data_points
                    (asset_key, asset_name, category, source, ts, value, meta_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (asset_key, ts) DO UPDATE
                SET asset_name = EXCLUDED.asset_name,
                    category = EXCLUDED.category,
                    source = EXCLUDED.source,
                    value = EXCLUDED.value,
                    meta_json = EXCLUDED.meta_json
                """,
                (asset_key, asset_name, category, source, ts, value, Json(meta_json)),
            )


def upsert_snapshot(conn, asset_key, asset_name, category, source, rows, meta_json):
    last_ts, last_value = rows[-1]
    prev_value = rows[-2][1] if len(rows) > 1 else last_value
    change = float(last_value - prev_value)
    change_percent = float((change / prev_value) * 100) if prev_value not in (0, None) else 0.0
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market_asset_snapshot
                (asset_key, asset_name, category, source, ts, value, change, change_percent, meta_json, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (asset_key) DO UPDATE
            SET asset_name = EXCLUDED.asset_name,
                category = EXCLUDED.category,
                source = EXCLUDED.source,
                ts = EXCLUDED.ts,
                value = EXCLUDED.value,
                change = EXCLUDED.change,
                change_percent = EXCLUDED.change_percent,
                meta_json = EXCLUDED.meta_json,
                updated_at = NOW()
            """,
            (
                asset_key,
                asset_name,
                category,
                source,
                last_ts,
                last_value,
                change,
                change_percent,
                Json(meta_json),
            ),
        )


def set_status(conn, source, ok, message):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_sync_status (source, last_run, ok, message, updated_at)
            VALUES (%s, NOW(), %s, %s, NOW())
            ON CONFLICT (source) DO UPDATE
            SET last_run = NOW(),
                ok = EXCLUDED.ok,
                message = EXCLUDED.message,
                updated_at = NOW()
            """,
            (source, ok, message),
        )


def sync_fred_series(conn, series_list: list[FredSeries], status_key: str):
    errors = []
    for series in series_list:
        try:
            rows = recent_rows(fred_csv(series.series_id))
            meta = {"series_id": series.series_id, "provider": "fredgraph"}
            upsert_points(conn, series.key, series.name, series.category, "fredgraph", rows, meta)
            upsert_snapshot(conn, series.key, series.name, series.category, "fredgraph", rows, meta)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(f"{series.key}: {exc}")
    if errors:
        set_status(conn, status_key, False, "; ".join(errors)[:900])
    else:
        set_status(conn, status_key, True, "OK")
    conn.commit()


def sync_market(conn):
    sync_fred_series(conn, MARKET_SERIES, "fredgraph_market")


def sync_macro(conn):
    sync_fred_series(conn, MACRO_SERIES, "fredgraph_macro")


def main():
    try:
        conn = connect_db()
    except Exception as exc:  # noqa: BLE001
        print(f"Database connection failed: {exc}", file=sys.stderr)
        return 1

    try:
        ensure_schema(conn)
        sync_market(conn)
        sync_macro(conn)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
