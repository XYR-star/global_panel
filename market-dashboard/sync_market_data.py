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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOOKBACK_DAYS = int(os.getenv("MARKET_SYNC_LOOKBACK_DAYS", "370"))
FRED_TIMEOUT = 30
HTTP_SESSION = requests.Session()
HTTP_SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
    ),
)

@dataclass(frozen=True)
class FredSeries:
    key: str
    name: str
    category: str
    series_id: str
    scale: float = 1.0
    unit: str | None = None


MARKET_SERIES = [
    FredSeries("SPX", "S&P 500", "indices", "SP500"),
    FredSeries("NDX", "Nasdaq Composite", "indices", "NASDAQCOM"),
    FredSeries("DJI", "Dow Jones Industrial Average", "indices", "DJIA"),
    FredSeries("NKY", "Nikkei 225", "indices", "NIKKEI225"),
    FredSeries("UKX", "NASDAQ UK Index", "indices", "NASDAQNQGB"),
    FredSeries("DAX", "NASDAQ Germany Index", "indices", "NASDAQNQDE"),
    FredSeries("CAC", "NASDAQ France Index", "indices", "NASDAQNQFR"),
    FredSeries("HKG", "NASDAQ Hong Kong Index", "indices", "NASDAQNQHK"),
    FredSeries("CHN", "NASDAQ China Index", "indices", "NASDAQNQCN"),
    FredSeries("IND", "NASDAQ India Index", "indices", "NASDAQNQIN"),
    FredSeries("AUS", "NASDAQ Australia Index", "indices", "NASDAQNQAU"),
    FredSeries("CAN", "NASDAQ Canada Index", "indices", "NASDAQNQCA"),
    FredSeries("BRA", "NASDAQ Brazil Index", "indices", "NASDAQNQBR"),
    FredSeries("KOR", "NASDAQ Korea Index", "indices", "NASDAQNQKR"),
    FredSeries("TWN", "NASDAQ Taiwan Index", "indices", "NASDAQNQTW"),
    FredSeries("WTI", "WTI Crude Oil", "commodities", "DCOILWTICO"),
    FredSeries("BRENT", "Brent Crude Oil", "commodities", "DCOILBRENTEU"),
    FredSeries("COPPER", "Copper (USD/lb)", "commodities", "PCOPPUSDM", 1 / 2204.62262185, "USD per pound"),
    FredSeries("GOLD_IDX", "Gold Index", "commodities", "NASDAQQGLDI"),
    FredSeries("DXY", "Broad Dollar Index", "fx", "DTWEXBGS"),
    FredSeries("EURUSD", "EUR/USD", "fx", "DEXUSEU"),
    FredSeries("GBPUSD", "GBP/USD", "fx", "DEXUSUK"),
    FredSeries("USDJPY", "USD/JPY", "fx", "DEXJPUS"),
    FredSeries("USDCNY", "USD/CNY", "fx", "DEXCHUS"),
    FredSeries("USDCHF", "USD/CHF", "fx", "DEXSZUS"),
    FredSeries("USDKRW", "USD/KRW", "fx", "DEXKOUS"),
    FredSeries("USDINR", "USD/INR", "fx", "DEXINUS"),
    FredSeries("USDBRL", "USD/BRL", "fx", "DEXBZUS"),
    FredSeries("USDCAD", "USD/CAD", "fx", "DEXCAUS"),
    FredSeries("AUDUSD", "AUD/USD", "fx", "DEXUSAL"),
    FredSeries("USDSGD", "USD/SGD", "fx", "DEXSIUS"),
]

MACRO_SERIES = [
    FredSeries("UST2Y", "US Treasury 2Y", "rates", "DGS2"),
    FredSeries("UST10Y", "US Treasury 10Y", "rates", "DGS10"),
    FredSeries("UST30Y", "US Treasury 30Y", "rates", "DGS30"),
    FredSeries("CPI", "Consumer Price Index", "macro", "CPIAUCSL"),
    FredSeries("UNRATE", "Unemployment Rate", "macro", "UNRATE"),
    FredSeries("FEDFUNDS", "Federal Funds Rate", "macro", "FEDFUNDS"),
    FredSeries("HY_YIELD", "US High Yield Effective Yield", "credit", "BAMLH0A0HYM2EY"),
    FredSeries("IG_OAS", "US Corporate OAS", "credit", "BAMLC0A0CM"),
    FredSeries("FED_BALANCE", "Fed Balance Sheet", "liquidity", "WALCL"),
    FredSeries("FED_TREASURY", "Fed Treasury Holdings", "liquidity", "TREAST"),
    FredSeries("ROWUST_OFFICIAL_FLOW", "Foreign Official Treasury Transactions", "flows", "BOGZ1FA263061130Q"),
    FredSeries("ROW_US_EQ_FLOW", "Rest of World U.S. Equity Transactions", "flows", "ROWCEAQ027S"),
    FredSeries("ROW_US_RISK_FLOW", "Rest of World U.S. Risk Asset Transactions", "flows", "BOGZ1FU263064003Q"),
    FredSeries("CN_RESERVES", "China Reserves ex Gold", "reserves", "TRESEGCNM052N"),
    FredSeries("JP_RESERVES", "Japan Reserves ex Gold", "reserves", "TRESEGJPM052N"),
    FredSeries("EZ_RESERVES", "Euro Area Reserves ex Gold", "reserves", "TRESEGEZA052N"),
    FredSeries("UK_RESERVES", "UK Reserves ex Gold", "reserves", "TRESEGGBM052N"),
    FredSeries("IN_RESERVES", "India Reserves ex Gold", "reserves", "TRESEGINM052N"),
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
    response = HTTP_SESSION.get(url, timeout=FRED_TIMEOUT)
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


def nyfed_gscpi_csv():
    url = "https://www.newyorkfed.org/medialibrary/research/interactives/data/gscpi/gscpi_interactive_data.csv"
    response = HTTP_SESSION.get(url, timeout=FRED_TIMEOUT)
    response.raise_for_status()
    text = response.text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or len(reader.fieldnames) < 2:
        raise RuntimeError("Unexpected GSCPI CSV format")
    date_column = reader.fieldnames[0]
    latest_vintage = reader.fieldnames[-1]
    rows = []
    for row in reader:
        value = row.get(latest_vintage)
        if not value or value == ".":
            continue
        ts = datetime.strptime(row[date_column], "%d-%b-%Y").replace(tzinfo=timezone.utc)
        rows.append((ts, float(value)))
    if not rows:
        raise RuntimeError("No NY Fed GSCPI data returned")
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
            if series.scale != 1.0:
                rows = [(ts, value * series.scale) for ts, value in rows]
            meta = {"series_id": series.series_id, "provider": "fredgraph"}
            if series.unit:
                meta["unit"] = series.unit
            if series.scale != 1.0:
                meta["scale"] = series.scale
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


def sync_shipping(conn):
    try:
        rows = recent_rows(nyfed_gscpi_csv())
        meta = {
            "provider": "newyorkfed",
            "dataset": "Global Supply Chain Pressure Index",
            "note": "Latest vintage column from official NY Fed interactive CSV",
        }
        upsert_points(conn, "GSCPI", "Global Supply Chain Pressure Index", "shipping", "newyorkfed", rows, meta)
        upsert_snapshot(conn, "GSCPI", "Global Supply Chain Pressure Index", "shipping", "newyorkfed", rows, meta)
        set_status(conn, "newyorkfed_shipping", True, "OK")
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        set_status(conn, "newyorkfed_shipping", False, str(exc)[:900])
        conn.commit()


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
        sync_shipping(conn)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
