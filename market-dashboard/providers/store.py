from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable

import psycopg2
from psycopg2.extras import Json

from providers.common import DailyBars, NumericSeries, TextRecord


LOOKBACK_DAYS = int(os.getenv("MARKET_SYNC_LOOKBACK_DAYS", "370"))


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


def recent_rows(rows: Iterable[tuple[datetime, float]]):
    rows = list(rows)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    filtered = [(ts, value) for ts, value in rows if ts >= cutoff]
    return filtered or rows[-30:]


def upsert_asset_catalog(
    conn,
    asset_key: str,
    asset_name: str,
    category: str,
    source: str,
    market: str,
    source_symbol: str | None,
    latest_observation_ts: datetime | None,
    meta_json: dict,
):
    asset_catalog_key = f"{source}:{category}:{asset_key}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_asset_catalog
                (asset_catalog_key, asset_key, asset_name, market, category, provider, source_symbol,
                 latest_observation_ts, last_synced_at, meta_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (asset_catalog_key) DO UPDATE
            SET asset_name = EXCLUDED.asset_name,
                asset_key = EXCLUDED.asset_key,
                market = EXCLUDED.market,
                category = EXCLUDED.category,
                provider = EXCLUDED.provider,
                source_symbol = EXCLUDED.source_symbol,
                latest_observation_ts = GREATEST(
                    COALESCE(data_asset_catalog.latest_observation_ts, '-infinity'::timestamptz),
                    COALESCE(EXCLUDED.latest_observation_ts, '-infinity'::timestamptz)
                ),
                last_synced_at = NOW(),
                meta_json = EXCLUDED.meta_json
            """,
            (
                asset_catalog_key,
                asset_key,
                asset_name,
                market,
                category,
                source,
                source_symbol,
                latest_observation_ts,
                Json(meta_json),
            ),
        )


def upsert_points(conn, series: NumericSeries):
    with conn.cursor() as cur:
        for ts, value in series.rows:
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
                (
                    series.asset_key,
                    series.asset_name,
                    series.category,
                    series.source,
                    ts,
                    value,
                    Json(series.meta),
                ),
            )


def upsert_snapshot(conn, series: NumericSeries):
    last_ts, last_value = series.rows[-1]
    prev_value = series.rows[-2][1] if len(series.rows) > 1 else last_value
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
                series.asset_key,
                series.asset_name,
                series.category,
                series.source,
                last_ts,
                last_value,
                change,
                change_percent,
                Json(series.meta),
            ),
        )


def upsert_numeric_series(conn, series: NumericSeries):
    upsert_points(conn, series)
    upsert_snapshot(conn, series)
    upsert_asset_catalog(
        conn,
        series.asset_key,
        series.asset_name,
        series.category,
        series.source,
        series.market,
        series.source_symbol,
        series.rows[-1][0] if series.rows else None,
        series.meta,
    )


def upsert_daily_bars(conn, bars: DailyBars):
    with conn.cursor() as cur:
        for row in bars.rows:
            cur.execute(
                """
                INSERT INTO daily_bars
                    (asset_key, asset_name, market, category, source, source_symbol, ts,
                     open, high, low, close, adj_close, volume, meta_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (asset_key, ts) DO UPDATE
                SET asset_name = EXCLUDED.asset_name,
                    market = EXCLUDED.market,
                    category = EXCLUDED.category,
                    source = EXCLUDED.source,
                    source_symbol = EXCLUDED.source_symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume = EXCLUDED.volume,
                    meta_json = EXCLUDED.meta_json,
                    updated_at = NOW()
                """,
                (
                    bars.asset_key,
                    bars.asset_name,
                    bars.market,
                    bars.category,
                    bars.source,
                    bars.source_symbol,
                    row["ts"].date() if isinstance(row["ts"], datetime) else row["ts"],
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("adj_close"),
                    row.get("volume"),
                    Json({**bars.meta, **(row.get("meta") or {})}),
                ),
            )
    upsert_asset_catalog(
        conn,
        bars.asset_key,
        bars.asset_name,
        bars.category,
        bars.source,
        bars.market,
        bars.source_symbol,
        bars.rows[-1]["ts"] if bars.rows else None,
        bars.meta,
    )


def upsert_asset_aliases(conn, aliases: list[dict]):
    with conn.cursor() as cur:
        for item in aliases:
            cur.execute(
                """
                INSERT INTO asset_aliases
                    (alias, asset_key, source_symbol, market, alias_type, provider, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (alias, asset_key) DO UPDATE
                SET source_symbol = EXCLUDED.source_symbol,
                    market = EXCLUDED.market,
                    alias_type = EXCLUDED.alias_type,
                    provider = EXCLUDED.provider,
                    updated_at = NOW()
                """,
                (
                    item["alias"],
                    item["asset_key"],
                    item["source_symbol"],
                    item["market"],
                    item["alias_type"],
                    item.get("provider", "yahoo_finance"),
                ),
            )


def upsert_text_record(conn, record: TextRecord):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO market_text_records
                (record_id, asset_key, asset_name, market, category, source, source_symbol,
                 ts, title, url, body, meta_json, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (record_id) DO UPDATE
            SET asset_key = EXCLUDED.asset_key,
                asset_name = EXCLUDED.asset_name,
                market = EXCLUDED.market,
                category = EXCLUDED.category,
                source = EXCLUDED.source,
                source_symbol = EXCLUDED.source_symbol,
                ts = EXCLUDED.ts,
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                body = EXCLUDED.body,
                meta_json = EXCLUDED.meta_json,
                updated_at = NOW()
            """,
            (
                record.record_id,
                record.asset_key,
                record.asset_name,
                record.market,
                record.category,
                record.source,
                record.source_symbol,
                record.ts,
                record.title,
                record.url,
                record.body,
                Json(record.meta),
            ),
        )
    upsert_asset_catalog(
        conn,
        record.asset_key,
        record.asset_name,
        record.category,
        record.source,
        record.market,
        record.source_symbol,
        record.ts,
        record.meta,
    )


SEC_8K_ITEMS = {
    "2.02": "sec_8k_item_2_02_results",
    "5.02": "sec_8k_item_5_02_management",
    "8.01": "sec_8k_item_8_01_other",
    "9.01": "sec_8k_item_9_01_exhibits",
}

SEC_PERIODIC_SECTIONS = (
    ("Risk Factors", "sec_risk_factors"),
    ("Management's Discussion and Analysis", "sec_mda"),
    ("Management Discussion and Analysis", "sec_mda"),
    ("Financial Statements", "sec_financial_statements"),
    ("Legal Proceedings", "sec_legal_proceedings"),
)


def _snippet_after_marker(body: str, marker_pattern: str, max_chars: int = 2400) -> str:
    match = re.search(marker_pattern, body, flags=re.IGNORECASE)
    if not match:
        return ""
    snippet = body[match.start() : match.start() + max_chars]
    return re.sub(r"\s+", " ", snippet).strip()


def _sec_section_items(row: dict) -> list[dict]:
    body = row.get("body") or ""
    meta = row.get("meta_json") or {}
    form = (meta.get("form") or "").upper()
    if not body:
        return []
    items: list[dict] = []
    if form == "8-K":
        for item, evidence_type in SEC_8K_ITEMS.items():
            escaped = item.replace(".", r"\.")
            snippet = _snippet_after_marker(body, rf"\bItem\s+{escaped}\b")
            if snippet:
                items.append(
                    {
                        "id_suffix": f"item:{item}",
                        "evidence_type": evidence_type,
                        "title": f"{row['title']} - Item {item}",
                        "summary": f"8-K Item {item}",
                        "body_excerpt": snippet,
                    }
                )
    elif form in {"10-K", "10-Q"}:
        seen_types: set[str] = set()
        for label, evidence_type in SEC_PERIODIC_SECTIONS:
            if evidence_type in seen_types:
                continue
            snippet = _snippet_after_marker(body, re.escape(label))
            if snippet:
                seen_types.add(evidence_type)
                items.append(
                    {
                        "id_suffix": f"section:{evidence_type}",
                        "evidence_type": evidence_type,
                        "title": f"{row['title']} - {label}",
                        "summary": label,
                        "body_excerpt": snippet,
                    }
                )
    return items


def rebuild_evidence_items(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO evidence_items
                (evidence_id, evidence_type, asset_key, asset_name, market, source, source_symbol,
                 ts, title, url, summary, body_excerpt, confidence, meta_json, updated_at)
            SELECT
                'text:' || record_id AS evidence_id,
                CASE
                    WHEN source = 'sec_edgar' THEN 'sec_filing'
                    WHEN category = 'cn_company_announcement'
                        THEN 'cn_announcement:' || COALESCE(NULLIF(meta_json->>'normalized_announcement_type', ''), '其他公告')
                    WHEN category = 'cn_company_news' THEN 'cn_news'
                    WHEN category = 'cn_financial_indicator' THEN 'cn_financial'
                    ELSE category
                END AS evidence_type,
                asset_key,
                asset_name,
                market,
                source,
                source_symbol,
                ts,
                title,
                url,
                CASE
                    WHEN category = 'cn_company_announcement'
                        THEN COALESCE(meta_json->>'normalized_announcement_type', meta_json->>'announcement_type', body)
                    WHEN source = 'sec_edgar' THEN COALESCE(meta_json->>'form', category)
                    ELSE left(coalesce(body, ''), 500)
                END AS summary,
                left(coalesce(body, ''), 2200) AS body_excerpt,
                1.0 AS confidence,
                meta_json,
                NOW()
            FROM market_text_records
            ON CONFLICT (evidence_id) DO UPDATE
            SET evidence_type = EXCLUDED.evidence_type,
                asset_key = EXCLUDED.asset_key,
                asset_name = EXCLUDED.asset_name,
                market = EXCLUDED.market,
                source = EXCLUDED.source,
                source_symbol = EXCLUDED.source_symbol,
                ts = EXCLUDED.ts,
                title = EXCLUDED.title,
                url = EXCLUDED.url,
                summary = EXCLUDED.summary,
                body_excerpt = EXCLUDED.body_excerpt,
                confidence = EXCLUDED.confidence,
                meta_json = EXCLUDED.meta_json,
                updated_at = NOW()
            """
        )
        cur.execute(
            """
            SELECT record_id, asset_key, asset_name, market, source, source_symbol, ts, title, url, body, meta_json
            FROM market_text_records
            WHERE source = 'sec_edgar' AND body IS NOT NULL AND body <> ''
            """
        )
        rows = [
            {
                "record_id": row[0],
                "asset_key": row[1],
                "asset_name": row[2],
                "market": row[3],
                "source": row[4],
                "source_symbol": row[5],
                "ts": row[6],
                "title": row[7],
                "url": row[8],
                "body": row[9],
                "meta_json": row[10],
            }
            for row in cur.fetchall()
        ]
        for row in rows:
            for item in _sec_section_items(row):
                meta = {**(row.get("meta_json") or {}), "derived_from_record_id": row["record_id"], "section_summary": item["summary"]}
                cur.execute(
                    """
                    INSERT INTO evidence_items
                        (evidence_id, evidence_type, asset_key, asset_name, market, source, source_symbol,
                         ts, title, url, summary, body_excerpt, confidence, meta_json, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (evidence_id) DO UPDATE
                    SET evidence_type = EXCLUDED.evidence_type,
                        asset_key = EXCLUDED.asset_key,
                        asset_name = EXCLUDED.asset_name,
                        market = EXCLUDED.market,
                        source = EXCLUDED.source,
                        source_symbol = EXCLUDED.source_symbol,
                        ts = EXCLUDED.ts,
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        summary = EXCLUDED.summary,
                        body_excerpt = EXCLUDED.body_excerpt,
                        confidence = EXCLUDED.confidence,
                        meta_json = EXCLUDED.meta_json,
                        updated_at = NOW()
                    """,
                    (
                        f"sec-section:{row['record_id']}:{item['id_suffix']}",
                        item["evidence_type"],
                        row["asset_key"],
                        row["asset_name"],
                        row["market"],
                        row["source"],
                        row["source_symbol"],
                        row["ts"],
                        item["title"],
                        row["url"],
                        item["summary"],
                        item["body_excerpt"],
                        0.85,
                        Json(meta),
                    ),
                )


def set_status(conn, source: str, ok: bool, message: str, latest_observation_ts: datetime | None = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_sync_status (source, last_run, ok, message, latest_observation_ts, updated_at)
            VALUES (%s, NOW(), %s, %s, %s, NOW())
            ON CONFLICT (source) DO UPDATE
            SET last_run = NOW(),
                ok = EXCLUDED.ok,
                message = EXCLUDED.message,
                latest_observation_ts = COALESCE(
                    EXCLUDED.latest_observation_ts,
                    data_sync_status.latest_observation_ts
                ),
                updated_at = NOW()
            """,
            (source, ok, message, latest_observation_ts),
        )
