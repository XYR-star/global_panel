from __future__ import annotations

import hashlib
import importlib
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import Json, RealDictCursor

from providers.secrets import decrypt_secret, encrypt_secret

FUND_SECURITY_TYPES = {
    "etf_listed",
    "bond_fund",
    "fund_linked",
    "equity_fund",
    "qdii_fund",
    "other_fund",
    "money_or_cash",
}

DATA_SOURCE_DEFAULTS = {
    "fund_eid": {"display_name": "证监会基金电子披露", "enabled": True, "fetch_days": 7},
    "cninfo": {"display_name": "巨潮资讯", "enabled": True, "fetch_days": 7},
    "tushare": {"display_name": "Tushare", "enabled": False, "fetch_days": 7},
    "sec_edgar": {"display_name": "SEC EDGAR", "enabled": False, "fetch_days": 7},
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_event_defaults(conn) -> None:
    with conn.cursor() as cur:
        for key, item in DATA_SOURCE_DEFAULTS.items():
            cur.execute(
                """
                INSERT INTO portfolio_data_sources
                    (source_key, display_name, enabled, fetch_days, public_config)
                VALUES (%s, %s, %s, %s, '{}'::jsonb)
                ON CONFLICT (source_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name
                """,
                (key, item["display_name"], item["enabled"], item["fetch_days"]),
            )
        cur.execute("INSERT INTO portfolio_ai_settings (id, provider, model, daily_limit) VALUES (1, 'none', NULL, 30) ON CONFLICT (id) DO NOTHING")
    conn.commit()


def data_sources(conn) -> list[dict[str, Any]]:
    ensure_event_defaults(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT source_key, display_name, enabled, fetch_days, public_config,
                   encrypted_secret IS NOT NULL AS configured,
                   last_sync_at, last_sync_status, last_sync_message, updated_at
            FROM portfolio_data_sources
            ORDER BY CASE source_key
                WHEN 'fund_eid' THEN 1
                WHEN 'cninfo' THEN 2
                WHEN 'tushare' THEN 3
                WHEN 'sec_edgar' THEN 4
                ELSE 99
            END
            """
        )
        return [dict(row) for row in cur.fetchall()]


def update_data_source(conn, source_key: str, enabled: bool | None, fetch_days: int | None, public_config: dict[str, Any] | None, secret: str | None, replace_secret: bool = False) -> dict[str, Any]:
    ensure_event_defaults(conn)
    if source_key not in DATA_SOURCE_DEFAULTS:
        raise KeyError(source_key)
    assignments = ["updated_at = NOW()"]
    params: list[Any] = []
    if enabled is not None:
        assignments.append("enabled = %s")
        params.append(enabled)
    if fetch_days is not None:
        assignments.append("fetch_days = %s")
        params.append(max(1, min(60, int(fetch_days))))
    if public_config is not None:
        assignments.append("public_config = %s")
        params.append(Json(public_config))
    if secret:
        assignments.append("encrypted_secret = %s")
        params.append(encrypt_secret(secret))
    elif replace_secret:
        assignments.append("encrypted_secret = NULL")
    params.append(source_key)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            UPDATE portfolio_data_sources
            SET {', '.join(assignments)}
            WHERE source_key = %s
            RETURNING source_key, display_name, enabled, fetch_days, public_config,
                      encrypted_secret IS NOT NULL AS configured, last_sync_at, last_sync_status,
                      last_sync_message, updated_at
            """,
            tuple(params),
        )
        row = dict(cur.fetchone())
    conn.commit()
    return row


def latest_watchlist(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT batch_id, as_of_date
            FROM portfolio_import_batches
            WHERE status IN ('complete', 'partial')
            ORDER BY as_of_date DESC, uploaded_at DESC, batch_id DESC
            LIMIT 1
            """
        )
        batch = cur.fetchone()
        if not batch:
            return []
        cur.execute(
            """
            SELECT security_code AS symbol, security_name AS symbol_name, security_type,
                   market, batch_id, as_of_date
            FROM portfolio_positions
            WHERE batch_id = %s AND security_type = ANY(%s)
            ORDER BY portfolio_weight DESC NULLS LAST, holding_amount DESC NULLS LAST
            """,
            (batch["batch_id"], list(FUND_SECURITY_TYPES)),
        )
        return [dict(row) for row in cur.fetchall()]


def normalize_title(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def dedupe_hash(source_key: str, source_event_id: str | None, title: str, announcement_date: date, symbols: list[str]) -> str:
    parts = [source_key, source_event_id or "", normalize_title(title), announcement_date.isoformat(), ",".join(sorted(set(symbols)))]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def upsert_announcement(conn, announcement, watch_by_symbol: dict[str, dict[str, Any]]) -> bool:
    symbols = [str(symbol).zfill(6) for symbol in (announcement.symbols or []) if symbol]
    digest = dedupe_hash(announcement.source_key, announcement.source_event_id, announcement.title, announcement.announcement_date, symbols)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO portfolio_events
                (title, announcement_date, source_key, source_event_id, source_url, pdf_url,
                 event_type, importance, dedupe_hash, raw_json, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (dedupe_hash) DO NOTHING
            RETURNING event_id
            """,
            (
                normalize_title(announcement.title),
                announcement.announcement_date,
                announcement.source_key,
                announcement.source_event_id,
                announcement.source_url,
                announcement.pdf_url,
                announcement.event_type or "公告",
                max(1, min(5, int(announcement.importance or 3))),
                digest,
                Json(announcement.raw_json or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            return False
        event_id = row["event_id"]
        for symbol in symbols:
            watch = watch_by_symbol.get(symbol, {})
            cur.execute(
                """
                INSERT INTO portfolio_event_symbols
                    (event_id, symbol, symbol_name, security_type, market, batch_id, as_of_date, relation_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'holding')
                ON CONFLICT DO NOTHING
                """,
                (
                    event_id,
                    symbol,
                    watch.get("symbol_name"),
                    watch.get("security_type"),
                    watch.get("market"),
                    watch.get("batch_id"),
                    watch.get("as_of_date"),
                ),
            )
        return True


def source_module(source_key: str):
    return importlib.import_module(f"providers.event_sources.{source_key}")


def sync_source(conn, source: dict[str, Any], watchlist: list[dict[str, Any]]) -> dict[str, Any]:
    started = now_utc()
    source_key = source["source_key"]
    fetch_days = int(source.get("fetch_days") or 7)
    since = date.today() - timedelta(days=fetch_days)
    watch_by_symbol = {str(item["symbol"]).zfill(6): item for item in watchlist}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO portfolio_event_fetch_runs (source_key, started_at, status, fetch_days, watchlist_count)
            VALUES (%s, %s, 'running', %s, %s)
            RETURNING run_id
            """,
            (source_key, started, fetch_days, len(watchlist)),
        )
        run_id = cur.fetchone()["run_id"]
    conn.commit()
    fetched_count = inserted_count = duplicate_count = 0
    status = "success"
    error_message = None
    try:
        module = source_module(source_key)
        secret = decrypt_secret(source.get("encrypted_secret"))
        announcements = module.fetch_events(watchlist, since, fetch_days, source.get("public_config") or {}, secret)
        fetched_count = len(announcements)
        for announcement in announcements:
            inserted = upsert_announcement(conn, announcement, watch_by_symbol)
            if inserted:
                inserted_count += 1
            else:
                duplicate_count += 1
        conn.commit()
    except Exception as exc:
        conn.rollback()
        status = "failed"
        error_message = str(exc)[:900]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE portfolio_event_fetch_runs
            SET finished_at = NOW(), status = %s, fetched_count = %s,
                inserted_count = %s, duplicate_count = %s, error_message = %s
            WHERE run_id = %s
            """,
            (status, fetched_count, inserted_count, duplicate_count, error_message, run_id),
        )
        cur.execute(
            """
            UPDATE portfolio_data_sources
            SET last_sync_at = NOW(), last_sync_status = %s, last_sync_message = %s, updated_at = NOW()
            WHERE source_key = %s
            """,
            (status, error_message or f"fetched={fetched_count}, inserted={inserted_count}, duplicates={duplicate_count}", source_key),
        )
    conn.commit()
    return {
        "run_id": run_id,
        "source_key": source_key,
        "status": status,
        "fetched_count": fetched_count,
        "inserted_count": inserted_count,
        "duplicate_count": duplicate_count,
        "error_message": error_message,
    }


def sync_enabled_sources(conn) -> dict[str, Any]:
    ensure_event_defaults(conn)
    watchlist = latest_watchlist(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM portfolio_data_sources WHERE enabled = TRUE ORDER BY source_key")
        sources = [dict(row) for row in cur.fetchall()]
    runs = [sync_source(conn, source, watchlist) for source in sources]
    return {"watchlist_count": len(watchlist), "runs": runs}


def test_source(conn, source_key: str) -> dict[str, Any]:
    ensure_event_defaults(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM portfolio_data_sources WHERE source_key = %s", (source_key,))
        source = cur.fetchone()
    if not source:
        raise KeyError(source_key)
    try:
        module = source_module(source_key)
        module.fetch_events([], date.today() - timedelta(days=1), 1, dict(source.get("public_config") or {}), decrypt_secret(source.get("encrypted_secret")))
        return {"ok": True, "source_key": source_key, "status": "available"}
    except Exception as exc:
        return {"ok": False, "source_key": source_key, "status": "unavailable", "message": str(exc)[:300]}
