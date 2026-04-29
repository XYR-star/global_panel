#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable

from providers import akshare_cn, yahoo_finance
from providers.asset_universe import alias_rows
from providers.fred_nyfed import MACRO_SERIES, MARKET_SERIES, fred_numeric_series, nyfed_shipping_series
from providers.sec_edgar import collect_sec_edgar
from providers.store import (
    connect_db,
    ensure_schema,
    rebuild_evidence_items,
    set_status,
    upsert_asset_aliases,
    upsert_daily_bars,
    upsert_numeric_series,
    upsert_text_record,
)

STATUS_CACHE_CATEGORIES = {
    "akshare_market_fund_flow": ("akshare", "cn_market_fund_flow"),
    "akshare_company_news": ("akshare", "cn_company_news"),
    "akshare_financial_indicators": ("akshare", "cn_financial_indicator"),
    "akshare_company_announcements": ("akshare", "cn_company_announcement"),
    "yahoo_cn_indices": ("yahoo_finance", "cn_indices"),
    "yahoo_cn_equity_daily": ("yahoo_finance", "cn_equity_quote"),
    "yahoo_us_equity_daily": ("yahoo_finance", "us_equity_daily"),
    "yahoo_daily_bars": ("yahoo_finance", "us_equity_daily_bar"),
}

RETIRED_AKSHARE_STATUS = (
    "akshare_a_share_quotes",
    "akshare_cn_indices",
    "akshare_company_profiles",
    "akshare_fund_flow",
    "akshare_industry_boards",
    "akshare_a_share_full_quotes",
    "akshare_a_share_histories",
    "akshare_concept_boards",
    "akshare_industry_constituents",
)

RETIRED_AKSHARE_CATEGORIES = (
    "cn_equity_quote",
    "cn_indices",
    "cn_company_profile",
    "cn_fund_flow",
    "cn_industry",
    "cn_equity_full_quote",
    "cn_equity_history",
    "cn_concept",
    "cn_industry_constituents",
)


def enabled(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def latest_ts(values) -> datetime | None:
    timestamps: list[datetime] = []
    for value in values:
        if hasattr(value, "rows") and value.rows:
            last_row = value.rows[-1]
            if isinstance(last_row, dict):
                timestamps.append(last_row["ts"])
            else:
                timestamps.append(last_row[0])
        elif hasattr(value, "ts"):
            timestamps.append(value.ts)
    return max(timestamps) if timestamps else None


def should_run(conn, status_key: str, interval_minutes: int) -> bool:
    if enabled("FORCE_MARKET_SYNC", "false"):
        return True
    if interval_minutes <= 0:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT last_run FROM data_sync_status WHERE source = %s AND ok = true", (status_key,))
        row = cur.fetchone()
    conn.commit()
    if not row or not row[0]:
        return True
    return datetime.now(timezone.utc) - row[0] >= timedelta(minutes=interval_minutes)


def interval_minutes(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def cached_observation_ts(conn, status_key: str) -> datetime | None:
    mapping = STATUS_CACHE_CATEGORIES.get(status_key)
    if not mapping:
        return None
    provider, category = mapping
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(latest_observation_ts)
            FROM data_asset_catalog
            WHERE provider = %s AND category = %s
            """,
            (provider, category),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_failure_status(conn, status_key: str, message: str):
    cached_ts = cached_observation_ts(conn, status_key)
    if cached_ts is not None and enabled("MARKET_SYNC_USE_CACHE_ON_FAILURE", "true"):
        set_status(conn, status_key, True, f"Using cached data; latest sync failed: {message}"[:900], cached_ts)
    else:
        set_status(conn, status_key, False, message[:900], cached_ts)


def cleanup_retired_akshare_sources(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM data_sync_status WHERE source = ANY(%s)", (list(RETIRED_AKSHARE_STATUS),))
        cur.execute(
            "DELETE FROM market_data_points WHERE source = 'akshare' AND category = ANY(%s)",
            (list(RETIRED_AKSHARE_CATEGORIES),),
        )
        cur.execute(
            "DELETE FROM market_asset_snapshot WHERE source = 'akshare' AND category = ANY(%s)",
            (list(RETIRED_AKSHARE_CATEGORIES),),
        )
        cur.execute(
            "DELETE FROM market_text_records WHERE source = 'akshare' AND category = ANY(%s)",
            (list(RETIRED_AKSHARE_CATEGORIES),),
        )
        cur.execute(
            "DELETE FROM data_asset_catalog WHERE provider = 'akshare' AND category = ANY(%s)",
            (list(RETIRED_AKSHARE_CATEGORIES),),
        )
    conn.commit()


def sync_numeric_group(conn, status_key: str, producer: Callable[[], list], ok_message: str):
    errors: list[str] = []
    synced = []
    try:
        items = producer()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        set_failure_status(conn, status_key, str(exc))
        conn.commit()
        return

    for item in items:
        try:
            upsert_numeric_series(conn, item)
            conn.commit()
            synced.append(item)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(f"{item.asset_key}: {exc}")

    if errors:
        set_status(conn, status_key, False, "; ".join(errors)[:900], latest_ts(synced))
    else:
        set_status(conn, status_key, True, ok_message.format(count=len(synced)), latest_ts(synced))
    conn.commit()


def sync_text_group(conn, status_key: str, producer: Callable[[], list], ok_message: str):
    errors: list[str] = []
    synced = []
    try:
        items = producer()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        set_failure_status(conn, status_key, str(exc))
        conn.commit()
        return

    for item in items:
        try:
            upsert_text_record(conn, item)
            conn.commit()
            synced.append(item)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(f"{item.record_id}: {exc}")

    if errors:
        set_status(conn, status_key, False, "; ".join(errors)[:900], latest_ts(synced))
    else:
        set_status(conn, status_key, True, ok_message.format(count=len(synced)), latest_ts(synced))
    conn.commit()


def sync_daily_bar_group(conn, status_key: str, producer: Callable[[], list], ok_message: str):
    errors: list[str] = []
    synced = []
    try:
        items = producer()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        set_failure_status(conn, status_key, str(exc))
        conn.commit()
        return

    for item in items:
        try:
            upsert_daily_bars(conn, item)
            conn.commit()
            synced.append(item)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            errors.append(f"{item.asset_key}: {exc}")

    if errors:
        set_status(conn, status_key, False, "; ".join(errors)[:900], latest_ts(synced))
    else:
        set_status(conn, status_key, True, ok_message.format(count=len(synced)), latest_ts(synced))
    conn.commit()


def sync_fred_series(conn, series_list, status_key: str):
    def produce():
        return [fred_numeric_series(series) for series in series_list]

    sync_numeric_group(conn, status_key, produce, "OK ({count} series)")


def sync_market(conn):
    sync_fred_series(conn, MARKET_SERIES, "fredgraph_market")


def sync_macro(conn):
    sync_fred_series(conn, MACRO_SERIES, "fredgraph_macro")


def sync_shipping(conn):
    sync_numeric_group(conn, "newyorkfed_shipping", lambda: [nyfed_shipping_series()], "OK ({count} series)")


def sync_akshare(conn):
    cleanup_retired_akshare_sources(conn)
    if not enabled("AKSHARE_ENABLED", "true"):
        set_status(conn, "akshare", True, "Disabled by AKSHARE_ENABLED=false")
        conn.commit()
        return
    if should_run(conn, "akshare_market_fund_flow", interval_minutes("AKSHARE_MARKET_FLOW_INTERVAL_MINUTES", 30)):
        sync_numeric_group(conn, "akshare_market_fund_flow", akshare_cn.collect_market_fund_flow, "OK ({count} series)")
    if should_run(conn, "akshare_financial_indicators", interval_minutes("AKSHARE_FINANCIAL_INTERVAL_MINUTES", 1440)):
        sync_text_group(conn, "akshare_financial_indicators", akshare_cn.collect_company_financial_indicators, "OK ({count} financial records)")
    if should_run(conn, "akshare_company_announcements", interval_minutes("AKSHARE_ANNOUNCEMENT_INTERVAL_MINUTES", 360)):
        sync_text_group(conn, "akshare_company_announcements", akshare_cn.collect_company_announcements, "OK ({count} announcements)")
    if should_run(conn, "akshare_company_news", interval_minutes("AKSHARE_NEWS_INTERVAL_MINUTES", 180)):
        sync_text_group(conn, "akshare_company_news", akshare_cn.collect_company_news, "OK ({count} news records)")


def sync_yahoo_finance(conn):
    if not enabled("YAHOO_FINANCE_ENABLED", "true"):
        set_status(conn, "yahoo_finance", True, "Disabled by YAHOO_FINANCE_ENABLED=false")
        conn.commit()
        return
    if should_run(conn, "yahoo_cn_indices", interval_minutes("YAHOO_CN_INDEX_INTERVAL_MINUTES", 360)):
        sync_numeric_group(conn, "yahoo_cn_indices", yahoo_finance.collect_cn_indices, "OK ({count} series)")
    if should_run(conn, "yahoo_cn_equity_daily", interval_minutes("YAHOO_CN_EQUITY_INTERVAL_MINUTES", 360)):
        sync_numeric_group(conn, "yahoo_cn_equity_daily", yahoo_finance.collect_cn_equity_daily, "OK ({count} series)")
    if should_run(conn, "yahoo_us_equity_daily", interval_minutes("YAHOO_US_EQUITY_INTERVAL_MINUTES", 360)):
        sync_numeric_group(conn, "yahoo_us_equity_daily", yahoo_finance.collect_us_equity_daily, "OK ({count} series)")
    if should_run(conn, "yahoo_daily_bars", interval_minutes("YAHOO_DAILY_BARS_INTERVAL_MINUTES", 360)):
        sync_daily_bar_group(
            conn,
            "yahoo_daily_bars",
            lambda: (
                yahoo_finance.collect_cn_index_daily_bars()
                + yahoo_finance.collect_cn_daily_bars()
                + yahoo_finance.collect_us_daily_bars()
            ),
            "OK ({count} assets)",
        )
    upsert_asset_aliases(conn, alias_rows())
    conn.commit()


def sync_sec(conn):
    if not enabled("SEC_EDGAR_ENABLED", "true"):
        set_status(conn, "sec_edgar", True, "Disabled by SEC_EDGAR_ENABLED=false")
        conn.commit()
        return
    if not should_run(conn, "sec_edgar", interval_minutes("SEC_EDGAR_INTERVAL_MINUTES", 360)):
        return
    try:
        numeric, text, errors = collect_sec_edgar()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        set_status(conn, "sec_edgar", False, str(exc)[:900])
        conn.commit()
        return

    synced = []
    write_errors: list[str] = []
    for item in numeric:
        try:
            upsert_numeric_series(conn, item)
            conn.commit()
            synced.append(item)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            write_errors.append(f"{item.asset_key}: {exc}")
    for item in text:
        try:
            upsert_text_record(conn, item)
            conn.commit()
            synced.append(item)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            write_errors.append(f"{item.record_id}: {exc}")

    all_errors = errors + write_errors
    if all_errors:
        set_status(conn, "sec_edgar", False, "; ".join(all_errors)[:900], latest_ts(synced))
    else:
        set_status(conn, "sec_edgar", True, f"OK ({len(numeric)} fact series, {len(text)} filings)", latest_ts(synced))
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
        sync_akshare(conn)
        sync_yahoo_finance(conn)
        sync_sec(conn)
        rebuild_evidence_items(conn)
        conn.commit()
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
