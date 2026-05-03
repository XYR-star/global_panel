#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

from providers import portfolio
from providers.store import connect_db, ensure_schema


POSITION_FIELDS = {
    "holding_amount": "持有金额",
    "today_pnl": "当日盈亏",
    "today_pnl_rate": "当日盈亏率",
    "related_sector": "关联板块",
    "sector_change_rate": "板块涨幅",
    "portfolio_pnl": "组合盈亏",
    "portfolio_return_rate": "组合涨幅",
    "holding_pnl": "持有盈亏",
    "holding_pnl_rate": "持有盈亏率",
    "cumulative_pnl": "累计盈亏",
    "cumulative_pnl_rate": "累计盈亏率",
    "weekly_pnl": "本周盈亏",
    "monthly_pnl": "本月盈亏",
    "yearly_pnl": "今年盈亏",
    "portfolio_weight": "仓位占比",
    "holding_quantity": "持有数量",
    "holding_days": "持仓天数",
    "latest_change_rate": "最新涨幅",
    "latest_price": "最新价",
    "unit_cost": "单位成本",
    "breakeven_change_rate": "回本涨幅",
    "return_1m": "近1月涨幅",
    "return_3m": "近3月涨幅",
    "return_6m": "近6月涨幅",
    "return_1y": "近1年涨幅",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import portfolio workbook and AkShare lookthrough data.")
    parser.add_argument("--excel", default="/root/portfolio_data/portfolio_2026-04-29.xlsx")
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--skip-akshare", action="store_true", help="Import workbook and computed fallbacks only.")
    return parser.parse_args()


def source_mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except FileNotFoundError:
        return None


def transaction_id(batch_id: str, row_number: int, row: dict[str, Any]) -> str:
    import hashlib

    parts = [
        batch_id,
        str(row_number),
        str(row.get("成交日期") or ""),
        str(row.get("成交时间") or ""),
        str(row.get("代码") or ""),
        str(row.get("交易类别") or ""),
        str(row.get("发生金额") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def insert_import_batch(conn, batch_id: str, excel_path: Path, as_of_date: date, workbook: portfolio.PortfolioWorkbook):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_import_batches
                (batch_id, source_file, source_file_mtime, as_of_date, ok, message, meta_json)
            VALUES (%s, %s, %s, %s, true, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE
            SET source_file = EXCLUDED.source_file,
                source_file_mtime = EXCLUDED.source_file_mtime,
                as_of_date = EXCLUDED.as_of_date,
                imported_at = NOW(),
                ok = true,
                message = EXCLUDED.message,
                meta_json = EXCLUDED.meta_json
            """,
            (
                batch_id,
                str(excel_path),
                source_mtime(excel_path),
                as_of_date,
                "import started",
                Json(
                    {
                        "summary": workbook.summary,
                        "position_rows": len(workbook.positions),
                        "closed_rows": len(workbook.closed_positions),
                        "transaction_rows": len(workbook.transactions),
                    }
                ),
            ),
        )


def clear_batch(conn, batch_id: str):
    tables = (
        "portfolio_daily_exposure",
        "portfolio_daily_allocation",
        "portfolio_daily_summary",
        "portfolio_risk_metrics",
        "portfolio_asset_allocation",
        "portfolio_industry_allocations",
        "portfolio_underlying_holdings",
        "portfolio_fund_metadata",
        "portfolio_transactions",
        "portfolio_closed_positions",
        "portfolio_positions",
    )
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"DELETE FROM {table} WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM portfolio_fund_manager_history WHERE batch_id = %s", (batch_id,))


def insert_positions(conn, batch_id: str, as_of_date: date, positions: list[dict[str, Any]]):
    columns = [
        "batch_id",
        "as_of_date",
        "security_code",
        "security_name",
        "security_type",
        "market",
        *POSITION_FIELDS.keys(),
        "raw_json",
    ]
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO portfolio_positions ({', '.join(columns)}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        for row in positions:
            values: list[Any] = [
                batch_id,
                as_of_date,
                str(row.get("代码")).zfill(6),
                row.get("名称") or "",
                row["security_type"],
                row.get("market"),
            ]
            for field, source in POSITION_FIELDS.items():
                if field == "holding_days":
                    values.append(portfolio.to_int(row.get(source)))
                elif field in {"related_sector"}:
                    values.append(row.get(source))
                elif field in {
                    "today_pnl_rate",
                    "sector_change_rate",
                    "portfolio_return_rate",
                    "holding_pnl_rate",
                    "cumulative_pnl_rate",
                    "portfolio_weight",
                    "latest_change_rate",
                    "breakeven_change_rate",
                    "return_1m",
                    "return_3m",
                    "return_6m",
                    "return_1y",
                }:
                    values.append(portfolio.to_float(row.get(source)))
                else:
                    values.append(portfolio.to_decimal(row.get(source)))
            values.append(Json(portfolio.raw_json_safe(row)))
            cur.execute(sql, values)


def insert_closed_positions(conn, batch_id: str, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            close_date = portfolio.to_date(row.get("清仓日期"))
            code = row.get("代码")
            name = row.get("名称")
            if close_date is None or not code or not name:
                continue
            cur.execute(
                """
                INSERT INTO portfolio_closed_positions
                    (batch_id, close_date, security_code, security_name, total_pnl, pnl_rate,
                     benchmark_return, excess_return, buy_avg_price, sell_avg_price,
                     days_since_close, holding_days, fees, open_date, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    close_date,
                    str(code).zfill(6),
                    name,
                    portfolio.to_decimal(row.get("总盈亏")),
                    portfolio.to_float(row.get("盈亏比")),
                    portfolio.to_float(row.get("同期大盘")),
                    portfolio.to_float(row.get("跑赢大盘")),
                    portfolio.to_decimal(row.get("买入均价")),
                    portfolio.to_decimal(row.get("卖出均价")),
                    portfolio.to_float(row.get("清仓距今")),
                    portfolio.to_int(row.get("持仓天数")),
                    portfolio.to_decimal(row.get("交易费用")),
                    portfolio.to_date(row.get("建仓日期")),
                    Json(portfolio.raw_json_safe(row)),
                ),
            )


def insert_transactions(conn, batch_id: str, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for index, row in enumerate(rows, start=2):
            trade_date = portfolio.to_date(row.get("成交日期"))
            tx_type = row.get("交易类别")
            if trade_date is None or not tx_type:
                continue
            code = row.get("代码")
            cur.execute(
                """
                INSERT INTO portfolio_transactions
                    (batch_id, transaction_id, trade_date, trade_time, security_code, security_name,
                     transaction_type, quantity, price, cash_flow_amount, gross_amount, fee, remark, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    transaction_id(batch_id, index, row),
                    trade_date,
                    portfolio.to_time(row.get("成交时间")),
                    str(code).zfill(6) if code else None,
                    row.get("名称"),
                    tx_type,
                    portfolio.to_decimal(row.get("成交数量")),
                    portfolio.to_decimal(row.get("成交价格")),
                    portfolio.to_decimal(row.get("发生金额")),
                    portfolio.to_decimal(row.get("成交金额")),
                    portfolio.to_decimal(row.get("费用")),
                    row.get("备注"),
                    Json(portfolio.raw_json_safe(row)),
                ),
            )


def insert_fund_metadata(conn, batch_id: str, as_of_date: date, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO portfolio_fund_metadata
                    (batch_id, as_of_date, fund_code, fund_name, fund_type, manager_name, fund_size,
                     size_unit, management_fee, custody_fee, sales_service_fee, purchase_fee,
                     redemption_fee, inception_date, benchmark, return_1m, return_3m, return_6m,
                     return_1y, max_drawdown_1y, nav_latest, nav_date, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    as_of_date,
                    row["fund_code"],
                    row["fund_name"],
                    row.get("fund_type"),
                    row.get("manager_name"),
                    row.get("fund_size"),
                    row.get("size_unit"),
                    row.get("management_fee"),
                    row.get("custody_fee"),
                    row.get("sales_service_fee"),
                    row.get("purchase_fee"),
                    row.get("redemption_fee"),
                    row.get("inception_date"),
                    row.get("benchmark"),
                    row.get("return_1m"),
                    row.get("return_3m"),
                    row.get("return_6m"),
                    row.get("return_1y"),
                    row.get("max_drawdown_1y"),
                    row.get("nav_latest"),
                    row.get("nav_date"),
                    row.get("source", "akshare"),
                    Json(row.get("raw_json") or {}),
                ),
            )
            cur.execute(
                """
                INSERT INTO portfolio_fund_manager_history
                    (fund_code, fund_name, as_of_date, manager_name, batch_id, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["fund_code"],
                    row["fund_name"],
                    as_of_date,
                    row.get("manager_name"),
                    batch_id,
                    row.get("source", "akshare"),
                    Json({"manager_changed": row.get("raw_json", {}).get("manager_changed")}),
                ),
            )


def previous_manager(conn, fund_code: str, batch_id: str, as_of_date: date) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT manager_name
            FROM portfolio_fund_manager_history
            WHERE fund_code = %s
              AND batch_id <> %s
              AND as_of_date <= %s
            ORDER BY as_of_date DESC, batch_id DESC
            LIMIT 1
            """,
            (fund_code, batch_id, as_of_date),
        )
        row = cur.fetchone()
    return row[0] if row else None


def insert_underlying(conn, batch_id: str, as_of_date: date, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO portfolio_underlying_holdings
                    (batch_id, as_of_date, parent_code, parent_name, parent_type, underlying_code,
                     underlying_name, underlying_type, report_period, holding_rank, holding_weight_in_parent,
                     parent_portfolio_weight, lookthrough_portfolio_weight, lookthrough_amount,
                     shares, market_value, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, parent_code, underlying_type, underlying_code, report_period)
                DO UPDATE SET
                    holding_rank = EXCLUDED.holding_rank,
                    holding_weight_in_parent = EXCLUDED.holding_weight_in_parent,
                    parent_portfolio_weight = EXCLUDED.parent_portfolio_weight,
                    lookthrough_portfolio_weight = EXCLUDED.lookthrough_portfolio_weight,
                    lookthrough_amount = EXCLUDED.lookthrough_amount,
                    shares = EXCLUDED.shares,
                    market_value = EXCLUDED.market_value,
                    source = EXCLUDED.source,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (
                    batch_id,
                    as_of_date,
                    row["parent_code"],
                    row["parent_name"],
                    row["parent_type"],
                    row["underlying_code"],
                    row["underlying_name"],
                    row["underlying_type"],
                    row.get("report_period") or "",
                    row.get("holding_rank"),
                    row.get("holding_weight_in_parent"),
                    row.get("parent_portfolio_weight"),
                    row.get("lookthrough_portfolio_weight"),
                    row.get("lookthrough_amount"),
                    row.get("shares"),
                    row.get("market_value"),
                    row.get("source", "akshare"),
                    Json(row.get("raw_json") or {}),
                ),
            )


def insert_industries(conn, batch_id: str, as_of_date: date, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO portfolio_industry_allocations
                    (batch_id, as_of_date, parent_code, parent_name, industry_name, report_period,
                     weight_in_parent, parent_portfolio_weight, lookthrough_portfolio_weight, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, parent_code, industry_name, report_period)
                DO UPDATE SET
                    weight_in_parent = EXCLUDED.weight_in_parent,
                    parent_portfolio_weight = EXCLUDED.parent_portfolio_weight,
                    lookthrough_portfolio_weight = EXCLUDED.lookthrough_portfolio_weight,
                    source = EXCLUDED.source,
                    raw_json = EXCLUDED.raw_json
                """,
                (
                    batch_id,
                    as_of_date,
                    row["parent_code"],
                    row["parent_name"],
                    row["industry_name"],
                    row.get("report_period") or "",
                    row.get("weight_in_parent"),
                    row.get("parent_portfolio_weight"),
                    row.get("lookthrough_portfolio_weight"),
                    row.get("source", "akshare"),
                    Json(row.get("raw_json") or {}),
                ),
            )


def insert_allocations(conn, batch_id: str, as_of_date: date, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO portfolio_asset_allocation
                    (batch_id, as_of_date, allocation_bucket, amount, weight, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    as_of_date,
                    row["allocation_bucket"],
                    row["amount"],
                    row["weight"],
                    row["source"],
                    Json(row.get("raw_json") or {}),
                ),
            )


def insert_risk_metrics(conn, batch_id: str, as_of_date: date, rows: list[dict[str, Any]]):
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO portfolio_risk_metrics
                    (batch_id, as_of_date, metric_scope, subject_code, subject_name, metric_name,
                     metric_value, metric_unit, source, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    as_of_date,
                    row["metric_scope"],
                    row["subject_code"],
                    row["subject_name"],
                    row["metric_name"],
                    row.get("metric_value"),
                    row.get("metric_unit"),
                    row["source"],
                    Json(row.get("raw_json") or {}),
                ),
            )



def _metric_map(conn, batch_id: str) -> dict[str, float | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metric_name, metric_value
            FROM portfolio_risk_metrics
            WHERE batch_id = %s AND metric_scope = 'portfolio'
            """,
            (batch_id,),
        )
        return {name: value for name, value in cur.fetchall()}


def _first_row(conn, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return None
        columns = [col.name for col in cur.description]
        row = cur.fetchone()
        return dict(zip(columns, row)) if row else None


def _many_rows(conn, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [col.name for col in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def rebuild_batch_summary(conn, batch_id: str) -> None:
    batch = _first_row(
        conn,
        """
        SELECT batch_id, as_of_date, uploaded_at, original_filename, status, total_assets, position_count
        FROM portfolio_import_batches
        WHERE batch_id = %s
        """,
        (batch_id,),
    )
    if not batch:
        raise ValueError(f"Batch not found: {batch_id}")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM portfolio_daily_exposure WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM portfolio_daily_allocation WHERE batch_id = %s", (batch_id,))
        cur.execute("DELETE FROM portfolio_daily_summary WHERE batch_id = %s", (batch_id,))

    if batch["status"] not in {"complete", "partial"}:
        return

    pnl = _first_row(
        conn,
        """
        SELECT COALESCE(SUM(today_pnl), 0) AS today_pnl,
               COALESCE(SUM(holding_pnl), 0) AS holding_pnl,
               COALESCE(SUM(cumulative_pnl), 0) AS cumulative_pnl,
               MAX(portfolio_weight) AS max_position_weight
        FROM portfolio_positions
        WHERE batch_id = %s
        """,
        (batch_id,),
    ) or {}
    metrics = _metric_map(conn, batch_id)
    industry = _first_row(
        conn,
        """
        SELECT industry_name, SUM(lookthrough_portfolio_weight) AS weight
        FROM portfolio_industry_allocations
        WHERE batch_id = %s
        GROUP BY industry_name
        ORDER BY weight DESC NULLS LAST
        LIMIT 1
        """,
        (batch_id,),
    )
    if not industry:
        industry = _first_row(
            conn,
            """
            SELECT COALESCE(NULLIF(related_sector, ''), security_type, 'unknown') AS industry_name,
                   SUM(COALESCE(portfolio_weight, 0)) AS weight
            FROM portfolio_positions
            WHERE batch_id = %s
            GROUP BY COALESCE(NULLIF(related_sector, ''), security_type, 'unknown')
            ORDER BY weight DESC
            LIMIT 1
            """,
            (batch_id,),
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_daily_summary
                (batch_id, as_of_date, uploaded_at, original_filename, status, total_assets,
                 position_count, today_pnl, holding_pnl, cumulative_pnl, max_position_weight,
                 top3_weight, top5_weight, top10_underlying_weight, equity_like_weight,
                 bond_like_weight, qdii_weight, cash_weight, max_industry_name, max_industry_weight)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                uploaded_at = EXCLUDED.uploaded_at,
                original_filename = EXCLUDED.original_filename,
                status = EXCLUDED.status,
                total_assets = EXCLUDED.total_assets,
                position_count = EXCLUDED.position_count,
                today_pnl = EXCLUDED.today_pnl,
                holding_pnl = EXCLUDED.holding_pnl,
                cumulative_pnl = EXCLUDED.cumulative_pnl,
                max_position_weight = EXCLUDED.max_position_weight,
                top3_weight = EXCLUDED.top3_weight,
                top5_weight = EXCLUDED.top5_weight,
                top10_underlying_weight = EXCLUDED.top10_underlying_weight,
                equity_like_weight = EXCLUDED.equity_like_weight,
                bond_like_weight = EXCLUDED.bond_like_weight,
                qdii_weight = EXCLUDED.qdii_weight,
                cash_weight = EXCLUDED.cash_weight,
                max_industry_name = EXCLUDED.max_industry_name,
                max_industry_weight = EXCLUDED.max_industry_weight,
                updated_at = NOW()
            """,
            (
                batch_id,
                batch["as_of_date"],
                batch["uploaded_at"],
                batch["original_filename"],
                batch["status"],
                batch["total_assets"],
                batch["position_count"],
                pnl.get("today_pnl"),
                pnl.get("holding_pnl"),
                pnl.get("cumulative_pnl"),
                pnl.get("max_position_weight"),
                metrics.get("top3_position_weight"),
                metrics.get("top5_position_weight"),
                metrics.get("top10_underlying_weight"),
                metrics.get("equity_like_weight"),
                metrics.get("bond_like_weight"),
                metrics.get("qdii_weight"),
                metrics.get("cash_weight_estimated"),
                industry.get("industry_name") if industry else None,
                industry.get("weight") if industry else None,
            ),
        )

        cur.execute(
            """
            INSERT INTO portfolio_daily_allocation
                (batch_id, as_of_date, allocation_bucket, amount, weight, source)
            SELECT batch_id, as_of_date, allocation_bucket, amount, weight, source
            FROM portfolio_asset_allocation
            WHERE batch_id = %s
            ON CONFLICT (batch_id, allocation_bucket) DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                amount = EXCLUDED.amount,
                weight = EXCLUDED.weight,
                source = EXCLUDED.source,
                updated_at = NOW()
            """,
            (batch_id,),
        )

        cur.execute(
            """
            INSERT INTO portfolio_daily_exposure
                (batch_id, as_of_date, exposure_type, exposure_code, exposure_name, amount, weight, source_count, contributors, source)
            SELECT batch_id,
                   MIN(as_of_date) AS as_of_date,
                   'underlying' AS exposure_type,
                   underlying_code AS exposure_code,
                   underlying_name AS exposure_name,
                   SUM(lookthrough_amount) AS amount,
                   SUM(lookthrough_portfolio_weight) AS weight,
                   COUNT(DISTINCT parent_code) AS source_count,
                   jsonb_agg(jsonb_build_object('parent_code', parent_code, 'parent_name', parent_name, 'weight', lookthrough_portfolio_weight)
                             ORDER BY lookthrough_portfolio_weight DESC) AS contributors,
                   'computed' AS source
            FROM portfolio_underlying_holdings
            WHERE batch_id = %s
            GROUP BY batch_id, underlying_code, underlying_name
            ON CONFLICT (batch_id, exposure_type, exposure_code, exposure_name) DO UPDATE SET
                amount = EXCLUDED.amount,
                weight = EXCLUDED.weight,
                source_count = EXCLUDED.source_count,
                contributors = EXCLUDED.contributors,
                updated_at = NOW()
            """,
            (batch_id,),
        )

        cur.execute(
            """
            INSERT INTO portfolio_daily_exposure
                (batch_id, as_of_date, exposure_type, exposure_code, exposure_name, amount, weight, source_count, contributors, source)
            SELECT batch_id,
                   MIN(as_of_date) AS as_of_date,
                   'industry' AS exposure_type,
                   '' AS exposure_code,
                   industry_name AS exposure_name,
                   NULL AS amount,
                   SUM(lookthrough_portfolio_weight) AS weight,
                   COUNT(DISTINCT parent_code) AS source_count,
                   jsonb_agg(jsonb_build_object('parent_code', parent_code, 'parent_name', parent_name, 'weight', lookthrough_portfolio_weight)
                             ORDER BY lookthrough_portfolio_weight DESC) AS contributors,
                   'computed' AS source
            FROM portfolio_industry_allocations
            WHERE batch_id = %s
            GROUP BY batch_id, industry_name
            ON CONFLICT (batch_id, exposure_type, exposure_code, exposure_name) DO UPDATE SET
                weight = EXCLUDED.weight,
                source_count = EXCLUDED.source_count,
                contributors = EXCLUDED.contributors,
                updated_at = NOW()
            """,
            (batch_id,),
        )

        cur.execute(
            """
            INSERT INTO portfolio_daily_exposure
                (batch_id, as_of_date, exposure_type, exposure_code, exposure_name, amount, weight, source_count, contributors, source)
            SELECT batch_id,
                   MIN(as_of_date) AS as_of_date,
                   'industry' AS exposure_type,
                   '' AS exposure_code,
                   COALESCE(NULLIF(related_sector, ''), security_type, 'unknown') AS exposure_name,
                   NULL AS amount,
                   SUM(COALESCE(portfolio_weight, 0)) AS weight,
                   COUNT(*) AS source_count,
                   jsonb_agg(jsonb_build_object('parent_code', security_code, 'parent_name', security_name, 'weight', portfolio_weight)
                             ORDER BY portfolio_weight DESC) AS contributors,
                   'fallback' AS source
            FROM portfolio_positions p
            WHERE batch_id = %s
              AND NOT EXISTS (SELECT 1 FROM portfolio_daily_exposure e WHERE e.batch_id = p.batch_id AND e.exposure_type = 'industry')
            GROUP BY batch_id, COALESCE(NULLIF(related_sector, ''), security_type, 'unknown')
            ON CONFLICT (batch_id, exposure_type, exposure_code, exposure_name) DO UPDATE SET
                weight = EXCLUDED.weight,
                source_count = EXCLUDED.source_count,
                contributors = EXCLUDED.contributors,
                source = EXCLUDED.source,
                updated_at = NOW()
            """,
            (batch_id,),
        )


def refresh_summary_derived_fields(conn) -> None:
    items = _many_rows(
        conn,
        """
        SELECT s.batch_id, s.total_assets, s.today_pnl,
               LAG(s.total_assets) OVER (ORDER BY s.as_of_date, s.uploaded_at, s.batch_id) AS previous_assets
        FROM portfolio_daily_summary s
        JOIN portfolio_import_batches b ON b.batch_id = s.batch_id
        WHERE b.status IN ('complete', 'partial')
        ORDER BY s.as_of_date, s.uploaded_at, s.batch_id
        """,
    )
    running_peak: float | None = None
    trailing_loss_streak = 0
    with conn.cursor() as cur:
        for item in items:
            total_assets = float(item["total_assets"] or 0)
            running_peak = total_assets if running_peak is None else max(running_peak, total_assets)
            drawdown = (total_assets / running_peak - 1) if running_peak else 0
            if float(item["today_pnl"] or 0) < 0:
                trailing_loss_streak += 1
            else:
                trailing_loss_streak = 0
            previous_assets = item.get("previous_assets")
            total_assets_change = None if previous_assets is None else total_assets - float(previous_assets or 0)
            cur.execute(
                """
                UPDATE portfolio_daily_summary
                SET total_assets_change = %s,
                    drawdown_estimated = %s,
                    trailing_loss_streak = %s,
                    updated_at = NOW()
                WHERE batch_id = %s
                """,
                (total_assets_change, drawdown, trailing_loss_streak, item["batch_id"]),
            )


def rebuild_all_summaries(conn) -> int:
    batch_ids = [
        row["batch_id"]
        for row in _many_rows(
            conn,
            """
            SELECT batch_id
            FROM portfolio_import_batches
            WHERE status IN ('complete', 'partial')
            ORDER BY as_of_date, uploaded_at, batch_id
            """,
        )
    ]
    for batch_id in batch_ids:
        rebuild_batch_summary(conn, batch_id)
    refresh_summary_derived_fields(conn)
    return len(batch_ids)

def enrich_positions(positions: list[dict[str, Any]], catalog: set[str]) -> list[dict[str, Any]]:
    enriched = []
    for row in positions:
        code = str(row.get("代码")).zfill(6)
        security_type, market = portfolio.classify_security(code, row.get("名称") or "", catalog)
        enriched.append(row | {"代码": code, "security_type": security_type, "market": market})
    return enriched


def collect_market_data(conn, batch_id: str, as_of_date: date, positions: list[dict[str, Any]], skip_akshare: bool):
    metadata_rows: list[dict[str, Any]] = []
    underlying_rows: list[dict[str, Any]] = []
    industry_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    years = [str(as_of_date.year), str(as_of_date.year - 1)]
    start_date = (as_of_date - timedelta(days=365)).strftime("%Y%m%d")
    end_date = as_of_date.strftime("%Y%m%d")

    total = len(positions)
    idx = 1
    for position in positions:
        code = str(position.get("代码")).zfill(6)
        name = position.get("名称") or ""
        security_type = position["security_type"]
        print(f"  [{idx}/{total}] {code} {name} ({security_type})...")
        idx += 1
        if security_type == "stock_a":
            underlying_rows.append(portfolio.build_direct_or_fallback_underlying(position, "stock", "direct_stock"))
            history = portfolio.fetch_price_history(code, security_type, start_date, end_date) if not skip_akshare else None
            risk = portfolio.compute_returns_and_drawdown(history) if history is not None else {}
            if risk:
                metadata_rows.append({"fund_code": code, "fund_name": name, "source": "akshare_price_proxy", "raw_json": risk, **risk})
            continue

        if security_type == "etf_listed":
            history = portfolio.fetch_price_history(code, security_type, start_date, end_date) if not skip_akshare else None
            risk = portfolio.compute_returns_and_drawdown(history) if history is not None else {}
            fallback = portfolio.build_direct_or_fallback_underlying(position, "etf_proxy", "fallback_parent")
            fallback["raw_json"] |= risk
            underlying_rows.append(fallback)
            metadata_rows.append({"fund_code": code, "fund_name": name, "source": "akshare_price_proxy", "raw_json": risk, **risk})
            continue

        if security_type in {"bond_fund", "fund_linked", "equity_fund", "qdii_fund", "other_fund"}:
            meta = {"raw_json": {"error": "akshare skipped"}} if skip_akshare else portfolio.fetch_fund_metadata(code)
            nav_risk = {} if skip_akshare else portfolio.compute_returns_and_drawdown(portfolio.fetch_open_fund_nav(code))
            raw = portfolio.raw_json_safe(meta.get("raw_json") or {})
            raw.update({f"ak_{key}": portfolio.json_safe(value) for key, value in nav_risk.items()})
            previous = previous_manager(conn, code, batch_id, as_of_date)
            manager = meta.get("manager_name")
            raw["manager_changed"] = previous is not None and manager is not None and previous != manager
            meta_row = {
                "fund_code": code,
                "fund_name": name,
                **meta,
                **nav_risk,
                "return_1m": portfolio.to_float(position.get("近1月涨幅")) or nav_risk.get("return_1m"),
                "return_3m": portfolio.to_float(position.get("近3月涨幅")) or nav_risk.get("return_3m"),
                "return_6m": portfolio.to_float(position.get("近6月涨幅")) or nav_risk.get("return_6m"),
                "return_1y": portfolio.to_float(position.get("近1年涨幅")) or nav_risk.get("return_1y"),
                "raw_json": raw,
                "source": "excel" if skip_akshare else "akshare",
            }
            metadata_rows.append(meta_row)

            stock_holdings = [] if skip_akshare else portfolio.fetch_fund_stock_holdings(code, years)
            bond_holdings = [] if skip_akshare else portfolio.fetch_fund_bond_holdings(code, years)
            if stock_holdings and stock_holdings[0].get("raw_json", {}).get("error"):
                errors.append(f"{code} stock holdings: {stock_holdings[0]['raw_json']['error']}")
            if bond_holdings and bond_holdings[0].get("raw_json", {}).get("error"):
                errors.append(f"{code} bond holdings: {bond_holdings[0]['raw_json']['error']}")
            holdings = [
                row
                for row in stock_holdings + bond_holdings
                if not row.get("raw_json", {}).get("error")
            ]
            enriched = portfolio.enrich_fund_underlying(position, holdings)
            if enriched:
                coverage = sum(row.get("holding_weight_in_parent") or 0.0 for row in enriched)
                for row in enriched:
                    row["raw_json"] = (row.get("raw_json") or {}) | {"coverage_ratio": coverage}
                underlying_rows.extend(enriched)
            else:
                underlying_rows.append(portfolio.build_direct_or_fallback_underlying(position, "fund_proxy", "fallback_parent"))

            industries = [] if skip_akshare else portfolio.fetch_fund_industry_allocation(code, years)
            for item in industries:
                if item.get("raw_json", {}).get("error"):
                    errors.append(f"{code} industry: {item['raw_json']['error']}")
                    continue
                weight = item.get("weight_in_parent")
                parent_weight = portfolio.to_float(position.get("仓位占比")) or 0.0
                industry_rows.append(
                    {
                        "parent_code": code,
                        "parent_name": name,
                        "industry_name": item["industry_name"],
                        "report_period": item.get("report_period") or "",
                        "weight_in_parent": weight,
                        "parent_portfolio_weight": parent_weight,
                        "lookthrough_portfolio_weight": parent_weight * weight if weight is not None else None,
                        "source": "akshare",
                        "raw_json": item.get("raw_json") or {},
                    }
                )

    return metadata_rows, underlying_rows, industry_rows, errors


def finalize_batch(conn, batch_id: str, ok: bool, message: str, errors: list[str]):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE portfolio_import_batches
            SET ok = %s,
                message = %s,
                meta_json = meta_json || %s::jsonb
            WHERE batch_id = %s
            """,
            (ok, message[:900], Json({"errors": errors}), batch_id),
        )


def main() -> int:
    args = parse_args()
    excel_path = Path(args.excel)
    as_of_date = date.fromisoformat(args.as_of_date)
    if not excel_path.exists():
        print(f"Excel file does not exist: {excel_path}", file=sys.stderr)
        return 1

    try:
        workbook = portfolio.read_portfolio_workbook(excel_path, as_of_date)
    except Exception as exc:  # noqa: BLE001
        print(f"Workbook read failed: {exc}", file=sys.stderr)
        return 1

    batch_id = portfolio.make_batch_id(excel_path, as_of_date)
    conn = None
    try:
        conn = connect_db()
        ensure_schema(conn)
        catalog = set() if args.skip_akshare else portfolio.fund_catalog()
        positions = enrich_positions(workbook.positions, catalog)

        clear_batch(conn, batch_id)
        insert_import_batch(conn, batch_id, excel_path, as_of_date, workbook)
        insert_positions(conn, batch_id, as_of_date, positions)
        insert_closed_positions(conn, batch_id, workbook.closed_positions)
        insert_transactions(conn, batch_id, workbook.transactions)

        metadata_rows, underlying_rows, industry_rows, errors = collect_market_data(
            conn, batch_id, as_of_date, positions, args.skip_akshare
        )
        insert_fund_metadata(conn, batch_id, as_of_date, metadata_rows)
        insert_underlying(conn, batch_id, as_of_date, underlying_rows)
        insert_industries(conn, batch_id, as_of_date, industry_rows)

        allocations = portfolio.compute_portfolio_allocations(positions)
        risk_metrics = portfolio.compute_risk_metrics(positions, underlying_rows, metadata_rows)
        insert_allocations(conn, batch_id, as_of_date, allocations)
        insert_risk_metrics(conn, batch_id, as_of_date, risk_metrics)
        rebuild_batch_summary(conn, batch_id)
        refresh_summary_derived_fields(conn)

        summary_amount = portfolio.to_decimal(workbook.summary.get("持有金额"))
        position_amount = sum((portfolio.to_decimal(row.get("持有金额")) or 0 for row in positions), 0)
        if summary_amount is not None and abs(float(summary_amount - position_amount)) > 1:
            errors.append(f"position amount {position_amount} differs from summary {summary_amount}")

        finalize_batch(conn, batch_id, not errors, f"OK ({len(positions)} positions, {len(underlying_rows)} underlying rows)", errors)
        conn.commit()
        print(f"Imported {batch_id}: {len(positions)} positions, {len(underlying_rows)} underlying rows, {len(errors)} warnings")
        return 0
    except Exception as exc:  # noqa: BLE001
        if conn is not None:
            conn.rollback()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO portfolio_import_batches
                            (batch_id, source_file, source_file_mtime, as_of_date, ok, message, meta_json)
                        VALUES (%s, %s, %s, %s, false, %s, %s)
                        ON CONFLICT (batch_id) DO UPDATE
                        SET ok = false, message = EXCLUDED.message, meta_json = EXCLUDED.meta_json
                        """,
                        (
                            batch_id,
                            str(excel_path),
                            source_mtime(excel_path),
                            as_of_date,
                            str(exc)[:900],
                            Json({"error": str(exc)}),
                        ),
                    )
                conn.commit()
            except Exception:  # noqa: BLE001
                conn.rollback()
        print(f"Portfolio sync failed: {type(exc).__name__}: {exc!r}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
