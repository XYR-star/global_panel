from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import secrets
import shutil
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from psycopg2.extras import Json, RealDictCursor

from providers import ai_insights, portfolio
from providers import events as event_provider
from providers.store import connect_db, ensure_schema
from sync_portfolio_data import (
    clear_batch,
    collect_market_data,
    enrich_positions,
    insert_allocations,
    insert_closed_positions,
    insert_fund_metadata,
    insert_industries,
    insert_positions,
    insert_risk_metrics,
    insert_transactions,
    insert_underlying,
    rebuild_all_summaries,
    rebuild_batch_summary,
    refresh_summary_derived_fields,
)

APP_TITLE = "Ricky Portfolio"
COOKIE_NAME = "portfolio_session"
MAX_UPLOAD_BYTES = int(os.getenv("PORTFOLIO_MAX_UPLOAD_BYTES", str(25 * 1024 * 1024)))
UPLOAD_ROOT = Path(os.getenv("PORTFOLIO_UPLOAD_ROOT", "/var/lib/portfolio-app/uploads"))
ADMIN_USERNAME = os.getenv("PORTFOLIO_ADMIN_USERNAME") or os.getenv("GRAFANA_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("PORTFOLIO_ADMIN_PASSWORD") or os.getenv("GRAFANA_ADMIN_PASSWORD", "")
SESSION_SECRET = (os.getenv("PORTFOLIO_SESSION_SECRET") or os.getenv("POSTGRES_PASSWORD") or "change-me").encode()
AKSHARE_ENABLED = os.getenv("PORTFOLIO_AKSHARE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
LOGIN_FAILURE_LIMIT = int(os.getenv("PORTFOLIO_LOGIN_FAILURE_LIMIT", "5"))
LOGIN_LOCKOUT_MINUTES = int(os.getenv("PORTFOLIO_LOGIN_LOCKOUT_MINUTES", "30"))
BACKUP_ROOT = Path(os.getenv("PORTFOLIO_BACKUP_ROOT", "/var/lib/portfolio-app/backups"))

app = FastAPI(title=APP_TITLE)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def db_conn():
    conn = connect_db()
    try:
        yield conn
    finally:
        conn.close()


def rows(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    result = rows(sql, params)
    return result[0] if result else None


def execute(sql: str, params: tuple[Any, ...] = ()) -> None:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()


def ensure_portfolio_schema() -> None:
    with db_conn() as conn:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE portfolio_import_batches
                    ADD COLUMN IF NOT EXISTS uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS original_filename TEXT,
                    ADD COLUMN IF NOT EXISTS file_sha256 TEXT,
                    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'complete',
                    ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS total_assets NUMERIC(20, 6),
                    ADD COLUMN IF NOT EXISTS position_count INTEGER NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS report_markdown TEXT NOT NULL DEFAULT '';

                CREATE INDEX IF NOT EXISTS idx_portfolio_batches_uploaded
                    ON portfolio_import_batches (uploaded_at DESC);
                CREATE INDEX IF NOT EXISTS idx_portfolio_batches_status_archived
                    ON portfolio_import_batches (status, is_archived, uploaded_at DESC);
                CREATE INDEX IF NOT EXISTS idx_portfolio_batches_sha
                    ON portfolio_import_batches (file_sha256);

                CREATE INDEX IF NOT EXISTS idx_portfolio_batches_asof_uploaded
                    ON portfolio_import_batches (as_of_date DESC, uploaded_at DESC, batch_id DESC)
                    WHERE status IN ('complete', 'partial');
                CREATE INDEX IF NOT EXISTS idx_portfolio_positions_security_asof
                    ON portfolio_positions (security_code, as_of_date DESC);
                CREATE INDEX IF NOT EXISTS idx_portfolio_transactions_batch_date
                    ON portfolio_transactions (batch_id, trade_date DESC);
                CREATE INDEX IF NOT EXISTS idx_portfolio_asset_allocation_bucket_asof
                    ON portfolio_asset_allocation (allocation_bucket, as_of_date);

                CREATE TABLE IF NOT EXISTS portfolio_daily_summary (
                    batch_id TEXT PRIMARY KEY REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
                    as_of_date DATE NOT NULL,
                    uploaded_at TIMESTAMPTZ NOT NULL,
                    original_filename TEXT,
                    status TEXT NOT NULL,
                    total_assets NUMERIC(20, 6),
                    total_assets_change NUMERIC(20, 6),
                    position_count INTEGER NOT NULL DEFAULT 0,
                    today_pnl NUMERIC(20, 6),
                    holding_pnl NUMERIC(20, 6),
                    cumulative_pnl NUMERIC(20, 6),
                    max_position_weight DOUBLE PRECISION,
                    top3_weight DOUBLE PRECISION,
                    top5_weight DOUBLE PRECISION,
                    top10_underlying_weight DOUBLE PRECISION,
                    equity_like_weight DOUBLE PRECISION,
                    bond_like_weight DOUBLE PRECISION,
                    qdii_weight DOUBLE PRECISION,
                    cash_weight DOUBLE PRECISION,
                    max_industry_name TEXT,
                    max_industry_weight DOUBLE PRECISION,
                    drawdown_estimated DOUBLE PRECISION,
                    trailing_loss_streak INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT 'computed',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_daily_summary_asof_uploaded
                    ON portfolio_daily_summary (as_of_date DESC, uploaded_at DESC, batch_id DESC);
                CREATE TABLE IF NOT EXISTS portfolio_daily_allocation (
                    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
                    as_of_date DATE NOT NULL,
                    allocation_bucket TEXT NOT NULL,
                    amount NUMERIC(20, 6) NOT NULL DEFAULT 0,
                    weight DOUBLE PRECISION NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT 'computed',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (batch_id, allocation_bucket)
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_daily_allocation_bucket_asof
                    ON portfolio_daily_allocation (allocation_bucket, as_of_date);
                CREATE TABLE IF NOT EXISTS portfolio_daily_exposure (
                    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
                    as_of_date DATE NOT NULL,
                    exposure_type TEXT NOT NULL,
                    exposure_code TEXT NOT NULL DEFAULT '',
                    exposure_name TEXT NOT NULL,
                    amount NUMERIC(20, 6),
                    weight DOUBLE PRECISION,
                    source_count INTEGER NOT NULL DEFAULT 1,
                    contributors JSONB NOT NULL DEFAULT '[]'::jsonb,
                    source TEXT NOT NULL DEFAULT 'computed',
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (batch_id, exposure_type, exposure_code, exposure_name)
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_daily_exposure_type_weight
                    ON portfolio_daily_exposure (batch_id, exposure_type, weight DESC);

                CREATE TABLE IF NOT EXISTS portfolio_login_failures (
                    failure_key TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    client_ip TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    locked_until TIMESTAMPTZ,
                    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_portfolio_login_failures_locked
                    ON portfolio_login_failures (locked_until);
                """
            )
        conn.commit()


@app.on_event("startup")
def startup() -> None:
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    ensure_portfolio_schema()
    with db_conn() as conn:
        event_provider.ensure_event_defaults(conn)


def sign(value: str) -> str:
    return hmac.new(SESSION_SECRET, value.encode(), hashlib.sha256).hexdigest()


def make_session(username: str) -> str:
    payload = json.dumps(
        {"u": username, "t": int(now_utc().timestamp()), "n": secrets.token_hex(8)},
        separators=(",", ":"),
    )
    return f"{payload}.{sign(payload)}"


def session_user(request: Request) -> str | None:
    token = request.cookies.get(COOKIE_NAME)
    if not token or "." not in token:
        return None
    payload, sig = token.rsplit(".", 1)
    if not hmac.compare_digest(sign(payload), sig):
        return None
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return str(data.get("u")) if data.get("u") == ADMIN_USERNAME else None


def require_user(request: Request) -> str:
    user = session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


def login_failure_key(username: str, ip: str) -> str:
    return hashlib.sha256(f"{username}|{ip}".encode()).hexdigest()


def current_login_lock(username: str, ip: str) -> dict[str, Any] | None:
    return one(
        """
        SELECT attempts, locked_until
        FROM portfolio_login_failures
        WHERE failure_key = %s AND locked_until IS NOT NULL AND locked_until > NOW()
        """,
        (login_failure_key(username, ip),),
    )


def record_login_failure(username: str, ip: str) -> dict[str, Any]:
    key = login_failure_key(username, ip)
    existing = one("SELECT attempts FROM portfolio_login_failures WHERE failure_key = %s", (key,))
    attempts = int(existing["attempts"] if existing else 0) + 1
    locked_until = now_utc() + timedelta(minutes=LOGIN_LOCKOUT_MINUTES) if attempts >= LOGIN_FAILURE_LIMIT else None
    with db_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO portfolio_login_failures
                (failure_key, username, client_ip, attempts, locked_until, last_failed_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (failure_key) DO UPDATE SET
                attempts = EXCLUDED.attempts,
                locked_until = EXCLUDED.locked_until,
                last_failed_at = NOW()
            RETURNING attempts, locked_until
            """,
            (key, username, ip, attempts, locked_until),
        )
        row = dict(cur.fetchone())
        conn.commit()
        return row


def clear_login_failures(username: str, ip: str) -> None:
    execute("DELETE FROM portfolio_login_failures WHERE failure_key = %s", (login_failure_key(username, ip),))


def redirect_if_guest(request: Request):
    return None if session_user(request) else RedirectResponse("/login", status_code=303)


def json_ready(value: Any) -> Any:
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def money(value: Any, signed: bool = False) -> str:
    if value is None:
        return "-"
    number = float(value)
    prefix = "+" if signed and number >= 0 else ""
    return f"{prefix}¥{number:,.2f}"


def pct(value: Any, signed: bool = False) -> str:
    if value is None:
        return "-"
    number = float(value)
    prefix = "+" if signed and number >= 0 else ""
    return f"{prefix}{number * 100:.2f}%"


def fmt_dt(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_date(value: Any) -> str:
    if not value:
        return "-"
    return value[:10] if isinstance(value, str) else value.isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_filename(name: str) -> str:
    out = [char if char.isalnum() or char in {".", "-", "_"} else "_" for char in name]
    return "".join(out).strip("._") or "portfolio.xlsx"


def make_batch_id(uploaded_at: datetime, digest: str) -> str:
    stamp = uploaded_at.strftime("%Y%m%dT%H%M%S") + f"{uploaded_at.microsecond:06d}Z"
    return f"portfolio-{stamp}-{digest[:12]}"


def batch_link(batch_id: str) -> str:
    return f"/reports/{batch_id}"


SUCCESS_STATUSES = ("complete", "partial")


def latest_batch() -> dict[str, Any] | None:
    return one(
        """
        SELECT * FROM portfolio_import_batches
        WHERE status IN ('complete', 'partial')
        ORDER BY as_of_date DESC, uploaded_at DESC, batch_id DESC
        LIMIT 1
        """
    )


def previous_success(batch_id: str) -> dict[str, Any] | None:
    current = one("SELECT as_of_date, uploaded_at, batch_id FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,))
    if not current:
        return None
    return one(
        """
        SELECT * FROM portfolio_import_batches
        WHERE status IN ('complete', 'partial')
          AND (as_of_date, uploaded_at, batch_id) < (%s, %s, %s)
        ORDER BY as_of_date DESC, uploaded_at DESC, batch_id DESC
        LIMIT 1
        """,
        (current["as_of_date"], current["uploaded_at"], current["batch_id"]),
    )


def list_batches(limit: int = 120) -> list[dict[str, Any]]:
    return rows(
        """
        SELECT batch_id, uploaded_at, as_of_date, original_filename, file_sha256, status,
               message, is_archived, total_assets, position_count, meta_json
        FROM portfolio_import_batches
        ORDER BY uploaded_at DESC, batch_id DESC
        LIMIT %s
        """,
        (limit,),
    )


def success_batches(limit: int = 80, descending: bool = False, months: int | None = None) -> list[dict[str, Any]]:
    direction = "DESC" if descending else "ASC"
    where = "WHERE status IN ('complete', 'partial')"
    params: list[Any] = []
    if months:
        latest = one("SELECT MAX(as_of_date) AS max_date FROM portfolio_import_batches WHERE status IN ('complete', 'partial')")
        anchor = latest.get("max_date") if latest else None
        if anchor:
            where += " AND as_of_date >= %s"
            params.append(anchor - timedelta(days=max(1, months) * 31))
    params.append(limit)
    return rows(
        f"""
        SELECT batch_id, uploaded_at, as_of_date, original_filename, total_assets, position_count, status
        FROM portfolio_import_batches
        {where}
        ORDER BY as_of_date {direction}, uploaded_at {direction}, batch_id {direction}
        LIMIT %s
        """,
        tuple(params),
    )


def find_existing_upload(digest: str) -> dict[str, Any] | None:
    return one(
        """
        SELECT batch_id, as_of_date, uploaded_at, original_filename, total_assets, position_count
        FROM portfolio_import_batches
        WHERE file_sha256 = %s AND status IN ('complete', 'partial')
        ORDER BY uploaded_at DESC
        LIMIT 1
        """,
        (digest,),
    )


def batch_positions(batch_id: str, limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT security_code, security_name, security_type, market, holding_amount,
               today_pnl, holding_pnl, cumulative_pnl, portfolio_weight,
               holding_days, return_1m, return_3m, return_6m, return_1y, related_sector
        FROM portfolio_positions
        WHERE batch_id = %s
        ORDER BY portfolio_weight DESC NULLS LAST, holding_amount DESC NULLS LAST
    """
    params: tuple[Any, ...] = (batch_id,)
    if limit:
        sql += " LIMIT %s"
        params = (batch_id, limit)
    return rows(sql, params)


def batch_metrics(batch_id: str) -> dict[str, Any]:
    summary = one(
        """
        SELECT top3_weight, top5_weight, top10_underlying_weight,
               equity_like_weight, bond_like_weight, qdii_weight, cash_weight
        FROM portfolio_daily_summary
        WHERE batch_id = %s
        """,
        (batch_id,),
    )
    if summary:
        return {
            "top3_position_weight": summary.get("top3_weight"),
            "top5_position_weight": summary.get("top5_weight"),
            "top10_underlying_weight": summary.get("top10_underlying_weight"),
            "equity_like_weight": summary.get("equity_like_weight"),
            "bond_like_weight": summary.get("bond_like_weight"),
            "qdii_weight": summary.get("qdii_weight"),
            "cash_weight_estimated": summary.get("cash_weight"),
        }
    data = rows(
        "SELECT metric_name, metric_value FROM portfolio_risk_metrics WHERE batch_id = %s AND metric_scope = 'portfolio'",
        (batch_id,),
    )
    return {item["metric_name"]: item["metric_value"] for item in data}


def asset_allocation(batch_id: str) -> list[dict[str, Any]]:
    data = rows(
        "SELECT allocation_bucket, amount, weight FROM portfolio_daily_allocation WHERE batch_id = %s ORDER BY weight DESC",
        (batch_id,),
    )
    if data:
        return data
    return rows(
        "SELECT allocation_bucket, amount, weight FROM portfolio_asset_allocation WHERE batch_id = %s ORDER BY weight DESC",
        (batch_id,),
    )


def industry_allocation(batch_id: str) -> list[dict[str, Any]]:
    data = rows(
        """
        SELECT exposure_name AS industry_name, weight
        FROM portfolio_daily_exposure
        WHERE batch_id = %s AND exposure_type = 'industry'
        ORDER BY weight DESC NULLS LAST
        LIMIT 12
        """,
        (batch_id,),
    )
    if data:
        return data
    data = rows(
        """
        SELECT industry_name, SUM(lookthrough_portfolio_weight) AS weight
        FROM portfolio_industry_allocations
        WHERE batch_id = %s
        GROUP BY industry_name
        ORDER BY weight DESC
        LIMIT 12
        """,
        (batch_id,),
    )
    if data:
        return data
    return rows(
        """
        SELECT COALESCE(NULLIF(related_sector, ''), security_type, 'unknown') AS industry_name,
               SUM(COALESCE(portfolio_weight, 0)) AS weight
        FROM portfolio_positions
        WHERE batch_id = %s
        GROUP BY COALESCE(NULLIF(related_sector, ''), security_type, 'unknown')
        ORDER BY weight DESC
        LIMIT 12
        """,
        (batch_id,),
    )


def underlying(batch_id: str) -> list[dict[str, Any]]:
    data = rows(
        """
        SELECT exposure_code AS underlying_code,
               exposure_name AS underlying_name,
               'lookthrough' AS underlying_type,
               amount,
               weight,
               source_count
        FROM portfolio_daily_exposure
        WHERE batch_id = %s AND exposure_type = 'underlying'
        ORDER BY weight DESC NULLS LAST
        LIMIT 20
        """,
        (batch_id,),
    )
    if data:
        return data
    return rows(
        """
        SELECT underlying_code, underlying_name, underlying_type,
               SUM(lookthrough_amount) AS amount,
               SUM(lookthrough_portfolio_weight) AS weight,
               COUNT(DISTINCT parent_code) AS source_count
        FROM portfolio_underlying_holdings
        WHERE batch_id = %s
        GROUP BY underlying_code, underlying_name, underlying_type
        ORDER BY weight DESC NULLS LAST
        LIMIT 20
        """,
        (batch_id,),
    )


def xray_data(batch_id: str) -> dict[str, Any]:
    batch = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,))
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    under = underlying(batch_id)
    overlaps = [item for item in under if int(item.get("source_count") or 0) > 1]
    industry_rows = rows(
        """
        SELECT exposure_name AS industry_name, weight, contributors
        FROM portfolio_daily_exposure
        WHERE batch_id = %s AND exposure_type = 'industry'
        ORDER BY weight DESC NULLS LAST
        LIMIT 12
        """,
        (batch_id,),
    )
    if not industry_rows:
        industry_rows = rows(
            """
            SELECT industry_name,
                   SUM(lookthrough_portfolio_weight) AS weight,
                   jsonb_agg(jsonb_build_object(
                        'parent_code', parent_code,
                        'parent_name', parent_name,
                        'weight', lookthrough_portfolio_weight
                   ) ORDER BY lookthrough_portfolio_weight DESC) AS contributors
            FROM portfolio_industry_allocations
            WHERE batch_id = %s
            GROUP BY industry_name
            ORDER BY weight DESC NULLS LAST
            LIMIT 12
            """,
            (batch_id,),
        )
    if not industry_rows:
        industry_rows = rows(
            """
            SELECT COALESCE(NULLIF(related_sector, ''), security_type, 'unknown') AS industry_name,
                   SUM(COALESCE(portfolio_weight, 0)) AS weight,
                   jsonb_agg(jsonb_build_object(
                        'parent_code', security_code,
                        'parent_name', security_name,
                        'weight', portfolio_weight
                   ) ORDER BY portfolio_weight DESC) AS contributors
            FROM portfolio_positions
            WHERE batch_id = %s
            GROUP BY COALESCE(NULLIF(related_sector, ''), security_type, 'unknown')
            ORDER BY weight DESC
            LIMIT 12
            """,
            (batch_id,),
        )
    return {"batch": batch, "underlying": under, "overlaps": overlaps, "industries": industry_rows}


def recent_transactions(batch_id: str) -> list[dict[str, Any]]:
    return rows(
        """
        SELECT trade_date, trade_time, security_code, security_name, transaction_type,
               quantity, price, cash_flow_amount, fee
        FROM portfolio_transactions
        WHERE batch_id = %s
        ORDER BY trade_date DESC, trade_time DESC NULLS LAST
        LIMIT 30
        """,
        (batch_id,),
    )


def compare_batches(from_batch: str, to_batch: str) -> dict[str, Any]:
    old = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (from_batch,))
    new = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (to_batch,))
    if not old or not new:
        raise HTTPException(status_code=404, detail="Batch not found")
    old_metrics = batch_metrics(from_batch)
    new_metrics = batch_metrics(to_batch)
    joined = rows(
        """
        SELECT COALESCE(n.security_code, o.security_code) AS security_code,
               COALESCE(n.security_name, o.security_name) AS security_name,
               COALESCE(n.holding_amount, 0)::double precision AS new_amount,
               COALESCE(o.holding_amount, 0)::double precision AS old_amount,
               COALESCE(n.portfolio_weight, 0) AS new_weight,
               COALESCE(o.portfolio_weight, 0) AS old_weight,
               COALESCE(n.holding_pnl, 0)::double precision AS new_pnl,
               COALESCE(o.holding_pnl, 0)::double precision AS old_pnl
        FROM (
            SELECT * FROM portfolio_positions WHERE batch_id = %s
        ) n
        FULL OUTER JOIN (
            SELECT * FROM portfolio_positions WHERE batch_id = %s
        ) o ON n.security_code = o.security_code
        ORDER BY ABS(COALESCE(n.portfolio_weight, 0) - COALESCE(o.portfolio_weight, 0)) DESC
        LIMIT 30
        """,
        (to_batch, from_batch),
    )
    return {
        "from": old,
        "to": new,
        "summary": {
            "total_assets_change": (new.get("total_assets") or 0) - (old.get("total_assets") or 0),
            "position_count_change": (new.get("position_count") or 0) - (old.get("position_count") or 0),
            "top5_weight_change": (new_metrics.get("top5_position_weight") or 0) - (old_metrics.get("top5_position_weight") or 0),
        },
        "positions": joined,
    }


def upsert_batch(conn, batch: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO portfolio_import_batches
                (batch_id, source_file, source_file_mtime, as_of_date, uploaded_at,
                 original_filename, file_sha256, status, message, is_archived,
                 total_assets, position_count, meta_json, report_markdown)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET
                source_file = EXCLUDED.source_file,
                source_file_mtime = EXCLUDED.source_file_mtime,
                as_of_date = EXCLUDED.as_of_date,
                uploaded_at = EXCLUDED.uploaded_at,
                original_filename = EXCLUDED.original_filename,
                file_sha256 = EXCLUDED.file_sha256,
                status = EXCLUDED.status,
                message = EXCLUDED.message,
                total_assets = EXCLUDED.total_assets,
                position_count = EXCLUDED.position_count,
                meta_json = EXCLUDED.meta_json,
                report_markdown = EXCLUDED.report_markdown
            """,
            (
                batch["batch_id"],
                batch["stored_path"],
                batch.get("source_file_mtime"),
                batch["as_of_date"],
                batch["uploaded_at"],
                batch["original_filename"],
                batch["file_sha256"],
                batch["status"],
                batch["message"],
                batch.get("total_assets"),
                batch.get("position_count", 0),
                Json(batch.get("meta_json") or {}),
                batch.get("report_markdown") or "",
            ),
        )


def build_report(batch: dict[str, Any], prev: dict[str, Any] | None) -> str:
    batch_id = batch["batch_id"]
    positions = batch_positions(batch_id, 12)
    metrics = batch_metrics(batch_id)
    alloc = asset_allocation(batch_id)
    industries = industry_allocation(batch_id)
    gain = sorted(positions, key=lambda item: float(item.get("holding_pnl") or 0), reverse=True)[:5]
    loss = sorted(positions, key=lambda item: float(item.get("holding_pnl") or 0))[:5]
    lines = [
        f"# 持仓分析报告 - {fmt_date(batch.get('as_of_date'))}",
        "",
        f"上传批次：`{batch_id}`",
        f"上传时间：{fmt_dt(batch.get('uploaded_at'))}",
        f"原始文件：{batch.get('original_filename') or '-'}",
        "",
        "## 核心结论",
        f"- 总资产：{money(batch.get('total_assets'))}",
        f"- 持仓数量：{batch.get('position_count') or 0}",
        f"- Top 3 集中度：{pct(metrics.get('top3_position_weight'))}",
        f"- Top 5 集中度：{pct(metrics.get('top5_position_weight'))}",
        f"- 估算现金权重：{pct(metrics.get('cash_weight_estimated'))}",
        f"- 股性资产权重：{pct(metrics.get('equity_like_weight'))}；债性资产权重：{pct(metrics.get('bond_like_weight'))}；QDII 权重：{pct(metrics.get('qdii_weight'))}",
    ]
    if prev:
        comp = compare_batches(prev["batch_id"], batch_id)["summary"]
        lines += [
            "",
            "## 较上一批变化",
            f"- 总资产变化：{money(comp['total_assets_change'])}",
            f"- 持仓数量变化：{comp['position_count_change']:+d}",
            f"- Top 5 集中度变化：{pct(comp['top5_weight_change'], signed=True)}",
        ]
    lines += ["", "## 资产分布"]
    lines += [f"- {item['allocation_bucket']}：{money(item.get('amount'))}，{pct(item.get('weight'))}" for item in alloc[:8]] or ["- 暂无资产分布数据"]
    lines += ["", "## 行业/主题暴露"]
    lines += [f"- {item['industry_name']}：{pct(item.get('weight'))}" for item in industries[:8]] or ["- 暂无行业/主题数据"]
    lines += ["", "## 贡献靠前"]
    lines += [f"- {item['security_code']} {item['security_name']}：持有盈亏 {money(item.get('holding_pnl'))}，仓位 {pct(item.get('portfolio_weight'))}" for item in gain] or ["- 暂无"]
    lines += ["", "## 拖累靠前"]
    lines += [f"- {item['security_code']} {item['security_name']}：持有盈亏 {money(item.get('holding_pnl'))}，仓位 {pct(item.get('portfolio_weight'))}" for item in loss] or ["- 暂无"]
    lines += ["", "## 操作提示", "- 优先检查单一产品和 Top 5 集中度是否符合自己的风险预算。", "- 无法穿透的基金/ETF 会先按产品本身作为代理暴露。", "- 本报告只用于个人研究辅助，不构成投资建议。"]
    return "\n".join(lines) + "\n"


def mark_failed(batch: dict[str, Any], message: str) -> None:
    with db_conn() as conn:
        upsert_batch(conn, batch | {"status": "failed", "message": message[:900], "total_assets": None, "position_count": 0})
        conn.commit()


def process_upload(stored_path: Path, original_filename: str, uploaded_at: datetime, digest: str, as_of_override: date | None, batch_id_override: str | None = None) -> dict[str, Any]:
    batch_id = batch_id_override or make_batch_id(uploaded_at, digest)
    inferred_date = as_of_override or portfolio.infer_as_of_date(stored_path, original_filename) or uploaded_at.date()
    existing_batch = one("SELECT meta_json FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,)) if batch_id_override else None
    base = {
        "batch_id": batch_id,
        "uploaded_at": uploaded_at,
        "original_filename": original_filename,
        "stored_path": str(stored_path),
        "file_sha256": digest,
        "source_file_mtime": datetime.fromtimestamp(stored_path.stat().st_mtime, tz=timezone.utc),
        "as_of_date": inferred_date,
        "status": "failed",
        "message": "import started",
        "meta_json": existing_batch.get("meta_json") if existing_batch else {},
        "total_assets": None,
        "position_count": 0,
        "report_markdown": "",
    }
    try:
        workbook = portfolio.read_portfolio_workbook(stored_path, base["as_of_date"])
        if not workbook.positions:
            raise ValueError("No positions found in 持仓数据 sheet.")
        with db_conn() as conn:
            ensure_schema(conn)
            upsert_batch(conn, base | {"status": "failed", "message": "importing"})
            catalog = portfolio.fund_catalog() if AKSHARE_ENABLED else set()
            positions = enrich_positions(workbook.positions, catalog)
            clear_batch(conn, batch_id)
            insert_positions(conn, batch_id, base["as_of_date"], positions)
            insert_closed_positions(conn, batch_id, workbook.closed_positions)
            insert_transactions(conn, batch_id, workbook.transactions)
            metadata, under, industries, errors = collect_market_data(conn, batch_id, base["as_of_date"], positions, not AKSHARE_ENABLED)
            insert_fund_metadata(conn, batch_id, base["as_of_date"], metadata)
            insert_underlying(conn, batch_id, base["as_of_date"], under)
            insert_industries(conn, batch_id, base["as_of_date"], industries)
            insert_allocations(conn, batch_id, base["as_of_date"], portfolio.compute_portfolio_allocations(positions))
            insert_risk_metrics(conn, batch_id, base["as_of_date"], portfolio.compute_risk_metrics(positions, under, metadata))
            total_assets = sum((portfolio.to_decimal(row.get("持有金额")) or Decimal("0") for row in positions), Decimal("0"))
            status = "partial" if errors else "complete"
            message = "; ".join(errors[:3]) if errors else "OK"
            base.update({
                "status": status,
                "message": message[:900],
                "total_assets": total_assets,
                "position_count": len(positions),
                "meta_json": (base.get("meta_json") or {}) | {"summary": workbook.summary, "position_rows": len(workbook.positions), "closed_rows": len(workbook.closed_positions), "transaction_rows": len(workbook.transactions), "akshare_enabled": AKSHARE_ENABLED, "errors": errors},
            })
            upsert_batch(conn, base)
            rebuild_batch_summary(conn, batch_id)
            refresh_summary_derived_fields(conn)
            conn.commit()
        report = build_report(base, previous_success(batch_id))
        execute("UPDATE portfolio_import_batches SET report_markdown = %s WHERE batch_id = %s", (report, batch_id))
        base["report_markdown"] = report
        return base
    except Exception as exc:
        mark_failed(base, str(exc))
        return base | {"status": "failed", "message": str(exc)}


def base_layout(title: str, body: str, user: str | None = None) -> str:
    nav = "" if not user else "<nav><a href='/'>总览</a><a href='/timeline'>整体分析</a><a href='/events'>公告雷达</a><a href='/uploads'>上传记录</a><a href='/settings/data-sources'>设置</a><a href='/admin/data'>数据健康</a><a href='/api/logout'>退出</a></nav>"
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{esc(title)} · {APP_TITLE}</title><style>{CSS}</style></head><body><header class="topbar"><a class="brand" href="/">{APP_TITLE}</a>{nav}</header><main>{body}</main></body></html>"""


def upload_form() -> str:
    return """<form class="upload" action="/api/upload" method="post" enctype="multipart/form-data"><label>上传持仓 Excel<input type="file" name="files" accept=".xlsx" multiple required></label><label>日期覆盖（可选）<input type="date" name="as_of_date"></label><button type="submit">上传并分析</button><p class="form-hint">可一次选择多个 .xlsx；不填日期时系统会先从文件内容识别，再从文件名识别。</p></form>"""


def metric_cards(batch: dict[str, Any] | None, metrics: dict[str, Any]) -> str:
    items = [("总资产", money(batch.get("total_assets") if batch else None)), ("持仓数", str(batch.get("position_count") or 0) if batch else "0"), ("Top 3", pct(metrics.get("top3_position_weight"))), ("Top 5", pct(metrics.get("top5_position_weight"))), ("股性", pct(metrics.get("equity_like_weight"))), ("债性", pct(metrics.get("bond_like_weight")))]
    return "<section class='metrics'>" + "".join(f"<article><span>{esc(k)}</span><strong>{esc(v)}</strong></article>" for k, v in items) + "</section>"


def bar_chart(items: list[dict[str, Any]], label_key: str, value_key: str, money_values: bool = False, limit: int = 12) -> str:
    if not items:
        return "<p class='muted'>暂无数据</p>"
    max_value = max([abs(float(item.get(value_key) or 0)) for item in items[:limit]] or [1]) or 1
    out = []
    for item in items[:limit]:
        value = float(item.get(value_key) or 0)
        width = max(2, abs(value) / max_value * 100)
        text = money(value) if money_values else pct(value)
        out.append(f"<div class='bar-row'><span>{esc(item.get(label_key))}</span><div><i style='width:{width:.2f}%'></i></div><b>{esc(text)}</b></div>")
    return "<div class='bars'>" + "".join(out) + "</div>"




def as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def date_label(value: Any) -> str:
    return fmt_date(value)[5:] if value else "-"


def metric_series(items: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return [{"x": item["as_of_date"], "y": item.get(key), "batch_id": item["batch_id"], "label": item.get("original_filename")} for item in items]


def allocation_history(batch_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not batch_ids:
        return {}
    data = rows(
        """
        SELECT batch_id, allocation_bucket, amount, weight
        FROM portfolio_daily_allocation
        WHERE batch_id = ANY(%s)
        """,
        (batch_ids,),
    )
    if not data:
        data = rows(
            """
            SELECT batch_id, allocation_bucket, amount, weight
            FROM portfolio_asset_allocation
            WHERE batch_id = ANY(%s)
            """,
            (batch_ids,),
        )
    by_batch = {batch_id: {} for batch_id in batch_ids}
    for item in data:
        by_batch.setdefault(item["batch_id"], {})[item["allocation_bucket"]] = item
    buckets = sorted({item["allocation_bucket"] for item in data})
    return {
        bucket: [{"batch_id": batch_id, "weight": (by_batch.get(batch_id, {}).get(bucket, {}).get("weight") or 0)} for batch_id in batch_ids]
        for bucket in buckets
    }


def analytics_timeline(months: int = 6) -> dict[str, Any]:
    latest = one("SELECT MAX(as_of_date) AS max_date FROM portfolio_daily_summary")
    params: list[Any] = []
    where = "WHERE true"
    if months and latest and latest.get("max_date"):
        where += " AND s.as_of_date >= %s"
        params.append(latest["max_date"] - timedelta(days=max(1, months) * 31))
    out = rows(
        f"""
        SELECT s.batch_id, s.uploaded_at, s.as_of_date, s.original_filename, s.status,
               s.total_assets, s.total_assets_change, s.position_count, s.today_pnl,
               s.holding_pnl, s.cumulative_pnl, s.max_position_weight,
               s.top3_weight, s.top5_weight, s.top10_underlying_weight AS top10_weight,
               s.equity_like_weight, s.bond_like_weight, s.qdii_weight, s.cash_weight,
               s.max_industry_name, s.max_industry_weight, s.drawdown_estimated AS drawdown,
               s.trailing_loss_streak,
               LAG(s.batch_id) OVER (ORDER BY s.as_of_date, s.uploaded_at, s.batch_id) AS previous_batch_id
        FROM portfolio_daily_summary s
        JOIN portfolio_import_batches b ON b.batch_id = s.batch_id
        {where} AND b.status IN ('complete', 'partial')
        ORDER BY s.as_of_date ASC, s.uploaded_at ASC, s.batch_id ASC
        LIMIT 500
        """,
        tuple(params),
    )
    if not out:
        items = success_batches(500, months=months)
        if not items:
            return {"points": [], "series": {}, "allocation_history": {}, "risk": {}, "top_changes": [], "xray": None}
        out = []
        previous: dict[str, Any] | None = None
        running_peak: float | None = None
        trailing_loss_streak = 0
        for batch in items:
            metrics = batch_metrics(batch["batch_id"])
            pnl = one(
                """
                SELECT COALESCE(SUM(today_pnl), 0) AS today_pnl,
                       COALESCE(SUM(holding_pnl), 0) AS holding_pnl,
                       COALESCE(SUM(cumulative_pnl), 0) AS cumulative_pnl,
                       MAX(portfolio_weight) AS max_position_weight
                FROM portfolio_positions
                WHERE batch_id = %s
                """,
                (batch["batch_id"],),
            ) or {}
            industry = industry_allocation(batch["batch_id"])
            total_assets = as_float(batch.get("total_assets"))
            running_peak = total_assets if running_peak is None else max(running_peak, total_assets)
            drawdown = (total_assets / running_peak - 1) if running_peak else 0
            trailing_loss_streak = trailing_loss_streak + 1 if as_float(pnl.get("today_pnl")) < 0 else 0
            out.append({
                **batch,
                "today_pnl": pnl.get("today_pnl"),
                "holding_pnl": pnl.get("holding_pnl"),
                "cumulative_pnl": pnl.get("cumulative_pnl"),
                "total_assets_change": None if previous is None else total_assets - as_float(previous.get("total_assets")),
                "top3_weight": metrics.get("top3_position_weight"),
                "top5_weight": metrics.get("top5_position_weight"),
                "top10_weight": metrics.get("top10_underlying_weight"),
                "equity_like_weight": metrics.get("equity_like_weight"),
                "bond_like_weight": metrics.get("bond_like_weight"),
                "qdii_weight": metrics.get("qdii_weight"),
                "cash_weight": metrics.get("cash_weight_estimated"),
                "max_position_weight": pnl.get("max_position_weight"),
                "max_industry_name": industry[0]["industry_name"] if industry else None,
                "max_industry_weight": industry[0]["weight"] if industry else None,
                "drawdown": drawdown,
                "trailing_loss_streak": trailing_loss_streak,
                "previous_batch_id": previous.get("batch_id") if previous else None,
            })
            previous = batch
    latest = out[-1]
    prev = out[-2] if len(out) > 1 else None
    max_drawdown = min((as_float(item.get("drawdown")) for item in out), default=0.0)
    trailing_loss_streak = int(latest.get("trailing_loss_streak") or 0)
    top_changes = compare_batches(prev["batch_id"], latest["batch_id"])["positions"] if prev else []
    return {
        "points": out,
        "series": {
            "assets": metric_series(out, "total_assets"),
            "today_pnl": metric_series(out, "today_pnl"),
            "holding_pnl": metric_series(out, "holding_pnl"),
            "cumulative_pnl": metric_series(out, "cumulative_pnl"),
            "top3_weight": metric_series(out, "top3_weight"),
            "top5_weight": metric_series(out, "top5_weight"),
            "top10_weight": metric_series(out, "top10_weight"),
            "equity_like_weight": metric_series(out, "equity_like_weight"),
            "bond_like_weight": metric_series(out, "bond_like_weight"),
            "qdii_weight": metric_series(out, "qdii_weight"),
            "cash_weight": metric_series(out, "cash_weight"),
            "drawdown": metric_series(out, "drawdown"),
        },
        "allocation_history": allocation_history([item["batch_id"] for item in out]),
        "risk": {
            "latest_batch_id": latest["batch_id"],
            "latest_as_of_date": latest["as_of_date"],
            "max_position_weight": latest.get("max_position_weight"),
            "top5_weight": latest.get("top5_weight"),
            "max_industry_name": latest.get("max_industry_name"),
            "max_industry_weight": latest.get("max_industry_weight"),
            "max_drawdown_estimated": max_drawdown,
            "trailing_loss_streak": trailing_loss_streak,
            "equity_like_weight": latest.get("equity_like_weight"),
            "bond_like_weight": latest.get("bond_like_weight"),
            "qdii_weight": latest.get("qdii_weight"),
            "cash_weight": latest.get("cash_weight"),
        },
        "top_changes": top_changes,
        "xray": xray_data(latest["batch_id"]),
    }


def svg_line_chart(series_items: list[tuple[str, list[dict[str, Any]], str]], money_values: bool = False, pct_values: bool = False, height: int = 260) -> str:
    points = [(idx, as_float(point.get("y")), point) for _, series, _ in series_items for idx, point in enumerate(series) if point.get("y") is not None]
    if not points:
        return "<p class='muted'>暂无数据</p>"
    width, pad = 760, 34
    min_y = min(value for _, value, _ in points)
    max_y = max(value for _, value, _ in points)
    if min_y == max_y:
        min_y -= 1
        max_y += 1
    max_idx = max((len(series) for _, series, _ in series_items), default=1) - 1 or 1

    def xy(idx: int, value: float) -> tuple[float, float]:
        x = pad + idx / max_idx * (width - pad * 2)
        y = height - pad - (value - min_y) / (max_y - min_y) * (height - pad * 2)
        return x, y

    paths = []
    dots = []
    labels = series_items[0][1]
    for name, series, color in series_items:
        coords = [xy(idx, as_float(point.get("y"))) for idx, point in enumerate(series) if point.get("y") is not None]
        if not coords:
            continue
        path = " ".join(("M" if idx == 0 else "L") + f"{x:.1f},{y:.1f}" for idx, (x, y) in enumerate(coords))
        paths.append(f"<path d='{path}' fill='none' stroke='{color}' stroke-width='2.4' stroke-linecap='round'/>")
        for idx, point in enumerate(series):
            if point.get("y") is None:
                continue
            x, y = xy(idx, as_float(point.get("y")))
            raw = point.get("y")
            text = money(raw) if money_values else pct(raw) if pct_values else f"{as_float(raw):.2f}"
            dots.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.2' fill='{color}'><title>{esc(name)} · {fmt_date(point.get('x'))}: {esc(text)}</title></circle>")
    axis_labels = "".join(
        f"<text x='{xy(idx, min_y)[0]:.1f}' y='{height-8}' text-anchor='middle'>{esc(date_label(point.get('x')))}</text>"
        for idx, point in enumerate(labels)
    )
    legend = "".join(f"<span><i style='background:{color}'></i>{esc(name)}</span>" for name, _, color in series_items)
    y_top = money(max_y) if money_values else pct(max_y) if pct_values else f"{max_y:.2f}"
    y_bottom = money(min_y) if money_values else pct(min_y) if pct_values else f"{min_y:.2f}"
    return f"<div class='chart-legend'>{legend}</div><svg class='chart' viewBox='0 0 {width} {height}' role='img'><line x1='{pad}' y1='{pad}' x2='{pad}' y2='{height-pad}'/><line x1='{pad}' y1='{height-pad}' x2='{width-pad}' y2='{height-pad}'/><text x='4' y='{pad+4}'>{esc(y_top)}</text><text x='4' y='{height-pad}'>{esc(y_bottom)}</text>{''.join(paths)}{''.join(dots)}{axis_labels}</svg>"


def svg_stacked_allocation(data: dict[str, list[dict[str, Any]]], labels: list[dict[str, Any]], height: int = 260) -> str:
    if not data or not labels:
        return "<p class='muted'>暂无数据</p>"
    colors = ["#0f766e", "#2563eb", "#d97706", "#7c3aed", "#475569", "#be123c", "#15803d"]
    width, pad = 760, 30
    max_idx = max(len(labels) - 1, 1)
    buckets = list(data.keys())[:7]
    cumulative = [0.0 for _ in labels]
    areas = []
    for bucket_index, bucket in enumerate(buckets):
        upper = []
        lower = []
        values = data[bucket]
        for idx, point in enumerate(values[:len(labels)]):
            lower_value = cumulative[idx]
            upper_value = min(1.0, lower_value + as_float(point.get("weight")))
            cumulative[idx] = upper_value
            x = pad + idx / max_idx * (width - pad * 2)
            y_upper = height - pad - upper_value * (height - pad * 2)
            y_lower = height - pad - lower_value * (height - pad * 2)
            upper.append((x, y_upper))
            lower.append((x, y_lower))
        path = " ".join(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}" for i, (x, y) in enumerate(upper))
        path += " " + " ".join(f"L{x:.1f},{y:.1f}" for x, y in reversed(lower)) + " Z"
        areas.append(f"<path d='{path}' fill='{colors[bucket_index % len(colors)]}' opacity='.78'><title>{esc(bucket)}</title></path>")
    axis_labels = "".join(
        f"<text x='{pad + idx / max_idx * (width - pad * 2):.1f}' y='{height-8}' text-anchor='middle'>{esc(date_label(point.get('as_of_date')))}</text>"
        for idx, point in enumerate(labels)
    )
    legend = "".join(f"<span><i style='background:{colors[idx % len(colors)]}'></i>{esc(bucket)}</span>" for idx, bucket in enumerate(buckets))
    return f"<div class='chart-legend'>{legend}</div><svg class='chart' viewBox='0 0 {width} {height}' role='img'><line x1='{pad}' y1='{pad}' x2='{pad}' y2='{height-pad}'/><line x1='{pad}' y1='{height-pad}' x2='{width-pad}' y2='{height-pad}'/>{''.join(areas)}{axis_labels}</svg>"


def top_changes_table(changes: list[dict[str, Any]]) -> str:
    if not changes:
        return "<p class='muted'>暂无相邻批次变化</p>"
    rows_html = []
    for item in changes[:20]:
        old_amount = as_float(item.get("old_amount"))
        new_amount = as_float(item.get("new_amount"))
        if old_amount == 0 and new_amount != 0:
            action = "新增"
        elif old_amount != 0 and new_amount == 0:
            action = "清仓"
        elif new_amount > old_amount:
            action = "加仓"
        elif new_amount < old_amount:
            action = "减仓"
        else:
            action = "持平"
        rows_html.append(
            f"<tr><td>{esc(action)}</td><td>{esc(item.get('security_code'))}</td><td>{esc(item.get('security_name'))}</td><td>{money(old_amount)}</td><td>{money(new_amount)}</td><td>{money(new_amount - old_amount, signed=True)}</td><td>{pct(as_float(item.get('new_weight')) - as_float(item.get('old_weight')), signed=True)}</td><td>{money(as_float(item.get('new_pnl')) - as_float(item.get('old_pnl')), signed=True)}</td></tr>"
        )
    return f"<table><thead><tr><th>动作</th><th>代码</th><th>名称</th><th>旧金额</th><th>新金额</th><th>金额变化</th><th>仓位变化</th><th>盈亏变化</th></tr></thead><tbody>{''.join(rows_html)}</tbody></table>"


def xray_panel(data: dict[str, Any]) -> str:
    under_rows = "".join(
        f"<tr><td>{esc(i.get('underlying_code'))}</td><td>{esc(i.get('underlying_name'))}</td><td>{esc(i.get('underlying_type'))}</td><td>{money(i.get('amount'))}</td><td>{pct(i.get('weight'))}</td><td>{esc(i.get('source_count'))}</td></tr>"
        for i in data.get("underlying", [])[:20]
    )
    overlap_rows = "".join(
        f"<tr><td>{esc(i.get('underlying_code'))}</td><td>{esc(i.get('underlying_name'))}</td><td>{pct(i.get('weight'))}</td><td>{esc(i.get('source_count'))}</td></tr>"
        for i in data.get("overlaps", [])[:12]
    ) or "<tr><td colspan='4' class='muted'>暂无重叠穿透持仓</td></tr>"
    industry_rows = []
    for item in data.get("industries", [])[:10]:
        contributors = item.get("contributors") or []
        detail = ", ".join(f"{c.get('parent_name')} {pct(c.get('weight'))}" for c in contributors[:4])
        industry_rows.append(f"<tr><td>{esc(item.get('industry_name'))}</td><td>{pct(item.get('weight'))}</td><td>{esc(detail)}</td></tr>")
    return f"<section class='grid two'><article class='panel'><h2>穿透重仓 Top 20</h2><table><thead><tr><th>代码</th><th>名称</th><th>类型</th><th>金额</th><th>权重</th><th>来源数</th></tr></thead><tbody>{under_rows}</tbody></table></article><article class='panel'><h2>重叠持仓</h2><table><thead><tr><th>代码</th><th>名称</th><th>合计权重</th><th>来源数</th></tr></thead><tbody>{overlap_rows}</tbody></table></article></section><section class='panel'><h2>行业/主题贡献来源</h2><table><thead><tr><th>行业/主题</th><th>权重</th><th>主要来源</th></tr></thead><tbody>{''.join(industry_rows)}</tbody></table></section>"

def positions_table(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p class='muted'>暂无持仓</p>"
    body = "".join(f"<tr><td>{esc(i['security_code'])}</td><td>{esc(i['security_name'])}</td><td>{esc(i.get('security_type'))}</td><td>{money(i.get('holding_amount'))}</td><td>{pct(i.get('portfolio_weight'))}</td><td>{money(i.get('holding_pnl'))}</td><td>{pct(i.get('return_1m'), signed=True)}</td><td>{pct(i.get('return_1y'), signed=True)}</td></tr>" for i in items)
    return f"<div class='table-wrap'><table><thead><tr><th>代码</th><th>名称</th><th>类型</th><th>金额</th><th>仓位</th><th>持有盈亏</th><th>近1月</th><th>近1年</th></tr></thead><tbody>{body}</tbody></table></div>"


def upload_result_page(results: list[dict[str, Any]], skipped: list[dict[str, Any]], user: str | None) -> HTMLResponse:
    accepted_rows = "".join(
        f"<tr><td><a href='{batch_link(item['batch_id'])}'>{esc(item['original_filename'])}</a></td><td>{fmt_date(item.get('as_of_date'))}</td><td>{esc(item.get('status'))}</td><td>{money(item.get('total_assets'))}</td><td>{esc(item.get('position_count'))}</td><td>{esc(item.get('message'))}</td></tr>"
        for item in results
    ) or "<tr><td colspan='6' class='muted'>没有新文件被接收</td></tr>"
    skipped_rows = "".join(
        f"<tr><td>{esc(item['filename'])}</td><td>{fmt_date(item.get('as_of_date'))}</td><td><a href='{batch_link(item['duplicate_of'])}'>{esc(item['duplicate_of'])}</a></td><td>{esc(item.get('reason'))}</td></tr>"
        for item in skipped
    ) or "<tr><td colspan='4' class='muted'>没有重复文件</td></tr>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>Upload result</p><h1>上传结果</h1><p class='subtitle'>新接收 {len(results)} 个文件，拒收重复 {len(skipped)} 个文件。</p></div></section><div class='actions'><a href='/timeline'>查看整体分析</a><a href='/uploads'>查看上传记录</a></div><section class='panel'><h2>已接收</h2><table><thead><tr><th>文件</th><th>持仓日期</th><th>状态</th><th>总资产</th><th>行数</th><th>消息</th></tr></thead><tbody>{accepted_rows}</tbody></table></section><section class='panel'><h2>重复拒收</h2><table><thead><tr><th>文件</th><th>持仓日期</th><th>已存在批次</th><th>原因</th></tr></thead><tbody>{skipped_rows}</tbody></table></section>"
    return HTMLResponse(base_layout("上传结果", body, user))


def event_filters(request: Request) -> dict[str, Any]:
    page_text = request.query_params.get("page") or "1"
    try:
        page = max(1, int(page_text))
    except ValueError:
        page = 1
    return {
        "source": request.query_params.get("source") or "",
        "symbol": request.query_params.get("symbol") or "",
        "type": request.query_params.get("type") or "",
        "status": request.query_params.get("status") or "",
        "start": request.query_params.get("start") or "",
        "end": request.query_params.get("end") or "",
        "page": page,
        "per_page": 30,
    }


def event_query(filters: dict[str, Any], funds_only: bool = False, latest_batch_only: bool = False, important_only: bool = False) -> dict[str, Any]:
    where = ["true"]
    params: list[Any] = []
    if filters.get("source"):
        where.append("e.source_key = %s")
        params.append(filters["source"])
    if filters.get("symbol"):
        where.append("EXISTS (SELECT 1 FROM portfolio_event_symbols s WHERE s.event_id = e.event_id AND s.symbol = %s)")
        params.append(str(filters["symbol"]).zfill(6))
    if filters.get("type"):
        where.append("e.event_type ILIKE %s")
        params.append(f"%{filters['type']}%")
    if filters.get("start"):
        where.append("e.announcement_date >= %s")
        params.append(filters["start"])
    if filters.get("end"):
        where.append("e.announcement_date <= %s")
        params.append(filters["end"])
    status = filters.get("status")
    if status == "unread":
        where.append("COALESCE(r.is_read, false) = false AND COALESCE(r.is_ignored, false) = false")
    elif status == "read":
        where.append("COALESCE(r.is_read, false) = true")
    elif status == "favorite":
        where.append("COALESCE(r.is_favorite, false) = true")
    elif status == "ignored":
        where.append("COALESCE(r.is_ignored, false) = true")
    if funds_only:
        where.append("EXISTS (SELECT 1 FROM portfolio_event_symbols s WHERE s.event_id = e.event_id AND s.security_type = ANY(%s))")
        params.append(list(event_provider.FUND_SECURITY_TYPES))
    if latest_batch_only:
        batch = latest_batch()
        if batch:
            where.append("EXISTS (SELECT 1 FROM portfolio_event_symbols s WHERE s.event_id = e.event_id AND s.batch_id = %s)")
            params.append(batch["batch_id"])
    if important_only:
        where.append("e.importance >= 3 AND COALESCE(r.is_read, false) = false AND COALESCE(r.is_ignored, false) = false")
    where_sql = " AND ".join(where)
    total = one(
        f"""
        SELECT COUNT(*) AS count
        FROM portfolio_events e
        LEFT JOIN portfolio_event_reads r ON r.event_id = e.event_id
        WHERE {where_sql}
        """,
        tuple(params),
    )["count"]
    offset = (filters["page"] - 1) * filters["per_page"]
    data = rows(
        f"""
        SELECT e.*, COALESCE(r.is_read, false) AS is_read,
               COALESCE(r.is_favorite, false) AS is_favorite,
               COALESCE(r.is_ignored, false) AS is_ignored,
               string_agg(DISTINCT s.symbol, ', ' ORDER BY s.symbol) AS symbols,
               string_agg(DISTINCT s.symbol_name, ', ' ORDER BY s.symbol_name) AS symbol_names
        FROM portfolio_events e
        LEFT JOIN portfolio_event_reads r ON r.event_id = e.event_id
        LEFT JOIN portfolio_event_symbols s ON s.event_id = e.event_id
        WHERE {where_sql}
        GROUP BY e.event_id, r.is_read, r.is_favorite, r.is_ignored
        ORDER BY e.announcement_date DESC, e.event_id DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [filters["per_page"], offset]),
    )
    return {"events": data, "total": total, "filters": filters}


def event_table(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p class='muted'>暂无公告</p>"
    body = "".join(
        f"<tr class='event-row {'is-read' if item.get('is_read') else ''}'><td>{fmt_date(item.get('announcement_date'))}</td><td><a href='/events/{item['event_id']}'>{esc(item.get('title'))}</a></td><td>{esc(item.get('symbols') or '')}</td><td>{esc(item.get('source_key'))}</td><td>{esc(item.get('event_type'))}</td><td>{esc(item.get('importance'))}</td><td>{'★' if item.get('is_favorite') else ''}{' 已读' if item.get('is_read') else ''}{' 忽略' if item.get('is_ignored') else ''}</td></tr>"
        for item in items
    )
    return f"<div class='table-wrap'><table><thead><tr><th>日期</th><th>标题</th><th>代码</th><th>来源</th><th>类型</th><th>重要性</th><th>状态</th></tr></thead><tbody>{body}</tbody></table></div>"


def event_filter_form(filters: dict[str, Any], action: str) -> str:
    status_options = "".join(
        f"<option value='{value}' {'selected' if filters.get('status') == value else ''}>{label}</option>"
        for value, label in [("", "全部状态"), ("unread", "未读"), ("read", "已读"), ("favorite", "收藏"), ("ignored", "忽略")]
    )
    source_options = "".join(
        f"<option value='{value}' {'selected' if filters.get('source') == value else ''}>{label}</option>"
        for value, label in [("", "全部来源"), ("fund_eid", "证监会基金电子披露"), ("cninfo", "巨潮资讯"), ("tushare", "Tushare"), ("sec_edgar", "SEC EDGAR")]
    )
    return f"<form class='filters' method='get' action='{action}'><label>来源<select name='source'>{source_options}</select></label><label>代码<input name='symbol' value='{esc(filters.get('symbol'))}'></label><label>类型<input name='type' value='{esc(filters.get('type'))}'></label><label>状态<select name='status'>{status_options}</select></label><label>开始<input type='date' name='start' value='{esc(filters.get('start'))}'></label><label>结束<input type='date' name='end' value='{esc(filters.get('end'))}'></label><button type='submit'>筛选</button></form>"


def event_detail(event_id: int) -> dict[str, Any]:
    event = one("SELECT * FROM portfolio_events WHERE event_id = %s", (event_id,))
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    symbols = rows("SELECT * FROM portfolio_event_symbols WHERE event_id = %s ORDER BY symbol", (event_id,))
    state = one("SELECT * FROM portfolio_event_reads WHERE event_id = %s", (event_id,)) or {"is_read": False, "is_favorite": False, "is_ignored": False}
    insight = one("SELECT * FROM portfolio_event_ai_insights WHERE event_id = %s", (event_id,))
    return {"event": event, "symbols": symbols, "state": state, "insight": insight}


async def request_payload(request: Request) -> dict[str, Any]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return dict(await request.json())
        except json.JSONDecodeError:
            return {}
    form = await request.form()
    return dict(form)


def bool_value(value: Any, default: bool | None = None) -> bool | None:
    if value is None or value == "":
        return default
    return str(value).lower() in {"1", "true", "yes", "on", "enabled"}


def is_html_form(request: Request) -> bool:
    content_type = request.headers.get("content-type", "")
    return "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type


@app.get("/events", response_class=HTMLResponse)
def events_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    filters = event_filters(request)
    result = event_query(filters, latest_batch_only=False)
    pages = max(1, (int(result["total"]) + filters["per_page"] - 1) // filters["per_page"])
    query = urlencode({key: value for key, value in filters.items() if key not in {"page", "per_page"} and value})
    pager = f"<div class='pager'><a href='/events?{query}&page={max(1, filters['page'] - 1)}'>上一页</a><span>{filters['page']} / {pages}</span><a href='/events?{query}&page={min(pages, filters['page'] + 1)}'>下一页</a></div>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>Event radar</p><h1>公告雷达</h1><p class='subtitle'>按持仓代码汇总公告，支持手动同步、筛选、已读、收藏和 AI 解读。</p></div></section><div class='actions'><a href='/events/funds'>国内基金公告</a><a href='/events/us'>美股/海外预留</a><a href='/settings/data-sources'>数据源设置</a><form class='inline-form' action='/api/events/sync-now' method='post'><button type='submit'>立即同步</button></form></div>{event_filter_form(filters, '/events')}<section class='panel'><div class='section-head'><h2>公告事件</h2><span class='muted'>共 {result['total']} 条</span></div>{event_table(result['events'])}{pager}</section>"
    return HTMLResponse(base_layout("公告雷达", body, session_user(request)))


@app.get("/events/funds", response_class=HTMLResponse)
def fund_events_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    filters = event_filters(request)
    result = event_query(filters, funds_only=True)
    body = f"<section class='hero compact'><div><p class='eyebrow'>Fund events</p><h1>国内基金公告</h1><p class='subtitle'>聚焦当前/历史持仓中的基金、ETF、LOF、QDII、债基和货币类产品。</p></div></section><div class='actions'><a href='/events'>全部公告</a><a href='/settings/data-sources'>数据源设置</a></div>{event_filter_form(filters, '/events/funds')}<section class='panel'><div class='section-head'><h2>基金公告</h2><span class='muted'>共 {result['total']} 条</span></div>{event_table(result['events'])}</section>"
    return HTMLResponse(base_layout("国内基金公告", body, session_user(request)))


@app.get("/events/us", response_class=HTMLResponse)
def us_events_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    body = "<section class='hero compact'><div><p class='eyebrow'>US filings</p><h1>美股/海外公告</h1><p class='subtitle'>SEC EDGAR 数据源已预留，默认未启用；第一版不执行海外公告抓取。</p></div></section><div class='actions'><a href='/settings/data-sources'>配置 SEC EDGAR</a><a href='/events'>返回公告雷达</a></div><section class='panel'><h2>未启用</h2><p class='muted'>开启后将从这里接入海外公告列表。</p></section>"
    return HTMLResponse(base_layout("美股/海外公告", body, session_user(request)))


@app.get("/events/{event_id}", response_class=HTMLResponse)
def event_detail_page(event_id: int, request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    data = event_detail(event_id)
    event = data["event"]
    symbols = "".join(f"<tr><td>{esc(i.get('symbol'))}</td><td>{esc(i.get('symbol_name'))}</td><td>{esc(i.get('security_type'))}</td><td>{esc(i.get('batch_id'))}</td></tr>" for i in data["symbols"]) or "<tr><td colspan='4' class='muted'>暂无关联持仓</td></tr>"
    source_link = f"<a href='{esc(event.get('source_url'))}' target='_blank' rel='noreferrer'>打开原文</a>" if event.get("source_url") else ""
    pdf_link = f"<a href='{esc(event.get('pdf_url'))}' target='_blank' rel='noreferrer'>打开 PDF</a>" if event.get("pdf_url") else ""
    state = data["state"]
    insight = data["insight"]
    risks = ", ".join(insight.get("risks") or []) if insight else ""
    insight_html = "<p class='muted'>暂无 AI 解读。</p>" if not insight else f"<div class='insight'><p>{esc(insight.get('summary'))}</p><p><strong>相关原因：</strong>{esc(insight.get('relevance'))}</p><p><strong>风险点：</strong>{esc(risks)}</p></div>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>{esc(event.get('source_key'))}</p><h1>{fmt_date(event.get('announcement_date'))}</h1><p class='subtitle'>{esc(event.get('title'))}</p></div><div class='hero-meta'><span>类型</span><strong>{esc(event.get('event_type'))}</strong><span>重要性</span><strong>{esc(event.get('importance'))}</strong></div></section><div class='actions'>{source_link}{pdf_link}<form class='inline-form' action='/api/events/{event_id}/read' method='post'><input type='hidden' name='is_read' value='true'><button type='submit'>{'已读' if state.get('is_read') else '标为已读'}</button></form><form class='inline-form' action='/api/events/{event_id}/favorite' method='post'><input type='hidden' name='is_favorite' value='{'false' if state.get('is_favorite') else 'true'}'><button type='submit'>{'取消收藏' if state.get('is_favorite') else '收藏'}</button></form><form class='inline-form' action='/api/events/{event_id}/ignore' method='post'><input type='hidden' name='is_ignored' value='{'false' if state.get('is_ignored') else 'true'}'><button type='submit'>{'取消忽略' if state.get('is_ignored') else '忽略'}</button></form><form class='inline-form' action='/api/events/{event_id}/ai-insight' method='post'><button type='submit'>AI 解读</button></form></div><section class='panel'><h2>关联持仓</h2><table><thead><tr><th>代码</th><th>名称</th><th>类型</th><th>批次</th></tr></thead><tbody>{symbols}</tbody></table></section><section class='panel'><h2>AI 解读</h2>{insight_html}</section><section class='panel'><h2>原始信息</h2><pre class='report-text'>{esc(json.dumps(json_ready(event.get('raw_json') or {}), ensure_ascii=False, indent=2))}</pre></section>"
    return HTMLResponse(base_layout("公告详情", body, session_user(request)))


@app.get("/settings/data-sources", response_class=HTMLResponse)
def data_sources_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    with db_conn() as conn:
        sources = event_provider.data_sources(conn)
    forms = []
    for item in sources:
        checked = "checked" if item.get("enabled") else ""
        configured = "已配置" if item.get("configured") else "未配置"
        forms.append(f"<article class='panel'><form class='settings-form' action='/api/settings/data-sources/{esc(item['source_key'])}' method='post'><div class='section-head'><h2>{esc(item['display_name'])}</h2><span class='muted'>{esc(item['source_key'])} · {configured}</span></div><label class='check'><input type='checkbox' name='enabled' value='true' {checked}> 启用</label><label>抓取天数<input type='number' min='1' max='60' name='fetch_days' value='{esc(item.get('fetch_days'))}'></label><label>Token/API Key（留空不变）<input type='password' name='secret' autocomplete='off'></label><button type='submit'>保存</button></form></article>")
    body = f"<section class='hero compact'><div><p class='eyebrow'>Settings</p><h1>数据源配置</h1><p class='subtitle'>默认启用证监会基金电子披露和巨潮；第三方 token 只写入不回显。</p></div></section><div class='actions'><a href='/settings/ai'>AI 设置</a><a href='/events'>公告雷达</a></div>{''.join(forms)}"
    return HTMLResponse(base_layout("数据源配置", body, session_user(request)))


@app.get("/settings/ai", response_class=HTMLResponse)
def ai_settings_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    with db_conn() as conn:
        settings = ai_insights.get_ai_settings(conn)
    provider_options = "".join(f"<option value='{value}' {'selected' if settings.get('provider') == value else ''}>{label}</option>" for value, label in [("none", "关闭"), ("deepseek", "DeepSeek"), ("openai", "OpenAI")])
    configured = "已配置" if settings.get("configured") else "未配置"
    body = f"<section class='hero compact'><div><p class='eyebrow'>AI</p><h1>AI 解读配置</h1><p class='subtitle'>AI 默认关闭，只处理已抓到的公告，不主动搜索新闻，不输出买卖建议。</p></div></section><div class='actions'><a href='/settings/data-sources'>数据源设置</a><a href='/events'>公告雷达</a></div><section class='panel'><form class='settings-form' action='/api/settings/ai' method='post'><label>Provider<select name='provider'>{provider_options}</select></label><label>模型<input name='model' value='{esc(settings.get('model') or '')}' placeholder='留空使用默认模型'></label><label>每日上限<input type='number' min='1' max='500' name='daily_limit' value='{esc(settings.get('daily_limit'))}'></label><label>API Key（{configured}，留空不变）<input type='password' name='api_key' autocomplete='off'></label><button type='submit'>保存</button></form></section>"
    return HTMLResponse(base_layout("AI 解读配置", body, session_user(request)))


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if session_user(request):
        return RedirectResponse("/", status_code=303)
    body = "<section class='login'><h1>登录</h1><form method='post' action='/api/login'><label>用户名<input name='username' autocomplete='username'></label><label>密码<input name='password' type='password' autocomplete='current-password'></label><button type='submit'>进入</button></form></section>"
    return HTMLResponse(base_layout("登录", body))


@app.post("/api/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    ip = client_ip(request)
    locked = current_login_lock(username, ip)
    if locked:
        body = "<section class='login'><h1>暂时锁定</h1><p>登录错误次数过多，请稍后再试。</p><a href='/login'>返回</a></section>"
        return HTMLResponse(base_layout("暂时锁定", body), status_code=429)

    if not ADMIN_PASSWORD or username != ADMIN_USERNAME or not hmac.compare_digest(password, ADMIN_PASSWORD):
        failure = record_login_failure(username, ip)
        if failure.get("locked_until"):
            body = "<section class='login'><h1>暂时锁定</h1><p>登录错误次数过多，系统已暂停该来源继续尝试。</p><a href='/login'>返回</a></section>"
            return HTMLResponse(base_layout("暂时锁定", body), status_code=429)
        body = "<section class='login'><h1>登录失败</h1><p>用户名或密码错误。</p><a href='/login'>返回</a></section>"
        return HTMLResponse(base_layout("登录失败", body), status_code=401)

    clear_login_failures(username, ip)
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(COOKIE_NAME, make_session(username), httponly=True, secure=True, samesite="lax", max_age=60 * 60 * 24 * 30)
    return response


@app.get("/api/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    user = session_user(request)
    batch = latest_batch()
    if not batch:
        body = f"<section class='hero'><div><p class='eyebrow'>Portfolio cockpit</p><h1>持仓分析</h1><p class='subtitle'>上传每日或每周多个持仓文件，每个文件都会形成独立批次和报告。</p></div></section>{upload_form()}<section class='panel'><h2>还没有成功导入的持仓</h2><p class='muted'>上传 .xlsx 后，这里会显示最新报告。</p></section>"
        return HTMLResponse(base_layout("总览", body, user))
    metrics = batch_metrics(batch["batch_id"])
    positions = batch_positions(batch["batch_id"], 20)
    alloc = asset_allocation(batch["batch_id"])
    industries = industry_allocation(batch["batch_id"])
    pnl_items = sorted(positions, key=lambda item: abs(float(item.get("holding_pnl") or 0)), reverse=True)
    important = event_query({"source": "", "symbol": "", "type": "", "status": "", "start": "", "end": "", "page": 1, "per_page": 8}, latest_batch_only=True, important_only=True)
    latest_events = f"<section class='panel'><div class='section-head'><h2>最新重要公告</h2><a href='/events'>全部公告</a></div>{event_table(important['events'][:8])}</section>"
    body = f"<section class='hero'><div><p class='eyebrow'>Latest portfolio</p><h1>持仓分析</h1><p class='subtitle'>最新成功批次：{esc(batch['batch_id'])}</p></div><div class='hero-meta'><span>上传</span><strong>{fmt_dt(batch.get('uploaded_at'))}</strong><span>文件</span><strong>{esc(batch.get('original_filename'))}</strong></div></section>{upload_form()}{metric_cards(batch, metrics)}{latest_events}<section class='grid two'><article class='panel'><h2>资产类型</h2>{bar_chart(alloc, 'allocation_bucket', 'weight')}</article><article class='panel'><h2>行业/主题</h2>{bar_chart(industries, 'industry_name', 'weight')}</article></section><section class='grid two'><article class='panel'><h2>持仓权重</h2>{bar_chart(positions, 'security_name', 'portfolio_weight')}</article><article class='panel'><h2>盈亏贡献</h2>{bar_chart(pnl_items, 'security_name', 'holding_pnl', True)}</article></section><section class='panel'><div class='section-head'><h2>最新报告</h2><a href='{batch_link(batch['batch_id'])}'>打开完整报告</a></div><pre class='report-text'>{esc((batch.get('report_markdown') or '')[:2200])}</pre></section><section class='panel'><h2>主要持仓</h2>{positions_table(positions)}</section>"
    return HTMLResponse(base_layout("总览", body, user))


def upload_filters(request: Request) -> dict[str, Any]:
    latest = one("SELECT to_char(MAX(as_of_date), 'YYYY-MM') AS month FROM portfolio_import_batches")
    month = request.query_params.get("month") or (latest or {}).get("month") or now_utc().strftime("%Y-%m")
    page = max(1, int(request.query_params.get("page") or "1"))
    return {
        "month": month,
        "status": request.query_params.get("status") or "",
        "q": request.query_params.get("q") or "",
        "start": request.query_params.get("start") or "",
        "end": request.query_params.get("end") or "",
        "page": page,
        "per_page": 30,
    }


def upload_query(filters: dict[str, Any]) -> dict[str, Any]:
    where = ["true"]
    params: list[Any] = []
    if filters.get("month"):
        where.append("to_char(as_of_date, 'YYYY-MM') = %s")
        params.append(filters["month"])
    if filters.get("status"):
        where.append("status = %s")
        params.append(filters["status"])
    if filters.get("q"):
        where.append("(original_filename ILIKE %s OR batch_id ILIKE %s)")
        params.extend([f"%{filters['q']}%", f"%{filters['q']}%"])
    if filters.get("start"):
        where.append("as_of_date >= %s")
        params.append(filters["start"])
    if filters.get("end"):
        where.append("as_of_date <= %s")
        params.append(filters["end"])
    where_sql = " AND ".join(where)
    total = one(f"SELECT COUNT(*) AS count FROM portfolio_import_batches WHERE {where_sql}", tuple(params))["count"]
    offset = (filters["page"] - 1) * filters["per_page"]
    data = rows(
        f"""
        SELECT batch_id, uploaded_at, as_of_date, original_filename, file_sha256, status,
               message, is_archived, total_assets, position_count, meta_json
        FROM portfolio_import_batches
        WHERE {where_sql}
        ORDER BY as_of_date DESC, uploaded_at DESC, batch_id DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [filters["per_page"], offset]),
    )
    months = rows(
        """
        SELECT to_char(as_of_date, 'YYYY-MM') AS month,
               COUNT(*) AS upload_count,
               COUNT(*) FILTER (WHERE status IN ('complete', 'partial')) AS success_count,
               COUNT(*) FILTER (WHERE status = 'failed') AS failed_count
        FROM portfolio_import_batches
        WHERE true
        GROUP BY to_char(as_of_date, 'YYYY-MM')
        ORDER BY month DESC
        """
    )
    return {"batches": data, "total": total, "months": months, "page": filters["page"], "per_page": filters["per_page"]}


@app.get("/uploads", response_class=HTMLResponse)
def uploads_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    filters = upload_filters(request)
    result = upload_query(filters)
    status_options = "".join(f"<option value='{value}' {'selected' if filters['status'] == value else ''}>{label}</option>" for value, label in [("", "全部状态"), ("complete", "complete"), ("partial", "partial"), ("failed", "failed")])
    month_cards = "".join(f"<a class='month-chip {'active' if item['month'] == filters['month'] else ''}' href='/uploads?month={item['month']}'>{esc(item['month'])}<b>{item['success_count']}/{item['upload_count']}</b></a>" for item in result["months"])
    body_rows = "".join(
        f"<tr class='status-{esc(i.get('status'))}'><td><a href='{batch_link(i['batch_id'])}'>{esc(i['batch_id'])}</a></td><td>{fmt_dt(i.get('uploaded_at'))}</td><td>{fmt_date(i.get('as_of_date'))}</td><td>{esc(i.get('original_filename'))}</td><td>{esc(i.get('status'))}</td><td>{money(i.get('total_assets'))}</td><td>{esc(i.get('position_count'))}</td><td>{esc(i.get('message'))}</td><td><form class='inline-form' action='/api/uploads/{esc(i['batch_id'])}/replace' method='post' enctype='multipart/form-data'><input type='file' name='file' accept='.xlsx' required><button type='submit'>替换</button></form></td></tr>"
        for i in result["batches"]
    ) or "<tr><td colspan='9' class='muted'>没有匹配记录</td></tr>"
    pages = max(1, (int(result["total"]) + filters["per_page"] - 1) // filters["per_page"])
    base_query = f"month={filters['month']}&status={filters['status']}&q={filters['q']}&start={filters['start']}&end={filters['end']}"
    pager = f"<div class='pager'><a href='/uploads?{base_query}&page={max(1, filters['page']-1)}'>上一页</a><span>{filters['page']} / {pages}</span><a href='/uploads?{base_query}&page={min(pages, filters['page']+1)}'>下一页</a></div>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>Files</p><h1>上传记录</h1><p class='subtitle'>按月份分页管理上传文件；重复文件会拒收。若某天文件有误，可直接在对应批次替换并重算分析。</p></div></section>{upload_form()}<section class='panel'><h2>月份</h2><div class='month-grid'>{month_cards}</div></section><section class='panel'><form class='filters' method='get'><label>月份<input name='month' value='{esc(filters['month'])}'></label><label>状态<select name='status'>{status_options}</select></label><label>文件/批次<input name='q' value='{esc(filters['q'])}'></label><label>开始<input type='date' name='start' value='{esc(filters['start'])}'></label><label>结束<input type='date' name='end' value='{esc(filters['end'])}'></label><button type='submit'>筛选</button></form></section><section class='panel'><div class='section-head'><h2>文件批次</h2><span class='muted'>替换会保留批次位置并重新解析报告</span></div><table><thead><tr><th>批次</th><th>上传时间</th><th>持仓日期</th><th>文件</th><th>状态</th><th>总资产</th><th>行数</th><th>消息</th><th>替换文件</th></tr></thead><tbody>{body_rows}</tbody></table>{pager}</section>"
    return HTMLResponse(base_layout("上传记录", body, session_user(request)))


@app.get("/timeline", response_class=HTMLResponse)
def timeline_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    months = max(1, min(60, int(request.query_params.get("months") or "6")))
    data = analytics_timeline(months)
    items = data["points"]
    latest = items[-1] if items else None
    table_rows = []
    for i in reversed(items):
        change = money(i.get("total_assets_change"), signed=True) if i.get("total_assets_change") is not None else "-"
        compare_link = f"<a href='/compare?from={i['previous_batch_id']}&to={i['batch_id']}'>对比</a>" if i.get("previous_batch_id") else "-"
        table_rows.append(
            f"<tr><td><a href='{batch_link(i['batch_id'])}'>{fmt_date(i.get('as_of_date'))}</a></td><td>{esc(i.get('original_filename'))}</td><td>{money(i.get('total_assets'))}</td><td>{change}</td><td>{money(i.get('today_pnl'), signed=True)}</td><td>{money(i.get('holding_pnl'), signed=True)}</td><td>{money(i.get('cumulative_pnl'), signed=True)}</td><td>{pct(i.get('top5_weight'))}</td><td>{pct(i.get('equity_like_weight'))}</td><td>{pct(i.get('bond_like_weight'))}</td><td>{pct(i.get('qdii_weight'))}</td><td>{pct(i.get('cash_weight'))}</td><td>{compare_link}</td></tr>"
        )
    rows_html = "".join(table_rows) or "<tr><td colspan='13' class='muted'>暂无成功批次</td></tr>"
    summary = metric_cards(latest, batch_metrics(latest["batch_id"])) if latest else ""
    risk = data.get("risk") or {}
    risk_cards = "<section class='metrics'>" + "".join(
        f"<article><span>{esc(label)}</span><strong>{esc(value)}</strong></article>"
        for label, value in [
            ("最大单仓", pct(risk.get("max_position_weight"))),
            ("Top 5", pct(risk.get("top5_weight"))),
            ("最大行业", f"{risk.get('max_industry_name') or '-'} {pct(risk.get('max_industry_weight'))}"),
            ("估算最大回撤", pct(risk.get("max_drawdown_estimated"), signed=True)),
            ("连续亏损批次", str(risk.get("trailing_loss_streak") or 0)),
            ("现金/货币", pct(risk.get("cash_weight"))),
        ]
    ) + "</section>"
    controls = f"<div class='actions'><a href='/timeline?months=3'>近3月</a><a href='/timeline?months=6'>近6月</a><a href='/timeline?months=12'>近12月</a><a href='/api/analytics/timeline?months={months}'>JSON</a></div>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>Portfolio cockpit</p><h1>趋势驾驶舱</h1><p class='subtitle'>用上传文件生成连续组合状态：资产、盈亏、集中度、股债漂移和 X-Ray 风险来源。</p></div></section>{upload_form()}{controls}{summary}{risk_cards}<section class='grid two'><article class='panel'><h2>总资产趋势</h2>{svg_line_chart([('总资产', data['series'].get('assets', []), '#0f766e')], money_values=True)}</article><article class='panel'><h2>盈亏曲线</h2>{svg_line_chart([('当日盈亏', data['series'].get('today_pnl', []), '#be123c'), ('持有盈亏', data['series'].get('holding_pnl', []), '#2563eb'), ('累计盈亏', data['series'].get('cumulative_pnl', []), '#d97706')], money_values=True)}</article></section><section class='grid two'><article class='panel'><h2>集中度变化</h2>{svg_line_chart([('Top3', data['series'].get('top3_weight', []), '#0f766e'), ('Top5', data['series'].get('top5_weight', []), '#2563eb'), ('Top10', data['series'].get('top10_weight', []), '#7c3aed')], pct_values=True)}</article><article class='panel'><h2>股债/QDII/现金漂移</h2>{svg_line_chart([('股性', data['series'].get('equity_like_weight', []), '#be123c'), ('债性', data['series'].get('bond_like_weight', []), '#0f766e'), ('QDII', data['series'].get('qdii_weight', []), '#2563eb'), ('现金', data['series'].get('cash_weight', []), '#475569')], pct_values=True)}</article></section><section class='panel'><h2>资产类型堆叠面积</h2>{svg_stacked_allocation(data.get('allocation_history') or {}, items)}</section><section class='panel'><h2>较上一批 Top 变化</h2>{top_changes_table(data.get('top_changes') or [])}</section>{xray_panel(data.get('xray') or {})}<section class='panel'><h2>批次时间线</h2><table><thead><tr><th>持仓日期</th><th>文件</th><th>总资产</th><th>较上批</th><th>当日盈亏</th><th>持有盈亏</th><th>累计盈亏</th><th>Top5</th><th>股性</th><th>债性</th><th>QDII</th><th>现金</th><th>操作</th></tr></thead><tbody>{rows_html}</tbody></table></section>"
    return HTMLResponse(base_layout("趋势驾驶舱", body, session_user(request)))


@app.get("/reports/{batch_id}", response_class=HTMLResponse)
def report_page(batch_id: str, request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    batch = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,))
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    metrics = batch_metrics(batch_id)
    positions = batch_positions(batch_id, 80)
    alloc = asset_allocation(batch_id)
    industries = industry_allocation(batch_id)
    under_rows = "".join(f"<tr><td>{esc(t.get('underlying_code'))}</td><td>{esc(t.get('underlying_name'))}</td><td>{esc(t.get('underlying_type'))}</td><td>{money(t.get('amount'))}</td><td>{pct(t.get('weight'))}</td><td>{esc(t.get('source_count'))}</td></tr>" for t in underlying(batch_id))
    tx_rows = "".join(f"<tr><td>{fmt_date(t.get('trade_date'))}</td><td>{esc(t.get('transaction_type'))}</td><td>{esc(t.get('security_code') or '')}</td><td>{esc(t.get('security_name') or '')}</td><td>{money(t.get('cash_flow_amount'))}</td><td>{money(t.get('fee'))}</td></tr>" for t in recent_transactions(batch_id))
    prev = previous_success(batch_id)
    compare = f"<a href='/compare?from={prev['batch_id']}&to={batch_id}'>较上一批</a>" if prev else ""
    body = f"<section class='hero compact'><div><p class='eyebrow'>Report</p><h1>{fmt_date(batch.get('as_of_date'))}</h1><p class='subtitle'>{esc(batch_id)}</p></div><div class='hero-meta'><span>状态</span><strong>{esc(batch.get('status'))}</strong><span>上传</span><strong>{fmt_dt(batch.get('uploaded_at'))}</strong></div></section><div class='actions'><a href='/api/portfolio/{batch_id}'>JSON</a>{compare}</div>{metric_cards(batch, metrics)}<section class='grid two'><article class='panel'><h2>资产类型</h2>{bar_chart(alloc, 'allocation_bucket', 'weight')}</article><article class='panel'><h2>行业/主题</h2>{bar_chart(industries, 'industry_name', 'weight')}</article></section><section class='panel'><h2>报告正文</h2><pre class='report-text'>{esc(batch.get('report_markdown'))}</pre></section><section class='panel'><h2>持仓明细</h2>{positions_table(positions)}</section><section class='grid two'><article class='panel'><h2>穿透/代理重仓</h2><table><thead><tr><th>代码</th><th>名称</th><th>类型</th><th>金额</th><th>权重</th><th>来源</th></tr></thead><tbody>{under_rows}</tbody></table></article><article class='panel'><h2>最近交易</h2><table><thead><tr><th>日期</th><th>类型</th><th>代码</th><th>名称</th><th>发生额</th><th>费用</th></tr></thead><tbody>{tx_rows}</tbody></table></article></section>"
    return HTMLResponse(base_layout("报告", body, session_user(request)))


@app.get("/compare", response_class=HTMLResponse)
def compare_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    from_batch = request.query_params.get("from")
    to_batch = request.query_params.get("to")
    if not from_batch or not to_batch:
        raise HTTPException(status_code=400, detail="from and to are required")
    data = compare_batches(from_batch, to_batch)
    rows_html = "".join(f"<tr><td>{esc(i['security_code'])}</td><td>{esc(i['security_name'])}</td><td>{money(i['old_amount'])}</td><td>{money(i['new_amount'])}</td><td>{pct(i['new_weight'] - i['old_weight'], signed=True)}</td><td>{money(i['new_pnl'] - i['old_pnl'])}</td></tr>" for i in data["positions"])
    body = f"<section class='hero compact'><div><p class='eyebrow'>Compare</p><h1>批次对比</h1><p class='subtitle'>{esc(from_batch)} → {esc(to_batch)}</p></div></section><section class='metrics'><article><span>总资产变化</span><strong>{money(data['summary']['total_assets_change'])}</strong></article><article><span>持仓数变化</span><strong>{data['summary']['position_count_change']:+d}</strong></article><article><span>Top5 变化</span><strong>{pct(data['summary']['top5_weight_change'], signed=True)}</strong></article></section><section class='panel'><table><thead><tr><th>代码</th><th>名称</th><th>旧金额</th><th>新金额</th><th>仓位变化</th><th>盈亏变化</th></tr></thead><tbody>{rows_html}</tbody></table></section>"
    return HTMLResponse(base_layout("批次对比", body, session_user(request)))


async def store_upload_file(file: UploadFile, uploaded_at: datetime, as_of_override: date | None, seen_hashes: set[str] | None = None) -> dict[str, Any]:
    filename = file.filename or "portfolio.xlsx"
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail=f"Only .xlsx files are supported: {filename}")
    seen_hashes = seen_hashes if seen_hashes is not None else set()
    day_dir = UPLOAD_ROOT / uploaded_at.strftime("%Y") / uploaded_at.strftime("%m") / uploaded_at.strftime("%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    temp_path = day_dir / f"tmp-{secrets.token_hex(8)}.xlsx"
    size = 0
    with temp_path.open("wb") as handle:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                temp_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail=f"File is too large: {filename}")
            handle.write(chunk)
    digest = sha256_file(temp_path)
    inferred_date = as_of_override or portfolio.infer_as_of_date(temp_path, filename)
    existing = find_existing_upload(digest)
    if existing or digest in seen_hashes:
        temp_path.unlink(missing_ok=True)
        duplicate_of = existing["batch_id"] if existing else "same upload request"
        return {
            "accepted": False,
            "filename": filename,
            "file_sha256": digest,
            "as_of_date": existing.get("as_of_date") if existing else inferred_date,
            "duplicate_of": duplicate_of,
            "reason": "重复文件，已拒收，未保存也未写入记录",
        }
    seen_hashes.add(digest)
    batch_id = make_batch_id(uploaded_at, digest)
    stored_path = day_dir / f"{batch_id}_{safe_filename(filename)}"
    shutil.move(str(temp_path), stored_path)
    result = process_upload(stored_path, filename, uploaded_at, digest, as_of_override or inferred_date)
    result["accepted"] = True
    return result


@app.post("/api/upload")
async def upload_portfolio(request: Request):
    require_user(request)
    form = await request.form()
    upload_items = []
    for field_name in ("files", "file"):
        upload_items.extend(item for item in form.getlist(field_name) if getattr(item, "filename", None))
    if not upload_items:
        raise HTTPException(status_code=400, detail="No files uploaded")
    date_text = str(form.get("as_of_date") or "").strip()
    try:
        parsed_date = date.fromisoformat(date_text) if date_text else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid as_of_date") from exc
    for item in upload_items:
        filename = item.filename or ""
        if not filename.lower().endswith(".xlsx"):
            raise HTTPException(status_code=400, detail=f"Only .xlsx files are supported: {filename}")
    results = []
    skipped = []
    seen_hashes: set[str] = set()
    base_time = now_utc()
    for index, item in enumerate(upload_items):
        result = await store_upload_file(item, base_time + timedelta(microseconds=index), parsed_date, seen_hashes)
        if result.get("accepted"):
            results.append(result)
        else:
            skipped.append(result)
    return upload_result_page(results, skipped, session_user(request))


@app.get("/api/portfolio/latest")
def api_latest(request: Request):
    require_user(request)
    batch = latest_batch()
    return {"ok": True, "batch": json_ready(batch)} if not batch else api_portfolio(batch["batch_id"], request)


@app.get("/api/portfolio/{batch_id}")
def api_portfolio(batch_id: str, request: Request):
    require_user(request)
    batch = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,))
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return json_ready({"ok": True, "batch": batch, "metrics": batch_metrics(batch_id), "positions": batch_positions(batch_id), "asset_allocation": asset_allocation(batch_id), "industry_allocation": industry_allocation(batch_id), "underlying": underlying(batch_id), "transactions": recent_transactions(batch_id)})


@app.get("/api/uploads")
def api_uploads(request: Request):
    require_user(request)
    filters = upload_filters(request)
    return json_ready({"ok": True, "filters": filters, **upload_query(filters)})


@app.get("/api/analytics/timeline")
def api_analytics_timeline(request: Request, months: int = 6):
    require_user(request)
    months = max(1, min(60, months))
    return json_ready({"ok": True, "months": months, **analytics_timeline(months)})


@app.get("/api/analytics/xray")
def api_analytics_xray(request: Request, batch_id: str = ""):
    require_user(request)
    batch = one("SELECT batch_id FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,)) if batch_id else latest_batch()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found")
    return json_ready({"ok": True, **xray_data(batch["batch_id"])})


@app.get("/api/settings/data-sources")
def api_data_sources(request: Request):
    require_user(request)
    with db_conn() as conn:
        return json_ready({"ok": True, "sources": event_provider.data_sources(conn)})


@app.post("/api/settings/data-sources/{source_key}")
async def api_update_data_source(source_key: str, request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    enabled = bool_value(payload.get("enabled"), False if is_form else None)
    fetch_days = int(payload["fetch_days"]) if str(payload.get("fetch_days") or "").strip() else None
    secret = str(payload.get("secret") or payload.get("token") or "").strip() or None
    replace_secret = bool_value(payload.get("replace_secret"), False) is True
    public_config = {key: value for key, value in payload.items() if key.startswith("config_")}
    public_config = {key.removeprefix("config_"): value for key, value in public_config.items()} if public_config else None
    try:
        with db_conn() as conn:
            item = event_provider.update_data_source(conn, source_key, enabled, fetch_days, public_config, secret, replace_secret)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Unknown data source") from exc
    if is_form:
        return RedirectResponse("/settings/data-sources", status_code=303)
    return json_ready({"ok": True, "source": item})


@app.post("/api/settings/data-sources/{source_key}/test")
def api_test_data_source(source_key: str, request: Request):
    require_user(request)
    with db_conn() as conn:
        try:
            result = event_provider.test_source(conn, source_key)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Unknown data source") from exc
    return json_ready(result)


@app.get("/api/settings/ai")
def api_ai_settings(request: Request):
    require_user(request)
    with db_conn() as conn:
        return json_ready({"ok": True, "settings": ai_insights.get_ai_settings(conn)})


@app.post("/api/settings/ai")
async def api_update_ai_settings(request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    provider = str(payload.get("provider") or "none")
    model = str(payload.get("model") or "").strip() or None
    daily_limit = int(payload["daily_limit"]) if str(payload.get("daily_limit") or "").strip() else 30
    api_key = str(payload.get("api_key") or "").strip() or None
    replace_key = bool_value(payload.get("replace_key"), False) is True
    try:
        with db_conn() as conn:
            settings = ai_insights.update_ai_settings(conn, provider, model, daily_limit, api_key, replace_key)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail="Unknown AI provider") from exc
    if is_form:
        return RedirectResponse("/settings/ai", status_code=303)
    return json_ready({"ok": True, "settings": settings})


@app.post("/api/settings/ai/test")
def api_test_ai(request: Request):
    require_user(request)
    with db_conn() as conn:
        return json_ready(ai_insights.test_ai_connection(conn))


@app.post("/api/events/sync-now")
def api_sync_events_now(request: Request):
    require_user(request)
    is_form = is_html_form(request)
    with db_conn() as conn:
        result = event_provider.sync_enabled_sources(conn)
    if is_form:
        return RedirectResponse("/events", status_code=303)
    return json_ready({"ok": True, **result})


@app.get("/api/events")
def api_events(request: Request):
    require_user(request)
    return json_ready({"ok": True, **event_query(event_filters(request))})


@app.get("/api/events/{event_id}")
def api_event_detail(event_id: int, request: Request):
    require_user(request)
    return json_ready({"ok": True, **event_detail(event_id)})


@app.post("/api/events/{event_id}/read")
async def api_mark_event_read(event_id: int, request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    is_read = bool_value(payload.get("is_read"), True)
    is_ignored = bool_value(payload.get("is_ignored"), None)
    execute(
        """
        INSERT INTO portfolio_event_reads (event_id, is_read, is_ignored, read_at, updated_at)
        VALUES (%s, %s, COALESCE(%s, false), CASE WHEN %s THEN NOW() ELSE NULL END, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            is_read = EXCLUDED.is_read,
            is_ignored = COALESCE(%s, portfolio_event_reads.is_ignored),
            read_at = CASE WHEN EXCLUDED.is_read THEN COALESCE(portfolio_event_reads.read_at, NOW()) ELSE NULL END,
            updated_at = NOW()
        """,
        (event_id, is_read, is_ignored, is_read, is_ignored),
    )
    if is_form:
        return RedirectResponse(f"/events/{event_id}", status_code=303)
    return {"ok": True}


@app.post("/api/events/{event_id}/favorite")
async def api_favorite_event(event_id: int, request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    favorite = bool_value(payload.get("is_favorite"), True)
    execute(
        """
        INSERT INTO portfolio_event_reads (event_id, is_favorite, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            is_favorite = EXCLUDED.is_favorite,
            updated_at = NOW()
        """,
        (event_id, favorite),
    )
    if is_form:
        return RedirectResponse(f"/events/{event_id}", status_code=303)
    return {"ok": True}


@app.post("/api/events/{event_id}/ignore")
async def api_ignore_event(event_id: int, request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    ignored = bool_value(payload.get("is_ignored"), True)
    execute(
        """
        INSERT INTO portfolio_event_reads (event_id, is_ignored, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (event_id) DO UPDATE SET
            is_ignored = EXCLUDED.is_ignored,
            updated_at = NOW()
        """,
        (event_id, ignored),
    )
    if is_form:
        return RedirectResponse(f"/events/{event_id}", status_code=303)
    return {"ok": True}


@app.post("/api/events/{event_id}/ai-insight")
async def api_event_ai_insight(event_id: int, request: Request):
    require_user(request)
    payload = await request_payload(request)
    is_form = is_html_form(request)
    force = bool_value(payload.get("force"), False) is True
    with db_conn() as conn:
        try:
            result = ai_insights.generate_insight(conn, event_id, force)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Event not found") from exc
    if is_form:
        return RedirectResponse(f"/events/{event_id}", status_code=303)
    return json_ready(result)


@app.post("/api/uploads/{batch_id}/replace")
async def replace_batch(batch_id: str, request: Request, file: UploadFile = File(...)):
    require_user(request)
    existing = one("SELECT * FROM portfolio_import_batches WHERE batch_id = %s", (batch_id,))
    if not existing:
        raise HTTPException(status_code=404, detail="Batch not found")
    filename = file.filename or "portfolio.xlsx"
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")
    uploaded_at = now_utc()
    day_dir = UPLOAD_ROOT / uploaded_at.strftime("%Y") / uploaded_at.strftime("%m") / uploaded_at.strftime("%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    temp_path = day_dir / f"tmp-replace-{secrets.token_hex(8)}.xlsx"
    size = 0
    with temp_path.open("wb") as handle:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_UPLOAD_BYTES:
                temp_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail=f"File is too large: {filename}")
            handle.write(chunk)
    digest = sha256_file(temp_path)
    if digest == existing.get("file_sha256"):
        temp_path.unlink(missing_ok=True)
        body = "<section class='hero compact'><div><p class='eyebrow'>No change</p><h1>无需替换</h1><p class='subtitle'>上传文件与当前批次完全相同，系统没有做任何改动。</p></div></section><div class='actions'><a href='/uploads'>返回上传记录</a></div>"
        return HTMLResponse(base_layout("无需替换", body, session_user(request)))
    duplicate = one(
        "SELECT batch_id FROM portfolio_import_batches WHERE file_sha256 = %s AND batch_id <> %s AND status IN ('complete', 'partial') LIMIT 1",
        (digest, batch_id),
    )
    if duplicate:
        temp_path.unlink(missing_ok=True)
        body = f"<section class='hero compact'><div><p class='eyebrow'>Replace rejected</p><h1>替换失败</h1><p class='subtitle'>这个文件和已有批次 {esc(duplicate['batch_id'])} 完全相同，已拒收。</p></div></section><div class='actions'><a href='/uploads'>返回上传记录</a></div>"
        return HTMLResponse(base_layout("替换失败", body, session_user(request)), status_code=409)
    stored_path = day_dir / f"{batch_id}_replacement_{safe_filename(filename)}"
    shutil.move(str(temp_path), stored_path)
    old_file = existing.get("source_file")
    meta = existing.get("meta_json") or {}
    history = list(meta.get("replacement_history") or [])
    history.append({
        "replaced_at": uploaded_at.isoformat(),
        "old_filename": existing.get("original_filename"),
        "old_file_sha256": existing.get("file_sha256"),
        "old_source_file": old_file,
        "new_filename": filename,
        "new_file_sha256": digest,
    })
    execute("UPDATE portfolio_import_batches SET meta_json = %s WHERE batch_id = %s", (Json(meta | {"replacement_history": history}), batch_id))
    as_of_date = portfolio.infer_as_of_date(stored_path, filename) or existing.get("as_of_date")
    result = process_upload(stored_path, filename, uploaded_at, digest, as_of_date, batch_id)
    if old_file and old_file != str(stored_path):
        try:
            Path(old_file).rename(Path(old_file).with_suffix(Path(old_file).suffix + ".replaced"))
        except OSError:
            pass
    return RedirectResponse(batch_link(result["batch_id"]), status_code=303)



def table_counts() -> list[dict[str, Any]]:
    names = [
        "portfolio_import_batches",
        "portfolio_positions",
        "portfolio_transactions",
        "portfolio_asset_allocation",
        "portfolio_underlying_holdings",
        "portfolio_daily_summary",
        "portfolio_daily_allocation",
        "portfolio_daily_exposure",
        "portfolio_events",
        "portfolio_event_symbols",
        "portfolio_event_fetch_runs",
    ]
    return [
        {"table": name, "rows": (one(f"SELECT COUNT(*) AS count FROM {name}") or {}).get("count", 0)}
        for name in names
    ]


def directory_stats(path: Path) -> dict[str, Any]:
    files = 0
    bytes_used = 0
    latest_mtime: float | None = None
    if path.exists():
        for item in path.rglob("*"):
            if item.is_file():
                files += 1
                try:
                    stat = item.stat()
                except OSError:
                    continue
                bytes_used += stat.st_size
                latest_mtime = stat.st_mtime if latest_mtime is None else max(latest_mtime, stat.st_mtime)
    return {
        "path": str(path),
        "files": files,
        "bytes": bytes_used,
        "latest_mtime": datetime.fromtimestamp(latest_mtime, tz=timezone.utc) if latest_mtime else None,
    }


def data_health() -> dict[str, Any]:
    batch_total = one("SELECT COUNT(*) AS count FROM portfolio_import_batches")["count"]
    success_total = one("SELECT COUNT(*) AS count FROM portfolio_import_batches WHERE status IN ('complete', 'partial')")["count"]
    summary_total = one(
        """
        SELECT COUNT(*) AS count
        FROM portfolio_daily_summary s
        JOIN portfolio_import_batches b ON b.batch_id = s.batch_id
        WHERE b.status IN ('complete', 'partial')
        """
    )["count"]
    missing = rows(
        """
        SELECT b.batch_id, b.as_of_date, b.original_filename
        FROM portfolio_import_batches b
        LEFT JOIN portfolio_daily_summary s ON s.batch_id = b.batch_id
        WHERE b.status IN ('complete', 'partial') AND s.batch_id IS NULL
        ORDER BY b.as_of_date DESC, b.uploaded_at DESC
        LIMIT 20
        """
    )
    upload_stats = directory_stats(UPLOAD_ROOT)
    backup_stats = directory_stats(BACKUP_ROOT)
    disk = shutil.disk_usage(UPLOAD_ROOT if UPLOAD_ROOT.exists() else UPLOAD_ROOT.parent)
    return {
        "batch_total": batch_total,
        "success_total": success_total,
        "summary_total": summary_total,
        "summary_coverage": (summary_total / success_total) if success_total else 1,
        "missing_summaries": missing,
        "tables": table_counts(),
        "uploads": upload_stats,
        "backups": backup_stats,
        "disk": {"total": disk.total, "used": disk.used, "free": disk.free},
    }


def size_text(value: Any) -> str:
    number = float(value or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if number < 1024 or unit == "TB":
            return f"{number:.1f} {unit}" if unit != "B" else f"{number:.0f} B"
        number /= 1024
    return f"{number:.1f} TB"


@app.get("/admin/data", response_class=HTMLResponse)
def admin_data_page(request: Request):
    redirect = redirect_if_guest(request)
    if redirect:
        return redirect
    health = data_health()
    table_rows = "".join(f"<tr><td>{esc(item['table'])}</td><td>{esc(item['rows'])}</td></tr>" for item in health["tables"])
    missing_rows = "".join(
        f"<tr><td><a href='{batch_link(item['batch_id'])}'>{esc(item['batch_id'])}</a></td><td>{fmt_date(item.get('as_of_date'))}</td><td>{esc(item.get('original_filename'))}</td></tr>"
        for item in health["missing_summaries"]
    ) or "<tr><td colspan='3' class='muted'>所有成功批次都有 summary</td></tr>"
    body = f"<section class='hero compact'><div><p class='eyebrow'>Data health</p><h1>数据健康</h1><p class='subtitle'>检查长期运行需要关心的批次、summary 覆盖率、文件和备份状态。</p></div></section><section class='metrics'><article><span>总批次</span><strong>{health['batch_total']}</strong></article><article><span>成功批次</span><strong>{health['success_total']}</strong></article><article><span>Summary 覆盖</span><strong>{pct(health['summary_coverage'])}</strong></article><article><span>上传文件</span><strong>{health['uploads']['files']}</strong></article><article><span>上传占用</span><strong>{size_text(health['uploads']['bytes'])}</strong></article><article><span>磁盘可用</span><strong>{size_text(health['disk']['free'])}</strong></article></section><section class='grid two'><article class='panel'><h2>表规模</h2><table><thead><tr><th>表</th><th>行数</th></tr></thead><tbody>{table_rows}</tbody></table></article><article class='panel'><h2>文件与备份</h2><table><tbody><tr><th>上传目录</th><td>{esc(health['uploads']['path'])}</td></tr><tr><th>最近上传文件时间</th><td>{fmt_dt(health['uploads']['latest_mtime'])}</td></tr><tr><th>备份目录</th><td>{esc(health['backups']['path'])}</td></tr><tr><th>备份文件数</th><td>{esc(health['backups']['files'])}</td></tr><tr><th>最近备份时间</th><td>{fmt_dt(health['backups']['latest_mtime'])}</td></tr><tr><th>备份占用</th><td>{size_text(health['backups']['bytes'])}</td></tr></tbody></table></article></section><section class='panel'><div class='section-head'><h2>缺失 Summary 的批次</h2><form class='inline-form' action='/admin/data/rebuild-summaries' method='post'><button type='submit'>重建 Summary</button></form></div><table><thead><tr><th>批次</th><th>日期</th><th>文件</th></tr></thead><tbody>{missing_rows}</tbody></table></section>"
    return HTMLResponse(base_layout("数据健康", body, session_user(request)))


@app.post("/admin/data/rebuild-summaries")
def rebuild_summaries_page(request: Request):
    require_user(request)
    with db_conn() as conn:
        count = rebuild_all_summaries(conn)
        conn.commit()
    body = f"<section class='hero compact'><div><p class='eyebrow'>Rebuild complete</p><h1>Summary 已重建</h1><p class='subtitle'>已重建 {count} 个成功批次的长期分析汇总。</p></div></section><div class='actions'><a href='/admin/data'>返回数据健康</a><a href='/timeline'>查看趋势</a></div>"
    return HTMLResponse(base_layout("Summary 已重建", body, session_user(request)))


@app.get("/api/admin/data-health")
def api_data_health(request: Request):
    require_user(request)
    return json_ready({"ok": True, **data_health()})


@app.get("/api/health")
def health():
    return {"ok": True, "app": "portfolio", "time": now_utc().isoformat()}


CSS = """
:root { color-scheme: light; --bg: #f6f7f4; --ink: #151817; --muted: #65706b; --line: #dfe4df; --panel: #fff; --accent: #0f766e; --bad: #b91c1c; }
* { box-sizing: border-box; } body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; } a { color: inherit; } .topbar { position: sticky; top: 0; z-index: 10; display:flex; justify-content:space-between; align-items:center; padding: 14px 28px; border-bottom:1px solid var(--line); background: rgba(246,247,244,.94); backdrop-filter: blur(10px); } .brand { font-weight: 800; text-decoration:none; } nav { display:flex; gap: 18px; color: var(--muted); flex-wrap:wrap; } nav a { text-decoration:none; } main { width:min(1440px, 100%); margin:0 auto; padding:28px; } .hero { display:flex; justify-content:space-between; align-items:end; gap:24px; min-height: 190px; padding: 34px 0 28px; border-bottom:1px solid var(--line); } .hero.compact { min-height: 130px; } .eyebrow { margin:0 0 8px; color:var(--accent); font-weight:800; text-transform:uppercase; } h1 { margin:0; font-size: clamp(42px, 7vw, 88px); line-height:.92; letter-spacing:0; } h2 { margin:0 0 14px; font-size: 18px; } .subtitle { margin:14px 0 0; max-width: 800px; color: var(--muted); font-size: 17px; } .hero-meta { display:grid; gap:4px; text-align:right; min-width:230px; } .hero-meta span, .muted { color:var(--muted); } .upload, .filters, .settings-form { display:flex; flex-wrap:wrap; gap:12px; align-items:end; padding:16px; margin:18px 0; border:1px solid var(--line); background:var(--panel); border-radius:8px; } label { display:grid; gap:6px; color:var(--muted); font-weight:700; } label.check { display:flex; align-items:center; gap:8px; min-height:40px; } label.check input { min-height:auto; } input, select { min-height:40px; padding:8px 10px; border:1px solid var(--line); border-radius:6px; background:#fff; color:var(--ink); } button, .actions a, .section-head a, .pager a { display:inline-flex; align-items:center; min-height:40px; padding:0 14px; border:1px solid #0f766e; border-radius:6px; background:#0f766e; color:white; text-decoration:none; font-weight:800; } .actions { display:flex; gap:10px; margin:16px 0; flex-wrap:wrap; } .metrics { display:grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap:12px; margin: 18px 0; } .metrics article, .panel { border:1px solid var(--line); border-radius:8px; background:var(--panel); } .metrics article { padding:14px; } .metrics span { display:block; color:var(--muted); font-size:12px; text-transform:uppercase; } .metrics strong { display:block; margin-top:6px; font-size:22px; overflow-wrap:anywhere; } .grid { display:grid; gap:14px; margin:14px 0; } .grid.two { grid-template-columns: repeat(2, minmax(0, 1fr)); } .panel { padding:16px; overflow:hidden; margin:14px 0; } .section-head { display:flex; justify-content:space-between; align-items:center; gap:16px; } .bars { display:grid; gap:10px; } .bar-row { display:grid; grid-template-columns: minmax(110px, 190px) minmax(120px, 1fr) 90px; gap:10px; align-items:center; } .bar-row span { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; } .bar-row div { height:12px; background:#edf1ed; border-radius:999px; overflow:hidden; } .bar-row i { display:block; height:100%; background:#0f766e; border-radius:999px; } .bar-row b { text-align:right; font-size:12px; } .table-wrap { overflow:auto; } table { width:100%; border-collapse:collapse; font-size:13px; } th, td { padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; } th { color:var(--muted); font-size:12px; text-transform:uppercase; } tr.status-failed td { color: var(--bad); } tr.status-duplicate td, tr.status-partial td { color:#92400e; } tr.event-row.is-read td { color: var(--muted); } .insight { padding:14px; border:1px solid var(--line); border-radius:8px; background:#fbfcfa; } .report-text { white-space:pre-wrap; margin:0; padding:14px; max-height: 520px; overflow:auto; border:1px solid var(--line); border-radius:8px; background:#fbfcfa; color:#23302a; } .login { width:min(420px, 100%); margin:80px auto; padding:24px; border:1px solid var(--line); border-radius:8px; background:var(--panel); } .login h1 { font-size:42px; margin-bottom:20px; } .login form { display:grid; gap:14px; } .form-hint { align-self:center; margin:0; color:var(--muted); max-width:420px; } .chart { width:100%; height:auto; min-height:220px; overflow:visible; } .chart line { stroke:var(--line); } .chart text { fill:var(--muted); font-size:12px; } .chart-legend { display:flex; flex-wrap:wrap; gap:12px; margin:0 0 8px; color:var(--muted); font-weight:700; } .chart-legend span { display:inline-flex; align-items:center; gap:6px; } .chart-legend i { width:10px; height:10px; border-radius:999px; display:inline-block; } .month-grid { display:flex; flex-wrap:wrap; gap:10px; } .month-chip { display:inline-grid; gap:2px; min-width:112px; padding:10px 12px; border:1px solid var(--line); border-radius:8px; background:#fbfcfa; text-decoration:none; } .month-chip.active { border-color:var(--accent); box-shadow: inset 0 0 0 1px var(--accent); } .month-chip b { color:var(--accent); } .inline-form { display:flex; gap:8px; align-items:center; margin:0; } .inline-form input { max-width:220px; } .inline-form button { min-height:36px; } .pager { display:flex; gap:12px; justify-content:flex-end; align-items:center; margin-top:14px; color:var(--muted); }
@media (max-width: 900px) { main { padding:18px; } .hero { display:block; } .hero-meta { text-align:left; margin-top:18px; } .grid.two, .metrics { grid-template-columns:1fr; } .bar-row { grid-template-columns: 1fr; gap:4px; } .bar-row b { text-align:left; } table { min-width: 900px; } .panel { overflow:auto; } }
"""
