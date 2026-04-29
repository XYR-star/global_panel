from __future__ import annotations

import os
import json
import html
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg2
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import Json, RealDictCursor


APP_ROOT = os.path.dirname(__file__)
STOP_TICKERS = {"AI", "API", "ETF", "SEC", "GDP", "CPI", "FRED", "USD", "A股", "OK"}


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@contextmanager
def db():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=env("POSTGRES_DB"),
        user=env("POSTGRES_USER"),
        password=env("POSTGRES_PASSWORD"),
    )
    try:
        yield conn
    finally:
        conn.close()


def rows(sql: str, params: tuple[Any, ...] = ()):
    with db() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def one(sql: str, params: tuple[Any, ...] = ()):
    result = rows(sql, params)
    return result[0] if result else None


def iso(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def serialize(items):
    for item in items:
        for key, value in list(item.items()):
            item[key] = iso(value)
    return items


def latest_status():
    return serialize(
        rows(
            """
            SELECT source, ok, message, latest_observation_ts, last_run,
                   ROUND(EXTRACT(EPOCH FROM (last_run - latest_observation_ts)) / 3600, 1) AS lag_hours
            FROM data_sync_status
            WHERE source <> 'fredgraph'
            ORDER BY source
            """
        )
    )


def market_summary():
    return {
        "latest_report": latest_report(),
        "status": latest_status(),
        "cn_indices": serialize(
            rows(
                """
                SELECT asset_key, asset_name, value, change, change_percent, ts, updated_at
                FROM market_asset_snapshot
                WHERE category = 'cn_indices'
                ORDER BY asset_name
                """
            )
        ),
        "cn_quotes": serialize(
            rows(
                """
                SELECT asset_key, asset_name, value, change_percent, ts, meta_json
                FROM market_asset_snapshot
                WHERE category = 'cn_equity_quote'
                ORDER BY ABS(COALESCE(change_percent, 0)) DESC, asset_name
                LIMIT 18
                """
            )
        ),
        "us_equities": serialize(
            rows(
                """
                SELECT asset_key, asset_name, regexp_replace(asset_key, '^YFUS:', '') AS source_symbol,
                       value, change_percent, ts, meta_json
                FROM market_asset_snapshot
                WHERE category = 'us_equity_daily'
                ORDER BY ABS(COALESCE(change_percent, 0)) DESC, asset_name
                LIMIT 18
                """
            )
        ),
        "fund_flow": serialize(
            rows(
                """
                SELECT asset_key, asset_name, value, ts
                FROM market_asset_snapshot
                WHERE category = 'cn_fund_flow'
                ORDER BY value DESC
                LIMIT 12
                """
            )
        ),
        "market_flow": serialize(
            rows(
                """
                SELECT asset_key, asset_name, value, ts
                FROM market_asset_snapshot
                WHERE category = 'cn_market_fund_flow'
                ORDER BY ts DESC
                LIMIT 8
                """
            )
        ),
        "concepts": serialize(
            rows(
                """
                SELECT asset_key, asset_name, value, change_percent, ts, meta_json
                FROM market_asset_snapshot
                WHERE category = 'cn_concept'
                ORDER BY ABS(COALESCE((meta_json->>'change_percent')::double precision, change_percent, 0)) DESC
                LIMIT 18
                """
            )
        ),
        "cn_text_evidence": serialize(
            rows(
                """
                SELECT asset_name, category, ts, title, body, url, meta_json
                FROM market_text_records
                WHERE source = 'akshare'
                  AND category IN ('cn_company_news', 'cn_financial_indicator', 'cn_industry_constituents', 'cn_company_announcement')
                ORDER BY ts DESC
                LIMIT 24
                """
            )
        ),
        "sec_filings": serialize(
            rows(
                """
                SELECT asset_name, source_symbol, ts, title, url, meta_json
                FROM market_text_records
                WHERE source = 'sec_edgar'
                ORDER BY ts DESC
                LIMIT 12
                """
            )
        ),
        "asset_counts": rows(
            """
            SELECT provider, category, count(*) AS rows, max(latest_observation_ts) AS latest
            FROM data_asset_catalog
            GROUP BY provider, category
            ORDER BY provider, category
            """
        ),
    }


def ai_context():
    data = market_summary()
    return {
        "latest_report": data.get("latest_report"),
        "status": data["status"],
        "cn_indices": data["cn_indices"],
        "active_cn_quotes": data["cn_quotes"],
        "us_equity_daily": data["us_equities"],
        "cn_fund_flow": data["fund_flow"],
        "cn_market_fund_flow": data["market_flow"],
        "cn_concepts": data["concepts"],
        "cn_text_evidence": data["cn_text_evidence"],
        "sec_filings": data["sec_filings"],
        "asset_counts": serialize(data["asset_counts"]),
    }


def latest_report():
    report = one(
        """
        SELECT report_id, report_type, status, title, summary, stance, confidence,
               recommendations, risks, watchlist, model, generated_at, data_as_of, error_message
        FROM agent_reports
        WHERE report_type = 'market_research_v1'
        ORDER BY generated_at DESC
        LIMIT 1
        """
    )
    if not report:
        return None
    report = serialize([report])[0]
    report["evidence"] = serialize(
        rows(
            """
            SELECT evidence_type, title, detail, source, asset_key, ts, url
            FROM agent_report_evidence
            WHERE report_id = %s
            ORDER BY evidence_type, ts DESC NULLS LAST
            LIMIT 18
            """,
            (report["report_id"],),
        )
    )
    return report


def fmt_dt(value):
    if not value:
        return "-"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_num(value, digits=2):
    if value is None:
        return "-"
    return f"{float(value):,.{digits}f}"


def fmt_pct(value):
    if value is None:
        return "-"
    return f"{float(value):+.2f}%"


def money_cn(value):
    if value is None:
        return "-"
    value = float(value)
    if abs(value) >= 100_000_000:
        return f"{value / 100_000_000:+,.2f} 亿"
    if abs(value) >= 10_000:
        return f"{value / 10_000:+,.2f} 万"
    return f"{value:+,.0f}"


def status_label(ok):
    return "OK" if ok else "同步失败"


def status_card_class(item):
    message = item.get("message") or ""
    if not item.get("ok"):
        return "bad"
    if message.startswith("Using cached data"):
        return "stale"
    return ""


def status_text(item):
    message = item.get("message") or ""
    if message.startswith("Using cached data"):
        return "使用缓存"
    return status_label(item.get("ok"))


def esc(value):
    return html.escape("" if value is None else str(value))


def pill_class(status):
    if status == "ok":
        return "ok"
    if status == "needs_config":
        return "warn"
    return "bad"


def list_items(values):
    if not values:
        return "<li>No items yet.</li>"
    return "\n".join(f"<li>{esc(value)}</li>" for value in values)


def quote_change(item):
    meta = item.get("meta_json")
    if not isinstance(meta, dict):
        meta = {}
    return meta.get("change_percent")


def json_ready(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    return value


def normalize_symbol(token: str) -> list[str]:
    token = token.strip().upper().replace(".SH", ".SS")
    if re.fullmatch(r"\d{6}", token):
        if token.startswith(("5", "6", "9")):
            return [f"{token}.SS"]
        return [f"{token}.SZ"]
    if re.fullmatch(r"\d{6}\.(SS|SZ)", token):
        return [token]
    if re.fullmatch(r"[A-Z]{1,5}", token) and token not in STOP_TICKERS:
        return [token]
    return []


def canonical_cn_asset_key(source_symbol: str) -> str | None:
    if source_symbol.endswith((".SS", ".SZ")) and re.fullmatch(r"\d{6}\.(SS|SZ)", source_symbol):
        return f"CN:{source_symbol[:6]}"
    return None


def search_assets(query: str, limit: int = 20):
    query = query.strip()
    if not query:
        return []
    pattern = f"%{query}%"
    symbols = [symbol.upper() for token in re.findall(r"\b\d{6}(?:\.(?:SS|SZ|SH))?\b|\b[A-Z]{1,5}\b", query.upper()) for symbol in normalize_symbol(token)]
    return serialize(
        rows(
            """
            WITH alias_hits AS (
                SELECT DISTINCT asset_key, source_symbol, market, provider, alias, alias_type
                FROM asset_aliases
                WHERE alias ILIKE %s OR source_symbol ILIKE %s OR upper(source_symbol) = ANY(%s)
            ),
            catalog_hits AS (
                SELECT DISTINCT asset_key, source_symbol, market, provider, asset_name, category
                FROM data_asset_catalog
                WHERE asset_name ILIKE %s OR source_symbol ILIKE %s OR upper(source_symbol) = ANY(%s)
            ),
            merged AS (
                SELECT
                    asset_key,
                    alias AS asset_name,
                    market,
                    provider,
                    source_symbol,
                    NULL::text AS category,
                    alias,
                    alias_type
                FROM alias_hits
                UNION ALL
                SELECT
                    asset_key,
                    asset_name,
                    market,
                    provider,
                    source_symbol,
                    category,
                    NULL::text AS alias,
                    NULL::text AS alias_type
                FROM catalog_hits
            )
            SELECT asset_key,
                   max(asset_name) AS asset_name,
                   max(market) AS market,
                   max(provider) AS provider,
                   max(source_symbol) AS source_symbol,
                   max(category) AS category,
                   array_agg(DISTINCT alias) FILTER (WHERE alias IS NOT NULL) AS matched_aliases
            FROM merged
            GROUP BY asset_key
            ORDER BY max(market), max(source_symbol)
            LIMIT %s
            """,
            (pattern, pattern, symbols or [""], pattern, pattern, symbols or [""], limit),
        )
    )


def detect_assets(question: str):
    raw_tokens = set(re.findall(r"\b\d{6}(?:\.(?:SS|SZ|SH))?\b|\b[A-Z]{1,5}\b", question.upper()))
    symbols = sorted({symbol for token in raw_tokens for symbol in normalize_symbol(token)})
    aliases = rows(
        """
        SELECT alias, asset_key, source_symbol, market, alias_type, provider
        FROM asset_aliases
        ORDER BY length(alias) DESC
        LIMIT 1000
        """
    )
    alias_matches = []
    alias_asset_keys = []
    question_upper = question.upper()
    for item in aliases:
        alias = str(item.get("alias") or "")
        if not alias:
            continue
        if alias.upper() in question_upper or alias in question:
            alias_matches.append(item)
            alias_asset_keys.append(item["asset_key"])
            if item.get("source_symbol"):
                symbols.append(str(item["source_symbol"]).upper())
    symbols = sorted(set(symbols))
    catalog = rows(
        """
        SELECT asset_key, asset_name, market, category, provider, source_symbol, latest_observation_ts
        FROM data_asset_catalog
        WHERE provider IN ('yahoo_finance', 'akshare', 'sec_edgar')
        ORDER BY latest_observation_ts DESC NULLS LAST
        LIMIT 500
        """
    )
    matched = []
    seen = set()
    for item in catalog:
        source_symbol = str(item.get("source_symbol") or "").upper()
        asset_name = str(item.get("asset_name") or "")
        if (
            item.get("asset_key") in alias_asset_keys
            or source_symbol in symbols
            or (asset_name and asset_name in question)
            or (source_symbol and source_symbol in question_upper)
        ):
            key = (item["provider"], item["category"], item["asset_key"])
            if key not in seen:
                matched.append(item)
                seen.add(key)
    if not matched and symbols:
        matched = rows(
            """
            SELECT asset_key, asset_name, market, category, provider, source_symbol, latest_observation_ts
            FROM data_asset_catalog
            WHERE upper(source_symbol) = ANY(%s)
            ORDER BY latest_observation_ts DESC NULLS LAST
            LIMIT 20
            """,
            (symbols,),
        )
    for item in matched:
        item["matched_aliases"] = [
            alias["alias"]
            for alias in alias_matches
            if alias["asset_key"] == item["asset_key"] or alias["source_symbol"] == item.get("source_symbol")
        ][:6]
    return matched[:16], symbols


def summarize_bars(source_symbol: str):
    bar_rows = rows(
        """
        SELECT asset_key, asset_name, market, category, source_symbol, ts, open, high, low, close, adj_close, volume
        FROM daily_bars
        WHERE upper(source_symbol) = upper(%s)
        ORDER BY ts DESC
        LIMIT 130
        """,
        (source_symbol,),
    )
    if not bar_rows:
        return None
    ordered = list(reversed(bar_rows))
    closes = [float(item["close"]) for item in ordered if item.get("close") is not None]
    latest = ordered[-1]

    def pct(days: int):
        if len(closes) <= days or closes[-days - 1] == 0:
            return None
        return (closes[-1] / closes[-days - 1] - 1) * 100

    return {
        "asset_key": latest["asset_key"],
        "asset_name": latest["asset_name"],
        "market": latest["market"],
        "source_symbol": latest["source_symbol"],
        "latest_date": latest["ts"],
        "latest_close": latest["close"],
        "latest_volume": latest["volume"],
        "return_5d_pct": pct(5),
        "return_20d_pct": pct(20),
        "return_60d_pct": pct(60),
        "recent_bars": ordered[-20:],
    }


def text_evidence_for_assets(asset_keys: list[str], source_symbols: list[str], limit: int = 30):
    if not asset_keys and not source_symbols:
        return []
    return serialize(
        rows(
            """
            SELECT evidence_id, evidence_type, asset_key, asset_name, market, source, source_symbol,
                   ts, title, url, summary, body_excerpt, meta_json
            FROM evidence_items
            WHERE asset_key = ANY(%s) OR upper(source_symbol) = ANY(%s)
            ORDER BY ts DESC NULLS LAST
            LIMIT %s
            """,
            (asset_keys or [""], [symbol.upper() for symbol in source_symbols] or [""], limit),
        )
    )


def macro_context():
    return serialize(
        rows(
            """
            SELECT asset_key, asset_name, category, value, change_percent, ts
            FROM market_asset_snapshot
            WHERE category IN ('rates', 'credit', 'commodities', 'fx', 'macro', 'liquidity', 'shipping')
            ORDER BY category, asset_key
            LIMIT 80
            """
        )
    )


def build_agent_trace(question: str, detected_assets: list[dict], daily: list[dict], evidence: list[dict], macro: list[dict]):
    return {
        "IntentAgent": {
            "question": question,
            "assets": [
                {
                    "asset_key": item.get("asset_key"),
                    "asset_name": item.get("asset_name"),
                    "source_symbol": item.get("source_symbol"),
                    "market": item.get("market"),
                    "matched_aliases": item.get("matched_aliases") or [],
                }
                for item in detected_assets
            ],
        },
        "MarketDataAgent": {"daily_bars_summary": daily},
        "MacroAgent": {"macro_cross_asset": macro},
        "FundamentalAgent": {
            "fundamental_evidence": [
                item for item in evidence if item.get("evidence_type") in {"cn_financial", "sec_filing"}
            ][:12]
        },
        "NewsFilingAgent": {
            "news_and_filings": [
                item for item in evidence if item.get("evidence_type") in {"cn_news", "cn_announcement", "sec_filing"}
            ][:20]
        },
        "RiskAgent": {
            "required_sections": ["风险", "数据缺口", "反方观点"],
            "constraints": ["不提供仓位比例", "不承诺收益", "引用证据日期"],
        },
    }


def build_question_context(question: str):
    detected_assets, symbols = detect_assets(question)
    source_symbols = sorted({item["source_symbol"] for item in detected_assets if item.get("source_symbol")})
    if not source_symbols and symbols:
        source_symbols = symbols[:8]
    daily = [summary for symbol in source_symbols for summary in [summarize_bars(symbol)] if summary]
    asset_keys = [item["asset_key"] for item in detected_assets]
    for symbol in source_symbols:
        if symbol.endswith((".SS", ".SZ")):
            asset_keys.append(f"CN:{symbol[:6]}")
    asset_keys = sorted(set(asset_keys))
    related_text = text_evidence_for_assets(asset_keys, source_symbols, 32)
    macro = macro_context()
    status = latest_status()
    inventory = rows(
        """
        SELECT provider, category, count(*) AS assets, max(latest_observation_ts) AS latest
        FROM data_asset_catalog
        GROUP BY provider, category
        ORDER BY provider, category
        """
    )
    agent_trace = build_agent_trace(question, detected_assets, daily, related_text, macro)
    return {
        "question": question,
        "agent_trace": agent_trace,
        "detected_symbols": symbols,
        "matched_assets": detected_assets,
        "daily_bars_summary": daily,
        "related_text_evidence": related_text,
        "macro_cross_asset": macro,
        "data_status": status,
        "data_inventory": inventory,
    }


def save_qa(question: str, answer: str, model: str | None, ok: bool, context: dict[str, Any], error_message: str | None = None):
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_question_answers
                    (qa_id, question, answer, model, ok, context_json, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    f"qa:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{uuid.uuid4().hex[:8]}",
                    question,
                    answer,
                    model,
                    ok,
                    Json(json_ready(context)),
                    error_message,
                ),
            )
            conn.commit()
    except Exception:
        pass


def ask_model(question: str):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "ok": False,
            "answer": "还没有配置模型 API Key。请在服务器环境里设置 OPENAI_API_KEY。",
            "model": None,
        }
    question = question.strip()
    if not question:
        return {"ok": False, "answer": "请输入一个问题。", "model": None}
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    research_context = build_question_context(question)
    context = json.dumps(json_ready(research_context), ensure_ascii=False)[:60000]
    response = requests.post(
        f"{base_url}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是一个中文金融研究 Agent。只能基于提供的数据回答。"
                        "必须按固定结构输出：结论、关键事实、推断、风险、数据缺口、证据列表、非投资建议声明。"
                        "证据列表必须包含来源、标题、日期、证据类型、URL（如有）。"
                        "如果数据不足，请明确说数据不足，并说明还需要什么。"
                        "不要给实盘下单指令、仓位比例或保证收益。"
                    ),
                },
                {
                    "role": "user",
                    "content": f"问题：{question}\n\n可用数据：\n{context}",
                },
            ],
            "temperature": float(os.getenv("AGENT_TEMPERATURE", "0.2")),
        },
        timeout=int(os.getenv("AGENT_LLM_TIMEOUT_SECONDS", "90")),
    )
    response.raise_for_status()
    payload = response.json()
    answer = payload["choices"][0]["message"]["content"]
    save_qa(question, answer, model, True, research_context)
    return {"ok": True, "answer": answer, "model": model}


def resolve_asset_query(symbol_or_query: str):
    matches = search_assets(symbol_or_query, 8)
    if matches:
        return matches[0]
    normalized = normalize_symbol(symbol_or_query)
    if normalized:
        symbol = normalized[0]
        return one(
            """
            SELECT asset_key, asset_name, market, provider, source_symbol, category
            FROM data_asset_catalog
            WHERE upper(source_symbol) = upper(%s)
            ORDER BY latest_observation_ts DESC NULLS LAST
            LIMIT 1
            """,
            (symbol,),
        )
    return None


def asset_summary(symbol_or_query: str):
    asset = resolve_asset_query(symbol_or_query)
    if not asset:
        return {"ok": False, "message": "没有找到匹配资产", "query": symbol_or_query}
    source_symbol = asset.get("source_symbol")
    asset_keys = [asset.get("asset_key")]
    cn_key = canonical_cn_asset_key(source_symbol or "")
    if cn_key:
        asset_keys.append(cn_key)
    bars = summarize_bars(source_symbol) if source_symbol else None
    evidence = text_evidence_for_assets([key for key in asset_keys if key], [source_symbol] if source_symbol else [], 24)
    fundamentals = [
        item for item in evidence if item.get("evidence_type") in {"cn_financial", "sec_filing"}
    ][:10]
    return {
        "ok": True,
        "asset": serialize([asset])[0],
        "daily_bars_summary": json_ready(bars),
        "evidence": evidence,
        "fundamentals": fundamentals,
        "macro": macro_context(),
    }


app = FastAPI(title="Market AI Research", root_path="/ai")
app.mount("/static", StaticFiles(directory=os.path.join(APP_ROOT, "static")), name="static")


@app.get("/api/health")
def health():
    db_ok = one("SELECT NOW() AS now")
    return {"ok": bool(db_ok), "db_time": iso(db_ok["now"]) if db_ok else None}


@app.get("/api/data-status")
def data_status():
    return latest_status()


@app.get("/api/summary")
def summary():
    data = market_summary()
    data["asset_counts"] = serialize(data["asset_counts"])
    return data


@app.get("/api/reports/latest")
def latest_report_api():
    return latest_report() or {"status": "missing", "message": "No report has been generated yet."}


@app.get("/api/assets/search")
def assets_search(request: Request):
    query = str(request.query_params.get("q") or "").strip()
    limit = max(1, min(int(request.query_params.get("limit") or 30), 80))
    return {"query": query, "items": search_assets(query, limit)}


@app.get("/api/assets/{symbol}/summary")
def asset_summary_api(symbol: str):
    return asset_summary(symbol)


@app.get("/api/qa/recent")
def recent_qa():
    return serialize(
        rows(
            """
            SELECT qa_id, question, model, ok, error_message, created_at
            FROM agent_question_answers
            ORDER BY created_at DESC
            LIMIT 20
            """
        )
    )


@app.get("/api/qa/{qa_id}")
def qa_detail(qa_id: str):
    item = one(
        """
        SELECT qa_id, question, answer, model, ok, error_message, context_json, created_at
        FROM agent_question_answers
        WHERE qa_id = %s
        """,
        (qa_id,),
    )
    if not item:
        return {"ok": False, "message": "没有找到这条问答"}
    context = item.get("context_json") if isinstance(item.get("context_json"), dict) else {}
    item["context_summary"] = {
        "matched_assets": (context.get("matched_assets") or [])[:12],
        "related_text_evidence": (context.get("related_text_evidence") or [])[:18],
        "detected_symbols": context.get("detected_symbols") or [],
    }
    item.pop("context_json", None)
    return {"ok": True, "item": json_ready(serialize([item])[0])}


@app.get("/api/evidence/search")
def evidence_search(request: Request):
    query = str(request.query_params.get("q") or "").strip()
    if not query:
        return {"query": query, "items": []}
    pattern = f"%{query}%"
    limit = max(1, min(int(request.query_params.get("limit") or 40), 100))
    items = rows(
        """
        SELECT evidence_id, evidence_type, asset_key, asset_name, market, source, source_symbol, ts,
               title, url, summary, body_excerpt, meta_json
        FROM evidence_items
        WHERE title ILIKE %s OR body_excerpt ILIKE %s OR summary ILIKE %s
           OR asset_name ILIKE %s OR source_symbol ILIKE %s
        ORDER BY ts DESC
        LIMIT %s
        """,
        (pattern, pattern, pattern, pattern, pattern, limit),
    )
    aliases = rows(
        """
        SELECT alias, asset_key, source_symbol, market, alias_type, provider
        FROM asset_aliases
        WHERE alias ILIKE %s OR source_symbol ILIKE %s
        ORDER BY length(alias), alias
        LIMIT 40
        """,
        (pattern, pattern),
    )
    return {"query": query, "items": serialize(items), "aliases": aliases}


@app.get("/api/evidence/{evidence_id}")
def evidence_detail(evidence_id: str):
    item = one(
        """
        SELECT evidence_id, evidence_type, asset_key, asset_name, market, source, source_symbol, ts,
               title, url, summary, body_excerpt, meta_json
        FROM evidence_items
        WHERE evidence_id = %s
        """,
        (evidence_id,),
    )
    if not item:
        return {"ok": False, "message": "没有找到这条证据"}
    return {"ok": True, "item": json_ready(serialize([item])[0])}


@app.post("/api/ask")
async def ask(request: Request):
    payload = await request.json()
    question = str(payload.get("question") or "")
    try:
        return ask_model(question)
    except Exception as exc:  # noqa: BLE001
        save_qa(question, f"AI 调用失败：{exc}", os.getenv("OPENAI_MODEL"), False, {}, str(exc))
        return {"ok": False, "answer": f"AI 调用失败：{exc}", "model": os.getenv("OPENAI_MODEL")}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    data = market_summary()
    data["asset_counts"] = serialize(data["asset_counts"])
    generated_at = datetime.now(timezone.utc)
    return render_page(data, generated_at)


def render_page(data, generated_at):
    failed = [item for item in data["status"] if not item["ok"]]
    report = data.get("latest_report")
    if report:
        report_html = f"""
        <section class="report {pill_class(report['status'])}">
          <div class="report-main">
            <div class="report-head">
              <span class="pill {pill_class(report['status'])}">{report['status']}</span>
              <span>{report.get('model') or 'No model configured'}</span>
              <span>Generated {fmt_dt(report.get('generated_at'))}</span>
              <span>Data as of {fmt_dt(report.get('data_as_of'))}</span>
            </div>
            <h2>{esc(report['title'])}</h2>
            <p>{esc(report['summary'])}</p>
            <div class="report-metrics">
              <div><span>Stance</span><strong>{esc(report.get('stance') or '-')}</strong></div>
              <div><span>Confidence</span><strong>{fmt_num((report.get('confidence') or 0) * 100, 0)}%</strong></div>
            </div>
          </div>
          <div class="report-lists">
            <section>
              <h3>Recommendations</h3>
              <ul>{list_items(report.get('recommendations'))}</ul>
            </section>
            <section>
              <h3>Risks</h3>
              <ul>{list_items(report.get('risks'))}</ul>
            </section>
            <section>
              <h3>Watchlist</h3>
              <ul>{list_items(report.get('watchlist'))}</ul>
            </section>
          </div>
        </section>
        """
    else:
        report_html = """
        <section class="report warn">
          <div class="report-main">
            <div class="report-head"><span class="pill warn">missing</span></div>
            <h2>还没有 Agent 报告</h2>
            <p>运行 generate_agent_report.py 后，这里会显示最新研究摘要。</p>
          </div>
        </section>
        """
    status_cards = "\n".join(
        f"""
        <article class="status-card {status_card_class(item)}">
          <div class="status-top">
            <span>{item['source']}</span>
            <strong>{status_text(item)}</strong>
          </div>
          <p>{item.get('message') or ''}</p>
          <dl>
            <div><dt>Observation</dt><dd>{fmt_dt(item.get('latest_observation_ts'))}</dd></div>
            <div><dt>Synced</dt><dd>{fmt_dt(item.get('last_run'))}</dd></div>
            <div><dt>Lag</dt><dd>{fmt_num(item.get('lag_hours'), 1)}h</dd></div>
          </dl>
        </article>
        """
        for item in data["status"]
    )
    index_rows = "\n".join(
        f"""
        <tr>
          <td>{item['asset_name']}</td>
          <td>{fmt_num(item['value'])}</td>
          <td class="{'up' if (item.get('change_percent') or 0) >= 0 else 'down'}">{fmt_pct(item.get('change_percent'))}</td>
          <td>{fmt_dt(item.get('ts'))}</td>
        </tr>
        """
        for item in data["cn_indices"]
    )
    quote_cards = "\n".join(
        f"""
        <article class="quote">
          <span>{item['asset_name']}</span>
          <strong>{fmt_num(item['value'])}</strong>
          <em class="{'up' if (quote_change(item) or 0) >= 0 else 'down'}">
            {fmt_pct(quote_change(item))}
          </em>
        </article>
        """
        for item in data["cn_quotes"]
    )
    flow_rows = "\n".join(
        f"""
        <tr>
          <td>{item['asset_name'].replace(' 主力净流入', '')}</td>
          <td class="{'up' if (item.get('value') or 0) >= 0 else 'down'}">{money_cn(item.get('value'))}</td>
          <td>{fmt_dt(item.get('ts'))}</td>
        </tr>
        """
        for item in data["market_flow"]
    )
    us_quote_cards = "\n".join(
        f"""
        <article class="quote">
          <span>{item['source_symbol'] or item['asset_name']}</span>
          <strong>{fmt_num(item['value'])}</strong>
          <em class="{'up' if (item.get('change_percent') or 0) >= 0 else 'down'}">
            {fmt_pct(item.get('change_percent'))}
          </em>
        </article>
        """
        for item in data["us_equities"]
    )
    filing_items = "\n".join(
        f"""
        <article class="filing">
          <div>
            <strong>{item['source_symbol']}</strong>
            <span>{fmt_dt(item.get('ts'))}</span>
          </div>
          <a href="{item.get('url') or '#'}" target="_blank" rel="noreferrer">{item['title']}</a>
          <p>{esc(item['asset_name'])}</p>
        </article>
        """
        for item in data["sec_filings"]
    )
    evidence_items = ""
    if report:
        evidence_items = "\n".join(
            f"""
            <article class="filing">
              <div>
                <strong>{item['evidence_type']}</strong>
                <span>{fmt_dt(item.get('ts'))}</span>
              </div>
              <a href="{item.get('url') or '#'}" target="_blank" rel="noreferrer">{item['title']}</a>
              <p>{esc(item.get('detail') or item.get('source') or '')}</p>
            </article>
            """
            for item in report.get("evidence", [])
        )
    count_rows = "\n".join(
        f"""
        <tr>
          <td>{item['provider']}</td>
          <td>{item['category']}</td>
          <td>{item['rows']}</td>
          <td>{fmt_dt(item.get('latest'))}</td>
        </tr>
        """
        for item in data["asset_counts"]
    )
    recent_qa_rows = "\n".join(
        f"""
        <button class="qa-item" type="button" data-qa-id="{esc(item.get('qa_id'))}">
          <div>
            <strong>{'OK' if item.get('ok') else '失败'}</strong>
            <span>{fmt_dt(item.get('created_at'))}</span>
          </div>
          <p>{esc(item.get('question') or '')}</p>
        </button>
        """
        for item in serialize(rows(
            """
            SELECT qa_id, question, ok, created_at
            FROM agent_question_answers
            ORDER BY created_at DESC
            LIMIT 6
            """
        ))
    )
    alert = (
        f"<div class='alert'>{len(failed)} 个数据源最近一次同步失败。通常是 AKShare/东方财富接口临时断连；已有历史数据仍会保留。</div>"
        if failed
        else "<div class='alert ok'>所有启用的数据源最近一次同步正常。</div>"
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Market AI Research</title>
  <link rel="stylesheet" href="/ai/static/styles.css">
</head>
<body>
  <header class="topbar">
    <a href="/" class="brand">Market Overview</a>
    <nav>
      <a href="/ai/">AI 研究</a>
      <a href="/ai/api/summary">JSON</a>
    </nav>
  </header>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">基于已有数据的研究工作台</p>
        <h1>AI 研究</h1>
        <p class="subtitle">用 A股行情、行业资金流、全球宏观和 SEC 证据生成研究报告，并支持直接提问。</p>
      </div>
      <div class="hero-meta">
        <span>页面生成时间</span>
        <strong>{fmt_dt(generated_at)}</strong>
      </div>
    </section>
    {alert}
    <section class="workspace-grid">
      <section class="ask-panel panel">
        <header>
          <div>
            <h2>问 AI</h2>
            <p>基于当前数据库里的行情、资金流、宏观与 SEC 证据回答。</p>
          </div>
        </header>
        <div class="ask-body">
          <textarea id="question" rows="3" placeholder="例如：根据现有数据，帮我写一份今天的市场研究报告，重点看A股资金流、指数表现和主要风险。"></textarea>
          <div class="ask-actions">
            <button id="askButton" type="button">生成回答</button>
            <span id="askStatus"></span>
          </div>
          <article id="answer" class="answer" hidden>
            <div class="answer-meta" id="answerMeta"></div>
            <div class="answer-content" id="answerContent"></div>
          </article>
        </div>
      </section>
      <aside class="panel qa-panel">
        <header>
          <h2>最近问答</h2>
          <p>点击可打开完整回答，也可以重新填入问题继续追问。</p>
        </header>
        <div id="recentQaList" class="qa-list">{recent_qa_rows}</div>
        <article id="qaDetail" class="answer compact" hidden>
          <div class="answer-meta" id="qaDetailMeta"></div>
          <div class="answer-content" id="qaDetailContent"></div>
        </article>
      </aside>
    </section>
    <section class="two-col tool-grid">
      <article class="panel tool-panel">
        <header>
          <h2>资产搜索</h2>
          <p>输入代码、中文名或主题词，例如 英伟达、宁德时代、半导体ETF。</p>
        </header>
        <div class="tool-body">
          <div class="inline-search">
            <input id="assetQuery" type="search" placeholder="搜索资产或主题">
            <button id="assetButton" type="button">搜索</button>
            <button id="assetClear" class="ghost-button" type="button">清空</button>
          </div>
          <div id="assetResults" class="result-list"></div>
          <button id="assetMore" class="ghost-button more-button" type="button" hidden>显示更多</button>
          <article id="assetSummary" class="answer compact" hidden>
            <div class="answer-meta" id="assetSummaryMeta"></div>
            <div class="answer-content" id="assetSummaryContent"></div>
          </article>
        </div>
      </article>
      <article class="panel tool-panel">
        <header>
          <h2>证据搜索</h2>
          <p>搜索公告、新闻、SEC 正文和财务文本，例如 配售新H股、CAO。</p>
        </header>
        <div class="tool-body">
          <div class="inline-search">
            <input id="evidenceQuery" type="search" placeholder="搜索证据关键词">
            <button id="evidenceButton" type="button">搜索</button>
            <button id="evidenceClear" class="ghost-button" type="button">清空</button>
          </div>
          <div id="evidenceResults" class="result-list"></div>
          <button id="evidenceMore" class="ghost-button more-button" type="button" hidden>显示更多</button>
          <article id="evidenceDetail" class="answer compact" hidden>
            <div class="answer-meta" id="evidenceDetailMeta"></div>
            <div class="answer-content" id="evidenceDetailContent"></div>
          </article>
        </div>
      </article>
    </section>
    {report_html}
    <p class="status-note">数据源状态说明：OK 表示最近一次同步成功；同步失败表示最近一次拉取接口失败，通常不代表历史数据被删除。</p>
    <details class="panel fold-panel">
      <summary>数据源状态</summary>
      <section class="grid status-grid">{status_cards}</section>
    </details>
    <details class="panel fold-panel">
      <summary>行情样本</summary>
      <section class="two-col nested-grid">
      <article class="inner-panel">
        <header><h2>A股指数</h2></header>
        <table>
          <thead><tr><th>名称</th><th>数值</th><th>涨跌</th><th>观测日期</th></tr></thead>
          <tbody>{index_rows}</tbody>
        </table>
      </article>
      <article class="inner-panel">
        <header><h2>A股日线样本</h2></header>
        <div class="quotes">{quote_cards}</div>
      </article>
      <article class="inner-panel">
        <header><h2>美股日线样本</h2></header>
        <div class="quotes">{us_quote_cards}</div>
      </article>
      <article class="inner-panel">
        <header><h2>A股市场资金流</h2></header>
        <table>
          <thead><tr><th>项目</th><th>主力净流入</th><th>观测日期</th></tr></thead>
          <tbody>{flow_rows}</tbody>
        </table>
      </article>
      </section>
    </details>
    <details class="panel fold-panel">
      <summary>SEC 近期文件</summary>
      <article class="inner-panel">
        <header><h2>SEC 证据</h2></header>
        <div class="filings">{filing_items}</div>
      </article>
    </details>
    <details class="panel fold-panel">
      <summary>数据资产清单</summary>
      <table>
        <thead><tr><th>来源</th><th>类别</th><th>行数</th><th>最新日期</th></tr></thead>
        <tbody>{count_rows}</tbody>
      </table>
    </details>
    <details class="panel fold-panel">
      <summary>Agent 报告证据</summary>
      <div class="filings">{evidence_items}</div>
    </details>
  </main>
</body>
<script>
const button = document.getElementById('askButton');
const question = document.getElementById('question');
const statusEl = document.getElementById('askStatus');
const answer = document.getElementById('answer');
const answerMeta = document.getElementById('answerMeta');
const answerContent = document.getElementById('answerContent');
const assetQuery = document.getElementById('assetQuery');
const assetButton = document.getElementById('assetButton');
const assetClear = document.getElementById('assetClear');
const assetResults = document.getElementById('assetResults');
const assetMore = document.getElementById('assetMore');
const assetSummary = document.getElementById('assetSummary');
const assetSummaryMeta = document.getElementById('assetSummaryMeta');
const assetSummaryContent = document.getElementById('assetSummaryContent');
const evidenceQuery = document.getElementById('evidenceQuery');
const evidenceButton = document.getElementById('evidenceButton');
const evidenceClear = document.getElementById('evidenceClear');
const evidenceResults = document.getElementById('evidenceResults');
const evidenceMore = document.getElementById('evidenceMore');
const evidenceDetail = document.getElementById('evidenceDetail');
const evidenceDetailMeta = document.getElementById('evidenceDetailMeta');
const evidenceDetailContent = document.getElementById('evidenceDetailContent');
const recentQaList = document.getElementById('recentQaList');
const qaDetail = document.getElementById('qaDetail');
const qaDetailMeta = document.getElementById('qaDetailMeta');
const qaDetailContent = document.getElementById('qaDetailContent');
let assetSearchItems = [];
let assetVisibleCount = 8;
let evidenceSearchItems = [];
let evidenceVisibleCount = 6;
let askInFlight = false;

function escapeHtml(text) {{
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}}

function inlineMarkdown(text) {{
  return escapeHtml(text).replace(/\\*\\*(.*?)\\*\\*/g, '<strong>$1</strong>');
}}

function renderMarkdown(text) {{
  const lines = String(text || '').replace(/\\r\\n/g, '\\n').split('\\n');
  const html = [];
  let paragraph = [];
  let list = [];

  function flushParagraph() {{
    if (paragraph.length) {{
      html.push(`<p>${{inlineMarkdown(paragraph.join(' '))}}</p>`);
      paragraph = [];
    }}
  }}

  function flushList() {{
    if (list.length) {{
      html.push(`<ul>${{list.map(item => `<li>${{inlineMarkdown(item)}}</li>`).join('')}}</ul>`);
      list = [];
    }}
  }}

  for (const rawLine of lines) {{
    const line = rawLine.trim();
    if (!line) {{
      flushParagraph();
      flushList();
      continue;
    }}
    const heading = line.match(/^(#{2,5})\\s+(.+)$/);
    if (heading) {{
      flushParagraph();
      flushList();
      const level = Math.min(heading[1].length, 4);
      html.push(`<h${{level}}>${{inlineMarkdown(heading[2])}}</h${{level}}>`);
      continue;
    }}
    const bullet = line.match(/^[-*]\\s+(.+)$/) || line.match(/^\\d+[.)]\\s+(.+)$/);
    if (bullet) {{
      flushParagraph();
      list.push(bullet[1]);
      continue;
    }}
    flushList();
    paragraph.push(line);
  }}
  flushParagraph();
  flushList();
  return html.join('');
}}

function postJson(url, payload) {{
  return new Promise((resolve, reject) => {{
    const xhr = new XMLHttpRequest();
    xhr.open('POST', url, true);
    xhr.timeout = 120000;
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.setRequestHeader('Accept', 'application/json');
    xhr.onload = () => {{
      if (xhr.status < 200 || xhr.status >= 300) {{
        reject(new Error(`HTTP ${{xhr.status}}: ${{xhr.responseText || '请求失败'}}`));
        return;
      }}
      try {{
        resolve(JSON.parse(xhr.responseText));
      }} catch (err) {{
        reject(new Error(`返回内容不是 JSON：${{xhr.responseText.slice(0, 120)}}`));
      }}
    }};
    xhr.onerror = () => reject(new Error('网络连接失败，请刷新页面后重试'));
    xhr.ontimeout = () => reject(new Error('请求超时，问题可以问得短一点再试'));
    xhr.send(JSON.stringify(payload));
  }});
}}

async function getJson(url) {{
  const response = await fetch(url, {{headers: {{'Accept': 'application/json'}}}});
  if (!response.ok) {{
    throw new Error(`HTTP ${{response.status}}`);
  }}
  return response.json();
}}

function renderAssetItems(items) {{
  if (!items || !items.length) return '<p class="empty">没有匹配资产</p>';
  return items.map(item => `
    <button class="result-item" type="button" data-symbol="${{escapeHtml(item.source_symbol || item.asset_key || '')}}">
      <strong>${{escapeHtml(item.source_symbol || '-')}}</strong>
      <span>${{escapeHtml(item.asset_name || '')}}</span>
      <em>${{escapeHtml(item.market || '')}}</em>
    </button>
  `).join('');
}}

function paintAssetResults() {{
  const visible = assetSearchItems.slice(0, assetVisibleCount);
  assetResults.innerHTML = renderAssetItems(visible);
  assetMore.hidden = assetSearchItems.length <= assetVisibleCount;
}}

function renderEvidenceItems(items) {{
  if (!items || !items.length) return '<p class="empty">没有匹配证据</p>';
  return items.map(item => `
    <button class="evidence-item" type="button" data-evidence-id="${{escapeHtml(item.evidence_id || '')}}">
      <div><strong>${{escapeHtml(item.evidence_type || item.category || item.source || '')}}</strong><span>${{escapeHtml(item.ts || '-')}}</span></div>
      <span class="evidence-title">${{escapeHtml(item.title || '')}}</span>
      <p>${{escapeHtml(item.summary || item.body_excerpt || item.asset_name || '')}}</p>
    </button>
  `).join('');
}}

function paintEvidenceResults() {{
  const visible = evidenceSearchItems.slice(0, evidenceVisibleCount);
  evidenceResults.innerHTML = renderEvidenceItems(visible);
  evidenceMore.hidden = evidenceSearchItems.length <= evidenceVisibleCount;
}}

function renderEvidenceDetail(item) {{
  const url = item.url
    ? `<p><a href="${{escapeHtml(item.url)}}" target="_blank" rel="noreferrer">打开原文链接</a></p>`
    : '';
  evidenceDetailMeta.textContent = `${{item.source || '-'}} · ${{item.evidence_type || '-'}} · ${{item.ts || '-'}}`;
  evidenceDetailContent.innerHTML = `
    <h3>${{escapeHtml(item.title || '证据详情')}}</h3>
    <p><strong>关联资产：</strong>${{escapeHtml(item.source_symbol || item.asset_name || item.asset_key || '-')}}</p>
    <p><strong>摘要：</strong>${{escapeHtml(item.summary || '-')}}</p>
    <p><strong>本地正文片段：</strong>${{escapeHtml(item.body_excerpt || '本条证据暂无正文片段，仅保存了元数据和原文链接。')}}</p>
    ${{url}}
  `;
  evidenceDetail.hidden = false;
}}

function renderQaList(items) {{
  if (!items || !items.length) return '<p class="empty">还没有问答记录</p>';
  return items.slice(0, 8).map(item => `
    <button class="qa-item" type="button" data-qa-id="${{escapeHtml(item.qa_id || '')}}">
      <div>
        <strong>${{item.ok ? 'OK' : '失败'}}</strong>
        <span>${{escapeHtml(item.created_at || '-')}}</span>
      </div>
      <p>${{escapeHtml(item.question || '')}}</p>
    </button>
  `).join('');
}}

async function refreshRecentQa() {{
  try {{
    const items = await getJson('/ai/api/qa/recent');
    recentQaList.innerHTML = renderQaList(items || []);
  }} catch (err) {{
    recentQaList.innerHTML = `<p class="empty">问答列表刷新失败：${{escapeHtml(err.message || err)}}</p>`;
  }}
}}

async function loadQaDetail(qaId) {{
  if (!qaId) return;
  qaDetail.hidden = true;
  qaDetailMeta.textContent = '正在加载历史问答...';
  const data = await getJson(`/ai/api/qa/${{encodeURIComponent(qaId)}}`);
  if (!data.ok) {{
    qaDetailMeta.textContent = '未找到问答';
    qaDetailContent.innerHTML = `<p>${{escapeHtml(data.message || '没有找到这条问答')}}</p>`;
    qaDetail.hidden = false;
    return;
  }}
  const item = data.item || {{}};
  const evidence = ((item.context_summary || {{}}).related_text_evidence || []).slice(0, 5);
  qaDetailMeta.textContent = `${{item.model || '模型'}} · ${{item.created_at || '-'}}`;
  qaDetailContent.innerHTML = `
    <h3>${{escapeHtml(item.question || '')}}</h3>
    <div class="qa-actions"><button id="reuseQuestion" class="ghost-button" type="button">重新提问</button></div>
    ${{renderMarkdown(item.answer || item.error_message || '没有回答内容')}}
    <h4>当时使用的证据</h4>
    ${{renderEvidenceItems(evidence)}}
  `;
  const reuse = qaDetailContent.querySelector('#reuseQuestion');
  if (reuse) {{
    reuse.addEventListener('click', () => {{
      question.value = item.question || '';
      question.focus();
      window.scrollTo({{top: question.getBoundingClientRect().top + window.scrollY - 110, behavior: 'smooth'}});
    }});
  }}
  qaDetail.hidden = false;
}}

async function loadAssetSummary(symbol) {{
  if (!symbol) return;
  assetSummary.hidden = true;
  assetSummaryMeta.textContent = '正在加载标的摘要...';
  const data = await getJson(`/ai/api/assets/${{encodeURIComponent(symbol)}}/summary`);
  if (!data.ok) {{
    assetSummaryMeta.textContent = '未找到资产';
    assetSummaryContent.innerHTML = `<p>${{escapeHtml(data.message || '没有匹配结果')}}</p>`;
    assetSummary.hidden = false;
    return;
  }}
  const bars = data.daily_bars_summary || {{}};
  assetSummaryMeta.textContent = `${{data.asset.source_symbol || ''}} · ${{data.asset.asset_name || ''}}`;
  assetSummaryContent.innerHTML = `
    <p><strong>最新收盘：</strong>${{escapeHtml(bars.latest_close ?? '-')}}，日期 ${{escapeHtml(bars.latest_date || '-')}}</p>
    <p><strong>收益率：</strong>5日 ${{escapeHtml((bars.return_5d_pct ?? '-').toString())}}%，20日 ${{escapeHtml((bars.return_20d_pct ?? '-').toString())}}%，60日 ${{escapeHtml((bars.return_60d_pct ?? '-').toString())}}%</p>
    <h4>相关证据</h4>
    ${{renderEvidenceItems((data.evidence || []).slice(0, 6))}}
  `;
  assetSummary.hidden = false;
}}

button.addEventListener('click', async () => {{
  if (askInFlight) return;
  const value = question.value.trim();
  if (!value) {{
    statusEl.textContent = '请输入问题';
    return;
  }}
  askInFlight = true;
  button.disabled = true;
  statusEl.textContent = '正在基于数据库上下文生成...';
  answer.hidden = true;
  answerMeta.textContent = '';
  answerContent.innerHTML = '';
  try {{
    const data = await postJson('/ai/api/ask', {{question: value}});
    answerMeta.textContent = data.model ? `模型：${{data.model}}` : '回答结果';
    answerContent.innerHTML = renderMarkdown(data.answer || '没有返回内容');
    answer.hidden = false;
    statusEl.textContent = '完成';
    await refreshRecentQa();
  }} catch (err) {{
    answerMeta.textContent = '回答失败';
    answerContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    answer.hidden = false;
    statusEl.textContent = '';
  }} finally {{
    askInFlight = false;
    button.disabled = false;
  }}
}});

assetButton.addEventListener('click', async () => {{
  const value = assetQuery.value.trim();
  if (!value) return;
  assetButton.disabled = true;
  assetVisibleCount = 8;
  assetSummary.hidden = true;
  assetResults.innerHTML = '<p class="empty">搜索中...</p>';
  assetMore.hidden = true;
  try {{
    const data = await getJson(`/ai/api/assets/search?q=${{encodeURIComponent(value)}}&limit=40`);
    assetSearchItems = data.items || [];
    paintAssetResults();
  }} catch (err) {{
    assetResults.innerHTML = `<p class="empty">搜索失败：${{escapeHtml(err.message || err)}}</p>`;
  }} finally {{
    assetButton.disabled = false;
  }}
}});

assetMore.addEventListener('click', () => {{
  assetVisibleCount += 8;
  paintAssetResults();
}});

assetClear.addEventListener('click', () => {{
  assetQuery.value = '';
  assetSearchItems = [];
  assetResults.innerHTML = '';
  assetMore.hidden = true;
  assetSummary.hidden = true;
}});

assetResults.addEventListener('click', async (event) => {{
  const item = event.target.closest('.result-item');
  if (!item) return;
  try {{
    await loadAssetSummary(item.dataset.symbol);
  }} catch (err) {{
    assetSummaryMeta.textContent = '加载失败';
    assetSummaryContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    assetSummary.hidden = false;
  }}
}});

evidenceButton.addEventListener('click', async () => {{
  const value = evidenceQuery.value.trim();
  if (!value) return;
  evidenceButton.disabled = true;
  evidenceVisibleCount = 6;
  evidenceDetail.hidden = true;
  evidenceResults.innerHTML = '<p class="empty">搜索中...</p>';
  evidenceMore.hidden = true;
  try {{
    const data = await getJson(`/ai/api/evidence/search?q=${{encodeURIComponent(value)}}&limit=60`);
    evidenceSearchItems = data.items || [];
    paintEvidenceResults();
  }} catch (err) {{
    evidenceResults.innerHTML = `<p class="empty">搜索失败：${{escapeHtml(err.message || err)}}</p>`;
  }} finally {{
    evidenceButton.disabled = false;
  }}
}});

evidenceMore.addEventListener('click', () => {{
  evidenceVisibleCount += 6;
  paintEvidenceResults();
}});

evidenceClear.addEventListener('click', () => {{
  evidenceQuery.value = '';
  evidenceSearchItems = [];
  evidenceResults.innerHTML = '';
  evidenceMore.hidden = true;
  evidenceDetail.hidden = true;
}});

evidenceResults.addEventListener('click', async (event) => {{
  const item = event.target.closest('.evidence-item');
  if (!item) return;
  try {{
    const data = await getJson(`/ai/api/evidence/${{encodeURIComponent(item.dataset.evidenceId)}}`);
    if (data.ok) {{
      renderEvidenceDetail(data.item || {{}});
    }} else {{
      evidenceDetailMeta.textContent = '未找到证据';
      evidenceDetailContent.innerHTML = `<p>${{escapeHtml(data.message || '没有找到这条证据')}}</p>`;
      evidenceDetail.hidden = false;
    }}
  }} catch (err) {{
    evidenceDetailMeta.textContent = '加载失败';
    evidenceDetailContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    evidenceDetail.hidden = false;
  }}
}});

recentQaList.addEventListener('click', async (event) => {{
  const item = event.target.closest('.qa-item');
  if (!item) return;
  try {{
    await loadQaDetail(item.dataset.qaId);
  }} catch (err) {{
    qaDetailMeta.textContent = '加载失败';
    qaDetailContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    qaDetail.hidden = false;
  }}
}});

qaDetailContent.addEventListener('click', async (event) => {{
  const item = event.target.closest('.evidence-item');
  if (!item) return;
  try {{
    const data = await getJson(`/ai/api/evidence/${{encodeURIComponent(item.dataset.evidenceId)}}`);
    if (data.ok) renderEvidenceDetail(data.item || {{}});
  }} catch (err) {{
    evidenceDetailMeta.textContent = '加载失败';
    evidenceDetailContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    evidenceDetail.hidden = false;
  }}
}});

assetSummaryContent.addEventListener('click', async (event) => {{
  const item = event.target.closest('.evidence-item');
  if (!item) return;
  try {{
    const data = await getJson(`/ai/api/evidence/${{encodeURIComponent(item.dataset.evidenceId)}}`);
    if (data.ok) renderEvidenceDetail(data.item || {{}});
  }} catch (err) {{
    evidenceDetailMeta.textContent = '加载失败';
    evidenceDetailContent.innerHTML = `<p>${{escapeHtml(err.message || err)}}</p>`;
    evidenceDetail.hidden = false;
  }}
}});
</script>
</html>"""
