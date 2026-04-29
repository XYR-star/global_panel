#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any

import requests
from psycopg2.extras import Json, RealDictCursor

from providers.store import connect_db, ensure_schema


REPORT_TYPE = os.getenv("AGENT_REPORT_TYPE", "market_research_v1")


def rows(conn, sql: str, params: tuple[Any, ...] = ()):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def latest_data_ts(items: list[dict[str, Any]]) -> datetime | None:
    values = [item.get("ts") or item.get("latest_observation_ts") for item in items]
    values = [value for value in values if isinstance(value, datetime)]
    return max(values) if values else None


def collect_context(conn):
    status = rows(
        conn,
        """
        SELECT source, ok, message, latest_observation_ts, last_run
        FROM data_sync_status
        WHERE source <> 'fredgraph'
        ORDER BY source
        """,
    )
    cn_indices = rows(
        conn,
        """
        SELECT asset_key, asset_name, source, value, change_percent, ts
        FROM market_asset_snapshot
        WHERE category = 'cn_indices'
        ORDER BY asset_name
        """,
    )
    quotes = rows(
        conn,
        """
        SELECT asset_key, asset_name, source, value, change_percent, ts, meta_json
        FROM market_asset_snapshot
        WHERE category = 'cn_equity_quote'
        ORDER BY ABS(COALESCE((meta_json->>'change_percent')::double precision, change_percent, 0)) DESC
        LIMIT 20
        """,
    )
    flows = rows(
        conn,
        """
        SELECT asset_key, asset_name, value, ts
        FROM market_asset_snapshot
        WHERE category = 'cn_fund_flow'
        ORDER BY value DESC
        LIMIT 15
        """,
    )
    market_flow = rows(
        conn,
        """
        SELECT asset_key, asset_name, value, ts
        FROM market_asset_snapshot
        WHERE category = 'cn_market_fund_flow'
        ORDER BY ts DESC
        LIMIT 8
        """,
    )
    us_equities = rows(
        conn,
        """
        SELECT asset_key, asset_name, regexp_replace(asset_key, '^YFUS:', '') AS source_symbol,
               value, change_percent, ts, meta_json
        FROM market_asset_snapshot
        WHERE category = 'us_equity_daily'
        ORDER BY ABS(COALESCE(change_percent, 0)) DESC, asset_name
        LIMIT 20
        """,
    )
    concepts = rows(
        conn,
        """
        SELECT asset_key, asset_name, value, change_percent, ts, meta_json
        FROM market_asset_snapshot
        WHERE category = 'cn_concept'
        ORDER BY ABS(COALESCE((meta_json->>'change_percent')::double precision, change_percent, 0)) DESC
        LIMIT 20
        """,
    )
    full_quotes = rows(
        conn,
        """
        SELECT asset_key, asset_name, value, ts, meta_json
        FROM market_asset_snapshot
        WHERE category = 'cn_equity_full_quote'
        ORDER BY COALESCE((meta_json->>'turnover')::double precision, 0) DESC
        LIMIT 30
        """,
    )
    cn_text = rows(
        conn,
        """
        SELECT asset_key, asset_name, category, ts, title, body, url, meta_json
        FROM market_text_records
        WHERE source = 'akshare'
          AND category IN ('cn_company_news', 'cn_financial_indicator', 'cn_industry_constituents', 'cn_company_announcement')
        ORDER BY ts DESC
        LIMIT 30
        """,
    )
    sec_filings = rows(
        conn,
        """
        SELECT asset_key, asset_name, source_symbol, ts, title, url,
               left(coalesce(body, ''), 1800) AS body, meta_json
        FROM market_text_records
        WHERE source = 'sec_edgar'
        ORDER BY ts DESC
        LIMIT 12
        """,
    )
    macro = rows(
        conn,
        """
        SELECT asset_key, asset_name, category, value, change_percent, ts
        FROM market_asset_snapshot
        WHERE category IN ('rates', 'credit', 'commodities', 'fx', 'macro', 'liquidity')
        ORDER BY category, asset_key
        LIMIT 60
        """,
    )
    context = {
        "status": status,
        "cn_indices": cn_indices,
        "active_cn_quotes": quotes,
        "cn_fund_flow": flows,
        "cn_market_fund_flow": market_flow,
        "us_equity_daily": us_equities,
        "cn_concepts": concepts,
        "cn_full_quote_turnover_leaders": full_quotes,
        "cn_company_text_evidence": cn_text,
        "sec_filings": sec_filings,
        "macro_cross_asset": macro,
    }
    evidence = build_evidence(context)
    all_items = status + cn_indices + quotes + flows + market_flow + us_equities + concepts + full_quotes + cn_text + sec_filings + macro
    return context, evidence, latest_data_ts(all_items)


def build_evidence(context):
    evidence: list[dict[str, Any]] = []
    for item in context["cn_indices"][:8]:
        evidence.append(
            {
                "evidence_id": f"idx:{item['asset_key']}",
                "evidence_type": "cn_index",
                "title": item["asset_name"],
                "detail": f"value={item.get('value')}, change_percent={item.get('change_percent')}",
                "source": item.get("source") or "yahoo_finance",
                "asset_key": item["asset_key"],
                "ts": item.get("ts"),
                "url": None,
                "meta_json": {},
            }
        )
    for item in (context["cn_fund_flow"] or context["cn_market_fund_flow"])[:8]:
        evidence.append(
            {
                "evidence_id": f"flow:{item['asset_key']}",
                "evidence_type": "cn_fund_flow",
                "title": item["asset_name"],
                "detail": f"main_flow={item.get('value')}",
                "source": "akshare",
                "asset_key": item["asset_key"],
                "ts": item.get("ts"),
                "url": None,
                "meta_json": {},
            }
        )
    for item in context["sec_filings"][:8]:
        evidence.append(
            {
                "evidence_id": f"filing:{evidence_key(item.get('source_symbol'), item.get('ts'), item.get('title'), item.get('url'))}",
                "evidence_type": "sec_filing",
                "title": item["title"],
                "detail": (item.get("body") or item["asset_name"])[:600],
                "source": "sec_edgar",
                "asset_key": item["asset_key"],
                "ts": item.get("ts"),
                "url": item.get("url"),
                "meta_json": item.get("meta_json") or {},
            }
        )
    for item in context["us_equity_daily"][:8]:
        evidence.append(
            {
                "evidence_id": f"us:{item['asset_key']}",
                "evidence_type": "us_equity_daily",
                "title": item["asset_name"],
                "detail": f"value={item.get('value')}, change_percent={item.get('change_percent')}",
                "source": "yahoo_finance",
                "asset_key": item["asset_key"],
                "ts": item.get("ts"),
                "url": None,
                "meta_json": item.get("meta_json") or {},
            }
        )
    return evidence


def evidence_key(*parts):
    text = "|".join(str(part) for part in parts)
    return sha1(text.encode("utf-8")).hexdigest()[:12]


def json_ready(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    return value


def fallback_report(context, data_as_of, reason):
    failed = [item["source"] for item in context["status"] if not item.get("ok")]
    top_flows = context["cn_fund_flow"][:3]
    flow_text = ", ".join(f"{item['asset_name']} {item['value']:.0f}" for item in top_flows if item.get("value") is not None)
    summary = (
        "Agent report generation is waiting for OpenAI-compatible model configuration. "
        "The data pipeline is populated and ready for model-backed analysis."
    )
    if failed:
        summary += f" Sources needing attention: {', '.join(failed)}."
    elif flow_text:
        summary += f" Current strongest A-share industry flow evidence: {flow_text}."
    return {
        "status": "needs_config",
        "title": "Agent research model is not configured",
        "summary": summary,
        "stance": "neutral",
        "confidence": 0.0,
        "recommendations": [
            "Set OPENAI_API_KEY and optionally OPENAI_BASE_URL/OPENAI_MODEL in /etc/market-dashboard.env.",
            "Run generate_agent_report.py once to create the first model-backed report.",
            "Treat this page as a research aid, not trading instruction.",
        ],
        "risks": [
            "No model-backed reasoning has been generated yet.",
            "Free public data sources can lag, throttle, or change fields.",
            "A-share quote coverage is currently a configurable sample, not a full universe ranking.",
        ],
        "watchlist": [
            "Data source status cards",
            "A-share industry fund flow leaders",
            "Recent SEC filings from tracked U.S. tickers",
        ],
        "raw_response": {"reason": reason, "data_as_of": data_as_of},
        "model": None,
        "error_message": reason,
    }


def llm_report(context):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None, "Missing OPENAI_API_KEY"
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = {
        "role": "user",
        "content": (
            "你是一个中文市场研究 Agent。只能使用我提供的数据，不要编造外部事实。"
            "请用中文输出严格 JSON，字段必须包含: title, summary, stance, confidence, "
            "recommendations, risks, watchlist。recommendations/risks/watchlist 必须是简洁中文字符串数组。"
            "stance must be one of bullish, cautious, neutral, defensive, mixed. "
            "不要给实盘下单指令、仓位比例或保证收益表述；请输出研究建议、证据、风险和观察清单。\n\n"
            f"DATA:\n{json.dumps(json_ready(context), ensure_ascii=False)[:45000]}"
        ),
    }
    response = requests.post(
        f"{base_url}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "你只用中文写数据驱动的投研摘要，并且只返回 JSON。",
                },
                prompt,
            ],
            "temperature": float(os.getenv("AGENT_TEMPERATURE", "0.2")),
            "response_format": {"type": "json_object"},
        },
        timeout=int(os.getenv("AGENT_LLM_TIMEOUT_SECONDS", "90")),
    )
    response.raise_for_status()
    payload = response.json()
    content = payload["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    parsed["status"] = "ok"
    parsed["model"] = model
    parsed["raw_response"] = {"provider_response": payload}
    parsed["error_message"] = None
    return parsed, None


def normalize_report(report):
    return {
        "status": str(report.get("status") or "ok"),
        "title": str(report.get("title") or "Market Research Report"),
        "summary": str(report.get("summary") or ""),
        "stance": str(report.get("stance") or "neutral"),
        "confidence": float(report.get("confidence") or 0.0),
        "recommendations": list(report.get("recommendations") or []),
        "risks": list(report.get("risks") or []),
        "watchlist": list(report.get("watchlist") or []),
        "raw_response": dict(report.get("raw_response") or {}),
        "model": report.get("model"),
        "error_message": report.get("error_message"),
    }


def save_report(conn, report, evidence, data_as_of):
    report = normalize_report(report)
    report_id = f"{REPORT_TYPE}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}:{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_reports
                (report_id, report_type, status, title, summary, stance, confidence,
                 recommendations, risks, watchlist, raw_response, model, generated_at, data_as_of, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
            """,
            (
                report_id,
                REPORT_TYPE,
                report["status"],
                report["title"],
                report["summary"],
                report["stance"],
                report["confidence"],
                Json(report["recommendations"]),
                Json(report["risks"]),
                Json(report["watchlist"]),
                Json(json_ready(report["raw_response"])),
                report["model"],
                data_as_of,
                report["error_message"],
            ),
        )
        for item in evidence:
            cur.execute(
                """
                INSERT INTO agent_report_evidence
                    (report_id, evidence_id, evidence_type, title, detail, source, asset_key, ts, url, meta_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    report_id,
                    item["evidence_id"],
                    item["evidence_type"],
                    item["title"],
                    item.get("detail"),
                    item.get("source"),
                    item.get("asset_key"),
                    item.get("ts"),
                    item.get("url"),
                    Json(json_ready(item.get("meta_json") or {})),
                ),
            )
    conn.commit()
    return report_id


def main():
    conn = connect_db()
    try:
        ensure_schema(conn)
        context, evidence, data_as_of = collect_context(conn)
        try:
            report, reason = llm_report(context)
        except Exception as exc:  # noqa: BLE001
            report, reason = None, f"LLM call failed: {exc}"
        if report is None:
            report = fallback_report(context, data_as_of, reason or "LLM unavailable")
        report_id = save_report(conn, report, evidence, data_as_of)
        print(report_id)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
