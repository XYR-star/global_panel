from __future__ import annotations

import json
from typing import Any

import httpx
from psycopg2.extras import Json, RealDictCursor

from providers.secrets import decrypt_secret, encrypt_secret

PROVIDER_DEFAULT_MODELS = {
    "none": None,
    "deepseek": "deepseek-chat",
    "openai": "gpt-4o-mini",
}


def ensure_ai_settings(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO portfolio_ai_settings (id, provider, model, daily_limit) VALUES (1, 'none', NULL, 30) ON CONFLICT (id) DO NOTHING")
    conn.commit()


def get_ai_settings(conn, include_secret: bool = False) -> dict[str, Any]:
    ensure_ai_settings(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT provider, model, daily_limit, public_config,
                   encrypted_api_key IS NOT NULL AS configured,
                   encrypted_api_key, updated_at
            FROM portfolio_ai_settings
            WHERE id = 1
            """
        )
        row = dict(cur.fetchone())
    if include_secret:
        row["api_key"] = decrypt_secret(row.get("encrypted_api_key"))
    row.pop("encrypted_api_key", None)
    return row


def update_ai_settings(conn, provider: str, model: str | None, daily_limit: int | None, api_key: str | None, replace_key: bool = False) -> dict[str, Any]:
    if provider not in PROVIDER_DEFAULT_MODELS:
        raise KeyError(provider)
    model = model or PROVIDER_DEFAULT_MODELS[provider]
    assignments = ["provider = %s", "model = %s", "daily_limit = %s", "updated_at = NOW()"]
    params: list[Any] = [provider, model, max(1, min(500, int(daily_limit or 30)))]
    if api_key:
        assignments.append("encrypted_api_key = %s")
        params.append(encrypt_secret(api_key))
    elif replace_key:
        assignments.append("encrypted_api_key = NULL")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            UPDATE portfolio_ai_settings
            SET {', '.join(assignments)}
            WHERE id = 1
            RETURNING provider, model, daily_limit, public_config,
                      encrypted_api_key IS NOT NULL AS configured, updated_at
            """,
            tuple(params),
        )
        row = dict(cur.fetchone())
    conn.commit()
    return row


def prompt_for_event(event: dict[str, Any], symbols: list[dict[str, Any]]) -> str:
    holdings = "；".join(f"{item.get('symbol')} {item.get('symbol_name') or ''} {item.get('security_type') or ''}" for item in symbols)
    return (
        "你是个人持仓公告阅读助手。只根据给定公告信息输出 JSON，不输出买卖建议或投资结论。\n"
        "JSON 字段：summary,event_type,importance,relevance,risks。\n"
        "importance 为 1-5 整数，risks 为中文字符串数组。\n\n"
        f"公告标题：{event.get('title')}\n"
        f"公告日期：{event.get('announcement_date')}\n"
        f"来源：{event.get('source_key')}\n"
        f"原类型：{event.get('event_type')}\n"
        f"关联持仓：{holdings or '无'}\n"
    )


def _chat_completion(provider: str, model: str, api_key: str, prompt: str) -> dict[str, Any]:
    if provider == "deepseek":
        url = "https://api.deepseek.com/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}"}
    elif provider == "openai":
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}"}
    else:
        raise RuntimeError("AI provider is disabled")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return compact valid JSON only."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    with httpx.Client(timeout=httpx.Timeout(30.0, connect=8.0), headers=headers) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        text = response.json()["choices"][0]["message"]["content"].strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text.removeprefix("json").strip()
    data = json.loads(text)
    return {
        "summary": str(data.get("summary") or "")[:2000],
        "event_type": str(data.get("event_type") or "公告")[:80],
        "importance": max(1, min(5, int(data.get("importance") or 3))),
        "relevance": str(data.get("relevance") or "")[:2000],
        "risks": [str(item)[:500] for item in (data.get("risks") or [])][:8],
        "raw_json": data,
    }


def generate_insight(conn, event_id: int, force: bool = False) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if not force:
            cur.execute("SELECT * FROM portfolio_event_ai_insights WHERE event_id = %s", (event_id,))
            cached = cur.fetchone()
            if cached:
                return {"ok": True, "cached": True, "insight": dict(cached)}
        cur.execute("SELECT * FROM portfolio_events WHERE event_id = %s", (event_id,))
        event = cur.fetchone()
        if not event:
            raise KeyError("event not found")
        cur.execute("SELECT * FROM portfolio_event_symbols WHERE event_id = %s ORDER BY symbol", (event_id,))
        symbols = [dict(row) for row in cur.fetchall()]
    settings = get_ai_settings(conn, include_secret=True)
    provider = settings.get("provider") or "none"
    if provider == "none":
        return {"ok": False, "status": "disabled", "message": "AI 解读未启用"}
    api_key = settings.get("api_key")
    if not api_key:
        return {"ok": False, "status": "not_configured", "message": "AI API key 未配置"}
    model = settings.get("model") or PROVIDER_DEFAULT_MODELS[provider]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS count
            FROM portfolio_event_ai_insights
            WHERE provider = %s AND generated_at::date = CURRENT_DATE
            """,
            (provider,),
        )
        used_today = int(cur.fetchone()["count"])
    if used_today >= int(settings.get("daily_limit") or 30):
        return {"ok": False, "status": "rate_limited", "message": "AI 每日生成上限已用完"}
    result = _chat_completion(provider, model, api_key, prompt_for_event(dict(event), symbols))
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO portfolio_event_ai_insights
                (event_id, provider, model, summary, event_type, importance, relevance, risks, raw_json, generated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (event_id) DO UPDATE SET
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                summary = EXCLUDED.summary,
                event_type = EXCLUDED.event_type,
                importance = EXCLUDED.importance,
                relevance = EXCLUDED.relevance,
                risks = EXCLUDED.risks,
                raw_json = EXCLUDED.raw_json,
                generated_at = NOW()
            RETURNING *
            """,
            (
                event_id,
                provider,
                model,
                result["summary"],
                result["event_type"],
                result["importance"],
                result["relevance"],
                Json(result["risks"]),
                Json(result["raw_json"]),
            ),
        )
        row = dict(cur.fetchone())
    conn.commit()
    return {"ok": True, "cached": False, "insight": row}


def test_ai_connection(conn) -> dict[str, Any]:
    settings = get_ai_settings(conn, include_secret=True)
    provider = settings.get("provider") or "none"
    if provider == "none":
        return {"ok": False, "status": "disabled", "message": "AI 未启用"}
    if not settings.get("api_key"):
        return {"ok": False, "status": "not_configured", "message": "API key 未配置"}
    try:
        _chat_completion(provider, settings.get("model") or PROVIDER_DEFAULT_MODELS[provider], settings["api_key"], "输出 {\"summary\":\"ok\",\"event_type\":\"测试\",\"importance\":1,\"relevance\":\"ok\",\"risks\":[]}")
        return {"ok": True, "status": "available"}
    except Exception as exc:
        return {"ok": False, "status": "unavailable", "message": str(exc)[:300]}
