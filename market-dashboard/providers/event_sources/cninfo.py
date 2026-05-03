from __future__ import annotations

from datetime import date, datetime
from typing import Any

import httpx

from .base import Announcement

BASE = "https://www.cninfo.com.cn"
QUERY_URL = f"{BASE}/new/hisAnnouncement/query"
STATIC_BASE = "https://static.cninfo.com.cn/"
OFFICIAL_BASE = "https://webapi.cninfo.com.cn"


def _parse_date(value: Any, fallback: date) -> date:
    if not value:
        return fallback
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000 if value > 10_000_000_000 else value).date()
        except (OSError, OverflowError, ValueError):
            return fallback
    text = str(value)[:10].replace("/", "-")
    try:
        return date.fromisoformat(text)
    except ValueError:
        return fallback


def _pdf_url(adjunct_url: str | None) -> str | None:
    if not adjunct_url:
        return None
    if adjunct_url.startswith("http"):
        return adjunct_url
    if adjunct_url.lower().endswith(".pdf") or adjunct_url.startswith("finalpage/"):
        return f"{STATIC_BASE}{adjunct_url.lstrip('/')}"
    return f"{BASE}/{adjunct_url.lstrip('/')}"


def _importance(title: str) -> int:
    return 4 if any(word in title for word in ("清算", "终止", "风险", "暂停", "退市", "变更", "重大")) else 3


def _event_from_row(row: dict[str, Any], code: str, fallback_date: date, source_mode: str) -> Announcement | None:
    title = (row.get("announcementTitle") or row.get("公告标题") or row.get("title") or row.get("TITLE") or row.get("F001V") or "").strip()
    if not title:
        return None
    ann_date = _parse_date(
        row.get("announcementTime") or row.get("公告时间") or row.get("ann_date") or row.get("DECLAREDATE") or row.get("PUBLISHDATE") or row.get("F002D"),
        fallback_date,
    )
    row_code = str(row.get("secCode") or row.get("代码") or row.get("SECCODE") or code).zfill(6)
    event_id = str(row.get("announcementId") or row.get("公告ID") or row.get("id") or row.get("ID") or row.get("RID") or f"{row_code}-{ann_date}-{title}")
    adjunct_url = row.get("adjunctUrl") or row.get("公告链接") or row.get("url") or row.get("URL")
    org_id = row.get("orgId") or ""
    source_url = row.get("公告链接") or row.get("source_url")
    if not source_url:
        source_url = f"{BASE}/new/disclosure/detail?stockCode={row_code}&announcementId={event_id}&orgId={org_id}"
    return Announcement(
        title=title,
        announcement_date=ann_date,
        source_key="cninfo",
        source_event_id=event_id,
        source_url=source_url,
        pdf_url=_pdf_url(adjunct_url),
        event_type=row.get("category") or row.get("公告类型") or row.get("ANNOUNCEMENTTYPE") or "公告",
        importance=_importance(title),
        symbols=[row_code],
        raw_json={**row, "cninfo_source_mode": source_mode},
    )


def _records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("announcements", "records", "data", "rows", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            nested = _records(value)
            if nested:
                return nested
    return []


def _public_fetch(watchlist: list[dict[str, Any]], since: date) -> list[Announcement]:
    out: list[Announcement] = []
    headers = {
        "User-Agent": "Mozilla/5.0 PortfolioEventRadar/1.0",
        "Referer": "https://www.cninfo.com.cn/new/disclosure",
    }
    timeout = httpx.Timeout(12.0, connect=6.0)
    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for item in watchlist:
            code = str(item.get("symbol") or "").zfill(6)
            if not code:
                continue
            params = {
                "stock": code,
                "searchkey": "",
                "plate": "",
                "category": "",
                "trade": "",
                "column": "szse",
                "columnTitle": "历史公告查询",
                "pageNum": "1",
                "pageSize": "30",
                "tabName": "fulltext",
                "sortName": "",
                "sortType": "",
                "limit": "",
                "seDate": f"{since.isoformat()}~{date.today().isoformat()}",
            }
            response = client.post(QUERY_URL, data=params)
            response.raise_for_status()
            payload = response.json()
            for row in _records(payload):
                event = _event_from_row(row, code, since, "public_his_announcement")
                if event:
                    out.append(event)
    return out


def _official_fetch(watchlist: list[dict[str, Any]], since: date, config: dict[str, Any], secret: str) -> list[Announcement]:
    path = str(config.get("api_path") or config.get("official_api_path") or "").strip()
    if not path:
        raise RuntimeError("CNINFO official API path is not configured")
    url = path if path.startswith("http") else f"{OFFICIAL_BASE}{path if path.startswith('/') else '/' + path}"
    token_param = str(config.get("token_param") or "key").strip() or "key"
    method = str(config.get("method") or "POST").upper()
    start_key = str(config.get("start_date_param") or "sdate")
    end_key = str(config.get("end_date_param") or "edate")
    symbol_key = str(config.get("symbol_param") or "scode")
    extra_params = config.get("extra_params") if isinstance(config.get("extra_params"), dict) else {}
    headers = {
        "User-Agent": "Mozilla/5.0 PortfolioEventRadar/1.0",
        "Referer": "https://webapi.cninfo.com.cn/",
    }
    auth_header = str(config.get("auth_header") or "").strip()
    if auth_header:
        headers[auth_header] = secret
    timeout = httpx.Timeout(15.0, connect=6.0)
    out: list[Announcement] = []
    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for item in watchlist:
            code = str(item.get("symbol") or "").zfill(6)
            params: dict[str, Any] = {
                symbol_key: code,
                start_key: since.isoformat(),
                end_key: date.today().isoformat(),
                token_param: secret,
                **extra_params,
            }
            response = client.request(method, url, params=params if method == "GET" else None, data=params if method != "GET" else None)
            response.raise_for_status()
            payload = response.json()
            records = _records(payload)
            if not records and isinstance(payload, dict) and payload.get("retCode") not in (None, 1, "1", 0, "0"):
                raise RuntimeError(str(payload.get("retMsg") or payload.get("msg") or payload)[:300])
            for row in records:
                event = _event_from_row(row, code, since, "official_webapi")
                if event:
                    out.append(event)
    return out


def fetch_events(watchlist: list[dict[str, Any]], since: date, days: int, config: dict[str, Any] | None = None, secret: str | None = None) -> list[Announcement]:
    del days
    config = config or {}
    mode = str(config.get("mode") or config.get("cninfo_mode") or "auto").lower()
    if secret and mode in {"auto", "official"}:
        try:
            return _official_fetch(watchlist, since, config, secret)
        except Exception:
            if mode == "official":
                raise
    return _public_fetch(watchlist, since)
