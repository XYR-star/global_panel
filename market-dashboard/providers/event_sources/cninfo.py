from __future__ import annotations

from datetime import date
from typing import Any

import httpx

from .base import Announcement

BASE = "https://www.cninfo.com.cn"
QUERY_URL = f"{BASE}/new/hisAnnouncement/query"


def _parse_date(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    text = value[:10].replace("/", "-")
    try:
        return date.fromisoformat(text)
    except ValueError:
        return fallback


def _pdf_url(adjunct_url: str | None) -> str | None:
    if not adjunct_url:
        return None
    return adjunct_url if adjunct_url.startswith("http") else f"{BASE}/new/disclosure/detail?stockCode=&announcementId=&orgId=&announcementTime=&announcementTitle=&adjunctUrl={adjunct_url}"


def fetch_events(watchlist: list[dict[str, Any]], since: date, days: int, config: dict[str, Any] | None = None, secret: str | None = None) -> list[Announcement]:
    del days, config, secret
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
            for row in payload.get("announcements") or []:
                title = (row.get("announcementTitle") or row.get("title") or "").strip()
                if not title:
                    continue
                adjunct_url = row.get("adjunctUrl")
                ann_date = _parse_date(row.get("announcementTime"), since)
                event_id = str(row.get("announcementId") or adjunct_url or f"{code}-{ann_date}-{title}")
                url = f"{BASE}/new/disclosure/detail?stockCode={code}&announcementId={event_id}" if event_id else None
                out.append(
                    Announcement(
                        title=title,
                        announcement_date=ann_date,
                        source_key="cninfo",
                        source_event_id=event_id,
                        source_url=url,
                        pdf_url=f"{BASE}/{adjunct_url}" if adjunct_url and not adjunct_url.startswith("http") else adjunct_url,
                        event_type=row.get("category") or "公告",
                        importance=4 if any(word in title for word in ("清算", "终止", "风险", "暂停", "退市", "变更")) else 3,
                        symbols=[code],
                        raw_json=row,
                    )
                )
    return out
