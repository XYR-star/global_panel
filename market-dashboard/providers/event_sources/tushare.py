from __future__ import annotations

from datetime import date
from typing import Any

import httpx

from .base import Announcement

API_URL = "https://api.tushare.pro"


def _parse_date(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    text = value.replace("-", "")
    if len(text) == 8:
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return fallback
    return fallback


def fetch_events(watchlist: list[dict[str, Any]], since: date, days: int, config: dict[str, Any] | None = None, secret: str | None = None) -> list[Announcement]:
    del days, config
    if not secret:
        raise RuntimeError("Tushare token is not configured")
    out: list[Announcement] = []
    timeout = httpx.Timeout(12.0, connect=6.0)
    start = since.strftime("%Y%m%d")
    end = date.today().strftime("%Y%m%d")
    with httpx.Client(timeout=timeout) as client:
        for item in watchlist:
            code = str(item.get("symbol") or "").zfill(6)
            if not code:
                continue
            payload = {
                "api_name": "anns",
                "token": secret,
                "params": {"ts_code": code, "start_date": start, "end_date": end},
                "fields": "ts_code,ann_date,ann_type,title,url",
            }
            response = client.post(API_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            if data.get("code") not in (0, None):
                raise RuntimeError(data.get("msg") or "Tushare request failed")
            fields = data.get("data", {}).get("fields") or []
            for values in data.get("data", {}).get("items") or []:
                row = dict(zip(fields, values))
                title = row.get("title") or ""
                if not title:
                    continue
                ann_date = _parse_date(row.get("ann_date"), since)
                out.append(
                    Announcement(
                        title=title,
                        announcement_date=ann_date,
                        source_key="tushare",
                        source_event_id=f"{code}-{row.get('ann_date')}-{title}",
                        source_url=row.get("url"),
                        pdf_url=row.get("url"),
                        event_type=row.get("ann_type") or "公告",
                        importance=4 if any(word in title for word in ("清算", "终止", "风险", "暂停")) else 3,
                        symbols=[code],
                        raw_json=row,
                    )
                )
    return out
