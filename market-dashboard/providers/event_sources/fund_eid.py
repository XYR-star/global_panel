from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any

import httpx

from .base import Announcement

BASE = "http://eid.csrc.gov.cn"
FUND_HOME = f"{BASE}/fund/disclose/index.html"


def _rows_from_home(html: str, since: date, watchlist: list[dict[str, Any]]) -> list[Announcement]:
    codes = {str(item.get("symbol") or "").zfill(6) for item in watchlist}
    out: list[Announcement] = []
    for code in codes:
        if not code or code == "000000" or code not in html:
            continue
        for match in re.finditer(r"(?P<title>[^<>]{0,80}" + re.escape(code) + r"[^<>]{0,120})", html):
            title = re.sub(r"\s+", " ", match.group("title")).strip()
            if title:
                out.append(
                    Announcement(
                        title=title,
                        announcement_date=since,
                        source_key="fund_eid",
                        source_event_id=f"{code}-{since.isoformat()}-{hashlib.sha256(title.encode()).hexdigest()[:16]}",
                        source_url=FUND_HOME,
                        event_type="基金公告",
                        importance=3,
                        symbols=[code],
                        raw_json={"source": "eid_home_fallback"},
                    )
                )
    return out


def fetch_events(watchlist: list[dict[str, Any]], since: date, days: int, config: dict[str, Any] | None = None, secret: str | None = None) -> list[Announcement]:
    del days, config, secret
    headers = {"User-Agent": "Mozilla/5.0 PortfolioEventRadar/1.0"}
    with httpx.Client(timeout=httpx.Timeout(12.0, connect=6.0), headers=headers, follow_redirects=True) as client:
        response = client.get(FUND_HOME)
        response.raise_for_status()
        return _rows_from_home(response.text, since, watchlist)
