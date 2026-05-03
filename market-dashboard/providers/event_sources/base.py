from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass
class Announcement:
    title: str
    announcement_date: date
    source_key: str
    source_event_id: str | None = None
    source_url: str | None = None
    pdf_url: str | None = None
    event_type: str = "公告"
    importance: int = 3
    symbols: list[str] = field(default_factory=list)
    raw_json: dict[str, Any] = field(default_factory=dict)
