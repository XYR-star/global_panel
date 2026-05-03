from __future__ import annotations

from datetime import date
from typing import Any

from .base import Announcement


def fetch_events(watchlist: list[dict[str, Any]], since: date, days: int, config: dict[str, Any] | None = None, secret: str | None = None) -> list[Announcement]:
    del watchlist, since, days, config, secret
    return []
