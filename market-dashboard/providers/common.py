from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class NumericSeries:
    asset_key: str
    asset_name: str
    category: str
    source: str
    rows: list[tuple[datetime, float]]
    market: str = "global"
    source_symbol: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TextRecord:
    record_id: str
    asset_key: str
    asset_name: str
    category: str
    source: str
    ts: datetime
    title: str
    url: str | None = None
    body: str | None = None
    market: str = "global"
    source_symbol: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DailyBars:
    asset_key: str
    asset_name: str
    category: str
    source: str
    rows: list[dict[str, Any]]
    market: str
    source_symbol: str
    meta: dict[str, Any] = field(default_factory=dict)
