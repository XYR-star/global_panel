from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from providers.asset_universe import CN_INDEX_NAMES, default_cn_index_symbols, default_cn_symbols, default_us_symbols
from providers.common import DailyBars, NumericSeries
from providers.store import recent_rows


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

DEFAULT_CN_INDEX_SYMBOLS = default_cn_index_symbols()
DEFAULT_CN_EQUITY_SYMBOLS = default_cn_symbols()
DEFAULT_US_EQUITY_SYMBOLS = default_us_symbols()


def _symbols_from_env(name: str, default: str) -> list[str]:
    symbols: list[str] = []
    seen = set()
    for symbol in os.getenv(name, default).split(","):
        symbol = symbol.strip().upper()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


def _date_from_timestamp(value: int) -> datetime:
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _number(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _name(symbol: str, meta: dict[str, Any], fallback: str | None = None) -> str:
    return str(fallback or meta.get("shortName") or meta.get("longName") or symbol)


def fetch_chart(symbol: str) -> tuple[dict[str, Any], list[tuple[datetime, float]], list[tuple[datetime, float]], list[dict[str, Any]]]:
    params = {
        "range": os.getenv("YAHOO_HISTORY_RANGE", "2y"),
        "interval": os.getenv("YAHOO_HISTORY_INTERVAL", "1d"),
        "includePrePost": "false",
        "events": "history",
    }
    url = f"{YAHOO_CHART_URL.format(symbol=urllib.parse.quote(symbol))}?{urllib.parse.urlencode(params)}"
    timeout = int(os.getenv("YAHOO_REQUEST_TIMEOUT_SECONDS", "15"))
    request = urllib.request.Request(url, headers={"User-Agent": os.getenv("YAHOO_USER_AGENT", "Mozilla/5.0")})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(f"{symbol}: {chart['error']}")
    result = (chart.get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"{symbol}: no Yahoo chart result")

    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adjclose = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    close_rows: list[tuple[datetime, float]] = []
    volume_rows: list[tuple[datetime, float]] = []
    bar_rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(timestamps):
        open_value = _number(opens[idx] if idx < len(opens) else None)
        high = _number(highs[idx] if idx < len(highs) else None)
        low = _number(lows[idx] if idx < len(lows) else None)
        close = _number(closes[idx] if idx < len(closes) else None)
        adj_close = _number(adjclose[idx] if idx < len(adjclose) else None)
        volume = _number(volumes[idx] if idx < len(volumes) else None)
        row_ts = _date_from_timestamp(ts)
        if close is not None:
            close_rows.append((row_ts, close))
            bar_rows.append(
                {
                    "ts": row_ts,
                    "open": open_value,
                    "high": high,
                    "low": low,
                    "close": close,
                    "adj_close": adj_close,
                    "volume": volume,
                }
            )
        if volume is not None:
            volume_rows.append((row_ts, volume))

    if not close_rows:
        raise RuntimeError(f"{symbol}: no close prices returned")
    cutoff_rows = {ts for ts, _value in recent_rows(close_rows)}
    return result.get("meta") or {}, recent_rows(close_rows), recent_rows(volume_rows), [row for row in bar_rows if row["ts"] in cutoff_rows]


def _sleep():
    time.sleep(float(os.getenv("YAHOO_REQUEST_DELAY_SECONDS", "0.25")))


def _warn(symbol: str, exc: Exception):
    if os.getenv("YAHOO_LOG_SKIPPED_SYMBOLS", "true").strip().lower() in {"1", "true", "yes", "on"}:
        print(f"Yahoo skip {symbol}: {exc}")


def collect_cn_indices() -> list[NumericSeries]:
    series: list[NumericSeries] = []
    for symbol in _symbols_from_env("YAHOO_CN_INDEX_SYMBOLS", DEFAULT_CN_INDEX_SYMBOLS):
        try:
            meta, close_rows, _volume_rows, _bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        series.append(
            NumericSeries(
                asset_key=f"YFIDX:{symbol}",
                asset_name=_name(symbol, meta, CN_INDEX_NAMES.get(symbol)),
                category="cn_indices",
                source="yahoo_finance",
                rows=close_rows,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "metric": "close", "currency": meta.get("currency")},
            )
        )
        _sleep()
    if not series:
        raise RuntimeError("No Yahoo CN index series returned")
    return series


def collect_cn_equity_daily() -> list[NumericSeries]:
    series: list[NumericSeries] = []
    for symbol in _symbols_from_env("YAHOO_CN_SYMBOLS", DEFAULT_CN_EQUITY_SYMBOLS):
        try:
            meta, close_rows, volume_rows, _bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        name = _name(symbol, meta)
        series.append(
            NumericSeries(
                asset_key=f"YFCN:{symbol}",
                asset_name=name,
                category="cn_equity_quote",
                source="yahoo_finance",
                rows=close_rows,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "metric": "daily_close", "currency": meta.get("currency")},
            )
        )
        if volume_rows:
            series.append(
                NumericSeries(
                    asset_key=f"YFCN:{symbol}:VOL",
                    asset_name=f"{name} 成交量",
                    category="cn_equity_volume",
                    source="yahoo_finance",
                    rows=volume_rows,
                    market="cn",
                    source_symbol=symbol,
                    meta={"provider": "yahoo_finance", "dataset": "chart", "metric": "daily_volume"},
                )
            )
        _sleep()
    if not series:
        raise RuntimeError("No Yahoo CN equity series returned")
    return series


def collect_us_equity_daily() -> list[NumericSeries]:
    series: list[NumericSeries] = []
    for symbol in _symbols_from_env("YAHOO_US_SYMBOLS", DEFAULT_US_EQUITY_SYMBOLS):
        try:
            meta, close_rows, volume_rows, _bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        name = _name(symbol, meta)
        series.append(
            NumericSeries(
                asset_key=f"YFUS:{symbol}",
                asset_name=name,
                category="us_equity_daily",
                source="yahoo_finance",
                rows=close_rows,
                market="us",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "metric": "daily_close", "currency": meta.get("currency")},
            )
        )
        if volume_rows:
            series.append(
                NumericSeries(
                    asset_key=f"YFUS:{symbol}:VOL",
                    asset_name=f"{name} Volume",
                    category="us_equity_volume",
                    source="yahoo_finance",
                    rows=volume_rows,
                    market="us",
                    source_symbol=symbol,
                    meta={"provider": "yahoo_finance", "dataset": "chart", "metric": "daily_volume"},
                )
            )
        _sleep()
    if not series:
        raise RuntimeError("No Yahoo US equity series returned")
    return series


def collect_cn_daily_bars() -> list[DailyBars]:
    bars: list[DailyBars] = []
    for symbol in _symbols_from_env("YAHOO_CN_SYMBOLS", DEFAULT_CN_EQUITY_SYMBOLS):
        try:
            meta, _close_rows, _volume_rows, bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        name = _name(symbol, meta)
        bars.append(
            DailyBars(
                asset_key=f"YFCN:{symbol}",
                asset_name=name,
                category="cn_equity_daily_bar",
                source="yahoo_finance",
                rows=bar_rows,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "currency": meta.get("currency")},
            )
        )
        _sleep()
    if not bars:
        raise RuntimeError("No Yahoo CN daily bars returned")
    return bars


def collect_cn_index_daily_bars() -> list[DailyBars]:
    bars: list[DailyBars] = []
    for symbol in _symbols_from_env("YAHOO_CN_INDEX_SYMBOLS", DEFAULT_CN_INDEX_SYMBOLS):
        try:
            meta, _close_rows, _volume_rows, bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        bars.append(
            DailyBars(
                asset_key=f"YFIDX:{symbol}",
                asset_name=_name(symbol, meta, CN_INDEX_NAMES.get(symbol)),
                category="cn_index_daily_bar",
                source="yahoo_finance",
                rows=bar_rows,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "currency": meta.get("currency")},
            )
        )
        _sleep()
    if not bars:
        raise RuntimeError("No Yahoo CN index daily bars returned")
    return bars


def collect_us_daily_bars() -> list[DailyBars]:
    bars: list[DailyBars] = []
    for symbol in _symbols_from_env("YAHOO_US_SYMBOLS", DEFAULT_US_EQUITY_SYMBOLS):
        try:
            meta, _close_rows, _volume_rows, bar_rows = fetch_chart(symbol)
        except Exception as exc:  # noqa: BLE001
            _warn(symbol, exc)
            continue
        name = _name(symbol, meta)
        bars.append(
            DailyBars(
                asset_key=f"YFUS:{symbol}",
                asset_name=name,
                category="us_equity_daily_bar",
                source="yahoo_finance",
                rows=bar_rows,
                market="us",
                source_symbol=symbol,
                meta={"provider": "yahoo_finance", "dataset": "chart", "currency": meta.get("currency")},
            )
        )
        _sleep()
    if not bars:
        raise RuntimeError("No Yahoo US daily bars returned")
    return bars
