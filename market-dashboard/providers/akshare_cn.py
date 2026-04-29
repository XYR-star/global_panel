from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any
from urllib.parse import urlparse

import requests
from providers.common import NumericSeries, TextRecord
from providers.store import recent_rows


def _require_akshare():
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:
        raise RuntimeError("AKShare is not installed. Run: pip install -r requirements.txt") from exc
    return ak


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
            if value in {"", "-", "--", "nan", "None"}:
                return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _date(value: Any) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    elif not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value)[:10])
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _pick_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _symbols_from_env(name: str, default: str) -> list[str]:
    return [symbol.strip().zfill(6) for symbol in os.getenv(name, default).split(",") if symbol.strip()]


def _secucode(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if "." in symbol:
        return symbol
    if symbol.startswith(("6", "9")):
        return f"{symbol}.SH"
    return f"{symbol}.SZ"


def _record_id(*parts: Any) -> str:
    return ":".join(str(part).replace(" ", "_").replace(":", "_") for part in parts if part is not None)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


ANNOUNCEMENT_TYPE_RULES = (
    ("财务报告", ("年度报告", "季度报告", "半年度报告", "业绩预告", "业绩快报", "审计报告", "财务报告")),
    ("董事会决议", ("董事会", "监事会", "股东大会决议", "会议决议")),
    ("融资/配售", ("配售", "增发", "定增", "可转债", "债券", "融资", "募集资金", "H股")),
    ("并购重组", ("收购", "并购", "重组", "资产购买", "资产出售", "交易预案")),
    ("风险提示", ("风险提示", "异常波动", "退市", "处罚", "诉讼", "仲裁", "减值", "亏损")),
    ("问询回复", ("问询函", "问询回复", "关注函", "监管函", "回复公告")),
    ("分红派息", ("分红", "派息", "权益分派", "利润分配", "股息")),
    ("高管变动", ("董事辞职", "监事辞职", "高级管理人员", "高管", "聘任", "离任")),
)


def _classify_announcement(title: str, raw_type: str) -> str:
    text = f"{title} {raw_type}"
    for normalized, keywords in ANNOUNCEMENT_TYPE_RULES:
        if any(keyword in text for keyword in keywords):
            return normalized
    return raw_type or "其他公告"


def _clean_html_text(raw: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", raw)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_pdf_text(content: bytes, max_chars: int) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return ""
    try:
        import io

        reader = PdfReader(io.BytesIO(content))
        parts = []
        for page in reader.pages[:8]:
            parts.append(page.extract_text() or "")
            if sum(len(part) for part in parts) >= max_chars:
                break
        return re.sub(r"\s+", " ", "\n".join(parts)).strip()[:max_chars]
    except Exception:
        return ""


def _fetch_announcement_body(url: str | None) -> tuple[str, dict[str, Any]]:
    if not url or os.getenv("AKSHARE_ANNOUNCEMENT_FETCH_BODY", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        return "", {}
    max_chars = int(os.getenv("AKSHARE_ANNOUNCEMENT_BODY_MAX_CHARS", "5000"))
    timeout = float(os.getenv("AKSHARE_ANNOUNCEMENT_BODY_TIMEOUT_SECONDS", "12"))
    headers = {"User-Agent": os.getenv("AKSHARE_USER_AGENT", "Mozilla/5.0 market-dashboard/2.0")}
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return "", {"body_fetch_error": str(exc)[:300]}
    content_type = response.headers.get("content-type", "").lower()
    path = urlparse(url).path.lower()
    if "pdf" in content_type or path.endswith(".pdf"):
        text = _extract_pdf_text(response.content, max_chars)
        return text, {"body_content_type": "pdf", "body_extracted": bool(text)}
    text = _clean_html_text(response.text)
    return text[:max_chars], {"body_content_type": content_type or "html", "body_extracted": bool(text)}


def _historical_index_series(ak, symbol: str, name: str) -> NumericSeries:
    df = ak.stock_zh_index_daily_em(symbol=symbol)
    date_col = _pick_column(df.columns, ["date", "日期"])
    close_col = _pick_column(df.columns, ["close", "收盘"])
    if date_col is None or close_col is None:
        raise RuntimeError(f"Unexpected AKShare index columns for {symbol}: {list(df.columns)}")
    rows = []
    for _, row in df.iterrows():
        value = _number(row[close_col])
        if value is None:
            continue
        rows.append((_date(row[date_col]), value))
    rows.sort(key=lambda item: item[0])
    rows = recent_rows(rows)
    if not rows:
        raise RuntimeError(f"No AKShare index data returned for {symbol}")
    return NumericSeries(
        asset_key=f"CNIDX:{symbol}",
        asset_name=name,
        category="cn_indices",
        source="akshare",
        rows=rows,
        market="cn",
        source_symbol=symbol,
        meta={"provider": "akshare", "dataset": "stock_zh_index_daily_em"},
    )


def collect_cn_indices() -> list[NumericSeries]:
    ak = _require_akshare()
    index_map = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000300": "沪深300",
        "sh000905": "中证500",
        "sh000852": "中证1000",
        "sh000688": "科创50",
    }
    return [_historical_index_series(ak, symbol, name) for symbol, name in index_map.items()]


def collect_a_share_quotes() -> list[NumericSeries]:
    ak = _require_akshare()
    limit = int(os.getenv("AKSHARE_A_SHARE_QUOTE_LIMIT", "80"))
    df = ak.stock_zh_a_spot_em()
    code_col = _pick_column(df.columns, ["代码", "code"])
    name_col = _pick_column(df.columns, ["名称", "name"])
    price_col = _pick_column(df.columns, ["最新价", "最新", "price"])
    change_col = _pick_column(df.columns, ["涨跌幅", "change_percent"])
    if code_col is None or name_col is None or price_col is None:
        raise RuntimeError(f"Unexpected AKShare A-share quote columns: {list(df.columns)}")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows: list[NumericSeries] = []
    for _, item in df.head(limit).iterrows():
        value = _number(item[price_col])
        if value is None:
            continue
        code = str(item[code_col]).zfill(6)
        meta = {
            "provider": "akshare",
            "dataset": "stock_zh_a_spot_em",
            "change_percent": _number(item[change_col]) if change_col else None,
        }
        rows.append(
            NumericSeries(
                asset_key=f"CN:{code}",
                asset_name=str(item[name_col]),
                category="cn_equity_quote",
                source="akshare",
                rows=[(now, value)],
                market="cn",
                source_symbol=code,
                meta=meta,
            )
        )
    return rows


def collect_a_share_full_quotes() -> list[NumericSeries]:
    ak = _require_akshare()
    limit = int(os.getenv("AKSHARE_A_SHARE_FULL_QUOTE_LIMIT", "600"))
    df = ak.stock_zh_a_spot_em()
    code_col = _pick_column(df.columns, ["代码", "code"])
    name_col = _pick_column(df.columns, ["名称", "name"])
    price_col = _pick_column(df.columns, ["最新价", "最新", "price"])
    turnover_col = _pick_column(df.columns, ["成交额"])
    volume_col = _pick_column(df.columns, ["成交量"])
    change_col = _pick_column(df.columns, ["涨跌幅"])
    pe_col = _pick_column(df.columns, ["市盈率-动态", "市盈率"])
    pb_col = _pick_column(df.columns, ["市净率"])
    market_value_col = _pick_column(df.columns, ["总市值"])
    if code_col is None or name_col is None or price_col is None:
        raise RuntimeError(f"Unexpected AKShare A-share quote columns: {list(df.columns)}")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    series_list: list[NumericSeries] = []
    for _, item in df.head(limit).iterrows():
        price = _number(item[price_col])
        if price is None:
            continue
        code = str(item[code_col]).zfill(6)
        meta = {
            "provider": "akshare",
            "dataset": "stock_zh_a_spot_em",
            "change_percent": _number(item[change_col]) if change_col else None,
            "turnover": _number(item[turnover_col]) if turnover_col else None,
            "volume": _number(item[volume_col]) if volume_col else None,
            "pe": _number(item[pe_col]) if pe_col else None,
            "pb": _number(item[pb_col]) if pb_col else None,
            "market_value": _number(item[market_value_col]) if market_value_col else None,
        }
        series_list.append(
            NumericSeries(
                asset_key=f"CNFULL:{code}",
                asset_name=str(item[name_col]),
                category="cn_equity_full_quote",
                source="akshare",
                rows=[(now, price)],
                market="cn",
                source_symbol=code,
                meta=meta,
            )
        )
    return series_list


def collect_a_share_histories() -> list[NumericSeries]:
    ak = _require_akshare()
    symbols = _symbols_from_env("AKSHARE_HISTORY_SYMBOLS", "600519,000001,300750,300059,688981")
    period = os.getenv("AKSHARE_HISTORY_PERIOD", "daily")
    adjust = os.getenv("AKSHARE_HISTORY_ADJUST", "qfq")
    series_list: list[NumericSeries] = []
    for symbol in symbols:
        df = ak.stock_zh_a_hist(symbol=symbol, period=period, adjust=adjust)
        date_col = _pick_column(df.columns, ["日期", "date"])
        close_col = _pick_column(df.columns, ["收盘", "close"])
        name = symbol
        if date_col is None or close_col is None:
            raise RuntimeError(f"Unexpected AKShare history columns for {symbol}: {list(df.columns)}")
        rows = []
        for _, row in df.tail(int(os.getenv("AKSHARE_HISTORY_DAYS", "260"))).iterrows():
            value = _number(row[close_col])
            if value is not None:
                rows.append((_date(row[date_col]), value))
        if rows:
            series_list.append(
                NumericSeries(
                    asset_key=f"CNHIST:{symbol}",
                    asset_name=f"{name} 历史收盘价",
                    category="cn_equity_history",
                    source="akshare",
                    rows=rows,
                    market="cn",
                    source_symbol=symbol,
                    meta={"provider": "akshare", "dataset": "stock_zh_a_hist", "period": period, "adjust": adjust},
                )
            )
        time.sleep(float(os.getenv("AKSHARE_REQUEST_DELAY_SECONDS", "0.2")))
    return series_list


def collect_industry_boards() -> list[NumericSeries]:
    ak = _require_akshare()
    limit = int(os.getenv("AKSHARE_INDUSTRY_LIMIT", "80"))
    df = ak.stock_board_industry_name_em()
    name_col = _pick_column(df.columns, ["板块名称", "名称"])
    price_col = _pick_column(df.columns, ["最新价", "最新"])
    change_col = _pick_column(df.columns, ["涨跌幅"])
    if name_col is None or price_col is None:
        raise RuntimeError(f"Unexpected AKShare industry columns: {list(df.columns)}")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    series_list: list[NumericSeries] = []
    for _, item in df.head(limit).iterrows():
        value = _number(item[price_col])
        if value is None:
            continue
        name = str(item[name_col])
        series_list.append(
            NumericSeries(
                asset_key=f"CNIND:{name}",
                asset_name=name,
                category="cn_industry",
                source="akshare",
                rows=[(now, value)],
                market="cn",
                source_symbol=name,
                meta={
                    "provider": "akshare",
                    "dataset": "stock_board_industry_name_em",
                    "change_percent": _number(item[change_col]) if change_col else None,
                },
            )
        )
    return series_list


def collect_fund_flow() -> list[NumericSeries]:
    ak = _require_akshare()
    limit = int(os.getenv("AKSHARE_FUND_FLOW_LIMIT", "80"))
    df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
    name_col = _pick_column(df.columns, ["名称", "行业"])
    value_col = _pick_column(df.columns, ["今日主力净流入-净额", "主力净流入-净额", "净额"])
    if name_col is None or value_col is None:
        raise RuntimeError(f"Unexpected AKShare fund-flow columns: {list(df.columns)}")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    series_list: list[NumericSeries] = []
    for _, item in df.head(limit).iterrows():
        value = _number(item[value_col])
        if value is None:
            continue
        name = str(item[name_col])
        series_list.append(
            NumericSeries(
                asset_key=f"CNFLOW:{name}",
                asset_name=f"{name} 主力净流入",
                category="cn_fund_flow",
                source="akshare",
                rows=[(now, value)],
                market="cn",
                source_symbol=name,
                meta={"provider": "akshare", "dataset": "stock_sector_fund_flow_rank", "unit": "CNY"},
            )
        )
    return series_list


def collect_market_fund_flow() -> list[NumericSeries]:
    ak = _require_akshare()
    df = ak.stock_market_fund_flow()
    date_col = _pick_column(df.columns, ["日期"])
    value_col = _pick_column(df.columns, ["主力净流入-净额"])
    if date_col is None or value_col is None:
        raise RuntimeError(f"Unexpected AKShare market fund-flow columns: {list(df.columns)}")
    rows = []
    for _, item in df.tail(int(os.getenv("AKSHARE_MARKET_FLOW_DAYS", "121"))).iterrows():
        value = _number(item[value_col])
        if value is not None:
            rows.append((_date(item[date_col]), value))
    return [
        NumericSeries(
            asset_key="CNFLOW:MARKET_MAIN",
            asset_name="A股市场主力净流入",
            category="cn_market_fund_flow",
            source="akshare",
            rows=rows,
            market="cn",
            source_symbol="market",
            meta={"provider": "akshare", "dataset": "stock_market_fund_flow", "unit": "CNY"},
        )
    ]


def collect_concept_boards() -> list[NumericSeries]:
    ak = _require_akshare()
    limit = int(os.getenv("AKSHARE_CONCEPT_LIMIT", "80"))
    df = ak.stock_board_concept_name_em()
    name_col = _pick_column(df.columns, ["板块名称", "名称"])
    price_col = _pick_column(df.columns, ["最新价", "最新"])
    change_col = _pick_column(df.columns, ["涨跌幅"])
    if name_col is None or price_col is None:
        raise RuntimeError(f"Unexpected AKShare concept columns: {list(df.columns)}")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    series_list: list[NumericSeries] = []
    for _, item in df.head(limit).iterrows():
        value = _number(item[price_col])
        if value is None:
            continue
        name = str(item[name_col])
        series_list.append(
            NumericSeries(
                asset_key=f"CNCONCEPT:{name}",
                asset_name=name,
                category="cn_concept",
                source="akshare",
                rows=[(now, value)],
                market="cn",
                source_symbol=name,
                meta={
                    "provider": "akshare",
                    "dataset": "stock_board_concept_name_em",
                    "change_percent": _number(item[change_col]) if change_col else None,
                },
            )
        )
    return series_list


def collect_industry_constituents() -> list[TextRecord]:
    ak = _require_akshare()
    symbols = [s.strip() for s in os.getenv("AKSHARE_INDUSTRY_CONSTITUENTS", "半导体,电子元件,消费电子").split(",") if s.strip()]
    limit = int(os.getenv("AKSHARE_CONSTITUENT_LIMIT", "40"))
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    records: list[TextRecord] = []
    for symbol in symbols:
        df = ak.stock_board_industry_cons_em(symbol=symbol)
        code_col = _pick_column(df.columns, ["代码"])
        name_col = _pick_column(df.columns, ["名称"])
        if code_col is None or name_col is None:
            raise RuntimeError(f"Unexpected AKShare industry constituent columns for {symbol}: {list(df.columns)}")
        members = [
            {"code": str(row[code_col]).zfill(6), "name": str(row[name_col])}
            for _, row in df.head(limit).iterrows()
        ]
        records.append(
            TextRecord(
                record_id=_record_id("akshare", "industry_cons", symbol),
                asset_key=f"CNIND:{symbol}",
                asset_name=symbol,
                category="cn_industry_constituents",
                source="akshare",
                ts=now,
                title=f"{symbol} 行业成分股",
                body="\n".join(f"{item['code']} {item['name']}" for item in members),
                market="cn",
                source_symbol=symbol,
                meta={"provider": "akshare", "dataset": "stock_board_industry_cons_em", "members": members},
            )
        )
        time.sleep(float(os.getenv("AKSHARE_REQUEST_DELAY_SECONDS", "0.2")))
    return records


def collect_company_financial_indicators() -> list[TextRecord]:
    ak = _require_akshare()
    symbols = _symbols_from_env("AKSHARE_FINANCIAL_SYMBOLS", "600519,000001,300750,300059,688981")
    records: list[TextRecord] = []
    for symbol in symbols:
        df = ak.stock_financial_analysis_indicator_em(symbol=_secucode(symbol), indicator="按报告期")
        if df.empty:
            continue
        latest = df.head(int(os.getenv("AKSHARE_FINANCIAL_ROWS", "8")))
        body = latest.to_csv(index=False)
        ts_col = _pick_column(latest.columns, ["REPORT_DATE", "日期", "报告期"])
        ts = _date(latest.iloc[0][ts_col]) if ts_col else datetime.now(timezone.utc)
        name_col = _pick_column(latest.columns, ["SECURITY_NAME_ABBR"])
        asset_name = str(latest.iloc[0][name_col]) if name_col else symbol
        records.append(
            TextRecord(
                record_id=_record_id("akshare", "financial_indicator", symbol, ts.date()),
                asset_key=f"CN:{symbol}",
                asset_name=asset_name,
                category="cn_financial_indicator",
                source="akshare",
                ts=ts,
                title=f"{asset_name} 财务分析指标",
                body=body,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "akshare", "dataset": "stock_financial_analysis_indicator_em", "columns": list(latest.columns)},
            )
        )
        time.sleep(float(os.getenv("AKSHARE_REQUEST_DELAY_SECONDS", "0.2")))
    return records


def collect_company_announcements() -> list[TextRecord]:
    ak = _require_akshare()
    symbols = _symbols_from_env("AKSHARE_ANNOUNCEMENT_SYMBOLS", "600519,000001,300750,300059,688981")
    notice_type = os.getenv("AKSHARE_ANNOUNCEMENT_TYPE", "全部")
    lookback_days = int(os.getenv("AKSHARE_ANNOUNCEMENT_LOOKBACK_DAYS", "90"))
    limit = int(os.getenv("AKSHARE_ANNOUNCEMENT_LIMIT", "30"))
    end_date = datetime.now(timezone.utc).date()
    begin_date = end_date - timedelta(days=lookback_days)
    records: list[TextRecord] = []
    for symbol in symbols:
        df = ak.stock_individual_notice_report(
            security=symbol,
            symbol=notice_type,
            begin_date=begin_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        title_col = _pick_column(df.columns, ["公告标题"])
        type_col = _pick_column(df.columns, ["公告类型"])
        date_col = _pick_column(df.columns, ["公告日期"])
        url_col = _pick_column(df.columns, ["网址"])
        name_col = _pick_column(df.columns, ["名称"])
        if title_col is None or date_col is None:
            raise RuntimeError(f"Unexpected AKShare announcement columns for {symbol}: {list(df.columns)}")
        body_fetches = 0
        body_limit = int(os.getenv("AKSHARE_ANNOUNCEMENT_BODY_LIMIT_PER_SYMBOL", "3"))
        for _, row in df.head(limit).iterrows():
            ts = _date(row[date_col])
            title = _safe_text(row[title_col])
            announcement_type = _safe_text(row[type_col]) if type_col else ""
            normalized_type = _classify_announcement(title, announcement_type)
            name = _safe_text(row[name_col]) if name_col else symbol
            url = _safe_text(row[url_col]) if url_col else None
            body_text = ""
            body_meta: dict[str, Any] = {}
            if body_fetches < body_limit:
                body_text, body_meta = _fetch_announcement_body(url)
                body_fetches += 1
            body = f"公告类型: {announcement_type or normalized_type}\n归类: {normalized_type}"
            if body_text:
                body = f"{body}\n正文片段: {body_text}"
            records.append(
                TextRecord(
                    record_id=_record_id("akshare", "announcement", symbol, ts.date(), title[:80]),
                    asset_key=f"CN:{symbol}",
                    asset_name=name,
                    category="cn_company_announcement",
                    source="akshare",
                    ts=ts,
                    title=title,
                    url=url,
                    body=body,
                    market="cn",
                    source_symbol=symbol,
                    meta={
                        "provider": "akshare",
                        "dataset": "stock_individual_notice_report",
                        "announcement_type": announcement_type,
                        "normalized_announcement_type": normalized_type,
                        **body_meta,
                    },
                )
            )
        time.sleep(float(os.getenv("AKSHARE_REQUEST_DELAY_SECONDS", "0.2")))
    return records


def collect_company_news() -> list[TextRecord]:
    ak = _require_akshare()
    symbols = _symbols_from_env("AKSHARE_NEWS_SYMBOLS", "300059,600519,300750")
    limit = int(os.getenv("AKSHARE_NEWS_LIMIT", "8"))
    records: list[TextRecord] = []
    for symbol in symbols:
        df = ak.stock_news_em(symbol=symbol)
        title_col = _pick_column(df.columns, ["新闻标题"])
        body_col = _pick_column(df.columns, ["新闻内容"])
        ts_col = _pick_column(df.columns, ["发布时间"])
        source_col = _pick_column(df.columns, ["文章来源"])
        url_col = _pick_column(df.columns, ["新闻链接"])
        if title_col is None or ts_col is None:
            raise RuntimeError(f"Unexpected AKShare news columns for {symbol}: {list(df.columns)}")
        for _, row in df.head(limit).iterrows():
            ts = _date(row[ts_col])
            title = _safe_text(row[title_col])
            records.append(
                TextRecord(
                    record_id=_record_id("akshare", "news", symbol, ts.isoformat(), title[:40]),
                    asset_key=f"CN:{symbol}",
                    asset_name=symbol,
                    category="cn_company_news",
                    source="akshare",
                    ts=ts,
                    title=title,
                    url=_safe_text(row[url_col]) if url_col else None,
                    body=_safe_text(row[body_col]) if body_col else None,
                    market="cn",
                    source_symbol=symbol,
                    meta={"provider": "akshare", "dataset": "stock_news_em", "source_name": _safe_text(row[source_col]) if source_col else None},
                )
            )
        time.sleep(float(os.getenv("AKSHARE_REQUEST_DELAY_SECONDS", "0.2")))
    return records


def collect_company_profiles() -> list[TextRecord]:
    ak = _require_akshare()
    symbols = [
        symbol.strip().zfill(6)
        for symbol in os.getenv("AKSHARE_PROFILE_SYMBOLS", "600519,000001,300750").split(",")
        if symbol.strip()
    ]
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    records: list[TextRecord] = []
    for symbol in symbols:
        df = ak.stock_individual_info_em(symbol=symbol)
        if not {"item", "value"}.issubset(set(df.columns)):
            raise RuntimeError(f"Unexpected AKShare profile columns for {symbol}: {list(df.columns)}")
        profile = {str(row["item"]): str(row["value"]) for _, row in df.iterrows()}
        name = profile.get("股票简称") or profile.get("简称") or symbol
        body = "\n".join(f"{key}: {value}" for key, value in profile.items())
        records.append(
            TextRecord(
                record_id=f"akshare:profile:{symbol}",
                asset_key=f"CN:{symbol}",
                asset_name=name,
                category="cn_company_profile",
                source="akshare",
                ts=now,
                title=f"{name} 公司资料",
                body=body,
                market="cn",
                source_symbol=symbol,
                meta={"provider": "akshare", "dataset": "stock_individual_info_em", "profile": profile},
            )
        )
    return records
