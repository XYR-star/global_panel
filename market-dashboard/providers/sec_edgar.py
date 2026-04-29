from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from html import unescape
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from providers.common import NumericSeries, TextRecord


SEC_BASE = "https://data.sec.gov"
SEC_FILES_BASE = "https://www.sec.gov"
SEC_TIMEOUT = 30
SEC_FORMS = {"10-K", "10-Q", "8-K"}
DEFAULT_SEC_TICKERS = (
    "AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,AVGO,AMD,INTC,MU,ORCL,CRM,"
    "JPM,BAC,GS,XOM,CVX,UNH,LLY,PFE,COST,WMT,MCD"
)
FACT_TAGS = {
    "Revenues": "Revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "Revenue",
    "NetIncomeLoss": "Net Income",
    "Assets": "Assets",
    "Liabilities": "Liabilities",
    "StockholdersEquity": "Stockholders Equity",
    "EarningsPerShareDiluted": "Diluted EPS",
}


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": os.getenv(
                "SEC_EDGAR_USER_AGENT",
                "market-dashboard/2.0 contact=admin@market.heyrickishere.com",
            ),
            "Accept-Encoding": "gzip, deflate",
        }
    )
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1.0,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
        ),
    )
    return session


def _json(session: requests.Session, url: str) -> Any:
    time.sleep(float(os.getenv("SEC_EDGAR_REQUEST_DELAY_SECONDS", "0.12")))
    response = session.get(url, timeout=SEC_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _text(session: requests.Session, url: str) -> str:
    time.sleep(float(os.getenv("SEC_EDGAR_REQUEST_DELAY_SECONDS", "0.12")))
    response = session.get(url, timeout=SEC_TIMEOUT)
    response.raise_for_status()
    return response.text


def load_company_tickers(session: requests.Session) -> dict[str, dict[str, Any]]:
    data = _json(session, f"{SEC_FILES_BASE}/files/company_tickers.json")
    return {item["ticker"].upper(): item for item in data.values()}


def _parse_filed_date(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _filing_url(cik: int, accession: str, primary_doc: str) -> str:
    cik_path = str(cik)
    accession_path = accession.replace("-", "")
    return f"{SEC_FILES_BASE}/Archives/edgar/data/{cik_path}/{accession_path}/{primary_doc}"


def _clean_filing_text(raw: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", raw)
    text = re.sub(r"(?is)<ix:header.*?</ix:header>", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def filing_body(session: requests.Session, url: str) -> str | None:
    if os.getenv("SEC_EDGAR_FETCH_FILING_TEXT", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    max_chars = int(os.getenv("SEC_EDGAR_FILING_TEXT_MAX_CHARS", "12000"))
    try:
        text = _clean_filing_text(_text(session, url))
    except Exception:
        return None
    return text[:max_chars] if text else None


def filing_records_for_ticker(session: requests.Session, ticker: str, company: dict[str, Any]) -> list[TextRecord]:
    cik = int(company["cik_str"])
    cik_padded = f"{cik:010d}"
    submissions = _json(session, f"{SEC_BASE}/submissions/CIK{cik_padded}.json")
    recent = submissions.get("filings", {}).get("recent", {})
    records: list[TextRecord] = []
    text_limit = int(os.getenv("SEC_EDGAR_TEXT_FILINGS_PER_TICKER", "4"))
    text_count = 0
    for idx, form in enumerate(recent.get("form", [])):
        if form not in SEC_FORMS:
            continue
        filed = recent["filingDate"][idx]
        accession = recent["accessionNumber"][idx]
        primary_doc = recent["primaryDocument"][idx]
        url = _filing_url(cik, accession, primary_doc)
        body = None
        if text_count < text_limit:
            body = filing_body(session, url)
            if body:
                text_count += 1
        title = f"{ticker.upper()} {form} filed {filed}"
        records.append(
            TextRecord(
                record_id=f"sec:{ticker.upper()}:{accession}",
                asset_key=f"US:{ticker.upper()}",
                asset_name=company["title"],
                category="sec_filing",
                source="sec_edgar",
                ts=_parse_filed_date(filed),
                title=title,
                url=url,
                body=body,
                market="us",
                source_symbol=ticker.upper(),
                meta={
                    "provider": "sec_edgar",
                    "cik": cik_padded,
                    "form": form,
                    "accession_number": accession,
                    "primary_document": primary_doc,
                    "report_date": recent.get("reportDate", [None])[idx],
                },
            )
        )
        if len(records) >= int(os.getenv("SEC_EDGAR_FILINGS_PER_TICKER", "12")):
            break
    return records


def _fact_rows(facts: dict[str, Any], tag: str):
    fact = facts.get("facts", {}).get("us-gaap", {}).get(tag)
    if not fact:
        return []
    units = fact.get("units", {})
    unit_key = "USD" if "USD" in units else "USD/shares" if "USD/shares" in units else None
    if unit_key is None:
        return []
    rows = []
    for item in units.get(unit_key, []):
        if item.get("form") not in {"10-K", "10-Q"} or "val" not in item or not item.get("end"):
            continue
        try:
            ts = _parse_filed_date(item["end"])
            rows.append((ts, float(item["val"])))
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda row: row[0])
    deduped = {}
    for ts, value in rows:
        deduped[ts] = value
    return list(deduped.items())[-24:]


def fact_series_for_ticker(session: requests.Session, ticker: str, company: dict[str, Any]) -> list[NumericSeries]:
    cik = int(company["cik_str"])
    cik_padded = f"{cik:010d}"
    facts = _json(session, f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik_padded}.json")
    series_list: list[NumericSeries] = []
    for tag, label in FACT_TAGS.items():
        rows = _fact_rows(facts, tag)
        if not rows:
            continue
        unit = "USD/shares" if tag == "EarningsPerShareDiluted" else "USD"
        series_list.append(
            NumericSeries(
                asset_key=f"SEC:{ticker.upper()}:{tag}",
                asset_name=f"{company['title']} {label}",
                category="fundamentals",
                source="sec_edgar",
                rows=rows,
                market="us",
                source_symbol=ticker.upper(),
                meta={
                    "provider": "sec_edgar",
                    "cik": cik_padded,
                    "taxonomy": "us-gaap",
                    "tag": tag,
                    "unit": unit,
                    "company_asset_key": f"US:{ticker.upper()}",
                },
            )
        )
    return series_list


def collect_sec_edgar():
    session = _session()
    tickers = [
        ticker.strip().upper()
        for ticker in os.getenv("SEC_EDGAR_TICKERS", DEFAULT_SEC_TICKERS).split(",")
        if ticker.strip()
    ]
    companies = load_company_tickers(session)
    numeric: list[NumericSeries] = []
    text: list[TextRecord] = []
    errors: list[str] = []
    for ticker in tickers:
        company = companies.get(ticker)
        if not company:
            errors.append(f"{ticker}: ticker not found in SEC company_tickers")
            continue
        try:
            text.extend(filing_records_for_ticker(session, ticker, company))
            numeric.extend(fact_series_for_ticker(session, ticker, company))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{ticker}: {exc}")
    return numeric, text, errors
