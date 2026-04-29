from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from providers.common import NumericSeries
from providers.store import recent_rows


FRED_TIMEOUT = 30
HTTP_SESSION = requests.Session()
HTTP_SESSION.mount(
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


@dataclass(frozen=True)
class FredSeries:
    key: str
    name: str
    category: str
    series_id: str
    scale: float = 1.0
    unit: str | None = None
    market: str = "global"


MARKET_SERIES = [
    FredSeries("SPX", "S&P 500", "indices", "SP500", market="us"),
    FredSeries("NDX", "Nasdaq Composite", "indices", "NASDAQCOM", market="us"),
    FredSeries("DJI", "Dow Jones Industrial Average", "indices", "DJIA", market="us"),
    FredSeries("NKY", "Nikkei 225", "indices", "NIKKEI225", market="jp"),
    FredSeries("UKX", "NASDAQ UK Index", "indices", "NASDAQNQGB", market="uk"),
    FredSeries("DAX", "NASDAQ Germany Index", "indices", "NASDAQNQDE", market="de"),
    FredSeries("CAC", "NASDAQ France Index", "indices", "NASDAQNQFR", market="fr"),
    FredSeries("HKG", "NASDAQ Hong Kong Index", "indices", "NASDAQNQHK", market="hk"),
    FredSeries("CHN", "NASDAQ China Index", "indices", "NASDAQNQCN", market="cn"),
    FredSeries("IND", "NASDAQ India Index", "indices", "NASDAQNQIN", market="in"),
    FredSeries("AUS", "NASDAQ Australia Index", "indices", "NASDAQNQAU", market="au"),
    FredSeries("CAN", "NASDAQ Canada Index", "indices", "NASDAQNQCA", market="ca"),
    FredSeries("BRA", "NASDAQ Brazil Index", "indices", "NASDAQNQBR", market="br"),
    FredSeries("KOR", "NASDAQ Korea Index", "indices", "NASDAQNQKR", market="kr"),
    FredSeries("TWN", "NASDAQ Taiwan Index", "indices", "NASDAQNQTW", market="tw"),
    FredSeries("WTI", "WTI Crude Oil", "commodities", "DCOILWTICO"),
    FredSeries("BRENT", "Brent Crude Oil", "commodities", "DCOILBRENTEU"),
    FredSeries("COPPER", "Copper (USD/lb)", "commodities", "PCOPPUSDM", 1 / 2204.62262185, "USD per pound"),
    FredSeries("GOLD_IDX", "Gold Index", "commodities", "NASDAQQGLDI"),
    FredSeries("DXY", "Broad Dollar Index", "fx", "DTWEXBGS"),
    FredSeries("EURUSD", "EUR/USD", "fx", "DEXUSEU"),
    FredSeries("GBPUSD", "GBP/USD", "fx", "DEXUSUK"),
    FredSeries("USDJPY", "USD/JPY", "fx", "DEXJPUS"),
    FredSeries("USDCNY", "USD/CNY", "fx", "DEXCHUS"),
    FredSeries("USDCHF", "USD/CHF", "fx", "DEXSZUS"),
    FredSeries("USDKRW", "USD/KRW", "fx", "DEXKOUS"),
    FredSeries("USDINR", "USD/INR", "fx", "DEXINUS"),
    FredSeries("USDBRL", "USD/BRL", "fx", "DEXBZUS"),
    FredSeries("USDCAD", "USD/CAD", "fx", "DEXCAUS"),
    FredSeries("AUDUSD", "AUD/USD", "fx", "DEXUSAL"),
    FredSeries("USDSGD", "USD/SGD", "fx", "DEXSIUS"),
]

MACRO_SERIES = [
    FredSeries("UST2Y", "US Treasury 2Y", "rates", "DGS2", market="us"),
    FredSeries("UST10Y", "US Treasury 10Y", "rates", "DGS10", market="us"),
    FredSeries("UST30Y", "US Treasury 30Y", "rates", "DGS30", market="us"),
    FredSeries("CPI", "Consumer Price Index", "macro", "CPIAUCSL", market="us"),
    FredSeries("UNRATE", "Unemployment Rate", "macro", "UNRATE", market="us"),
    FredSeries("FEDFUNDS", "Federal Funds Rate", "macro", "FEDFUNDS", market="us"),
    FredSeries("HY_YIELD", "US High Yield Effective Yield", "credit", "BAMLH0A0HYM2EY", market="us"),
    FredSeries("IG_OAS", "US Corporate OAS", "credit", "BAMLC0A0CM", market="us"),
    FredSeries("FED_BALANCE", "Fed Balance Sheet", "liquidity", "WALCL", market="us"),
    FredSeries("FED_TREASURY", "Fed Treasury Holdings", "liquidity", "TREAST", market="us"),
    FredSeries("ROWUST_OFFICIAL_FLOW", "Foreign Official Treasury Transactions", "flows", "BOGZ1FA263061130Q"),
    FredSeries("ROW_US_EQ_FLOW", "Rest of World U.S. Equity Transactions", "flows", "ROWCEAQ027S"),
    FredSeries("ROW_US_RISK_FLOW", "Rest of World U.S. Risk Asset Transactions", "flows", "BOGZ1FU263064003Q"),
    FredSeries("CN_RESERVES", "China Reserves ex Gold", "reserves", "TRESEGCNM052N", market="cn"),
    FredSeries("JP_RESERVES", "Japan Reserves ex Gold", "reserves", "TRESEGJPM052N", market="jp"),
    FredSeries("EZ_RESERVES", "Euro Area Reserves ex Gold", "reserves", "TRESEGEZA052N", market="ez"),
    FredSeries("UK_RESERVES", "UK Reserves ex Gold", "reserves", "TRESEGGBM052N", market="uk"),
    FredSeries("IN_RESERVES", "India Reserves ex Gold", "reserves", "TRESEGINM052N", market="in"),
]


def fred_csv(series_id: str):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    response = HTTP_SESSION.get(url, timeout=FRED_TIMEOUT)
    response.raise_for_status()
    rows = []
    reader = csv.DictReader(io.StringIO(response.text))
    date_key = "DATE" if "DATE" in reader.fieldnames else "observation_date"
    for row in reader:
        value = row.get(series_id)
        if not value or value == ".":
            continue
        ts = datetime.fromisoformat(row[date_key]).replace(tzinfo=timezone.utc)
        rows.append((ts, float(value)))
    if not rows:
        raise RuntimeError(f"No FRED data returned for {series_id}")
    return rows


def nyfed_gscpi_csv():
    url = "https://www.newyorkfed.org/medialibrary/research/interactives/data/gscpi/gscpi_interactive_data.csv"
    response = HTTP_SESSION.get(url, timeout=FRED_TIMEOUT)
    response.raise_for_status()
    text = response.text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or len(reader.fieldnames) < 2:
        raise RuntimeError("Unexpected GSCPI CSV format")
    date_column = reader.fieldnames[0]
    latest_vintage = reader.fieldnames[-1]
    rows = []
    for row in reader:
        value = row.get(latest_vintage)
        if not value or value == ".":
            continue
        ts = datetime.strptime(row[date_column], "%d-%b-%Y").replace(tzinfo=timezone.utc)
        rows.append((ts, float(value)))
    if not rows:
        raise RuntimeError("No NY Fed GSCPI data returned")
    return rows


def fred_numeric_series(series: FredSeries) -> NumericSeries:
    rows = recent_rows(fred_csv(series.series_id))
    if series.scale != 1.0:
        rows = [(ts, value * series.scale) for ts, value in rows]
    meta = {"series_id": series.series_id, "provider": "fredgraph"}
    if series.unit:
        meta["unit"] = series.unit
    if series.scale != 1.0:
        meta["scale"] = series.scale
    return NumericSeries(
        asset_key=series.key,
        asset_name=series.name,
        category=series.category,
        source="fredgraph",
        rows=rows,
        market=series.market,
        source_symbol=series.series_id,
        meta=meta,
    )


def nyfed_shipping_series() -> NumericSeries:
    meta = {
        "provider": "newyorkfed",
        "dataset": "Global Supply Chain Pressure Index",
        "note": "Latest vintage column from official NY Fed interactive CSV",
    }
    return NumericSeries(
        asset_key="GSCPI",
        asset_name="Global Supply Chain Pressure Index",
        category="shipping",
        source="newyorkfed",
        rows=recent_rows(nyfed_gscpi_csv()),
        market="global",
        source_symbol="gscpi_interactive_data",
        meta=meta,
    )

