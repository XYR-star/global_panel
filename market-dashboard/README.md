# Market Dashboard

Grafana-based market dashboard for `market.heyrickishere.com`.

## What is here

- `docker-compose.yml`: Grafana + Postgres
- `sync_market_data.py`: fetches market and macro series into Postgres
- `providers/`: provider modules for FRED, NY Fed, AKShare, and SEC EDGAR
- `ai_app/`: `/ai/` research subpage and JSON APIs
- `grafana/provisioning/`: datasource + dashboard provisioning
- `deploy/nginx/`: nginx site config
- `deploy/systemd/`: timer-driven sync service

## Default data providers

- Global market, macro, rates, credit, FX, and commodity data: FRED `fredgraph.csv`
- Global supply-chain pressure: NY Fed GSCPI CSV
- China A-share indices, sample quotes, industry boards, fund flow, and company profiles: AKShare
- AKShare V2 adds broader A-share snapshots, selected stock histories, market fund flow, concept boards, industry constituents, financial indicators, and company news.
- U.S. company filings and XBRL facts: SEC EDGAR official JSON APIs

The default stack is free-first and does not require paid market data keys. AKShare and SEC EDGAR are provider modules, so later additions such as Tushare, BaoStock, paid market feeds, or iFinD/Skill tools can be added without changing the Grafana datasource or the core Postgres tables.

## V2 data model

- `market_data_points`: numeric time series for charts and agent evidence.
- `market_asset_snapshot`: latest numeric values; existing Grafana panels continue to use this table.
- `data_asset_catalog`: asset/source metadata with `latest_observation_ts` and `last_synced_at`.
- `market_text_records`: structured text evidence such as SEC filings and A-share company profiles.
- `data_sync_status`: per-provider run status, including sync time and latest observation time.

The dashboard refreshes every 10 minutes, and the systemd timer runs the sync every 10 minutes. Some sources update daily, monthly, or quarterly, so the status panel separates `sync_run` from `latest_observation`.

## AI research subpage

The V2 research page is served at `/ai/` by the `ai-research` service. It reads Postgres directly and exposes:

- `/ai/`: HTML research workspace
- `/ai/api/health`: database health
- `/ai/api/data-status`: provider status
- `/ai/api/summary`: A-share, SEC, inventory, and freshness summary

The first version shows data-backed research evidence. LLM-generated agent reports can be added on top of the same API/data model.

## Agent reports

`generate_agent_report.py` builds a compact research context from the database, calls an OpenAI-compatible chat completions endpoint, and stores the result in:

- `agent_reports`
- `agent_report_evidence`

If `OPENAI_API_KEY` is missing, the script still writes a `needs_config` report so `/ai/` can explain what is missing without failing.

Required/optional model settings:

- `OPENAI_API_KEY=`
- `OPENAI_BASE_URL=https://api.openai.com/v1`
- `OPENAI_MODEL=gpt-4.1-mini`
- `AGENT_REPORT_TYPE=market_research_v1`
- `AGENT_TEMPERATURE=0.2`
- `AGENT_LLM_TIMEOUT_SECONDS=90`
- `MARKET_DATA_ROOT=/www/market-dashboard-data`

Run once:

```bash
cd /root/global_panel/market-dashboard
set -a; . /etc/market-dashboard.env; set +a
./.venv/bin/python generate_agent_report.py
```

Install the hourly report timer from `deploy/systemd/market-agent-report.*` after copying the files to `/etc/systemd/system/`.

## Provider configuration

Set these in `/etc/market-dashboard.env` as needed:

- `AKSHARE_ENABLED=true`
- `AKSHARE_FINANCIAL_SYMBOLS=600519,000001,300750,300059,688981`
- `AKSHARE_ANNOUNCEMENT_SYMBOLS=600519,000001,300750,300059,688981`
- `AKSHARE_NEWS_SYMBOLS=300059,600519,300750`
- `MARKET_SYNC_USE_CACHE_ON_FAILURE=true`
- `AKSHARE_MARKET_FLOW_INTERVAL_MINUTES=30`
- `AKSHARE_FINANCIAL_INTERVAL_MINUTES=1440`
- `AKSHARE_ANNOUNCEMENT_INTERVAL_MINUTES=360`
- `AKSHARE_NEWS_INTERVAL_MINUTES=180`
- `YAHOO_FINANCE_ENABLED=true`
- `YAHOO_HISTORY_RANGE=2y`
- `YAHOO_HISTORY_INTERVAL=1d`
- `YAHOO_CN_INDEX_SYMBOLS=000001.SS,399001.SZ,399006.SZ,000300.SS,000905.SS,000852.SS,000688.SS`
- `YAHOO_CN_SYMBOLS=600519.SS,000001.SZ,300750.SZ,300059.SZ,688981.SS,...`
- `YAHOO_US_SYMBOLS=AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,AVGO,AMD,ASML,TSM,...`
- `YAHOO_CN_INDEX_INTERVAL_MINUTES=360`
- `YAHOO_CN_EQUITY_INTERVAL_MINUTES=360`
- `YAHOO_US_EQUITY_INTERVAL_MINUTES=360`
- `YAHOO_DAILY_BARS_INTERVAL_MINUTES=360`
- `SEC_EDGAR_ENABLED=true`
- `SEC_EDGAR_TICKERS=AAPL,MSFT,NVDA,GOOGL,AMZN,META,TSLA,AVGO,AMD,INTC,MU,ORCL,CRM,JPM,BAC,GS,XOM,CVX,UNH,LLY,PFE,COST,WMT,MCD`
- `SEC_EDGAR_FILINGS_PER_TICKER=12`
- `SEC_EDGAR_INTERVAL_MINUTES=360`
- `SEC_EDGAR_USER_AGENT=market-dashboard/2.0 contact=admin@market.heyrickishere.com`
- `SEC_EDGAR_REQUEST_DELAY_SECONDS=0.12`
- `SEC_EDGAR_FETCH_FILING_TEXT=true`
- `SEC_EDGAR_TEXT_FILINGS_PER_TICKER=4`
- `SEC_EDGAR_FILING_TEXT_MAX_CHARS=12000`
- `FORCE_MARKET_SYNC=false`

SEC asks automated clients to identify themselves. Replace the default `SEC_EDGAR_USER_AGENT` contact with a real contact address before production use.

## Local bootstrap outline

1. Copy `.env.example` to `/etc/market-dashboard.env` and set passwords.
2. Install Docker, Docker Compose v2, Python venv support.
3. Create `.venv` and install `requirements.txt`.
4. `docker compose --env-file /etc/market-dashboard.env up -d`
5. Run the sync once.
6. Install the systemd timer and nginx config.
7. Add DNS for `market.heyrickishere.com` and issue a cert with certbot.

## Validation queries

```bash
docker exec market-dashboard-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT source, ok, message, latest_observation_ts, last_run FROM data_sync_status ORDER BY source;"

docker exec market-dashboard-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT provider, category, count(*) FROM data_asset_catalog GROUP BY provider, category ORDER BY provider, category;"

docker exec market-dashboard-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT source, category, count(*) FROM market_text_records GROUP BY source, category ORDER BY source, category;"

docker exec market-dashboard-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT status, title, model, generated_at FROM agent_reports ORDER BY generated_at DESC LIMIT 5;"
```
