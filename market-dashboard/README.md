# Ricky Portfolio App

Private, login-protected portfolio upload and analysis system. The app is designed for a personal workflow where one or more brokerage/exported holding files may be uploaded every trading day, while preserving every accepted file as historical evidence.

This repository used to contain a Grafana-style market dashboard. That stack has been removed. The current project is a focused portfolio analysis app.

## What It Does

- Accepts single or multi-file `.xlsx` uploads.
- Automatically infers the portfolio snapshot date from workbook labels or filename.
- Rejects duplicate files before persistence, so duplicates do not create records or copied files.
- Keeps every accepted file as an independent historical batch.
- Lets an existing batch be replaced when an upload was incomplete or wrong.
- Shows the latest effective portfolio by holding date, not by raw upload order.
- Maintains summary tables for long-term trend queries, so future hundreds/thousands of uploads do not require rescanning raw holdings every page load.
- Includes a login-protected data health page for row counts, summary coverage, storage usage, and backup status.
- Provides report pages, adjacent-batch comparison, trend cockpit, X-Ray style lookthrough, and monthly file management.
- Protects all portfolio data behind login and temporary lockout after repeated failed login attempts.

## Tech Stack

- **Backend:** FastAPI served by Uvicorn.
- **Database:** PostgreSQL 16.
- **Runtime:** Docker Compose with two services: `postgres` and `portfolio-app`.
- **Parsing:** `openpyxl` for workbook reading; `pandas` for date/value normalization helpers.
- **Portfolio analytics:** deterministic Python logic in `providers/portfolio.py` and import helpers in `sync_portfolio_data.py`.
- **Charts/UI:** server-rendered HTML/CSS with inline SVG charts. No frontend build pipeline is required.
- **Optional enrichment:** AkShare can be enabled for fund metadata/lookthrough enrichment, but the core dashboard works from uploaded files without external market APIs.

## Main Screens

- `/` — latest portfolio overview, upload form, core metrics, allocation bars, latest report excerpt, major holdings.
- `/timeline` — trend cockpit across accepted historical files.
- `/uploads` — monthly paged file management with filters and replacement controls.
- `/admin/data` — data health, summary coverage, storage, and backup status.
- `/reports/{batch_id}` — full report for a single upload batch.
- `/compare?from=...&to=...` — position and risk changes between two batches.

## Trend Cockpit

The timeline page uses accepted `complete` and `partial` batches ordered by `as_of_date`, then `uploaded_at` for same-day versions. It currently shows:

- Total assets trend.
- Daily, holding, and cumulative P/L trend.
- Top 3, Top 5, and Top 10 concentration trend.
- Equity-like, bond-like, QDII, and cash/monetary drift.
- Asset-type stacked area chart.
- Estimated drawdown based on uploaded portfolio values.
- Consecutive loss batch count.
- Top position changes versus the previous accepted batch.

No market benchmark or external price API is required for this first version.

## X-Ray / Lookthrough

The X-Ray section uses existing imported data to show:

- Top underlying holdings.
- Underlying holdings that appear through more than one parent fund/ETF.
- Industry/theme exposure.
- Main contributors behind each industry/theme exposure.

When external enrichment is unavailable, the system falls back to product-level exposure and workbook-derived categories.

## File Management

Uploads are treated as historical records:

- Each accepted file creates one `batch_id`.
- Multiple files on the same day are allowed.
- Duplicate SHA-256 files are rejected before saving.
- `/uploads` defaults to the latest month and supports month, status, filename/batch search, date range, and pagination.
- Replacement keeps the same `batch_id`, clears old derived rows for that batch, saves the new source file, and regenerates analysis.
- Replacing with the exact same file is a no-op.
- Replacing with a file already used by another batch is rejected.

There is intentionally no public delete or archive workflow in the UI because accepted files are considered real historical data.

## APIs

All APIs except health require login.

- `GET /api/health` — app health.
- `POST /api/upload` — single or multi-file upload. Form fields: `files` and optional `as_of_date`.
- `GET /api/uploads?month=YYYY-MM&status=complete&q=...&page=1` — paged upload records.
- `POST /api/uploads/{batch_id}/replace` — replace one batch with a new `.xlsx` file.
- `GET /api/portfolio/latest` — latest effective portfolio by holding date.
- `GET /api/portfolio/{batch_id}` — batch report data as JSON.
- `GET /api/analytics/timeline?months=6` — trend cockpit data.
- `GET /api/analytics/xray?batch_id=...` — lookthrough/X-Ray data.
- `GET /api/admin/data-health` — login-protected operational health and storage summary.

## Data Model

Important tables:

- `portfolio_import_batches` — one row per accepted upload batch.
- `portfolio_positions` — normalized current holdings for each batch.
- `portfolio_transactions` — workbook transaction records.
- `portfolio_closed_positions` — closed position history.
- `portfolio_asset_allocation` — computed asset buckets.
- `portfolio_risk_metrics` — deterministic concentration and exposure metrics.
- `portfolio_underlying_holdings` — lookthrough/proxy underlying holdings.
- `portfolio_industry_allocations` — industry/theme exposure.
- `portfolio_daily_summary` — one row per successful/partial batch for fast long-term trend queries.
- `portfolio_daily_allocation` — cached asset bucket time series.
- `portfolio_daily_exposure` — cached X-Ray/industry exposure summaries.
- `portfolio_login_failures` — login lockout tracking.

## Runtime Data

These paths are deployment examples and should not be committed with real data:

- PostgreSQL volume: `/www/market-dashboard-data/postgres`
- Uploaded files: `/var/lib/portfolio-app/uploads`
- Local backups: `/var/lib/portfolio-app/backups`
- Environment file: `.env.deploy` or `.env`

Both `.env` and `.env.deploy` are ignored by Git.

## Configuration

Copy `.env.example` to `.env.deploy` or `.env` and set private values:

```bash
POSTGRES_DB=market
POSTGRES_USER=market
POSTGRES_PASSWORD=change-me
PORTFOLIO_ADMIN_USERNAME=admin
PORTFOLIO_ADMIN_PASSWORD=change-me
PORTFOLIO_SESSION_SECRET=change-me
MARKET_DATA_ROOT=/www/market-dashboard-data
PORTFOLIO_DATA_ROOT=/var/lib/portfolio-app
PORTFOLIO_HTTP_PORT=8001
PORTFOLIO_AKSHARE_ENABLED=false
PORTFOLIO_MAX_UPLOAD_BYTES=26214400
PORTFOLIO_LOGIN_FAILURE_LIMIT=5
PORTFOLIO_LOGIN_LOCKOUT_MINUTES=30
PORTFOLIO_BACKUP_ROOT=/var/lib/portfolio-app/backups
PORTFOLIO_BACKUP_RETENTION_DAYS=30
```

Do not commit real passwords, cookies, uploaded files, or portfolio exports.

## Common Commands

```bash
docker compose --env-file .env.deploy ps
docker compose --env-file .env.deploy up -d --build
docker compose --env-file .env.deploy logs --tail=100 portfolio-app
docker compose --env-file .env.deploy restart portfolio-app
python3 scripts/rebuild_summaries.py
scripts/backup_portfolio.sh
docker compose --env-file .env.deploy exec postgres psql -U market -d market
```

## Long-Term Data Operations

The app keeps raw normalized tables for reports and cached summary tables for fast dashboards:

- Run `python3 scripts/rebuild_summaries.py` after manual database repairs or historical imports.
- `/admin/data` shows whether every successful batch has a summary row.
- `scripts/backup_portfolio.sh` creates a compressed PostgreSQL dump and upload-directory archive.
- The systemd units in `systemd/` run the backup daily at 03:20 server time when installed.

Restore outline:

1. Stop `portfolio-app`.
2. Restore the PostgreSQL dump into the configured database.
3. Restore the upload archive under the configured upload root.
4. Start `portfolio-app`, then run `python3 scripts/rebuild_summaries.py` if summary rows need refreshing.

## Validation Checklist

- Upload several `.xlsx` files and confirm each accepted file has a separate batch.
- Re-upload the same file and confirm it is rejected without adding a DB row or copied file.
- Replace one batch with a corrected file and confirm the batch id stays stable.
- Confirm `/api/portfolio/latest` uses the newest `as_of_date`.
- Confirm `/timeline`, `/uploads`, `/reports/{batch_id}`, and `/compare` require login.
- Confirm `/api/analytics/timeline` and `/api/analytics/xray` return JSON after login.
