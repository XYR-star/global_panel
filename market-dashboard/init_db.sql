CREATE TABLE IF NOT EXISTS portfolio_import_batches (
    batch_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_file_mtime TIMESTAMPTZ,
    as_of_date DATE NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    original_filename TEXT,
    file_sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'complete',
    ok BOOLEAN NOT NULL DEFAULT TRUE,
    message TEXT,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    total_assets NUMERIC(20, 6),
    position_count INTEGER NOT NULL DEFAULT 0,
    report_markdown TEXT NOT NULL DEFAULT '',
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_portfolio_batches_uploaded
    ON portfolio_import_batches (uploaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_batches_status_archived
    ON portfolio_import_batches (status, is_archived, uploaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_batches_sha
    ON portfolio_import_batches (file_sha256);

CREATE TABLE IF NOT EXISTS portfolio_positions (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    security_code TEXT NOT NULL,
    security_name TEXT NOT NULL,
    security_type TEXT NOT NULL,
    market TEXT,
    holding_amount NUMERIC(20, 6),
    today_pnl NUMERIC(20, 6),
    today_pnl_rate DOUBLE PRECISION,
    related_sector TEXT,
    sector_change_rate DOUBLE PRECISION,
    portfolio_pnl NUMERIC(20, 6),
    portfolio_return_rate DOUBLE PRECISION,
    holding_pnl NUMERIC(20, 6),
    holding_pnl_rate DOUBLE PRECISION,
    cumulative_pnl NUMERIC(20, 6),
    cumulative_pnl_rate DOUBLE PRECISION,
    weekly_pnl NUMERIC(20, 6),
    monthly_pnl NUMERIC(20, 6),
    yearly_pnl NUMERIC(20, 6),
    portfolio_weight DOUBLE PRECISION,
    holding_quantity NUMERIC(24, 8),
    holding_days INTEGER,
    latest_change_rate DOUBLE PRECISION,
    latest_price NUMERIC(20, 6),
    unit_cost NUMERIC(20, 6),
    breakeven_change_rate DOUBLE PRECISION,
    return_1m DOUBLE PRECISION,
    return_3m DOUBLE PRECISION,
    return_6m DOUBLE PRECISION,
    return_1y DOUBLE PRECISION,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, security_code)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_positions_asof_weight
    ON portfolio_positions (as_of_date, portfolio_weight DESC);

CREATE TABLE IF NOT EXISTS portfolio_closed_positions (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    close_date DATE NOT NULL,
    security_code TEXT NOT NULL,
    security_name TEXT NOT NULL,
    total_pnl NUMERIC(20, 6),
    pnl_rate DOUBLE PRECISION,
    benchmark_return DOUBLE PRECISION,
    excess_return DOUBLE PRECISION,
    buy_avg_price NUMERIC(20, 6),
    sell_avg_price NUMERIC(20, 6),
    days_since_close DOUBLE PRECISION,
    holding_days INTEGER,
    fees NUMERIC(20, 6),
    open_date DATE,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (batch_id, close_date, security_code, security_name)
);

CREATE TABLE IF NOT EXISTS portfolio_transactions (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    transaction_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_time TIME,
    security_code TEXT,
    security_name TEXT,
    transaction_type TEXT NOT NULL,
    quantity NUMERIC(24, 8),
    price NUMERIC(20, 6),
    cash_flow_amount NUMERIC(20, 6),
    gross_amount NUMERIC(20, 6),
    fee NUMERIC(20, 6),
    remark TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (batch_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_transactions_date_type
    ON portfolio_transactions (trade_date DESC, transaction_type);

CREATE TABLE IF NOT EXISTS portfolio_fund_metadata (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    fund_code TEXT NOT NULL,
    fund_name TEXT NOT NULL,
    fund_type TEXT,
    manager_name TEXT,
    fund_size NUMERIC(20, 6),
    size_unit TEXT,
    management_fee DOUBLE PRECISION,
    custody_fee DOUBLE PRECISION,
    sales_service_fee DOUBLE PRECISION,
    purchase_fee DOUBLE PRECISION,
    redemption_fee TEXT,
    inception_date DATE,
    benchmark TEXT,
    return_1m DOUBLE PRECISION,
    return_3m DOUBLE PRECISION,
    return_6m DOUBLE PRECISION,
    return_1y DOUBLE PRECISION,
    max_drawdown_1y DOUBLE PRECISION,
    nav_latest NUMERIC(20, 6),
    nav_date DATE,
    source TEXT NOT NULL DEFAULT 'akshare',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, fund_code)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_fund_metadata_asof
    ON portfolio_fund_metadata (as_of_date, fund_type);

CREATE TABLE IF NOT EXISTS portfolio_fund_manager_history (
    fund_code TEXT NOT NULL,
    fund_name TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    manager_name TEXT,
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'akshare',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (fund_code, as_of_date, batch_id)
);

CREATE TABLE IF NOT EXISTS portfolio_underlying_holdings (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    parent_code TEXT NOT NULL,
    parent_name TEXT NOT NULL,
    parent_type TEXT NOT NULL,
    underlying_code TEXT NOT NULL,
    underlying_name TEXT NOT NULL,
    underlying_type TEXT NOT NULL,
    report_period TEXT NOT NULL DEFAULT '',
    holding_rank INTEGER,
    holding_weight_in_parent DOUBLE PRECISION,
    parent_portfolio_weight DOUBLE PRECISION,
    lookthrough_portfolio_weight DOUBLE PRECISION,
    lookthrough_amount NUMERIC(20, 6),
    shares NUMERIC(24, 8),
    market_value NUMERIC(20, 6),
    source TEXT NOT NULL DEFAULT 'akshare',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, parent_code, underlying_type, underlying_code, report_period)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_underlying_asof_weight
    ON portfolio_underlying_holdings (as_of_date, lookthrough_portfolio_weight DESC);

CREATE INDEX IF NOT EXISTS idx_portfolio_underlying_asset
    ON portfolio_underlying_holdings (underlying_code, as_of_date DESC);

CREATE TABLE IF NOT EXISTS portfolio_industry_allocations (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    parent_code TEXT NOT NULL,
    parent_name TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    report_period TEXT NOT NULL DEFAULT '',
    weight_in_parent DOUBLE PRECISION,
    parent_portfolio_weight DOUBLE PRECISION,
    lookthrough_portfolio_weight DOUBLE PRECISION,
    source TEXT NOT NULL DEFAULT 'akshare',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (batch_id, parent_code, industry_name, report_period)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_industry_asof_weight
    ON portfolio_industry_allocations (as_of_date, lookthrough_portfolio_weight DESC);

CREATE TABLE IF NOT EXISTS portfolio_asset_allocation (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    allocation_bucket TEXT NOT NULL,
    amount NUMERIC(20, 6) NOT NULL,
    weight DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (batch_id, allocation_bucket)
);

CREATE TABLE IF NOT EXISTS portfolio_risk_metrics (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    metric_scope TEXT NOT NULL,
    subject_code TEXT NOT NULL DEFAULT 'PORTFOLIO',
    subject_name TEXT NOT NULL DEFAULT '组合',
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    metric_unit TEXT,
    source TEXT NOT NULL,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, metric_scope, subject_code, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_risk_metrics_asof
    ON portfolio_risk_metrics (as_of_date, metric_scope, metric_name);

CREATE TABLE IF NOT EXISTS portfolio_data_sources (
    source_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    fetch_days INTEGER NOT NULL DEFAULT 7,
    public_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    encrypted_secret TEXT,
    last_sync_at TIMESTAMPTZ,
    last_sync_status TEXT,
    last_sync_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolio_events (
    event_id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    announcement_date DATE NOT NULL,
    source_key TEXT NOT NULL,
    source_event_id TEXT,
    source_url TEXT,
    pdf_url TEXT,
    event_type TEXT NOT NULL DEFAULT '公告',
    importance INTEGER NOT NULL DEFAULT 3,
    dedupe_hash TEXT NOT NULL UNIQUE,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_events_date_source
    ON portfolio_events (announcement_date DESC, source_key);
CREATE INDEX IF NOT EXISTS idx_portfolio_events_type
    ON portfolio_events (event_type);

CREATE TABLE IF NOT EXISTS portfolio_event_symbols (
    event_id BIGINT NOT NULL REFERENCES portfolio_events(event_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    symbol_name TEXT,
    security_type TEXT,
    market TEXT,
    batch_id TEXT REFERENCES portfolio_import_batches(batch_id) ON DELETE SET NULL,
    as_of_date DATE,
    relation_type TEXT NOT NULL DEFAULT 'holding',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_event_symbols_unique
    ON portfolio_event_symbols (event_id, symbol, COALESCE(batch_id, ''));
CREATE INDEX IF NOT EXISTS idx_portfolio_event_symbols_symbol
    ON portfolio_event_symbols (symbol, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_portfolio_event_symbols_batch
    ON portfolio_event_symbols (batch_id, event_id DESC);

CREATE TABLE IF NOT EXISTS portfolio_event_fetch_runs (
    run_id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    fetch_days INTEGER NOT NULL DEFAULT 7,
    watchlist_count INTEGER NOT NULL DEFAULT 0,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_event_fetch_runs_started
    ON portfolio_event_fetch_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_event_reads (
    event_id BIGINT PRIMARY KEY REFERENCES portfolio_events(event_id) ON DELETE CASCADE,
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
    is_favorite BOOLEAN NOT NULL DEFAULT FALSE,
    is_ignored BOOLEAN NOT NULL DEFAULT FALSE,
    read_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolio_ai_settings (
    id INTEGER PRIMARY KEY DEFAULT 1,
    provider TEXT NOT NULL DEFAULT 'none',
    model TEXT,
    daily_limit INTEGER NOT NULL DEFAULT 30,
    public_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    encrypted_api_key TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT portfolio_ai_settings_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS portfolio_event_ai_insights (
    event_id BIGINT PRIMARY KEY REFERENCES portfolio_events(event_id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    model TEXT,
    summary TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT '公告',
    importance INTEGER NOT NULL DEFAULT 3,
    relevance TEXT NOT NULL DEFAULT '',
    risks JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);



CREATE INDEX IF NOT EXISTS idx_portfolio_batches_asof_uploaded
    ON portfolio_import_batches (as_of_date DESC, uploaded_at DESC, batch_id DESC)
    WHERE status IN ('complete', 'partial');

CREATE INDEX IF NOT EXISTS idx_portfolio_positions_security_asof
    ON portfolio_positions (security_code, as_of_date DESC);

CREATE INDEX IF NOT EXISTS idx_portfolio_transactions_batch_date
    ON portfolio_transactions (batch_id, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_portfolio_asset_allocation_bucket_asof
    ON portfolio_asset_allocation (allocation_bucket, as_of_date);

CREATE TABLE IF NOT EXISTS portfolio_daily_summary (
    batch_id TEXT PRIMARY KEY REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL,
    original_filename TEXT,
    status TEXT NOT NULL,
    total_assets NUMERIC(20, 6),
    total_assets_change NUMERIC(20, 6),
    position_count INTEGER NOT NULL DEFAULT 0,
    today_pnl NUMERIC(20, 6),
    holding_pnl NUMERIC(20, 6),
    cumulative_pnl NUMERIC(20, 6),
    max_position_weight DOUBLE PRECISION,
    top3_weight DOUBLE PRECISION,
    top5_weight DOUBLE PRECISION,
    top10_underlying_weight DOUBLE PRECISION,
    equity_like_weight DOUBLE PRECISION,
    bond_like_weight DOUBLE PRECISION,
    qdii_weight DOUBLE PRECISION,
    cash_weight DOUBLE PRECISION,
    max_industry_name TEXT,
    max_industry_weight DOUBLE PRECISION,
    drawdown_estimated DOUBLE PRECISION,
    trailing_loss_streak INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'computed',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_summary_asof_uploaded
    ON portfolio_daily_summary (as_of_date DESC, uploaded_at DESC, batch_id DESC);

CREATE TABLE IF NOT EXISTS portfolio_daily_allocation (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    allocation_bucket TEXT NOT NULL,
    amount NUMERIC(20, 6) NOT NULL DEFAULT 0,
    weight DOUBLE PRECISION NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'computed',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, allocation_bucket)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_allocation_bucket_asof
    ON portfolio_daily_allocation (allocation_bucket, as_of_date);

CREATE TABLE IF NOT EXISTS portfolio_daily_exposure (
    batch_id TEXT NOT NULL REFERENCES portfolio_import_batches(batch_id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    exposure_type TEXT NOT NULL,
    exposure_code TEXT NOT NULL DEFAULT '',
    exposure_name TEXT NOT NULL,
    amount NUMERIC(20, 6),
    weight DOUBLE PRECISION,
    source_count INTEGER NOT NULL DEFAULT 1,
    contributors JSONB NOT NULL DEFAULT '[]'::jsonb,
    source TEXT NOT NULL DEFAULT 'computed',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, exposure_type, exposure_code, exposure_name)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_daily_exposure_type_weight
    ON portfolio_daily_exposure (batch_id, exposure_type, weight DESC);

CREATE TABLE IF NOT EXISTS portfolio_login_failures (
    failure_key TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    client_ip TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_login_failures_locked
    ON portfolio_login_failures (locked_until);
