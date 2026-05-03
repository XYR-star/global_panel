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
