CREATE TABLE IF NOT EXISTS market_data_points (
    asset_key TEXT NOT NULL,
    asset_name TEXT NOT NULL,
    category TEXT NOT NULL,
    source TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (asset_key, ts)
);

CREATE INDEX IF NOT EXISTS idx_market_data_points_category_ts
    ON market_data_points (category, ts DESC);

CREATE TABLE IF NOT EXISTS daily_bars (
    asset_key TEXT NOT NULL,
    asset_name TEXT NOT NULL,
    market TEXT NOT NULL,
    category TEXT NOT NULL,
    source TEXT NOT NULL,
    source_symbol TEXT NOT NULL,
    ts DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_key, ts)
);

CREATE INDEX IF NOT EXISTS idx_daily_bars_symbol_ts
    ON daily_bars (source_symbol, ts DESC);

CREATE INDEX IF NOT EXISTS idx_daily_bars_market_category_ts
    ON daily_bars (market, category, ts DESC);

CREATE INDEX IF NOT EXISTS idx_daily_bars_source_ts
    ON daily_bars (source, ts DESC);

CREATE TABLE IF NOT EXISTS market_asset_snapshot (
    asset_key TEXT PRIMARY KEY,
    asset_name TEXT NOT NULL,
    category TEXT NOT NULL,
    source TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    change DOUBLE PRECISION,
    change_percent DOUBLE PRECISION,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_sync_status (
    source TEXT PRIMARY KEY,
    last_run TIMESTAMPTZ,
    ok BOOLEAN NOT NULL,
    message TEXT,
    latest_observation_ts TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE data_sync_status
    ADD COLUMN IF NOT EXISTS latest_observation_ts TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS data_asset_catalog (
    asset_catalog_key TEXT,
    asset_key TEXT PRIMARY KEY,
    asset_name TEXT NOT NULL,
    market TEXT NOT NULL,
    category TEXT NOT NULL,
    provider TEXT NOT NULL,
    source_symbol TEXT,
    latest_observation_ts TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE data_asset_catalog
    ADD COLUMN IF NOT EXISTS asset_catalog_key TEXT;

UPDATE data_asset_catalog
SET asset_catalog_key = provider || ':' || category || ':' || asset_key
WHERE asset_catalog_key IS NULL;

ALTER TABLE data_asset_catalog
    ALTER COLUMN asset_catalog_key SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'data_asset_catalog_pkey'
          AND conrelid = 'data_asset_catalog'::regclass
    ) THEN
        ALTER TABLE data_asset_catalog DROP CONSTRAINT data_asset_catalog_pkey;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'data_asset_catalog_asset_catalog_key_pkey'
          AND conrelid = 'data_asset_catalog'::regclass
    ) THEN
        ALTER TABLE data_asset_catalog
            ADD CONSTRAINT data_asset_catalog_asset_catalog_key_pkey PRIMARY KEY (asset_catalog_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_data_asset_catalog_provider_category
    ON data_asset_catalog (provider, category);

CREATE INDEX IF NOT EXISTS idx_data_asset_catalog_latest_observation
    ON data_asset_catalog (latest_observation_ts DESC);

CREATE TABLE IF NOT EXISTS asset_aliases (
    alias TEXT NOT NULL,
    asset_key TEXT NOT NULL,
    source_symbol TEXT NOT NULL,
    market TEXT NOT NULL,
    alias_type TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT 'yahoo_finance',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (alias, asset_key)
);

CREATE INDEX IF NOT EXISTS idx_asset_aliases_alias_lower
    ON asset_aliases (lower(alias));

CREATE INDEX IF NOT EXISTS idx_asset_aliases_symbol
    ON asset_aliases (source_symbol);

CREATE TABLE IF NOT EXISTS market_text_records (
    record_id TEXT PRIMARY KEY,
    asset_key TEXT NOT NULL,
    asset_name TEXT NOT NULL,
    market TEXT NOT NULL,
    category TEXT NOT NULL,
    source TEXT NOT NULL,
    source_symbol TEXT,
    ts TIMESTAMPTZ NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    body TEXT,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_text_records_asset_ts
    ON market_text_records (asset_key, ts DESC);

CREATE INDEX IF NOT EXISTS idx_market_text_records_category_ts
    ON market_text_records (category, ts DESC);

CREATE TABLE IF NOT EXISTS evidence_items (
    evidence_id TEXT PRIMARY KEY,
    evidence_type TEXT NOT NULL,
    asset_key TEXT,
    asset_name TEXT,
    market TEXT,
    source TEXT NOT NULL,
    source_symbol TEXT,
    ts TIMESTAMPTZ,
    title TEXT NOT NULL,
    url TEXT,
    summary TEXT,
    body_excerpt TEXT,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evidence_items_asset_ts
    ON evidence_items (asset_key, ts DESC);

CREATE INDEX IF NOT EXISTS idx_evidence_items_type_ts
    ON evidence_items (evidence_type, ts DESC);

CREATE INDEX IF NOT EXISTS idx_evidence_items_source_symbol
    ON evidence_items (source_symbol);

CREATE TABLE IF NOT EXISTS agent_reports (
    report_id TEXT PRIMARY KEY,
    report_type TEXT NOT NULL,
    status TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    stance TEXT,
    confidence DOUBLE PRECISION,
    recommendations JSONB NOT NULL DEFAULT '[]'::jsonb,
    risks JSONB NOT NULL DEFAULT '[]'::jsonb,
    watchlist JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_response JSONB NOT NULL DEFAULT '{}'::jsonb,
    model TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data_as_of TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_agent_reports_type_generated
    ON agent_reports (report_type, generated_at DESC);

CREATE TABLE IF NOT EXISTS agent_report_evidence (
    report_id TEXT NOT NULL REFERENCES agent_reports(report_id) ON DELETE CASCADE,
    evidence_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT,
    source TEXT,
    asset_key TEXT,
    ts TIMESTAMPTZ,
    url TEXT,
    meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (report_id, evidence_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_report_evidence_report
    ON agent_report_evidence (report_id);

CREATE TABLE IF NOT EXISTS agent_question_answers (
    qa_id TEXT PRIMARY KEY,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    model TEXT,
    ok BOOLEAN NOT NULL,
    context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_question_answers_created
    ON agent_question_answers (created_at DESC);
