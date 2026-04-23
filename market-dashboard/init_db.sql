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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

