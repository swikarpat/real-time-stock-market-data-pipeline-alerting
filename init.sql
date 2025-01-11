CREATE TABLE stock_aggs (
    symbol TEXT,
    window TIMESTAMPTZ,
    avg_price DOUBLE PRECISION,
    total_volume BIGINT,
    sma_5 DOUBLE PRECISION,
    latest_timestamp TIMESTAMPTZ,
    PRIMARY KEY (symbol, window)
);

CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    alert_type TEXT,
    timestamp TIMESTAMPTZ,
    message TEXT
);