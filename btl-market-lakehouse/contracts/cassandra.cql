-- Cassandra contract v2, keyspace "market".
-- PDF "Kiến trúc – Phân công": realtime tables (B) ohlcv_1m, indicators_latest, alerts_by_day;
-- batch tables (C) daily_summary, correlation_matrix, yield_curve. ohlcv_5m (B task 2) and
-- asset_clusters (C task 5, ML result for D) are additive.
-- Tables are designed per query ("giá BTC hôm nay theo từng phút" -> partition (symbol, day)).
-- Dev uses SimpleStrategy RF 1; the K8ssandra cluster (DC dc1) overrides replication.

CREATE KEYSPACE IF NOT EXISTS market
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Realtime, written by B. TTL 7 days: hot data only, history lives in MinIO.
CREATE TABLE IF NOT EXISTS market.ohlcv_1m (
    symbol text,
    day date,
    window_start timestamp,
    window_end timestamp,
    asset_class text,
    open double,
    high double,
    low double,
    close double,
    volume double,
    vwap double,
    trade_count bigint,
    last_event_time timestamp,  -- newest trade inside the window
    updated_at timestamp,       -- when the speed layer wrote this row (latency = updated_at - last_event_time)
    PRIMARY KEY ((symbol, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS market.ohlcv_5m (
    symbol text,
    day date,
    window_start timestamp,
    window_end timestamp,
    asset_class text,
    open double,
    high double,
    low double,
    close double,
    volume double,
    vwap double,
    trade_count bigint,
    last_event_time timestamp,
    updated_at timestamp,
    PRIMARY KEY ((symbol, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS market.indicators_latest (
    symbol text PRIMARY KEY,
    asset_class text,
    window_start timestamp,
    close double,
    vwap double,
    return_1m double,
    sma_5 double,
    sma_15 double,
    ema_12 double,
    ema_26 double,
    macd double,
    macd_signal double,
    macd_hist double,
    rsi_14 double,
    volatility_30m double,
    zscore_30m double,
    volume_zscore_30m double,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS market.alerts_by_day (
    day date,
    alert_time timestamp,
    symbol text,
    alert_id text,
    asset_class text,
    alert_type text,
    severity text,
    price double,
    zscore double,
    volume_zscore double,
    message text,
    PRIMARY KEY ((day), alert_time, symbol, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, symbol ASC, alert_id ASC)
  AND default_time_to_live = 604800;

-- Batch, written by C every night (no TTL: small tables, full history wanted).
CREATE TABLE IF NOT EXISTS market.daily_summary (
    symbol text,
    day date,
    asset_class text,
    open double,
    high double,
    low double,
    close double,
    volume double,
    vwap double,
    trade_count bigint,
    return_1d double,
    volatility_20d double,
    PRIMARY KEY ((symbol), day)
) WITH CLUSTERING ORDER BY (day DESC);

CREATE TABLE IF NOT EXISTS market.correlation_matrix (
    window_days int,
    as_of date,
    base_symbol text,           -- base_symbol < quote_symbol; yields appear as US2Y, US10Y, REAL10Y
    quote_symbol text,
    correlation double,
    n_obs int,
    PRIMARY KEY ((window_days), as_of, base_symbol, quote_symbol)
) WITH CLUSTERING ORDER BY (as_of DESC, base_symbol ASC, quote_symbol ASC);

CREATE TABLE IF NOT EXISTS market.yield_curve (
    curve text,                 -- 'UST'
    day date,
    y_1m double,
    y_3m double,
    y_6m double,
    y_1y double,
    y_2y double,
    y_5y double,
    y_10y double,
    y_30y double,
    real_10y double,
    spread_10y_2y double,
    spread_10y_3m double,
    inverted boolean,
    PRIMARY KEY ((curve), day)
) WITH CLUSTERING ORDER BY (day DESC);

CREATE TABLE IF NOT EXISTS market.asset_clusters (
    model text,                 -- 'kmeans'
    as_of date,
    symbol text,
    asset_class text,
    cluster int,
    mean_return double,
    volatility double,
    corr_us10y double,
    silhouette double,
    PRIMARY KEY ((model), as_of, symbol)
) WITH CLUSTERING ORDER BY (as_of DESC, symbol ASC);
