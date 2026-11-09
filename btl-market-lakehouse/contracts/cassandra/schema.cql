-- Logical Cassandra contract for the speed layer (role B implements it).
CREATE KEYSPACE IF NOT EXISTS market
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE market;

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    symbol text,
    window_start timestamp,
    open double,
    high double,
    low double,
    close double,
    volume double,
    vwap double,
    trade_count int,
    sma_5 double,
    sma_15 double,
    rsi_14 double,
    macd double,
    macd_signal double,
    zscore_30m double,
    PRIMARY KEY ((symbol), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS alerts (
    symbol text,
    alert_time timestamp,
    alert_id text,
    alert_type text,
    severity text,
    price double,
    zscore double,
    volume_zscore double,
    message text,
    PRIMARY KEY ((symbol), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);

CREATE TABLE IF NOT EXISTS latest_price (
    symbol text PRIMARY KEY,
    price double,
    event_time timestamp,
    source text
);
