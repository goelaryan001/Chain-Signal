-- Feature tables written by the Phase 3 Spark job (scripts/build_features.py).
-- Recreated on every run; Spark writes Parquet, ClickHouse matches columns by name.

CREATE TABLE IF NOT EXISTS market_features
(
    coin_id           LowCardinality(String),
    date              Date,
    price             Float64,
    market_cap        Nullable(Float64),
    volume            Float64,
    log_return        Nullable(Float64),
    extreme_move      Bool,
    market_return     Nullable(Float64),
    n_history         Int64,
    return_z          Nullable(Float64),
    volume_z          Nullable(Float64),
    turnover_z        Nullable(Float64),
    market_corr       Nullable(Float64),
    market_beta       Nullable(Float64),
    residual_return   Nullable(Float64),
    residual_z        Nullable(Float64),
    volume_price_gap  Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (coin_id, date);

CREATE TABLE IF NOT EXISTS onchain_features
(
    token                     LowCardinality(String),
    day                       Date,
    transfers                 Int64,
    amount_total              Float64,
    unique_senders            Int64,
    unique_receivers          Int64,
    unique_addresses          Int64,
    large_transfers           Int64,
    hub_share                 Float64,
    new_addresses             Int64,
    top10_sender_share        Nullable(Float64),
    rt_transfers              Int64,
    rt_amount                 Float64,
    rt_pairs                  Int64,
    rt_share                  Float64,
    rt_amount_share           Float64,
    transfers_per_address     Float64,
    new_address_share         Float64,
    n_history                 Int64,
    transfers_z               Nullable(Float64),
    amount_z                  Nullable(Float64),
    addresses_z               Nullable(Float64),
    rt_share_z                Nullable(Float64),
    tpa_z                     Nullable(Float64),
    concentration_z           Nullable(Float64),
    large_transfers_z         Nullable(Float64),
    activity_vs_participants  Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (token, day);
