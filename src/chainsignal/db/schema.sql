-- ChainSignal ClickHouse schema.
-- Statements are separated by a semicolon at the end of a line (the loader splits on that).
--
-- Design notes
-- * MergeTree stores each table sorted by ORDER BY. That sort key doubles as a sparse
--   primary index, so it is chosen to match how the table is queried: one coin over
--   time, one token over a time window.
-- * Monthly partitions keep parts small and let a reload or retention job drop whole
--   months cheaply. With one year of data that is about 13 partitions, which is fine.
-- * LowCardinality(String) dictionary-encodes columns with few distinct values
--   (coin ids, token symbols, function names). Addresses (~350k distinct) and tx hashes
--   are left as plain String because dictionary encoding only pays off below ~10k values.
-- * Loads are full reloads (DROP, CREATE, INSERT) from the raw files, which are the
--   source of truth. That keeps every load idempotent without relying on background
--   deduplication, and schema changes always take effect.
-- * Codecs on token_transfers_raw were chosen by measurement (2026-10-06, 1.64M rows):
--     plain String + default LZ4 ............ 173 MB, ~25 ms typical query
--     binary FixedString(32/20) + codecs ....  89 MB, ~17 ms, but unreadable (needs hex())
--     String + ZSTD, Delta on block/time ....  96 MB, ~27 ms   <- chosen
--   Hashes and addresses are random hex text (68% of the bytes). ZSTD's entropy coding
--   recovers most of the 2-chars-per-byte overhead while keeping them readable. At
--   billions of rows the binary layout would be worth its awkwardness.

CREATE TABLE IF NOT EXISTS coins
(
    coin_id          LowCardinality(String),
    symbol           LowCardinality(String),
    name             String,
    market_cap_rank  Nullable(UInt32),
    snapshot_at      DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY coin_id;

CREATE TABLE IF NOT EXISTS market_daily_raw
(
    coin_id     LowCardinality(String),
    date        Date,
    price       Float64,
    market_cap  Nullable(Float64),
    volume      Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (coin_id, date);

CREATE TABLE IF NOT EXISTS token_transfers_raw
(
    token          LowCardinality(String),
    block_number   UInt64 CODEC(Delta, ZSTD),          -- increases steadily: store the differences
    block_time     DateTime('UTC') CODEC(Delta, ZSTD),
    tx_hash        String CODEC(ZSTD(3)),
    tx_seq         UInt16,
    tx_index       UInt32 CODEC(ZSTD),
    from_address   String CODEC(ZSTD(3)),
    to_address     String CODEC(ZSTD(3)),
    value_raw      UInt256 CODEC(ZSTD),   -- exact on-chain integer; PEPE sums exceed UInt64
    amount         Float64 CODEC(ZSTD),   -- value_raw / 10^decimals, for analytics
    decimals       UInt8,
    gas_used       UInt64 CODEC(ZSTD),
    gas_price      UInt64 CODEC(ZSTD),
    method_id      LowCardinality(String),
    function_name  LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (token, block_time, tx_hash, tx_seq);

-- Per token per UTC day, filled by an explicit rollup after each load (see rollup.sql).
-- Not a materialized view: an MV fires on every insert, so a batch reload would
-- double-count unless its target were also cleared. An explicit step is easier to verify.
CREATE TABLE IF NOT EXISTS token_daily
(
    token              LowCardinality(String),
    day                Date,
    transfers          UInt64,
    amount_total       Float64,
    amount_median      Float64,
    amount_p99         Float64,
    unique_senders     UInt64,
    unique_receivers   UInt64,
    unique_addresses   UInt64,
    swap_transfers     UInt64,
    plain_transfers    UInt64,
    is_partial_day     UInt8             -- first/last day of the pull window are incomplete
)
ENGINE = MergeTree
ORDER BY (token, day);
