-- ChainSignal ClickHouse schema.
-- Statements are separated by a semicolon at the end of a line (the loader splits on that).
--
-- Design notes
-- * MergeTree stores each table sorted by ORDER BY. That sort key doubles as a sparse
--   primary index, so it is chosen to match how the table is queried: one coin over
--   time, one token over a time window.
-- * DAILY partitions on the two raw tables (monthly until Phase 8). The daily incremental
--   load replaces a day atomically with ALTER TABLE ... REPLACE PARTITION from a staging
--   table, so re-running a day can never duplicate it. That needs a day to be a partition.
--   ~365 + ~90 small partitions is fine at this scale; at much larger scale monthly
--   partitions with a ReplacingMergeTree would be the usual alternative.
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
    volume      Float64,
    -- where the row came from: 'history' (market_chart backfill), 'snapshot' (provisional
    -- daily /coins/markets snapshot) or 'reconciled' (snapshot day later replaced with the
    -- authoritative market_chart value by the weekly reconciliation)
    source      LowCardinality(String) DEFAULT 'history'
)
ENGINE = MergeTree
PARTITION BY date
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
PARTITION BY toDate(block_time)
ORDER BY (token, block_time, tx_hash, tx_seq);

-- Contract-or-wallet labels for addresses in back-and-forth pairs (Phase 3, Etherscan).
CREATE TABLE IF NOT EXISTS address_labels
(
    address        String,
    is_contract    Bool,
    creator        String,
    factory        String,
    created_block  UInt64
)
ENGINE = MergeTree
ORDER BY address;

-- The transformation layer (token_daily rollup, quality views, clean view) is the dbt
-- project in dbt/, built and tested by `dbt build` after every load.

