-- Detector output from scripts/run_models.py (Phase 4), one row per scored entity-day.
CREATE TABLE IF NOT EXISTS anomaly_scores
(
    entity_type    LowCardinality(String),   -- 'coin' (market) or 'token' (on-chain)
    entity         LowCardinality(String),
    date           Date,
    zscore_score   Float64,
    iforest_score  Float64,
    lof_score      Float64,
    zscore_flag    Bool,
    iforest_flag   Bool,
    lof_flag       Bool,
    votes          UInt8                     -- how many of the 3 methods flagged this row
)
ENGINE = MergeTree
ORDER BY (entity_type, entity, date);
