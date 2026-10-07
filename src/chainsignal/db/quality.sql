-- Data-quality layer over market_daily_raw. Views only: raw data is never modified.
-- Thresholds were set from the data (Phase 2 exploration, 2026-10-06):
-- * 324 day-over-day moves beyond 10x across 41 coins. Every coin with 2 or more of them
--   is a feed problem (Bittensor subnet tokens flipping between TAO and USD quotes on
--   shared dates, bad prints that snap back). Coins with exactly one are a mix of real
--   events (rug pulls and launch pumps on tens of millions of volume) and glitches, so
--   they are kept and the row is flagged for the evaluation phase to judge.
-- * 47 coins have zero volume on half or more of their days: a price with no trading.
-- * Correction found in Phase 4: VOLUME glitches too. Bitcoin traded $35-46B on the days
--   around 2026-03-12 and $0.38B on that day, with a smooth price. An isolated one-day
--   collapse below 5% of BOTH neighbouring days is flagged as volume_glitch and dropped
--   from the clean view. Spikes are deliberately NOT treated this way: a one-day volume
--   spike can be a real event or wash trading, which is what we are looking for.

CREATE OR REPLACE VIEW market_daily_flagged AS
SELECT
    coin_id, date, price, market_cap, volume, prev_price,
    log(price / prev_price)                              AS log_return,
    coalesce(abs(log(price / prev_price)) > log(10), 0)  AS extreme_move,
    volume = 0                                           AS zero_volume,
    coalesce(volume < 0.05 * prev_volume AND volume < 0.05 * next_volume, 0) AS volume_glitch,
    dateDiff('day', prev_date, date) - 1                 AS missing_days_before
FROM
(
    SELECT
        coin_id, date, price, market_cap, volume,
        lagInFrame(toNullable(price)) OVER w   AS prev_price,
        lagInFrame(toNullable(volume)) OVER w  AS prev_volume,
        leadInFrame(toNullable(volume)) OVER w AS next_volume,
        lagInFrame(toNullable(date)) OVER w    AS prev_date
    FROM market_daily_raw
    WINDOW w AS (PARTITION BY coin_id ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
);

CREATE OR REPLACE VIEW coin_quality AS
SELECT
    coin_id,
    count()                                              AS days,
    min(date)                                            AS first_date,
    max(date)                                            AS last_date,
    countIf(extreme_move)                                AS extreme_moves,
    countIf(zero_volume)                                 AS zero_volume_days,
    countIf(volume_glitch)                               AS volume_glitch_days,
    dateDiff('day', min(date), max(date)) + 1 - count()  AS missing_days,
    multiIf(extreme_moves >= 2, 'feed_glitch',
            zero_volume_days / days > 0.2, 'stale_feed',
            'ok')                                        AS status
FROM market_daily_flagged
GROUP BY coin_id;

CREATE OR REPLACE VIEW market_daily_clean AS
SELECT *
FROM market_daily_flagged
WHERE coin_id IN (SELECT coin_id FROM coin_quality WHERE status = 'ok')
  AND NOT volume_glitch;
