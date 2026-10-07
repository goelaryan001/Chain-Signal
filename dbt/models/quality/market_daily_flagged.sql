-- Raw market rows plus data-quality flags. Thresholds were set from the data (Phase 2 / 4):
-- * extreme_move: a day-over-day move beyond 10x. Coins with 2+ of these are feed problems
--   (Bittensor TAO/USD quote flips on shared dates, bad prints); exactly one is kept and
--   flagged, because several were real rug pulls / launch pumps.
-- * volume_glitch: an isolated one-day collapse below 5% of BOTH neighbouring days
--   (Bitcoin: $0.38B on 2026-03-11 between $35-46B days). Spikes are deliberately not
--   treated this way: a spike can be a real event or wash trading.
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
    FROM {{ source('raw', 'market_daily_raw') }}
    WINDOW w AS (PARTITION BY coin_id ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
)
