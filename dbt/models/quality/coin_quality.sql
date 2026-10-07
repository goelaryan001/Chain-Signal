-- One verdict per coin: ok, feed_glitch (2+ moves beyond 10x) or stale_feed (>20% zero-volume days).
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
FROM {{ ref('market_daily_flagged') }}
GROUP BY coin_id
