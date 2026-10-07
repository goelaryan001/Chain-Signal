-- One row per (coin, day): re-runs of the daily load must never duplicate a day.
SELECT coin_id, date, count() AS n FROM {{ source('raw', 'market_daily_raw') }} GROUP BY coin_id, date HAVING n > 1
