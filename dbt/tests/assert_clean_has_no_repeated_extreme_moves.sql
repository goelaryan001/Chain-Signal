-- At most one >10x move per kept coin (two or more means a broken feed).
SELECT coin_id, countIf(extreme_move) AS n FROM {{ ref('market_daily_clean') }} GROUP BY coin_id HAVING n > 1
