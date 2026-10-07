-- A coin's first row has no previous price, so it must never count as an extreme move.
SELECT coin_id, date FROM {{ ref('market_daily_flagged') }} WHERE prev_price IS NULL AND extreme_move
