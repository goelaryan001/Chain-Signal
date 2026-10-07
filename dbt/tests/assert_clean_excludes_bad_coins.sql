-- No coin excluded by coin_quality may appear in the modelling input.
SELECT coin_id, date FROM {{ ref('market_daily_clean') }}
WHERE coin_id IN (SELECT coin_id FROM {{ ref('coin_quality') }} WHERE status != 'ok')
