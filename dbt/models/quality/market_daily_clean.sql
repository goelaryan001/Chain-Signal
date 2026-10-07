-- The modelling input: coins with an ok verdict, minus isolated volume collapses.
SELECT *
FROM {{ ref('market_daily_flagged') }}
WHERE coin_id IN (SELECT coin_id FROM {{ ref('coin_quality') }} WHERE status = 'ok')
  AND NOT volume_glitch
