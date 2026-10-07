-- Isolated volume collapses are feed errors and must not reach the features.
SELECT coin_id, date FROM {{ ref('market_daily_clean') }} WHERE volume_glitch
