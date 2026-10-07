-- Only the first and last day of each token's window may be partial.
SELECT token, countIf(is_partial_day) AS partial FROM {{ ref('token_daily') }} GROUP BY token HAVING partial > 2
