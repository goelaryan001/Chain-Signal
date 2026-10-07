SELECT token, day, count() AS n FROM {{ ref('token_daily') }} GROUP BY token, day HAVING n > 1
