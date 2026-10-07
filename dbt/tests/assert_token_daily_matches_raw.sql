-- The rollup must add back up to the raw transfer counts for every token.
SELECT r.token, r.n AS raw_transfers, d.n AS rollup_transfers
FROM (SELECT token, count() AS n FROM {{ source('raw', 'token_transfers_raw') }} GROUP BY token) AS r
FULL OUTER JOIN (SELECT token, sum(transfers) AS n FROM {{ ref('token_daily') }} GROUP BY token) AS d ON r.token = d.token
WHERE r.n != d.n OR r.token = '' OR d.token = ''
