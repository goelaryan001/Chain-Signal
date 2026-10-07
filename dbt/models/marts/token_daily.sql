{{ config(engine='MergeTree()', order_by='(token, day)') }}
-- Per token per UTC day. The first and last day of each token's window are partial.
WITH bounds AS
(
    SELECT token, toDate(min(block_time)) AS first_day, toDate(max(block_time)) AS last_day
    FROM {{ source('raw', 'token_transfers_raw') }}
    GROUP BY token
)
SELECT
    t.token                                                 AS token,
    toDate(t.block_time)                                    AS day,
    count()                                                 AS transfers,
    sum(t.amount)                                           AS amount_total,
    quantileExact(0.5)(t.amount)                            AS amount_median,
    quantileExact(0.99)(t.amount)                           AS amount_p99,
    uniqExact(t.from_address)                               AS unique_senders,
    uniqExact(t.to_address)                                 AS unique_receivers,
    uniqExactArray([t.from_address, t.to_address])          AS unique_addresses,
    countIf(startsWith(lower(t.function_name), 'swap'))     AS swap_transfers,
    countIf(startsWith(lower(t.function_name), 'transfer')) AS plain_transfers,
    toUInt8(day = any(b.first_day) OR day = any(b.last_day)) AS is_partial_day
FROM {{ source('raw', 'token_transfers_raw') }} AS t
INNER JOIN bounds AS b ON t.token = b.token
GROUP BY t.token, day
