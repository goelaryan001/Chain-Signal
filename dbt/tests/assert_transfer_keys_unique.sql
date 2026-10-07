-- One row per (token, tx_hash, tx_seq): refetching the last day must never duplicate transfers.
SELECT token, tx_hash, tx_seq, count() AS n FROM {{ source('raw', 'token_transfers_raw') }}
GROUP BY token, tx_hash, tx_seq HAVING n > 1
