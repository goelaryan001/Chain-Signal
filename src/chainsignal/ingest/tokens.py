"""Tokens traced on-chain, keyed by symbol -> CoinGecko id.

LINK  : steady large-cap (baseline behaviour)
PEPE  : high-volume memecoin (volatile, bursty activity)
LOOKS : LooksRare, with a documented wash-trading history (the named target)
"""
TRACKED_TOKENS = {
    "LINK": "chainlink",
    "PEPE": "pepe",
    "LOOKS": "looksrare",
}
