"""Phase 3: label every address that appears in a back-and-forth pair as contract or wallet.

Run: .venv/bin/python scripts/label_addresses.py
Reads candidate addresses from ClickHouse, caches labels in
data/raw/etherscan/address_labels.jsonl (resumable).
"""
from pathlib import Path

from chainsignal.config import get_settings
from chainsignal.db.clickhouse import get_client
from chainsignal.ingest.labels import label_addresses

CACHE = Path("data/raw/etherscan/address_labels.jsonl")

CANDIDATES_SQL = """
SELECT DISTINCT arrayJoin([a, b]) FROM (
    SELECT least(from_address, to_address) AS a, greatest(from_address, to_address) AS b,
           countIf(from_address = a) AS ab, countIf(from_address = b) AS ba
    FROM token_transfers_raw GROUP BY token, a, b)
WHERE ab >= 1 AND ba >= 1
"""


def main() -> None:
    addresses = [r[0] for r in get_client().query(CANDIDATES_SQL).result_rows]
    print(f"{len(addresses):,} addresses take part in back-and-forth pairs")
    stats = label_addresses(get_settings().etherscan_api_key, addresses, CACHE)
    print(f"done: {stats}")


if __name__ == "__main__":
    main()
