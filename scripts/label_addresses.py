"""Phase 3: label every address that appears in a back-and-forth pair as contract or wallet.

Run: .venv/bin/python scripts/label_addresses.py
Reads candidate addresses from ClickHouse, caches labels in
data/raw/etherscan/address_labels.jsonl (resumable).
"""
from pathlib import Path

from chainsignal.config import get_settings
from chainsignal.db.clickhouse import get_client
from chainsignal.ingest.labels import label_addresses, label_candidates

CACHE = Path("data/raw/etherscan/address_labels.jsonl")

def main() -> None:
    addresses = label_candidates(get_client())
    print(f"{len(addresses):,} addresses take part in back-and-forth pairs")
    stats = label_addresses(get_settings().etherscan_api_key, addresses, CACHE)
    print(f"done: {stats}")


if __name__ == "__main__":
    main()
