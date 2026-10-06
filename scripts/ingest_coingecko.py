"""Phase 1: pull top-N coins and 365 days of daily market data from CoinGecko.

Run: .venv/bin/python scripts/ingest_coingecko.py --coins 1000
Re-running only fetches coins not already cached in data/raw/coingecko/.
"""
import argparse
from pathlib import Path

from chainsignal.config import get_settings
from chainsignal.ingest.coingecko import CoinGeckoClient, pull_market_data

RAW_DIR = Path("data/raw/coingecko")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coins", type=int, default=1000)
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    client = CoinGeckoClient(get_settings().coingecko_api_key)
    stats = pull_market_data(client, args.coins, RAW_DIR, args.days)
    print(f"done: {stats['coins']} coins, {stats['fetched']} fetched, {stats['cached']} cached, "
          f"{len(stats['failed'])} failed")
    for coin_id, err in stats["failed"]:
        print(f"  failed {coin_id}: {err}")


if __name__ == "__main__":
    main()
