"""Phase 1: pull every ERC-20 transfer of the tracked tokens over the last N days.

Run: .venv/bin/python scripts/ingest_etherscan.py --days 90
Contract addresses come from CoinGecko, not hard-coded. Each token's pull is
resumable from data/raw/etherscan/<SYMBOL>/state.json.
"""
import argparse
import json
from pathlib import Path
import time

from chainsignal.config import get_settings
from chainsignal.ingest.coingecko import CoinGeckoClient
from chainsignal.ingest.etherscan import EtherscanClient, pull_token_transfers
from chainsignal.ingest.tokens import TRACKED_TOKENS

RAW_DIR = Path("data/raw/etherscan")


def resolve_contracts(cg: CoinGeckoClient) -> dict[str, dict]:
    path = RAW_DIR / "contracts.json"
    if path.exists():
        return json.loads(path.read_text())
    contracts = {}
    for symbol, coin_id in TRACKED_TOKENS.items():
        detail = cg.coin_detail(coin_id)
        contracts[symbol] = {
            "coin_id": coin_id,
            "contract": detail["platforms"]["ethereum"].lower(),
            "decimals": detail.get("detail_platforms", {}).get("ethereum", {}).get("decimal_place"),
        }
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contracts, indent=2))
    return contracts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--tokens", nargs="*", default=list(TRACKED_TOKENS))
    args = parser.parse_args()

    settings = get_settings()
    contracts = resolve_contracts(CoinGeckoClient(settings.coingecko_api_key))
    es = EtherscanClient(settings.etherscan_api_key)

    end_block = es.latest_block()
    start_block = es.block_at(int(time.time()) - args.days * 86_400)
    print(f"block range {start_block:,} -> {end_block:,} (~{args.days} days)")

    for symbol in args.tokens:
        info = contracts[symbol]
        print(f"{symbol} ({info['coin_id']}) contract {info['contract']}")
        state = pull_token_transfers(es, symbol, info["contract"], start_block, end_block, RAW_DIR)
        print(f"  {symbol}: {state['rows']:,} transfers, {state['calls']:,} API calls, "
              f"blocks {state['start_block']:,} -> {state['end_block']:,}")


if __name__ == "__main__":
    main()
