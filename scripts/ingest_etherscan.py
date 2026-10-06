"""Phase 1: pull every ERC-20 transfer of the tracked tokens over the last N days.

Run: .venv/bin/python scripts/ingest_etherscan.py --days 90 [--tokens PEPE] [--rate 1.6]
Contract addresses come from CoinGecko, not hard-coded. The block range is
fixed on first run (range.json) so every token covers the same window, and
each token's pull is resumable from data/raw/etherscan/<SYMBOL>/state.json.
Calls are server-latency bound, so tokens can run as parallel processes, each
with --rate set so the total stays under the 5 calls/sec free-tier limit.
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


def resolve_block_range(es: EtherscanClient, days: int) -> tuple[int, int]:
    path = RAW_DIR / "range.json"
    if path.exists():
        r = json.loads(path.read_text())
    else:
        r = {"days": days, "end_block": es.latest_block(),
             "start_block": es.block_at(int(time.time()) - days * 86_400)}
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(r, indent=2))
    return r["start_block"], r["end_block"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--tokens", nargs="*", default=list(TRACKED_TOKENS))
    parser.add_argument("--rate", type=float, default=4.0, help="calls/sec for this process")
    args = parser.parse_args()

    settings = get_settings()
    contracts = resolve_contracts(CoinGeckoClient(settings.coingecko_api_key))
    es = EtherscanClient(settings.etherscan_api_key, calls_per_second=args.rate)
    start_block, end_block = resolve_block_range(es, args.days)
    print(f"block range {start_block:,} -> {end_block:,} (~{args.days} days)")

    for symbol in args.tokens:
        info = contracts[symbol]
        print(f"{symbol} ({info['coin_id']}) contract {info['contract']}")
        state = pull_token_transfers(es, symbol, info["contract"], start_block, end_block, RAW_DIR)
        print(f"  {symbol}: {state['rows']:,} transfers, {state['calls']:,} API calls, "
              f"blocks {state['start_block']:,} -> {state['end_block']:,}")


if __name__ == "__main__":
    main()
