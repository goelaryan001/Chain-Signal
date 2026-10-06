"""Phase 1: print real samples and sanity checks from the raw pulls.

Run: .venv/bin/python scripts/inspect_raw.py
"""
from collections import Counter
import gzip
import json
from pathlib import Path

import pandas as pd

from chainsignal.ingest.coingecko import parse_market_chart
from chainsignal.ingest.etherscan import parse_transfer

CG_DIR = Path("data/raw/coingecko")
ES_DIR = Path("data/raw/etherscan")


def inspect_coingecko() -> None:
    coins = json.loads((CG_DIR / "coins_markets.json").read_text())
    rows = []
    for path in sorted((CG_DIR / "market_chart").glob("*.json")):
        rows.extend(parse_market_chart(path.stem, json.loads(path.read_text())))
    df = pd.DataFrame(rows)
    per_coin = df.groupby("coin_id").size()

    print("=== CoinGecko ===")
    print(f"coins listed: {len(coins)}, with history: {df.coin_id.nunique()}, rows: {len(df):,}")
    print(f"date range: {df.date.min()} -> {df.date.max()}")
    print(f"days per coin: median {per_coin.median():.0f}, min {per_coin.min()}, "
          f"coins with < 365 days: {(per_coin < 365).sum()} (listed within the year)")
    print(f"null price / market_cap / volume: {df.price.isna().sum()} / "
          f"{df.market_cap.isna().sum()} / {df.volume.isna().sum()}")
    print(f"zero-volume days: {(df.volume == 0).sum():,}")
    print("\nsample (top 5 by rank, latest day):")
    top = [c["id"] for c in coins[:5]]
    latest = df[df.coin_id.isin(top) & (df.date == df.date.max())]
    print(latest.to_string(index=False, float_format=lambda v: f"{v:,.2f}"))
    print("\nbiggest single-day price moves (|return|), a first look at what 'anomalous' means here:")
    df = df.sort_values(["coin_id", "date"])
    df["ret"] = df.groupby("coin_id").price.pct_change()
    print(df.loc[df.ret.abs().nlargest(5).index, ["coin_id", "date", "price", "ret"]].to_string(index=False))

    print("\ndata quality (raw data is kept as-is; these feed a cleaning step before modelling):")
    spikes = df[df.ret > 9].groupby("coin_id").size().sort_values(ascending=False)
    print(f"  coins with a >10x single-day jump: {len(spikes)} -> {spikes.head(8).to_dict()}")
    span = df.groupby("coin_id").date.agg(["min", "max", "size"])
    expected = (pd.to_datetime(span["max"]) - pd.to_datetime(span["min"])).dt.days + 1
    gappy = (expected - span["size"])
    print(f"  coins with missing days inside their range: {(gappy > 0).sum()} "
          f"(total missing coin-days: {gappy.sum():,})")


def inspect_etherscan() -> None:
    print("\n=== Etherscan ===")
    for state_path in sorted(ES_DIR.glob("*/state.json")):
        state = json.loads(state_path.read_text())
        data_path = state_path.parent / "transfers.jsonl.gz"
        with gzip.open(data_path, "rt") as f:
            df = pd.DataFrame(parse_transfer(json.loads(line)) for line in f)
        complete = state["next_block"] > state["end_block"]
        dupes = df.duplicated(["tx_hash", "tx_seq"]).sum()
        daily = df.set_index("timestamp").resample("D").size()
        addrs = pd.concat([df.from_address, df.to_address]).nunique()
        print(f"\n{state['symbol']}  ({'complete' if complete else 'IN PROGRESS'}, {state['calls']:,} calls)")
        print(f"  transfers: {len(df):,}  duplicates on (tx_hash, tx_seq): {dupes}")
        print(f"  time range: {df.timestamp.min()} -> {df.timestamp.max()}")
        print(f"  transfers/day: median {daily.median():,.0f}, max {daily.max():,} on {daily.idxmax().date()}")
        print(f"  unique addresses: {addrs:,}")
        print(f"  amount: median {df.amount.median():,.2f}, p99 {df.amount.quantile(.99):,.0f}, "
              f"max {df.amount.max():,.0f}")
        top_fn = Counter(f.split("(")[0] or "(none)" for f in df.function_name).most_common(4)
        print(f"  top functions: {top_fn}")
        print("  sample row:", df.iloc[len(df) // 2].to_dict())


if __name__ == "__main__":
    inspect_coingecko()
    inspect_etherscan()
