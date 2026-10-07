"""The daily incremental update: new market day + new transfers, loaded idempotently.

Pattern for both sources: fetch once, store the raw response, load from the raw file.
Re-running a day reloads the same raw file and replaces the same partitions, so the
result is identical (same rows, same checksums).
"""
from datetime import date, datetime, time, timedelta, timezone
import gzip
import json
from pathlib import Path

from clickhouse_connect.driver.client import Client

from chainsignal.db.incremental import replace_partitions
from chainsignal.db.load import MARKET_COLUMNS, TRANSFER_COLUMNS, history_end, transfer_row
from chainsignal.ingest.coingecko import (CoinGeckoClient, parse_market_chart, parse_markets_snapshot,
                                          snapshot_day)
from chainsignal.ingest.etherscan import EtherscanClient, fetch_transfers

SNAPSHOT_BATCH = 250


def snapshot_path(raw_dir: Path, day: date) -> Path:
    return raw_dir / "coingecko" / "snapshots" / f"{day.isoformat()}.json"


def run_market_snapshot(client: Client, cg: CoinGeckoClient, raw_dir: Path,
                        day: date | None = None, now: datetime | None = None) -> dict:
    """Load one market day from a /coins/markets snapshot of exactly the tracked coins."""
    now = now or datetime.now(timezone.utc)
    day = day or (now - timedelta(days=1)).date()
    if day <= history_end(raw_dir):
        raise ValueError(f"{day} is covered by the historical pull (ends {history_end(raw_dir)}); "
                         "a snapshot must not overwrite it")
    tracked = [r[0] for r in client.query("SELECT coin_id FROM coins ORDER BY coin_id").result_rows]
    path = snapshot_path(raw_dir, day)
    fetched = False
    if not path.exists():
        if snapshot_day(now) != day:   # raises outside the post-midnight window
            raise ValueError(f"no stored snapshot for {day}, and a snapshot taken now is for {snapshot_day(now)}")
        coins = []
        for i in range(0, len(tracked), SNAPSHOT_BATCH):
            coins += cg._get("/coins/markets", {"vs_currency": "usd", "ids": ",".join(tracked[i:i + SNAPSHOT_BATCH]),
                                                "per_page": SNAPSHOT_BATCH, "page": 1})
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"fetched_at": now.isoformat(), "day": day.isoformat(), "coins": coins}))
        fetched = True
    payload = json.loads(path.read_text())
    rows = parse_markets_snapshot(payload["coins"], day, set(tracked))
    ch_rows = [[r["coin_id"], day, r["price"], r["market_cap"], r["volume"], "snapshot"] for r in rows]
    result = replace_partitions(client, "market_daily_raw", ch_rows, MARKET_COLUMNS, [day])
    return {**result, "day": day.isoformat(), "fetched": fetched, "fetched_at": payload["fetched_at"],
            "tracked": len(tracked), "missing": len(tracked) - len(rows)}


def incremental_path(raw_dir: Path, start_day: date, end_block: int) -> Path:
    return raw_dir / "etherscan" / "incremental" / f"{start_day.isoformat()}_{end_block}.jsonl.gz"


def run_transfer_update(client: Client, es: EtherscanClient, contracts: dict[str, dict], raw_dir: Path,
                        end_block: int | None = None) -> dict:
    """Refetch every tracked token from the start of the last stored day to `end_block`.

    Starting at the last stored day's first block completes that (partial) day; all tokens
    share the day partitions, so they are fetched and replaced together.
    """
    last_days = dict(client.query("SELECT token, max(toDate(block_time)) FROM token_transfers_raw GROUP BY token").result_rows)
    start_day = min(last_days.values())
    start_block = es.block_at(int(datetime.combine(start_day, time(0), timezone.utc).timestamp()))
    end_block = end_block or es.latest_block()
    path = incremental_path(raw_dir, start_day, end_block)
    fetched = False
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with gzip.open(tmp, "wt") as f:
            for symbol, info in contracts.items():
                for rec in fetch_transfers(es, info["contract"], start_block, end_block):
                    f.write(json.dumps({**rec, "token": symbol}) + "\n")
        tmp.rename(path)   # only a complete fetch becomes visible
        fetched = True
    with gzip.open(path, "rt") as f:
        records = [json.loads(line) for line in f]
    rows = [transfer_row(r["token"], r) for r in records]
    days = {start_day} | {row[2].date() for row in rows}
    result = replace_partitions(client, "token_transfers_raw", rows, TRANSFER_COLUMNS, days)
    per_token = {}
    for r in records:
        per_token[r["token"]] = per_token.get(r["token"], 0) + 1
    return {**result, "start_day": start_day.isoformat(), "start_block": start_block, "end_block": end_block,
            "fetched": fetched, "per_token": per_token, "raw_file": path.name}


def run_market_reconcile(client: Client, cg: CoinGeckoClient, raw_dir: Path, days: int = 8,
                         coins: list[str] | None = None, run_day: date | None = None) -> dict:
    """Replace provisional snapshot days with authoritative market_chart values.

    Partial runs are safe: each touched day's existing rows are read back, only the
    reconciled coins are overridden, and the whole day partition is replaced.
    """
    run_day = run_day or datetime.now(timezone.utc).date()
    end = history_end(raw_dir)
    tracked = coins or [r[0] for r in client.query("SELECT coin_id FROM coins ORDER BY coin_id").result_rows]
    out_dir = raw_dir / "coingecko" / "reconciled" / run_day.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    recon, fetched = {}, 0
    for coin in tracked:
        path = out_dir / f"{coin}.json"
        if not path.exists():
            path.write_text(json.dumps(cg.market_chart(coin, days)))
            fetched += 1
        for r in parse_market_chart(coin, json.loads(path.read_text())):
            d = date.fromisoformat(r["date"])
            if end < d < run_day:
                recon[(coin, d)] = [coin, d, r["price"], r["market_cap"], r["volume"], "reconciled"]
    touched = sorted({d for _, d in recon})
    if not touched:
        return {"rows": 0, "days": [], "fetched": fetched, "coins": len(tracked)}
    existing = client.query(f"SELECT {', '.join(MARKET_COLUMNS)} FROM market_daily_raw WHERE date IN %(days)s",
                            parameters={"days": touched}).result_rows
    before = {(r[0], r[1]): r for r in existing}
    merged = {**{k: list(v) for k, v in before.items()}, **recon}
    result = replace_partitions(client, "market_daily_raw", list(merged.values()), MARKET_COLUMNS, touched)
    diffs = [abs(recon[k][4] / before[k][4] - 1) for k in recon if k in before and before[k][4]]
    return {**result, "fetched": fetched, "coins": len(tracked), "reconciled": len(recon),
            "median_volume_change": sorted(diffs)[len(diffs) // 2] if diffs else None}
