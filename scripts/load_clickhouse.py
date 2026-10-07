"""Phase 2: load raw files into ClickHouse, then verify the load against the files.

Run: .venv/bin/python scripts/load_clickhouse.py
Safe to re-run: every run truncates and reloads (the raw files are the source of truth).
"""
import gzip
import json
from pathlib import Path
import sys
import time

from chainsignal.db.clickhouse import get_client
from chainsignal.db.load import later_market_rows, load_all, market_rows

RAW_DIR = Path("data/raw")


def file_truth() -> dict:
    """Recompute counts and checksums straight from the raw files, independent of the loader.

    Transfers: the base pull plus every daily-update file, deduplicated here by its own
    (token, hash, txSeq) key with the newest fetch winning, separately from the loader's code.
    """
    truth = {"market_rows": sum(1 for _ in market_rows(RAW_DIR / "coingecko" / "market_chart"))
                            + len(later_market_rows(RAW_DIR)),
             "coins": len(json.loads((RAW_DIR / "coingecko" / "coins_markets.json").read_text())),
             "tokens": {}}
    latest = {}
    sources = [(p.parent.name, p) for p in sorted((RAW_DIR / "etherscan").glob("*/transfers.jsonl.gz"))]
    sources += [(None, p) for p in sorted((RAW_DIR / "etherscan" / "incremental").glob("*.jsonl.gz"),
                                          key=lambda p: int(p.name.split("_")[1].split(".")[0]))]
    for token, path in sources:
        with gzip.open(path, "rt") as f:
            for line in f:
                rec = json.loads(line)
                tok = token or rec["token"]
                latest[(tok, rec["hash"], int(rec["txSeq"]))] = int(rec["value"])
    for (tok, _, _), value in latest.items():
        t = truth["tokens"].setdefault(tok, {"rows": 0, "value_sum": 0})
        t["rows"] += 1
        t["value_sum"] += value
    return truth


def verify(client, truth: dict) -> bool:
    checks = []

    def check(name, expected, actual):
        checks.append(expected == actual)
        print(f"  [{'ok' if expected == actual else 'FAIL'}] {name}: files={expected:,} clickhouse={actual:,}")

    q = lambda sql: client.query(sql).result_rows
    check("coins", truth["coins"], q("SELECT count() FROM coins")[0][0])
    check("market rows", truth["market_rows"], q("SELECT count() FROM market_daily_raw")[0][0])
    check("market (coin, date) keys unique", truth["market_rows"],
          q("SELECT uniqExact(coin_id, date) FROM market_daily_raw")[0][0])

    ch = {r[0]: r[1:] for r in q("""
        SELECT token, count(), uniqExact(tx_hash, tx_seq), toString(sum(value_raw))
        FROM token_transfers_raw GROUP BY token""")}
    for token, t in truth["tokens"].items():
        rows, unique_keys, value_sum = ch.get(token, (0, 0, "0"))
        check(f"{token} rows", t["rows"], rows)
        check(f"{token} (tx_hash, tx_seq) unique", t["rows"], unique_keys)
        check(f"{token} sum(value_raw) exact", t["value_sum"], int(value_sum))

    rolled = dict(q("SELECT token, sum(transfers) FROM token_daily GROUP BY token"))
    for token, t in truth["tokens"].items():
        check(f"{token} token_daily transfers add up", t["rows"], rolled.get(token, 0))
    return all(checks)


def main() -> int:
    client = get_client()
    start = time.time()
    counts = load_all(client, RAW_DIR)
    dbt = counts.pop("dbt")
    print(f"loaded in {time.time() - start:.0f}s: " + ", ".join(f"{k}={v:,}" for k, v in counts.items()))
    print(f"dbt build: {dbt}")

    print("verifying against raw files...")
    ok = verify(client, file_truth())

    print("\ndata quality (coin_quality view):")
    for status, coins, rows in client.query("""
        SELECT status, count(), sum(days) FROM coin_quality GROUP BY status ORDER BY status""").result_rows:
        print(f"  {status:<12} {coins:>4} coins  {rows:>8,} coin-days")
    clean = client.query("SELECT uniqExact(coin_id), count(), countIf(extreme_move) FROM market_daily_clean").result_rows[0]
    print(f"  market_daily_clean: {clean[0]} coins, {clean[1]:,} rows, {clean[2]} flagged extreme moves kept")

    print("\nstorage (compressed vs uncompressed):")
    for table, rows, comp, raw, ratio in client.query("""
        SELECT table, sum(rows), formatReadableSize(sum(data_compressed_bytes)),
               formatReadableSize(sum(data_uncompressed_bytes)),
               round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1)
        FROM system.parts WHERE active AND database = currentDatabase()
        GROUP BY table ORDER BY table""").result_rows:
        print(f"  {table:<22} {rows:>10,} rows  {comp:>10} on disk  ({raw} raw, {ratio}x)")
    print("\nLOAD VERIFIED" if ok else "\nLOAD VERIFICATION FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
