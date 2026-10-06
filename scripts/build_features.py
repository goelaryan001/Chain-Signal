"""Phase 3: ClickHouse -> Parquet -> Spark features -> Parquet -> ClickHouse, then verify.

Run: .venv/bin/python scripts/build_features.py
Needs scripts/load_clickhouse.py to have loaded the raw data and address labels.
Safe to re-run: feature tables are dropped and rebuilt.
"""
from pathlib import Path
import sys
import time

from pyspark.sql import functions as F

from chainsignal.db.clickhouse import get_client
from chainsignal.db.load import run_sql_file
from chainsignal.features.io import EXPORTS, export_parquet, import_parquet_dir, reset_dir
from chainsignal.features.market import add_market_features
from chainsignal.features.onchain import add_onchain_features, daily_activity
from chainsignal.spark import get_spark

STAGE = Path("data/staging")


def main() -> int:
    client = get_client()
    start = time.time()

    print("export ClickHouse -> Parquet")
    for name, query in EXPORTS.items():
        size = export_parquet(client, query, STAGE / f"{name}.parquet")
        print(f"  {name:<10} {size / 1e6:6.1f} MB")

    spark = get_spark("chainsignal-features")
    spark.sparkContext.setLogLevel("ERROR")

    market = (spark.read.parquet(str(STAGE / "market.parquet"))
              .withColumn("date", F.to_date("date_str")).drop("date_str")
              .withColumn("extreme_move", F.col("extreme_move") == 1))
    transfers = (spark.read.parquet(str(STAGE / "transfers.parquet"))
                 .withColumn("block_time", F.timestamp_seconds("ts")).drop("ts"))
    labels = (spark.read.parquet(str(STAGE / "labels.parquet"))
              .withColumn("is_contract", F.col("is_contract") == 1))

    print("compute features in Spark")
    market_out, onchain_out = STAGE / "market_features", STAGE / "onchain_features"
    reset_dir(market_out)
    reset_dir(onchain_out)
    add_market_features(market).coalesce(1).write.parquet(str(market_out))
    add_onchain_features(daily_activity(transfers, labels)).coalesce(1).write.parquet(str(onchain_out))
    spark.stop()

    print("import Parquet -> ClickHouse")
    for table in ("market_features", "onchain_features"):
        client.command(f"DROP TABLE IF EXISTS {table}")
    run_sql_file(client, "features.sql")
    import_parquet_dir(client, "market_features", market_out)
    import_parquet_dir(client, "onchain_features", onchain_out)
    print(f"pipeline finished in {time.time() - start:.0f}s")

    return 0 if verify(client) else 1


def verify(client) -> bool:
    q = lambda sql: client.query(sql).result_rows[0]
    ok = True

    def check(name, cond, detail):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'ok' if cond else 'FAIL'}] {name}: {detail}")

    print("verify")
    src, out = q("SELECT count() FROM market_daily_clean")[0], q("SELECT count() FROM market_features")[0]
    check("market rows preserved", src == out, f"clean={src:,} features={out:,}")
    inf = q("""SELECT countIf(isInfinite(return_z) OR isInfinite(volume_z) OR isInfinite(residual_z)
               OR isInfinite(turnover_z)) FROM market_features""")[0]
    check("no infinite market z-scores", inf == 0, f"{inf} infinite")
    covered, coins = q("""SELECT countIf(return_z IS NOT NULL), uniqExact(coin_id) FROM market_features""")
    check("market features populated", covered > 0, f"{covered:,} rows with return_z across {coins} coins")

    days = dict(client.query("SELECT token, count() FROM onchain_features GROUP BY token").result_rows)
    raw_days = dict(client.query("SELECT token, count() FROM token_daily GROUP BY token").result_rows)
    check("on-chain days = rollup days minus 2 partial edges",
          all(days.get(t, 0) == n - 2 for t, n in raw_days.items()), f"{days} vs rollup {raw_days}")
    same = q("""SELECT countIf(o.transfers != d.transfers) FROM onchain_features o
                INNER JOIN token_daily d ON o.token = d.token AND o.day = d.day""")[0]
    check("Spark daily transfers match ClickHouse rollup", same == 0, f"{same} mismatched days")
    return ok


if __name__ == "__main__":
    sys.exit(main())
