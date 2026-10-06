"""Phase 0 check: confirm every piece of the environment works before writing pipeline logic.

Run: .venv/bin/python scripts/check_env.py
"""
import sys

from chainsignal.config import get_settings


def check_clickhouse() -> None:
    from chainsignal.db.clickhouse import get_client

    client = get_client()
    version = client.query("SELECT version()").result_rows[0][0]
    db = client.query("SELECT currentDatabase()").result_rows[0][0]
    print(f"[ok]   ClickHouse {version} reachable, database '{db}'")


def check_spark() -> None:
    from chainsignal.spark import get_spark

    spark = get_spark("chainsignal-check")
    spark.sparkContext.setLogLevel("ERROR")
    total = spark.range(1_000_000).selectExpr("sum(id) AS s").collect()[0]["s"]
    # spark.range runs entirely in the JVM; a Python-data job also proves the Python workers start
    worker_py = spark.sparkContext.parallelize([0], 1).map(lambda _: __import__("sys").version.split()[0]).first()
    print(f"[ok]   Spark {spark.version} ran a local job (sum 0..999999 = {total}); "
          f"Python workers run {worker_py}")
    spark.stop()


def check_api_keys() -> None:
    s = get_settings()
    for name, value in [("COINGECKO_API_KEY", s.coingecko_api_key), ("ETHERSCAN_API_KEY", s.etherscan_api_key)]:
        status = "[ok]  " if value else "[todo]"
        print(f"{status} {name} {'set' if value else 'not set yet (needed in Phase 1)'}")


def main() -> int:
    failed = False
    for check in (check_clickhouse, check_spark):
        try:
            check()
        except Exception as exc:  # report every failure, not just the first
            failed = True
            print(f"[FAIL] {check.__name__}: {exc}")
    check_api_keys()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
