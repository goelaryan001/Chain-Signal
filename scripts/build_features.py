"""Phase 3: ClickHouse -> Parquet -> Spark features -> Parquet -> ClickHouse, then verify.

Run: .venv/bin/python scripts/build_features.py
Needs scripts/load_clickhouse.py to have loaded the raw data and address labels.
Safe to re-run: feature tables are dropped and rebuilt. The logic lives in
chainsignal.features.job, shared with the Dagster pipeline.
"""
import sys

from chainsignal.db.clickhouse import get_client
from chainsignal.features.job import run_feature_job, verify_features


def main() -> int:
    client = get_client()
    run_feature_job(client)
    print("verify")
    checks = verify_features(client)
    for name, ok, detail in checks:
        print(f"  [{'ok' if ok else 'FAIL'}] {name}: {detail}")
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
