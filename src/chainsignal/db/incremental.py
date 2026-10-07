"""Idempotent day-level loads: stage the rows, then atomically replace whole day partitions.

ALTER TABLE target REPLACE PARTITION ID '<yyyymmdd>' FROM staging swaps the target's
partition for the staging table's copy in one step. Running the same day twice replaces
it twice, so a re-run can never duplicate rows. A day with no rows in staging is replaced
by nothing, which is also correct: the source says that day is empty.
"""
from collections.abc import Iterable
from datetime import date

from clickhouse_connect.driver.client import Client


def partition_id(day: date) -> str:
    return day.strftime("%Y%m%d")


def replace_partitions(client: Client, table: str, rows: list[list], columns: list[str],
                       days: Iterable[date]) -> dict:
    days = sorted(set(days))
    staging = f"{table}__staging"
    client.command(f"DROP TABLE IF EXISTS {staging}")
    client.command(f"CREATE TABLE {staging} AS {table}")   # same columns, engine and partition key
    try:
        if rows:
            client.insert(staging, rows, column_names=columns)
        for d in days:
            client.command(f"ALTER TABLE {table} REPLACE PARTITION ID '{partition_id(d)}' FROM {staging}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")
    return {"rows": len(rows), "days": [d.isoformat() for d in days]}


def table_fingerprint(client: Client, table: str, columns: list[str]) -> tuple[int, int]:
    """(row count, XOR of per-row hashes): exact and independent of row order.

    Float sums are not usable for this: ClickHouse sums in parallel, so the last cents
    of a $66-trillion volume total vary between runs on identical data.
    """
    return tuple(client.query(f"SELECT count(), groupBitXor(cityHash64({', '.join(columns)})) FROM {table}").result_rows[0])
