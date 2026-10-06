"""Move data between ClickHouse and Spark through Parquet files.

Why Parquet instead of a JDBC connection: no driver jar to manage, both ends are
columnar, and the files are inspectable checkpoints between the two systems.
Dates and times cross the boundary as plain text / epoch seconds, because the two
systems encode Parquet dates and timestamps differently.
"""
from pathlib import Path
import shutil

from clickhouse_connect.driver.client import Client

EXPORTS = {
    "market": """
        SELECT coin_id, toString(date) AS date_str, price, market_cap, volume, log_return,
               toUInt8(extreme_move) AS extreme_move, missing_days_before
        FROM market_daily_clean""",
    "transfers": """
        SELECT token, tx_hash, toInt32(tx_seq) AS tx_seq, toInt64(toUnixTimestamp(block_time)) AS ts,
               from_address, to_address, amount, toString(function_name) AS function_name
        FROM token_transfers_raw""",
    "labels": "SELECT address, toUInt8(is_contract) AS is_contract FROM address_labels",
}


def export_parquet(client: Client, query: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = client.raw_query(query, fmt="Parquet")
    path.write_bytes(data)
    return len(data)


def import_parquet_dir(client: Client, table: str, spark_dir: Path) -> None:
    """Insert every part file Spark wrote into a ClickHouse table."""
    for part in sorted(spark_dir.glob("part-*.parquet")):
        client.raw_insert(table, insert_block=part.read_bytes(), fmt="Parquet")


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
