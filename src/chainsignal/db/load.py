"""Load the raw Phase 1 files into ClickHouse (full, idempotent reload) and verify."""
from collections.abc import Iterator
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path

from clickhouse_connect.driver.client import Client

from chainsignal.ingest.coingecko import parse_market_chart
from chainsignal.ingest.etherscan import parse_transfer

SQL_DIR = Path(__file__).parent

MARKET_COLUMNS = ["coin_id", "date", "price", "market_cap", "volume"]
TRANSFER_COLUMNS = [
    "token", "block_number", "block_time", "tx_hash", "tx_seq", "tx_index", "from_address",
    "to_address", "value_raw", "amount", "decimals", "gas_used", "gas_price", "method_id", "function_name",
]


def split_statements(sql: str) -> list[str]:
    """Split a .sql file on semicolons that end a line; drop comment-only chunks."""
    statements = []
    for chunk in sql.split(";\n"):
        body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
        if body:
            statements.append(body.rstrip(";"))
    return statements


def run_sql_file(client: Client, name: str) -> None:
    for statement in split_statements((SQL_DIR / name).read_text()):
        client.command(statement)


# ---------- raw file -> rows ----------

def coin_rows(coins_path: Path) -> list[list]:
    snapshot = datetime.fromtimestamp(coins_path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0)
    return [[c["id"], c["symbol"], c["name"], c.get("market_cap_rank"), snapshot]
            for c in json.loads(coins_path.read_text())]


def market_rows(chart_dir: Path) -> Iterator[list]:
    for path in sorted(chart_dir.glob("*.json")):
        for r in parse_market_chart(path.stem, json.loads(path.read_text())):
            yield [r["coin_id"], datetime.fromisoformat(r["date"]).date(), r["price"], r["market_cap"], r["volume"]]


def transfer_row(token: str, record: dict) -> list:
    t = parse_transfer(record)
    return [token, t["block_number"], t["timestamp"], t["tx_hash"], t["tx_seq"], t["tx_index"],
            t["from_address"], t["to_address"], int(t["value_raw"]), t["amount"], t["decimals"],
            t["gas_used"], t["gas_price"], t["method_id"], t["function_name"]]


def transfer_rows(token_dir: Path) -> Iterator[list]:
    with gzip.open(token_dir / "transfers.jsonl.gz", "rt") as f:
        for line in f:
            yield transfer_row(token_dir.name, json.loads(line))


# ---------- load ----------

def insert_batched(client: Client, table: str, rows: Iterator[list], columns: list[str],
                   batch_size: int = 100_000) -> int:
    total, batch = 0, []
    for row in rows:
        batch.append(row)
        if len(batch) == batch_size:
            client.insert(table, batch, column_names=columns)
            total += len(batch)
            batch = []
    if batch:
        client.insert(table, batch, column_names=columns)
        total += len(batch)
    return total


def load_all(client: Client, raw_dir: Path) -> dict[str, int]:
    """Drop and recreate the tables, insert everything, rebuild the daily rollup."""
    for table in ("coins", "market_daily_raw", "token_transfers_raw", "token_daily"):
        client.command(f"DROP TABLE IF EXISTS {table}")
    run_sql_file(client, "schema.sql")

    cg, es = raw_dir / "coingecko", raw_dir / "etherscan"
    counts = {
        "coins": insert_batched(client, "coins", iter(coin_rows(cg / "coins_markets.json")),
                                ["coin_id", "symbol", "name", "market_cap_rank", "snapshot_at"]),
        "market_daily_raw": insert_batched(client, "market_daily_raw", market_rows(cg / "market_chart"),
                                           MARKET_COLUMNS),
    }
    for token_dir in sorted(p for p in es.iterdir() if (p / "transfers.jsonl.gz").exists()):
        counts[f"transfers:{token_dir.name}"] = insert_batched(
            client, "token_transfers_raw", transfer_rows(token_dir), TRANSFER_COLUMNS)

    run_sql_file(client, "rollup.sql")
    run_sql_file(client, "quality.sql")
    return counts
