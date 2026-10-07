"""Load the raw Phase 1 files into ClickHouse (full, idempotent reload) and verify."""
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timezone
import gzip
import itertools
import json
from pathlib import Path

from clickhouse_connect.driver.client import Client

from chainsignal.ingest.coingecko import parse_market_chart
from chainsignal.ingest.etherscan import parse_transfer

SQL_DIR = Path(__file__).parent

MARKET_COLUMNS = ["coin_id", "date", "price", "market_cap", "volume", "source"]
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
    """Historical rows from the market_chart backfill."""
    for path in sorted(chart_dir.glob("*.json")):
        for r in parse_market_chart(path.stem, json.loads(path.read_text())):
            yield [r["coin_id"], datetime.fromisoformat(r["date"]).date(), r["price"], r["market_cap"], r["volume"],
                   "history"]


def history_end(raw_dir: Path) -> date:
    """Last day covered by the historical market_chart pull (cached: it takes a few seconds)."""
    marker = raw_dir / "coingecko" / "history_end.txt"
    if marker.exists():
        return date.fromisoformat(marker.read_text().strip())
    last = max(r["date"] for p in (raw_dir / "coingecko" / "market_chart").glob("*.json")
               for r in parse_market_chart(p.stem, json.loads(p.read_text()))[-1:])
    marker.write_text(last)
    return date.fromisoformat(last)


def merge_later_days(snapshots: Iterable[list], reconciled: Iterable[list]) -> list[list]:
    """Rows for days after the historical pull: a reconciled value replaces the snapshot
    for the same (coin, day); within each kind, later inputs win."""
    merged = {}
    for row in [*snapshots, *reconciled]:
        merged[(row[0], row[1])] = row
    return list(merged.values())


def later_market_rows(raw_dir: Path) -> list[list]:
    """Snapshot and reconciled rows for days after the historical pull, as a full reload sees them."""
    from chainsignal.ingest.coingecko import parse_markets_snapshot
    end = history_end(raw_dir)
    snaps = []
    for path in sorted((raw_dir / "coingecko" / "snapshots").glob("*.json")):
        payload = json.loads(path.read_text())
        day = date.fromisoformat(payload["day"])
        snaps += [[r["coin_id"], day, r["price"], r["market_cap"], r["volume"], "snapshot"]
                  for r in parse_markets_snapshot(payload["coins"], day)]
    recon = []
    for run_dir in sorted((raw_dir / "coingecko" / "reconciled").glob("*")):   # oldest run first
        run_day = date.fromisoformat(run_dir.name)
        for path in sorted(run_dir.glob("*.json")):
            for r in parse_market_chart(path.stem, json.loads(path.read_text())):
                d = date.fromisoformat(r["date"])
                if end < d < run_day:
                    recon.append([r["coin_id"], d, r["price"], r["market_cap"], r["volume"], "reconciled"])
    return merge_later_days(snaps, recon)


def transfer_row(token: str, record: dict) -> list:
    t = parse_transfer(record)
    return [token, t["block_number"], t["timestamp"], t["tx_hash"], t["tx_seq"], t["tx_index"],
            t["from_address"], t["to_address"], int(t["value_raw"]), t["amount"], t["decimals"],
            t["gas_used"], t["gas_price"], t["method_id"], t["function_name"]]


def transfer_rows(token_dir: Path) -> Iterator[list]:
    with gzip.open(token_dir / "transfers.jsonl.gz", "rt") as f:
        for line in f:
            yield transfer_row(token_dir.name, json.loads(line))


def incremental_files(raw_dir: Path) -> list[Path]:
    """Daily-update files named <start_day>_<end_block>.jsonl.gz, oldest fetch first."""
    files = (raw_dir / "etherscan" / "incremental").glob("*.jsonl.gz")
    return sorted(files, key=lambda p: int(p.name.split("_")[1].split(".")[0]))


def dedupe_latest(rows: Iterable[list]) -> list[list]:
    """Keep one row per (token, tx_hash, tx_seq); a later input replaces an earlier one.

    The daily update refetches the last stored day, so its files overlap the base pull."""
    key = lambda r: (r[0], r[3], r[4])   # token, tx_hash, tx_seq (TRANSFER_COLUMNS order)
    latest = {}
    for r in rows:
        latest[key(r)] = r
    return list(latest.values())


def all_transfer_rows(raw_dir: Path) -> list[list]:
    """Base pull plus every daily update, deduplicated, as a full reload sees them."""
    es = raw_dir / "etherscan"
    base = (r for d in sorted(p for p in es.iterdir() if (p / "transfers.jsonl.gz").exists()) for r in transfer_rows(d))
    def updates():
        for path in incremental_files(raw_dir):
            with gzip.open(path, "rt") as f:
                for line in f:
                    rec = json.loads(line)
                    yield transfer_row(rec["token"], rec)
    return dedupe_latest(itertools.chain(base, updates()))


def label_rows(path: Path) -> Iterator[list]:
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            yield [r["address"], r["is_contract"], r["creator"], r["factory"], r["created_block"]]


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
    for table in ("coins", "market_daily_raw", "token_transfers_raw", "token_daily", "address_labels"):
        client.command(f"DROP TABLE IF EXISTS {table}")
    run_sql_file(client, "schema.sql")

    cg, es = raw_dir / "coingecko", raw_dir / "etherscan"
    counts = {
        "coins": insert_batched(client, "coins", iter(coin_rows(cg / "coins_markets.json")),
                                ["coin_id", "symbol", "name", "market_cap_rank", "snapshot_at"]),
        "market_daily_raw": insert_batched(client, "market_daily_raw",
                                           itertools.chain(market_rows(cg / "market_chart"), later_market_rows(raw_dir)),
                                           MARKET_COLUMNS),
    }
    transfers = all_transfer_rows(raw_dir)
    for token in sorted({r[0] for r in transfers}):
        counts[f"transfers:{token}"] = insert_batched(
            client, "token_transfers_raw", (r for r in transfers if r[0] == token), TRANSFER_COLUMNS)

    labels_path = es / "address_labels.jsonl"
    if labels_path.exists():
        counts["address_labels"] = insert_batched(
            client, "address_labels", label_rows(labels_path),
            ["address", "is_contract", "creator", "factory", "created_block"])

    run_sql_file(client, "rollup.sql")
    run_sql_file(client, "quality.sql")
    return counts
