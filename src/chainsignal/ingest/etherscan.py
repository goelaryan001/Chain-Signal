"""Etherscan V2 API client: on-chain depth (ERC-20 transfer events for a few tokens).

Free-tier limits (docs, checked 2026-10): 5 calls/sec, 100,000 calls/day, and
at most 1,000 records per request since 2026-07-01.

Pagination walks forward by block number instead of by page, because page
numbers stop working past page * offset = 10,000 records. Each request asks for
up to 1,000 transfers from `start_block`. A full batch may cut the last block
off part-way, so that block is dropped and becomes the next `start_block`.
Only complete blocks are ever written, which means no duplicates, no gaps and
an exact resume point. A single block holding 1,000+ transfers of one token is
paged on its own with startblock = endblock.

A short or empty answer is NOT taken as the end of the range: under load
Etherscan returned one mid-range and our first full pull silently stopped ~2
days early. So the last block of every multi-block batch is dropped and
re-asked (short batches included), and the walk ends only after two empty
answers in a row.

The V2 tokentx response has no logIndex (V1 had one), and one transaction can
emit several transfers of the same token. Records arrive in on-chain order and
whole blocks are always written together, so each transfer is numbered within
its transaction (txSeq) to give a unique (hash, txSeq) key.
"""
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import time

import requests

from chainsignal.ingest.http import RateLimiter, get_json

BASE_URL = "https://api.etherscan.io/v2/api"
MAX_RECORDS = 1_000

# Fields kept from each tokentx record; the rest (token name/symbol repeated on
# every row, confirmations, input) is redundant or changes over time.
TRANSFER_FIELDS = (
    "blockNumber", "timeStamp", "hash", "txSeq", "transactionIndex",
    "from", "to", "value", "tokenDecimal", "tokenSymbol", "gasUsed", "gasPrice",
    "methodId", "functionName",
)

FetchPage = Callable[[int, int, int], list[dict]]  # (start_block, end_block, page) -> records


class EtherscanError(RuntimeError):
    pass


class EtherscanClient:
    def __init__(self, api_key: str, calls_per_second: float = 4, chain_id: int = 1):
        self.session = requests.Session()
        self.api_key = api_key
        self.chain_id = chain_id
        self.limiter = RateLimiter(calls_per_second)

    def _call(self, params: dict, retries: int = 5):
        params = {"chainid": self.chain_id, "apikey": self.api_key, **params}
        for attempt in range(retries):
            data = get_json(self.session, BASE_URL, self.limiter, params, max_retries=10)
            if data.get("status") == "1" or data.get("jsonrpc"):
                return data["result"]
            message = f"{data.get('message')}: {data.get('result')}"
            if "No transactions found" in message or "No records found" in message:
                return []
            if "rate limit" in message.lower():
                time.sleep(1 + attempt)
                continue
            raise EtherscanError(message)
        raise EtherscanError(f"rate limited {retries} times: {params.get('action')}")

    def latest_block(self) -> int:
        return int(self._call({"module": "proxy", "action": "eth_blockNumber"}), 16)

    def block_at(self, timestamp: int) -> int:
        """First block mined at or after a unix timestamp."""
        return int(self._call({
            "module": "block", "action": "getblocknobytime", "timestamp": timestamp, "closest": "after",
        }))

    def block_time(self, block: int) -> datetime:
        block_data = self._call({"module": "proxy", "action": "eth_getBlockByNumber",
                                 "tag": hex(block), "boolean": "false"})
        return datetime.fromtimestamp(int(block_data["timestamp"], 16), tz=timezone.utc)

    def token_transfers_page(self, contract: str, start_block: int, end_block: int, page: int = 1) -> list[dict]:
        return self._call({
            "module": "account", "action": "tokentx", "contractaddress": contract,
            "startblock": start_block, "endblock": end_block,
            "page": page, "offset": MAX_RECORDS, "sort": "asc",
        })


def iter_complete_blocks(
    fetch: FetchPage, start_block: int, end_block: int, page_size: int = MAX_RECORDS,
) -> Iterator[tuple[list[dict], int]]:
    """Yield (records, next_start_block) batches that each cover only complete blocks."""
    cursor = start_block
    confirmed_empty = False
    while cursor <= end_block:
        batch = fetch(cursor, end_block, 1)
        if not batch:
            if confirmed_empty:
                yield [], end_block + 1
                return
            confirmed_empty = True  # ask once more before believing the range is finished
            continue
        confirmed_empty = False
        last_block = int(batch[-1]["blockNumber"])
        if last_block > cursor:
            # The last block may be cut off (full batch or truncated answer): drop it, re-ask from it.
            complete = [r for r in batch if int(r["blockNumber"]) < last_block]
            yield complete, last_block
            cursor = last_block
            continue
        # The whole batch is the single block `cursor`; page within it only if it filled the page.
        records = list(batch)
        page = 2
        while len(batch) == page_size:
            batch = fetch(cursor, cursor, page)
            records.extend(batch)
            page += 1
        yield records, cursor + 1
        cursor += 1


def number_within_tx(records: list[dict]) -> list[dict]:
    """Add txSeq: 0, 1, 2... per transaction hash, in the order the API returned them."""
    seen: dict[str, int] = {}
    out = []
    for r in records:
        seq = seen.get(r["hash"], 0)
        seen[r["hash"]] = seq + 1
        out.append({**r, "txSeq": seq})
    return out


def slim_transfer(record: dict) -> dict:
    return {k: record.get(k) for k in TRANSFER_FIELDS}


def parse_transfer(record: dict) -> dict:
    """Typed transfer row: integer block/log index, UTC timestamp, human token amount."""
    decimals = int(record["tokenDecimal"])
    return {
        "block_number": int(record["blockNumber"]),
        "timestamp": datetime.fromtimestamp(int(record["timeStamp"]), tz=timezone.utc),
        "tx_hash": record["hash"],
        "tx_seq": int(record["txSeq"]),
        "from_address": record["from"].lower(),
        "to_address": record["to"].lower(),
        "value_raw": record["value"],
        "amount": int(record["value"]) / 10 ** decimals,
        "token_symbol": record["tokenSymbol"],
        "function_name": record.get("functionName") or "",
    }


def trim_to_checkpoint(data_path: Path, rows: int) -> int:
    """Drop lines written after the last saved checkpoint (a crash between append and state save).

    Returns the number of lines removed.
    """
    if not data_path.exists():
        return 0
    with gzip.open(data_path, "rt") as f:
        lines = f.readlines()
    extra = len(lines) - rows
    if extra > 0:
        with gzip.open(data_path, "wt") as f:
            f.writelines(lines[:rows])
    return max(extra, 0)


def last_stored_block(data_path: Path) -> int | None:
    last = None
    if data_path.exists():
        with gzip.open(data_path, "rt") as f:
            for line in f:
                last = line
    return int(json.loads(last)["blockNumber"]) if last else None


def pull_token_transfers(
    client: EtherscanClient, symbol: str, contract: str, start_block: int, end_block: int, raw_dir: Path,
    recheck: bool = False,
) -> dict:
    """Pull every transfer of `contract` in [start_block, end_block] to gzipped JSON lines, resumably.

    recheck=True resumes from the block after the last one actually stored, to
    fill a range a previous run wrongly treated as finished.
    """
    token_dir = raw_dir / symbol
    token_dir.mkdir(parents=True, exist_ok=True)
    state_path = token_dir / "state.json"
    data_path = token_dir / "transfers.jsonl.gz"

    if state_path.exists():
        state = json.loads(state_path.read_text())
        trim_to_checkpoint(data_path, state["rows"])
        if recheck:
            last = last_stored_block(data_path)
            state["next_block"] = state["start_block"] if last is None else last + 1
    else:
        state = {"symbol": symbol, "contract": contract, "start_block": start_block,
                 "end_block": end_block, "next_block": start_block, "rows": 0, "calls": 0}
        data_path.unlink(missing_ok=True)

    calls = 0
    batches = 0

    def fetch(sb: int, eb: int, page: int) -> list[dict]:
        nonlocal calls
        calls += 1
        return client.token_transfers_page(contract, sb, eb, page)

    for records, next_block in iter_complete_blocks(fetch, state["next_block"], state["end_block"]):
        if records:
            with gzip.open(data_path, "at") as f:
                for r in number_within_tx(records):
                    f.write(json.dumps(slim_transfer(r)) + "\n")
        state["rows"] += len(records)
        state["next_block"] = next_block
        state["calls"] += calls
        calls = 0
        state_path.write_text(json.dumps(state, indent=2))
        batches += 1
        if batches % 100 == 0:
            done = (next_block - state["start_block"]) / max(1, state["end_block"] - state["start_block"])
            print(f"  etherscan {symbol}: {state['rows']:,} transfers, {done:.0%} of block range")
    return state
