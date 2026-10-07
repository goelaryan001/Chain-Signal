"""Tests for the pure ingestion logic (no network)."""
from chainsignal.ingest.coingecko import MS_PER_DAY, parse_market_chart
import gzip

from chainsignal.ingest.etherscan import iter_complete_blocks, number_within_tx, parse_transfer, trim_to_checkpoint


# ---------- CoinGecko ----------

def test_parse_market_chart_joins_series_and_drops_partial_day():
    day0 = 1_767_225_600_000  # 2026-01-01 00:00 UTC
    payload = {
        "prices": [[day0, 10.0], [day0 + MS_PER_DAY, 11.0], [day0 + MS_PER_DAY + 3_600_000, 11.5]],
        "market_caps": [[day0, 1000.0], [day0 + MS_PER_DAY, 1100.0], [day0 + MS_PER_DAY + 3_600_000, 1150.0]],
        "total_volumes": [[day0, 50.0], [day0 + MS_PER_DAY, 60.0], [day0 + MS_PER_DAY + 3_600_000, 65.0]],
    }
    rows = parse_market_chart("bitcoin", payload)
    # a midnight stamp on day D is day D-1's close
    assert [r["date"] for r in rows] == ["2025-12-31", "2026-01-01"]
    assert rows[1] == {"coin_id": "bitcoin", "date": "2026-01-01", "price": 11.0,
                       "market_cap": 1100.0, "volume": 60.0}


def test_parse_market_chart_tolerates_missing_series():
    day0 = 1_767_225_600_000
    rows = parse_market_chart("x", {"prices": [[day0, 1.0]], "market_caps": None})
    assert rows == [{"coin_id": "x", "date": "2025-12-31", "price": 1.0, "market_cap": None, "volume": None}]


# ---------- Etherscan pagination ----------

def make_chain(blocks: dict[int, int]) -> list[dict]:
    """Fake transfer log: {block_number: n_transfers}, in block/log order."""
    return [{"blockNumber": str(b), "hash": f"0x{b:x}-{i}"}
            for b in sorted(blocks) for i in range(blocks[b])]


def make_fetch(chain: list[dict], page_size: int):
    calls = []

    def fetch(start: int, end: int, page: int) -> list[dict]:
        calls.append((start, end, page))
        hits = [r for r in chain if start <= int(r["blockNumber"]) <= end]
        return hits[(page - 1) * page_size: page * page_size]

    return fetch, calls


def collect(chain, start, end, page_size):
    fetch, calls = make_fetch(chain, page_size)
    out, cursors = [], []
    for records, next_block in iter_complete_blocks(fetch, start, end, page_size):
        out.extend(records)
        cursors.append(next_block)
    return out, cursors, calls


def keys(records):
    return [(r["blockNumber"], r["hash"]) for r in records]


def test_pagination_returns_every_record_exactly_once():
    chain = make_chain({100: 3, 101: 2, 105: 4, 106: 1, 110: 3})
    out, _, _ = collect(chain, 100, 120, page_size=4)
    assert keys(out) == keys(chain)


def test_pagination_never_splits_a_block_across_batches():
    chain = make_chain({100: 3, 101: 2, 105: 4})
    fetch, _ = make_fetch(chain, 4)
    for records, next_block in iter_complete_blocks(fetch, 100, 120, 4):
        assert all(int(r["blockNumber"]) < next_block for r in records)


def test_pagination_handles_block_larger_than_page():
    chain = make_chain({100: 1, 101: 9, 102: 2})  # block 101 alone exceeds page_size
    out, _, calls = collect(chain, 100, 120, page_size=4)
    assert keys(out) == keys(chain)
    assert (101, 101, 3) in calls  # paged within the single block


def test_pagination_respects_end_block_and_empty_range():
    chain = make_chain({100: 2, 200: 2})
    out, cursors, _ = collect(chain, 100, 150, page_size=4)
    assert keys(out) == keys(make_chain({100: 2}))
    assert cursors[-1] == 151
    out, _, _ = collect(chain, 300, 400, page_size=4)
    assert out == []


def test_pagination_resume_from_cursor_matches_single_run():
    chain = make_chain({100: 3, 101: 2, 105: 4, 106: 1, 110: 3})
    fetch, _ = make_fetch(chain, 4)
    gen = iter_complete_blocks(fetch, 100, 120, 4)
    first, cursor = next(gen)  # "crash" after the first batch
    rest, _, _ = collect(chain, cursor, 120, page_size=4)
    assert keys(first + rest) == keys(chain)


# ---------- Etherscan parsing ----------

def test_parse_transfer_scales_by_decimals_and_normalises():
    row = parse_transfer({
        "blockNumber": "21000000", "timeStamp": "1767225600", "hash": "0xabc", "txSeq": 7,
        "from": "0xAAA", "to": "0xBBB", "value": "1500000000000000000", "tokenDecimal": "18",
        "tokenSymbol": "LINK", "functionName": "transfer(address to, uint256 value)",
    })
    assert row["amount"] == 1.5
    assert row["value_raw"] == "1500000000000000000"
    assert row["from_address"] == "0xaaa" and row["to_address"] == "0xbbb"
    assert row["timestamp"].isoformat() == "2026-01-01T00:00:00+00:00"
    assert row["block_number"] == 21_000_000 and row["tx_seq"] == 7


def test_number_within_tx_gives_unique_keys_for_multi_transfer_txs():
    recs = [{"hash": "0xa"}, {"hash": "0xa"}, {"hash": "0xb"}, {"hash": "0xa"}]
    assert [r["txSeq"] for r in number_within_tx(recs)] == [0, 1, 0, 2]


def test_trim_to_checkpoint_removes_rows_written_after_last_save(tmp_path):
    path = tmp_path / "t.jsonl.gz"
    with gzip.open(path, "wt") as f:
        f.writelines(f"{i}\n" for i in range(10))
    assert trim_to_checkpoint(path, 7) == 3
    with gzip.open(path, "rt") as f:
        assert f.read().split() == [str(i) for i in range(7)]
    assert trim_to_checkpoint(path, 7) == 0


def flaky_fetch(chain, page_size, at_start, mangle):
    """Wrap the fake fetch so the first answer starting at `at_start` is mangled."""
    real_fetch, _ = make_fetch(chain, page_size)
    state = {"done": False}

    def fetch(start, end, page):
        batch = real_fetch(start, end, page)
        if start == at_start and not state["done"]:
            state["done"] = True
            return mangle(batch)
        return batch

    return fetch


def test_pagination_survives_a_spurious_empty_answer():
    """Under load the API answered 'nothing here' mid-range; our first pull stopped 2 days early."""
    chain = make_chain({100: 3, 101: 2, 105: 4, 106: 1, 110: 3})
    fetch = flaky_fetch(chain, 4, at_start=105, mangle=lambda b: [])
    out = [r for records, _ in iter_complete_blocks(fetch, 100, 120, 4) for r in records]
    assert keys(out) == keys(chain)


def test_pagination_survives_a_truncated_answer_cut_mid_block():
    chain = make_chain({100: 3, 101: 2, 105: 1})
    fetch = flaky_fetch(chain, 6, at_start=100, mangle=lambda b: b[:4])  # cuts block 101 in half
    out = [r for records, _ in iter_complete_blocks(fetch, 100, 120, 6) for r in records]
    assert keys(out) == keys(chain)


def test_label_batch_marks_only_returned_addresses_as_contracts():
    from chainsignal.ingest.labels import label_batch
    rows = label_batch(["0xAAA", "0xbbb"], [{"contractAddress": "0xaaa", "contractCreator": "0xC",
                                             "contractFactory": "", "blockNumber": "42"}])
    assert [(r["address"], r["is_contract"], r["created_block"]) for r in rows] == [
        ("0xaaa", True, 42), ("0xbbb", False, 0)]
