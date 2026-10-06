"""Tests for the ClickHouse loader's pure logic (no database)."""
from datetime import datetime, timezone

from chainsignal.db.load import TRANSFER_COLUMNS, split_statements, transfer_row


def test_split_statements_drops_comments_and_splits_on_line_end_semicolons():
    sql = """-- header comment
CREATE TABLE a (x UInt8) ENGINE = Memory;

-- another; with a semicolon inside a comment
CREATE TABLE b (y UInt8) ENGINE = Memory;
"""
    assert split_statements(sql) == [
        "CREATE TABLE a (x UInt8) ENGINE = Memory",
        "CREATE TABLE b (y UInt8) ENGINE = Memory",
    ]


def test_transfer_row_matches_column_order_and_keeps_exact_value():
    huge = "11314727688952716072621876000000"  # larger than UInt64; must stay exact
    row = transfer_row("PEPE", {
        "blockNumber": "25795247", "timeStamp": "1767225600", "hash": "0xabc", "txSeq": 2,
        "transactionIndex": "33", "from": "0xAA", "to": "0xBB", "value": huge, "tokenDecimal": "18",
        "tokenSymbol": "PEPE", "gasUsed": "55468", "gasPrice": "2451326921",
        "methodId": "0xa9059cbb", "functionName": "transfer(address to, uint256 value)",
    })
    named = dict(zip(TRANSFER_COLUMNS, row))
    assert len(row) == len(TRANSFER_COLUMNS)
    assert named["value_raw"] == int(huge)
    assert named["block_time"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert named["from_address"] == "0xaa" and named["tx_seq"] == 2 and named["decimals"] == 18
