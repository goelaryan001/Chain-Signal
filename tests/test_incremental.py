"""Phase 8: incremental-load logic (unit) and idempotent partition replacement (integration)."""
from datetime import date, datetime, timedelta, timezone

import pytest

from chainsignal.db.incremental import partition_id
from chainsignal.db.load import dedupe_latest, merge_later_days
from chainsignal.ingest.coingecko import parse_markets_snapshot, snapshot_day

UTC = timezone.utc


def test_snapshot_just_after_midnight_stands_for_yesterday():
    assert snapshot_day(datetime(2026, 10, 7, 0, 20, tzinfo=UTC)) == date(2026, 10, 6)


def test_snapshot_later_in_the_day_is_refused():
    with pytest.raises(ValueError, match="mid-day price"):
        snapshot_day(datetime(2026, 10, 7, 14, 0, tzinfo=UTC))


def test_snapshot_needs_a_timezone():
    with pytest.raises(ValueError, match="timezone"):
        snapshot_day(datetime(2026, 10, 7, 0, 20))


def test_snapshot_day_uses_utc_not_local_time():
    # 17:20 in UTC-7 is 00:20 UTC the next day: still inside the window
    pdt = timezone(timedelta(hours=-7))
    assert snapshot_day(datetime(2026, 10, 6, 17, 20, tzinfo=pdt)) == date(2026, 10, 6)


def test_parse_snapshot_keeps_tracked_coins_with_a_price():
    payload = [{"id": "a", "current_price": 1.0, "market_cap": 10.0, "total_volume": 5.0},
               {"id": "b", "current_price": None, "market_cap": 1.0, "total_volume": 1.0},   # no price: skipped
               {"id": "z", "current_price": 2.0, "market_cap": 1.0, "total_volume": 1.0}]    # untracked
    rows = parse_markets_snapshot(payload, date(2026, 10, 6), tracked={"a", "b"})
    assert rows == [{"coin_id": "a", "date": "2026-10-06", "price": 1.0, "market_cap": 10.0, "volume": 5.0}]


def test_reconciled_value_replaces_snapshot_for_same_coin_and_day():
    d = date(2026, 10, 6)
    snaps = [["a", d, 1.0, 10.0, 5.0, "snapshot"], ["b", d, 2.0, 20.0, 6.0, "snapshot"]]
    recon = [["a", d, 1.1, 11.0, 0.5, "reconciled"]]
    merged = {r[0]: r for r in merge_later_days(snaps, recon)}
    assert merged["a"][5] == "reconciled" and merged["a"][4] == 0.5
    assert merged["b"][5] == "snapshot"   # untouched coins keep their snapshot row


def test_dedupe_latest_keeps_one_row_per_transfer_and_prefers_the_newer_fetch():
    old = ["LINK", 1, None, "0xa", 0, "old"]
    new = ["LINK", 1, None, "0xa", 0, "new"]
    other = ["LINK", 1, None, "0xa", 1, "x"]          # same tx, different transfer: kept
    rows = dedupe_latest([old, other, new])
    assert len(rows) == 2 and ["LINK", 1, None, "0xa", 0, "new"] in rows


def test_partition_id_matches_clickhouse_daily_partition_ids():
    assert partition_id(date(2026, 10, 6)) == "20261006"


@pytest.mark.integration
def test_replace_partitions_is_idempotent_and_leaves_other_days_alone():
    from chainsignal.db.clickhouse import get_client
    from chainsignal.db.incremental import replace_partitions, table_fingerprint
    c = get_client()
    c.command("DROP TABLE IF EXISTS t_incremental_test")
    c.command("CREATE TABLE t_incremental_test (d Date, k String, v Float64) ENGINE = MergeTree PARTITION BY d ORDER BY k")
    try:
        c.insert("t_incremental_test", [[date(2026, 10, 5), "x", 1.0], [date(2026, 10, 6), "y", 2.0]], column_names=["d", "k", "v"])
        rows = [[date(2026, 10, 6), "y", 3.0], [date(2026, 10, 6), "z", 4.0]]
        replace_partitions(c, "t_incremental_test", rows, ["d", "k", "v"], [date(2026, 10, 6)])
        first = table_fingerprint(c, "t_incremental_test", ["d", "k", "v"])
        replace_partitions(c, "t_incremental_test", rows, ["d", "k", "v"], [date(2026, 10, 6)])   # re-run
        assert table_fingerprint(c, "t_incremental_test", ["d", "k", "v"]) == first
        got = sorted(c.query("SELECT toString(d), k, v FROM t_incremental_test").result_rows)
        assert got == [("2026-10-05", "x", 1.0), ("2026-10-06", "y", 3.0), ("2026-10-06", "z", 4.0)]
    finally:
        c.command("DROP TABLE IF EXISTS t_incremental_test")
