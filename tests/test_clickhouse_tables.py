"""Integration tests against the loaded ClickHouse database (run scripts/load_clickhouse.py first)."""
import pytest

from chainsignal.db.clickhouse import get_client

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def q():
    client = get_client()
    return lambda sql: client.query(sql).result_rows


def test_clean_view_excludes_every_glitch_and_stale_coin(q):
    leaked = q("""SELECT count() FROM market_daily_clean
                  WHERE coin_id IN (SELECT coin_id FROM coin_quality WHERE status != 'ok')""")[0][0]
    assert leaked == 0


def test_no_clean_coin_has_repeated_extreme_moves(q):
    assert q("""SELECT max(n) FROM (SELECT coin_id, countIf(extreme_move) n
                FROM market_daily_clean GROUP BY coin_id)""")[0][0] <= 1


def test_first_row_per_coin_is_never_flagged(q):
    # lagInFrame on the first row has no previous price; it must not count as an extreme move
    assert q("SELECT countIf(extreme_move) FROM market_daily_flagged WHERE prev_price IS NULL")[0][0] == 0


def test_token_daily_totals_match_raw(q):
    raw = dict(q("SELECT token, count() FROM token_transfers_raw GROUP BY token"))
    rolled = dict(q("SELECT token, sum(transfers) FROM token_daily GROUP BY token"))
    assert raw == rolled


def test_only_window_edge_days_are_partial(q):
    assert q("SELECT max(c) FROM (SELECT token, countIf(is_partial_day) c FROM token_daily GROUP BY token)")[0][0] <= 2


def test_isolated_volume_collapses_are_flagged_and_excluded(q):
    # Bitcoin 2026-03-12: $0.38B between days of $35-46B, a feed error found in Phase 4
    assert q("SELECT volume_glitch FROM market_daily_flagged WHERE coin_id='bitcoin' AND date='2026-03-12'")[0][0]
    assert q("SELECT countIf(volume_glitch) FROM market_daily_clean")[0][0] == 0
