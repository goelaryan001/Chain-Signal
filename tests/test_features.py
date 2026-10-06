"""Feature logic tests on tiny hand-made Spark DataFrames (needs Java, no services)."""
from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from chainsignal.features.market import add_market_features, market_return
from chainsignal.features.onchain import add_onchain_features, daily_activity, round_trip_legs
from chainsignal.spark import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("chainsignal-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s


# ---------- market ----------

def market_df(spark, coin_returns: dict[str, list[float]], volumes=None):
    rows = []
    for coin, rets in coin_returns.items():
        for i, r in enumerate(rets):
            vol = (volumes or {}).get(coin, [100.0] * len(rets))[i]
            rows.append((coin, date(2026, 1, 1) + timedelta(days=i), 1.0, 1000.0, vol,
                         None if i == 0 else r, False, None if i == 0 else 0))
    return spark.createDataFrame(rows, "coin_id string, date date, price double, market_cap double, volume double, "
                                       "log_return double, extreme_move boolean, missing_days_before int")


def test_market_return_is_cross_sectional_median(spark):
    df = market_df(spark, {"a": [0, 0.01], "b": [0, 0.02], "c": [0, 0.30]})
    assert market_return(df).where(F.col("date") == date(2026, 1, 2)).first()["market_return"] == pytest.approx(0.02)


def test_return_z_uses_only_past_days_and_flags_a_spike(spark):
    calm = [0.01 * ((-1) ** i) for i in range(25)]
    df = market_df(spark, {"a": calm + [0.5], "b": [0.0] * 26})
    out = add_market_features(df, window_days=30, min_history=20).where(F.col("coin_id") == "a")
    last = out.orderBy(F.desc("date")).first()
    # the spike is judged against the calm past (std ~0.01), so it scores far above 10
    assert last["return_z"] > 10


def test_no_features_before_min_history(spark):
    df = market_df(spark, {"a": [0.01] * 15})
    out = add_market_features(df, window_days=30, min_history=20)
    assert out.where(F.col("return_z").isNotNull()).count() == 0


def test_return_after_a_gap_is_not_used(spark):
    df = market_df(spark, {"a": [0.0, 0.01, 0.02]}).withColumn(
        "missing_days_before", F.when(F.col("date") == date(2026, 1, 3), 4).otherwise(F.col("missing_days_before")))
    out = add_market_features(df, min_history=1).orderBy("date").collect()
    assert out[2]["log_return"] is None


def test_volume_price_gap_is_high_for_volume_spike_without_price_move(spark):
    rets = [0.01 * ((-1) ** i) for i in range(26)]
    vols = [100.0 + (i % 3) for i in range(25)] + [10_000.0]
    out = add_market_features(market_df(spark, {"a": rets, "b": [0.0] * 26}, {"a": vols}), min_history=20)
    last = out.where(F.col("coin_id") == "a").orderBy(F.desc("date")).first()
    assert last["volume_z"] > 5 and last["volume_price_gap"] > 5


# ---------- on-chain ----------

T0 = datetime(2026, 8, 1, 12, 0, 0)


def transfers_df(spark, rows):
    """rows: (from, to, hours_after_T0, amount)"""
    return spark.createDataFrame(
        [("TOK", f"0xtx{i}", 0, T0 + timedelta(hours=h), f, t, float(a), "transfer")
         for i, (f, t, h, a) in enumerate(rows)],
        "token string, tx_hash string, tx_seq int, block_time timestamp, from_address string, "
        "to_address string, amount double, function_name string")


def labels_df(spark, wallets=(), contracts=()):
    return spark.createDataFrame([(a, False) for a in wallets] + [(a, True) for a in contracts],
                                 "address string, is_contract boolean")


def test_round_trip_counts_reply_within_window_between_wallets(spark):
    t = transfers_df(spark, [("a", "b", 0, 10), ("b", "a", 24, 10)])
    legs = round_trip_legs(t, labels_df(spark, wallets=["a", "b"]))
    assert [r["tx_hash"] for r in legs.collect()] == ["0xtx1"]  # only the reply is a leg (trailing)


def test_round_trip_ignores_replies_outside_window(spark):
    t = transfers_df(spark, [("a", "b", 0, 10), ("b", "a", 24 * 8, 10)])
    assert round_trip_legs(t, labels_df(spark, wallets=["a", "b"]), days=7).count() == 0


def test_round_trip_excludes_contracts_and_unlabelled(spark):
    t = transfers_df(spark, [("a", "pool", 0, 10), ("pool", "a", 1, 10), ("a", "x", 0, 1), ("x", "a", 1, 1)])
    # "pool" is a contract and "x" was never labelled: neither pair counts
    assert round_trip_legs(t, labels_df(spark, wallets=["a"], contracts=["pool"])).count() == 0


def test_daily_activity_counts(spark):
    t = transfers_df(spark, [("a", "b", 0, 10), ("b", "a", 1, 10), ("c", "d", 2, 1000)])
    row = daily_activity(t, labels_df(spark, wallets=["a", "b", "c", "d"])).first()
    assert (row["transfers"], row["unique_addresses"], row["rt_transfers"], row["new_addresses"]) == (3, 4, 1, 4)
    assert row["rt_share"] == pytest.approx(1 / 3)


def test_onchain_features_drop_partial_edge_days(spark):
    daily = spark.createDataFrame(
        [("TOK", date(2026, 8, 1) + timedelta(days=i), 100 + i, 1000.0, 50, 0.0, 1.0, 2.0, 0.5, 0)
         for i in range(5)],
        "token string, day date, transfers long, amount_total double, unique_addresses long, rt_share double, "
        "transfers_per_address double, top10_sender_share double, hub_share double, large_transfers long")
    days = [r["day"] for r in add_onchain_features(daily, min_history=1).orderBy("day").collect()]
    assert days == [date(2026, 8, 2), date(2026, 8, 3), date(2026, 8, 4)]
