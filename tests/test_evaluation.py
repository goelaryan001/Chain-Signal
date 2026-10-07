"""Tests for the injection and evaluation logic."""
import numpy as np
import pandas as pd
import pytest

from chainsignal.evaluation.injection import (choose_sites, inject, precision_at_k, recall_table,
                                              trailing_scale)


def history(n_coins=6, n_days=60, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for c in range(n_coins):
        for d in range(n_days):
            rows.append((f"c{c}", pd.Timestamp("2026-01-01") + pd.Timedelta(days=d),
                         float(rng.normal(scale=0.02)), 1000.0 + rng.normal(scale=50), 1e6, 0.0))
    df = pd.DataFrame(rows, columns=["coin_id", "date", "log_return", "volume", "market_cap", "residual_return"])
    return df.sort_values(["coin_id", "date"]).reset_index(drop=True)


def test_sites_one_per_coin_after_min_history_with_types_balanced():
    df = history()
    sites = choose_sites(df, np.random.default_rng(1))
    assert sites.coin_id.is_unique and len(sites) == 6
    positions = [df.index[df.coin_id == r.coin_id].get_loc(r.row) for r in sites.itertuples()]
    assert min(positions) >= 40
    assert sites.type.value_counts().tolist() == [2, 2, 2]


def test_injection_changes_only_what_each_type_should():
    df = history()
    sites = pd.DataFrame([("c0", 45, "price_spike"), ("c1", 105, "volume_spike"), ("c2", 165, "combo")],
                         columns=["coin_id", "row", "type"])
    out = inject(df, sites, strength=6, rng=np.random.default_rng(0))
    sr = trailing_scale(df.log_return.to_numpy(), 45, floor=0.005)
    assert abs(out.loc[45, "log_return"] - df.loc[45, "log_return"]) == pytest.approx(6 * sr)
    assert out.loc[45, "volume"] == df.loc[45, "volume"]                    # price spike: volume untouched
    assert out.loc[105, "log_return"] == df.loc[105, "log_return"]          # volume spike: price untouched
    assert out.loc[105, "volume"] > df.loc[105, "volume"]
    assert out.loc[165, "volume"] < df.loc[165, "volume"]                   # combo: move on falling volume
    assert out.loc[165, "log_return"] != df.loc[165, "log_return"]
    unchanged = out.drop(index=[45, 105, 165]).drop(columns="market_cap")
    assert unchanged.equals(df.drop(index=[45, 105, 165]).drop(columns="market_cap"))


def test_recall_and_precision_at_k():
    scored = pd.DataFrame({"a_flag": [True, False, True, False], "a_score": [9, 1, 8, 2.0],
                           "votes": [2, 0, 1, 0]})
    sites = pd.DataFrame({"row": [0, 1], "type": ["price_spike", "volume_spike"]})
    table = recall_table(scored, sites, ["a"])
    assert table.loc["price_spike", "a"] == 1.0 and table.loc["volume_spike", "a"] == 0.0
    assert table.loc["all", "consensus_2plus"] == 0.5
    assert precision_at_k(scored, sites, ["a"]) == {"a": 0.5}   # top-2 scores are rows 0 and 2


def test_event_coin_rows_nulls_returns_across_gaps_and_uses_beta_one_residual():
    from chainsignal.evaluation.backtest import event_coin_rows
    day = 86_400_000
    t0 = 1_767_312_000_000  # stamp 2026-01-02 00:00 -> row 2026-01-01
    payload = {"prices": [[t0, 1.0], [t0 + day, 2.0], [t0 + 3 * day, 4.0]],
               "market_caps": [[t0, 10.0], [t0 + day, 20.0], [t0 + 3 * day, 40.0]],
               "total_volumes": [[t0, 5.0], [t0 + day, 5.0], [t0 + 3 * day, 5.0]]}
    mr = pd.Series([0.1, 0.1, 0.1], index=pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-04"]))
    rows = event_coin_rows("x", payload, mr)
    assert rows.log_return.isna().tolist() == [True, False, True]          # day 3 follows a missing day
    assert rows.residual_return.iloc[1] == pytest.approx(np.log(2) - 0.1)


def test_window_hits_reports_zero_rows_when_there_is_no_data():
    from datetime import date
    from chainsignal.evaluation.backtest import window_hits
    scored = pd.DataFrame({"coin_id": ["x"], "date": pd.to_datetime(["2026-01-01"]), "a_flag": [True],
                           "rz_return": [1.0], "rz_volume": [1.0]})
    assert window_hits(scored, "x", date(2026, 6, 1), ["a"])["rows"] == 0   # "no data", not "missed"
    assert window_hits(scored, "x", date(2026, 1, 2), ["a"])["a"] is True
