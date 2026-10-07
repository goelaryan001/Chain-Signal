"""Detector tests on synthetic data where the right answer is known."""
import numpy as np
import pandas as pd

from chainsignal.models.compare import anomaly_type, jaccard
from chainsignal.models.detectors import (flag_top, isolation_forest_score, lof_score,
                                          robust_rolling_z, zscore_score)

rng = np.random.default_rng(0)


def correlated_cloud(n=2000):
    x = rng.normal(size=n)
    return np.column_stack([x, x + rng.normal(scale=0.2, size=n)])


def test_zscore_catches_a_univariate_spike():
    X = rng.normal(size=(1000, 2))
    X[10] = [9.0, 0.0]
    assert flag_top(zscore_score(X), 0.002)[10]


def test_isolation_forest_catches_what_zscore_misses():
    """The design-doc claim: each feature individually normal, the combination unusual."""
    X = correlated_cloud()
    X[0] = [2.0, -2.0]   # |z| = 2 on both axes, but far off the x ~ y line
    assert not flag_top(zscore_score(X), 0.01)[0]
    assert flag_top(isolation_forest_score(X), 0.01)[0]
    assert flag_top(lof_score(X), 0.01)[0]


def test_lof_catches_a_local_outlier_next_to_a_dense_cluster():
    # In z-score units, like the real inputs. lof_score rounds to 0.1 sigma, so structure
    # must be coarser than that: a 0.05-wide cluster would be merged into a few points.
    dense = rng.normal(scale=0.5, size=(500, 2))
    sparse = rng.normal(loc=60, scale=15, size=(500, 2))
    X = np.vstack([dense, sparse, [[5.0, 5.0]]])   # close in absolute terms, far for the dense cluster
    assert lof_score(X)[-1] > np.quantile(lof_score(X), 0.99)


def test_robust_z_is_not_dragged_by_one_past_extreme():
    dates = pd.Series(pd.date_range("2026-01-01", periods=40))
    vals = pd.Series(np.r_[np.tile([1.0, 1.1, 0.9, 1.05, 0.95], 6), [500.0], np.tile([1.0, 1.1, 0.9], 3)])
    z = robust_rolling_z(vals, dates, min_periods=20)
    assert abs(z.iloc[-1]) < 3            # the 500 earlier in the window barely moves median/MAD
    assert z.iloc[30] > 100                # and the 500 itself is extreme


def test_robust_z_excludes_current_day_and_needs_history():
    dates = pd.Series(pd.date_range("2026-01-01", periods=25))
    z = robust_rolling_z(pd.Series(np.arange(25, dtype=float)), dates, min_periods=20)
    assert z.iloc[:20].isna().all() and z.iloc[20:].notna().all()


def test_flag_top_keeps_exact_budget_with_ties():
    assert flag_top(np.array([1, 5, 5, 5, 2.0]), 0.4).sum() == 2


def test_anomaly_types_and_jaccard():
    t = anomaly_type(pd.Series([5, 0, 5, 0, 0.0]), pd.Series([0, 5, 5, 0, 0.0]), pd.Series([0, 0, 0, 4, 1.0]))
    assert list(t) == ["price_shock", "volume_only", "price_and_vol", "other_feature", "multivariate"]
    assert jaccard(np.array([1, 1, 0], bool), np.array([1, 0, 1], bool)) == 1 / 3


def test_robust_z_floor_stops_near_constant_series_from_exploding():
    dates = pd.Series(pd.date_range("2026-01-01", periods=31))
    stable = pd.Series(np.r_[1.0 + rng.normal(scale=1e-6, size=30), [1.001]])  # a stablecoin's 0.1% wiggle
    assert abs(robust_rolling_z(stable, dates).iloc[-1]) > 100              # unfloored: "extreme"
    assert abs(robust_rolling_z(stable, dates, min_scale=0.005).iloc[-1]) < 1  # floored: not extreme


def test_lof_is_not_fooled_by_a_pile_of_duplicate_points():
    X = np.vstack([np.zeros((300, 2)), rng.normal(size=(1000, 2)), [[0.01, 0.0]]])  # 300 identical "flat days"
    scores = lof_score(X)
    assert scores[-1] < np.quantile(scores, 0.9)   # a point beside the pile is ordinary, not an outlier
