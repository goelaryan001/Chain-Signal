"""The three anomaly-detection candidates, plus the robust z-score features they share.

Every score is oriented the same way: higher = more anomalous. Scores live on
different scales, so methods are compared at an equal alert budget (flag_top),
not by raw score values.
"""
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

MAD_TO_STD = 1.4826   # MAD * 1.4826 estimates the standard deviation for normal data
EPS = 1e-9


def robust_rolling_z(values: pd.Series, dates: pd.Series, window: str = "30D",
                     min_periods: int = 20, min_scale: float = EPS) -> pd.Series:
    """Trailing robust z-score: (x - median) / max(1.4826 * MAD, min_scale) over the previous `window`.

    closed="left" excludes the current day, as with the Spark features. Median and
    MAD barely move when one extreme value enters the window, unlike mean and std,
    which is what produced a volume z-score of 201 in Phase 3.

    min_scale is a floor on the denominator. Median/MAD does not protect against a
    near-constant series (a stablecoin): any tiny wiggle divided by a near-zero MAD
    scored z = 74,309 in the first Phase 4 run. The floor says "a move this small is
    never extreme, whatever the history".
    """
    s = pd.Series(values.to_numpy(dtype=float), index=pd.DatetimeIndex(dates))
    roll = s.rolling(window, closed="left", min_periods=min_periods)
    med = roll.median()
    mad = roll.apply(lambda w: np.nanmedian(np.abs(w - np.nanmedian(w))), raw=True)
    z = (s - med) / np.maximum(MAD_TO_STD * mad, min_scale)
    return pd.Series(z.to_numpy(), index=values.index)


def zscore_score(X: np.ndarray) -> np.ndarray:
    """Candidate 1: largest absolute z across features. One feature at a time, fully interpretable."""
    return np.nanmax(np.abs(X), axis=1)


def isolation_forest_score(X: np.ndarray, seed: int = 7, n_estimators: int = 300) -> np.ndarray:
    """Candidate 2: points isolated by random splits in fewer steps score higher."""
    model = IsolationForest(n_estimators=n_estimators, max_samples=min(len(X), 4096), random_state=seed)
    model.fit(X)
    return -model.score_samples(X)


def lof_score(X: np.ndarray, n_neighbors: int = 20, decimals: int = 1) -> np.ndarray:
    """Candidate 3: local density relative to the k nearest neighbours' density.

    LOF assumes no duplicate points. With many identical rows (a tokenized treasury
    bill's flat days all land on 0,0,0,0) the k-distance is 0, local density becomes
    infinite and points next to the pile get absurd scores: in the first Phase 4 run
    LOF's top "anomalies" were perfectly ordinary days. So identical points (after
    rounding) are collapsed, each unique point is scored once, and scores are mapped
    back to every row. Rounding to 0.1 sigma (decimals=1) was chosen by measurement:
    the share of LOF flags on ordinary days (no |z| > 1) was 15% at 0.001 sigma, 10% at
    0.01 and 0.1% at 0.1, while LOF stayed equally distinct from the z-score baseline.
    Finer differences between z-scores carry no information. Trade-off: LOF becomes
    blind to structure finer than the rounding step, so inputs must be in z-score
    units (or the step changed to suit the data's scale).
    """
    unique, inverse = np.unique(np.round(X, decimals), axis=0, return_inverse=True)
    model = LocalOutlierFactor(n_neighbors=min(n_neighbors, len(unique) - 1))
    model.fit(unique)
    return -model.negative_outlier_factor_[inverse.ravel()]


def flag_top(scores: np.ndarray, rate: float) -> np.ndarray:
    """Flag the top `rate` share of rows: an equal alert budget across methods."""
    k = max(1, int(round(len(scores) * rate)))
    cut = np.partition(scores, -k)[-k]
    flags = scores >= cut
    if flags.sum() > k:  # ties at the cut: keep exactly k, deterministic by position
        idx = np.argsort(-scores, kind="stable")[:k]
        flags = np.zeros(len(scores), dtype=bool)
        flags[idx] = True
    return flags
