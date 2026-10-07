"""Shared scoring pipeline: robust features -> three detectors -> equal-budget flags.

Used by scripts/run_models.py (Phase 4) and the evaluation (Phase 5), so injected
or appended data is scored exactly the way the real data is.
"""
import numpy as np
import pandas as pd

from chainsignal.models.detectors import (flag_top, isolation_forest_score, lof_score,
                                          robust_rolling_z, zscore_score)

METHODS = ["zscore", "iforest", "lof"]
R_FEATURES = ["rz_return", "rz_volume", "rz_turnover", "rz_residual"]
# robust z source column and its scale floor: a 3-sigma flag then needs at least a ~1.5%
# price move or a ~30% volume / turnover change
ROBUST_SOURCES = {"rz_return": ("log_return", 0.005), "rz_volume": ("log_volume", 0.10),
                  "rz_turnover": ("log_turnover", 0.10), "rz_residual": ("residual_return", 0.005)}


def robust_market_features(df: pd.DataFrame) -> pd.DataFrame:
    """Input: coin_id, date (datetime), volume, market_cap, log_return, residual_return."""
    df = df.sort_values(["coin_id", "date"]).reset_index(drop=True)
    df["log_volume"] = np.log1p(df["volume"])
    mcap = df["market_cap"].where(df["market_cap"] > 0)
    df["log_turnover"] = np.log(df["volume"].where(df["volume"] > 0) / mcap)
    for out, (src, floor) in ROBUST_SOURCES.items():
        parts = [robust_rolling_z(g[src], g["date"], min_scale=floor) for _, g in df.groupby("coin_id")]
        df[out] = pd.concat(parts)
    return df


def model_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Rows the detectors can score. Turnover is missing where market cap is unknown:
    impute 0 (= "typical") so the row still counts on its other features."""
    df = df.dropna(subset=["rz_return", "rz_volume", "rz_residual"]).reset_index(drop=True)
    df["rz_turnover"] = df["rz_turnover"].fillna(0.0)
    return df


def score(df: pd.DataFrame, budget: float) -> pd.DataFrame:
    """Add <method>_score and <method>_flag for each method, flags at an equal budget."""
    X = df[R_FEATURES].to_numpy(dtype=float)
    df["zscore_score"] = zscore_score(X)
    df["iforest_score"] = isolation_forest_score(X)
    df["lof_score"] = lof_score(X)
    for m in METHODS:
        df[f"{m}_flag"] = flag_top(df[f"{m}_score"].to_numpy(), budget)
    df["votes"] = df[[f"{m}_flag" for m in METHODS]].sum(axis=1)
    return df
