"""Compare detectors without ground truth: overlap, consensus, and what kind of anomaly each catches."""
import numpy as np
import pandas as pd

EXTREME = 3.0   # a single robust z beyond this counts as "extreme on that feature"


def jaccard(a: np.ndarray, b: np.ndarray) -> float:
    union = np.logical_or(a, b).sum()
    return float(np.logical_and(a, b).sum() / union) if union else 1.0


def anomaly_type(ret_z: pd.Series, vol_z: pd.Series, other_max: pd.Series, extreme: float = EXTREME) -> pd.Series:
    """Describe a row by which features are extreme.

    price_shock   : price extreme, volume not
    volume_only   : volume extreme, price not   (a positive spike here is the wash-trading shape)
    price_and_vol : both
    other_feature : only turnover / residual extreme
    multivariate  : no single feature extreme; only the combination is unusual
    """
    p, v, o = ret_z.abs() > extreme, vol_z.abs() > extreme, other_max > extreme
    return pd.Series(np.select([p & v, p, v, o], ["price_and_vol", "price_shock", "volume_only", "other_feature"],
                               default="multivariate"), index=ret_z.index)


def agreement_table(flags: dict[str, np.ndarray]) -> pd.DataFrame:
    names = list(flags)
    return pd.DataFrame([[jaccard(flags[a], flags[b]) for b in names] for a in names], index=names, columns=names)
