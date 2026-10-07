"""Synthetic anomaly injection: plant known anomalies into copies of real coin histories.

Anomalies are injected into the RAW columns (log_return, volume, market_cap, residual
return) before robust features are recomputed, so they travel through the same
feature code as real data. Strength is in each coin's own units: s times its trailing
robust scale at the injection day, so a "6 sigma" spike means the same thing for a
stablecoin and a memecoin.

One injection per coin, at a random day with enough history, so injections never
share a trailing window.
"""
import numpy as np
import pandas as pd

from chainsignal.models.detectors import MAD_TO_STD

TYPES = ["price_spike", "volume_spike", "combo"]
RETURN_FLOOR, VOLUME_FLOOR = 0.005, 0.10   # same floors as the robust z features
MIN_POSITION = 40                           # rows of history before an injection site


def trailing_scale(values: np.ndarray, i: int, window: int = 30, floor: float = 0.0) -> float:
    w = values[max(0, i - window):i]
    w = w[~np.isnan(w)]
    if len(w) == 0:
        return floor
    return max(MAD_TO_STD * float(np.median(np.abs(w - np.median(w)))), floor)


def choose_sites(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """One (coin, row index, type) per coin with enough history; types assigned evenly."""
    sites = []
    for coin, idx in df.groupby("coin_id").indices.items():
        if len(idx) <= MIN_POSITION + 1:
            continue
        sites.append((coin, int(rng.choice(idx[MIN_POSITION:]))))
    rng.shuffle(sites)
    return pd.DataFrame([(c, i, TYPES[k % len(TYPES)]) for k, (c, i) in enumerate(sites)],
                        columns=["coin_id", "row", "type"])


def inject(df: pd.DataFrame, sites: pd.DataFrame, strength: float, rng: np.random.Generator) -> pd.DataFrame:
    """Return a copy of df (sorted by coin, date, index 0..n-1) with anomalies planted at `sites`.

    price_spike  : log return +/- s*sigma_r; the price level shift persists, so market cap
                   (and hence turnover) moves with it from that day on
    volume_spike : volume x exp(+s*sigma_v), price untouched  (the wash-trading shape)
    combo        : return +/- (s/2)*sigma_r AND volume x exp(-(s/2)*sigma_v): a move on
                   falling volume, each half strength so neither feature is extreme alone
    """
    out = df.copy()
    ret = out["log_return"].to_numpy(dtype=float)
    logv = np.log1p(out["volume"].to_numpy(dtype=float))
    coin_end = out.groupby("coin_id").indices
    for site in sites.itertuples():
        i = site.row
        sr = trailing_scale(ret, i, floor=RETURN_FLOOR)
        sv = trailing_scale(logv, i, floor=VOLUME_FLOOR)
        sign = rng.choice([-1.0, 1.0])
        last = coin_end[site.coin_id][-1]
        if site.type in ("price_spike", "combo"):
            delta = sign * strength * sr * (0.5 if site.type == "combo" else 1.0)
            out.loc[i, ["log_return", "residual_return"]] += delta
            out.loc[i:last, "market_cap"] *= np.exp(delta)
        if site.type == "volume_spike":
            out.loc[i, "volume"] *= np.exp(strength * sv)
        if site.type == "combo":
            out.loc[i, "volume"] *= np.exp(-0.5 * strength * sv)
    return out


def recall_table(scored: pd.DataFrame, sites: pd.DataFrame, methods: list[str]) -> pd.DataFrame:
    """Recall at the alert budget per method and type, plus consensus (2+ methods)."""
    hit = scored.loc[sites["row"].to_numpy()]
    rows = {}
    for t in TYPES:
        sel = hit[(sites["type"] == t).to_numpy()]
        rows[t] = {**{m: sel[f"{m}_flag"].mean() for m in methods}, "consensus_2plus": (sel["votes"] >= 2).mean()}
    rows["all"] = {**{m: hit[f"{m}_flag"].mean() for m in methods}, "consensus_2plus": (hit["votes"] >= 2).mean()}
    return pd.DataFrame(rows).T


def precision_at_k(scored: pd.DataFrame, sites: pd.DataFrame, methods: list[str]) -> dict[str, float]:
    """Of each method's top-k scores (k = number of injections), the share that are injections.

    A conservative lower bound: real, unlabelled anomalies in the data count as misses here.
    """
    injected = np.zeros(len(scored), dtype=bool)
    injected[sites["row"].to_numpy()] = True
    k = len(sites)
    return {m: float(injected[np.argsort(-scored[f"{m}_score"].to_numpy())[:k]].mean()) for m in methods}
