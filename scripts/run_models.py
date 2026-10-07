"""Phase 4: run the three detectors on market and on-chain features and compare them.

Run: .venv/bin/python scripts/run_models.py
Writes every score and flag to ClickHouse (anomaly_scores) for Phase 5.
"""
import time

import numpy as np
import pandas as pd

from chainsignal.db.clickhouse import get_client
from chainsignal.db.load import run_sql_file
from chainsignal.models.compare import agreement_table, anomaly_type
from chainsignal.models.detectors import flag_top, isolation_forest_score, lof_score, zscore_score
from chainsignal.models.pipeline import METHODS, R_FEATURES, model_rows, robust_market_features, score

MARKET_BUDGET = 0.01    # each method flags its top 1% of coin-days
ONCHAIN_BUDGET = 0.05   # ~225 token-days in total, so a larger share to get a handful per token
ONCHAIN_FEATURES = ["transfers_z", "addresses_z", "rt_share_z", "concentration_z",
                    "large_transfers_z", "tpa_z"]
PHASE3_CANDIDATES = ["wojak-5", "celer-network", "beldex", "novachargex-coin"]


def report_market(df: pd.DataFrame) -> None:
    flags = {m: df[f"{m}_flag"].to_numpy() for m in METHODS}
    print(f"\n=== market: {len(df):,} coin-days, {df.coin_id.nunique()} coins, "
          f"budget {MARKET_BUDGET:.0%} = {flags['zscore'].sum():,} flags per method ===")

    print("\nagreement (Jaccard overlap of flag sets):")
    print(agreement_table(flags).round(2).to_string())
    votes = sum(flags.values())
    print(f"\nconsensus: flagged by all 3: {(votes == 3).sum():,}   by 2+: {(votes >= 2).sum():,}   "
          f"by exactly 1: {(votes == 1).sum():,}")

    df["type"] = anomaly_type(df.rz_return, df.rz_volume, df[["rz_turnover", "rz_residual"]].abs().max(axis=1))
    print("\nwhat each method flags (share of its flags by anomaly type):")
    table = pd.DataFrame({m: df.loc[flags[m], "type"].value_counts(normalize=True) for m in METHODS}).fillna(0)
    print((table * 100).round(1).astype(str).add("%").to_string())

    calm = df[R_FEATURES].abs().max(axis=1) < 1
    print("\nsanity: share of each method's flags where NO feature exceeds |z| = 1 (ordinary days):")
    print("  " + "   ".join(f"{m} {(flags[m] & calm).sum() / flags[m].sum():.1%}" for m in METHODS))
    print("  most-flagged coins: " + "   ".join(
        f"{m} {df.loc[flags[m], 'coin_id'].value_counts().head(2).to_dict()}" for m in METHODS))

    print("\nflagged by ONLY this method (what the others miss):")
    for m in METHODS:
        only = df[flags[m] & (votes == 1)]
        kinds = only["type"].value_counts().head(3).to_dict()
        print(f"  {m:<8} {len(only):>5,} rows  {kinds}")
        for _, r in only.sort_values(f"{m}_score", ascending=False).head(3).iterrows():
            print(f"           {r.coin_id:<26} {r.date}  ret {r.rz_return:6.1f}  vol {r.rz_volume:6.1f}  "
                  f"turn {r.rz_turnover:6.1f}  resid {r.rz_residual:6.1f}")

    print("\nwash-trading shape (volume SPIKE, price calm) among consensus flags (2+ methods), largest volume first:")
    wash = df[(votes >= 2) & (df.type == "volume_only") & (df.rz_volume > 0)].sort_values("volume", ascending=False)
    print(f"  {len(wash):,} coin-days")
    for _, r in wash.head(8).iterrows():
        who = "+".join(m for m in METHODS if r[f"{m}_flag"])
        print(f"  {r.coin_id:<26} {r.date}  vol ${r.volume:>15,.0f}  rz_vol {r.rz_volume:6.1f}  "
              f"rz_ret {r.rz_return:5.1f}  [{who}]")

    print("\nPhase 3 candidates (their biggest-volume day) and who flags them:")
    for coin in PHASE3_CANDIDATES:
        rows = df[df.coin_id == coin]
        if rows.empty:
            print(f"  {coin:<26} not in modelled rows")
            continue
        r = rows.loc[rows.volume.idxmax()]
        who = [m for m in METHODS if r[f"{m}_flag"]] or ["none"]
        print(f"  {coin:<26} {r.date}  rz_vol {r.rz_volume:6.1f}  rz_ret {r.rz_return:5.1f}  -> {', '.join(who)}")

    kept = df[df.extreme_move]
    print(f"\nsingle >10x moves kept in Phase 2 ({len(kept)} in modelled rows): flagged by "
          + ", ".join(f"{m} {int(kept[f'{m}_flag'].sum())}" for m in METHODS))


def compare_standard_vs_robust(df: pd.DataFrame) -> None:
    std = df[["return_z", "volume_z", "turnover_z", "residual_z"]].to_numpy(dtype=float)
    ok = ~np.isnan(std).all(axis=1)
    std_score = np.where(ok, np.nanmax(np.abs(np.where(np.isnan(std), 0, std)), axis=1), 0)
    std_flags = flag_top(std_score, MARKET_BUDGET)
    rob_flags = df["zscore_flag"].to_numpy()
    robust_max = df[R_FEATURES].abs().max(axis=1).to_numpy()
    blowups = std_flags & (robust_max < 3)
    print("\n=== standard (mean/std) vs robust (median/MAD) z-score baseline, same budget ===")
    print(f"  overlap (Jaccard): {np.logical_and(std_flags, rob_flags).sum() / np.logical_or(std_flags, rob_flags).sum():.2f}")
    print(f"  standard-z flags that are NOT extreme on any robust feature: {blowups.sum():,} of {std_flags.sum():,}")
    print(f"  max standard |z| seen: {np.nanmax(np.abs(std)):.0f}   max robust |z| seen: {np.nanmax(robust_max):.0f}")


def run_onchain(client) -> pd.DataFrame:
    od = client.query_df(f"SELECT token, day, {', '.join(ONCHAIN_FEATURES)}, transfers, unique_addresses, rt_share "
                         "FROM onchain_features WHERE transfers_z IS NOT NULL ORDER BY token, day")
    X = od[ONCHAIN_FEATURES].fillna(0).to_numpy(dtype=float)
    od["zscore_score"], od["iforest_score"], od["lof_score"] = (
        zscore_score(X), isolation_forest_score(X), lof_score(X, n_neighbors=10))
    for m in METHODS:
        od[f"{m}_flag"] = flag_top(od[f"{m}_score"].to_numpy(), ONCHAIN_BUDGET)
    votes = od[[f"{m}_flag" for m in METHODS]].sum(axis=1)
    print(f"\n=== on-chain: {len(od)} token-days (small sample: treat as illustrative), "
          f"budget {ONCHAIN_BUDGET:.0%} = {od.zscore_flag.sum()} flags per method ===")
    print(agreement_table({m: od[f'{m}_flag'].to_numpy() for m in METHODS}).round(2).to_string())
    print("days flagged by 2+ methods:")
    for _, r in od[votes >= 2].iterrows():
        who = "+".join(m for m in METHODS if r[f"{m}_flag"])
        top = max(ONCHAIN_FEATURES, key=lambda f: abs(r[f]) if pd.notna(r[f]) else 0)
        print(f"  {r.token:<5} {pd.Timestamp(r.day).date()}  transfers {r.transfers:>6,}  addresses {r.unique_addresses:>6,}  "
              f"rt {r.rt_share:.1%}  strongest: {top}={r[top]:.1f}  [{who}]")
    return od


def write_scores(client, market: pd.DataFrame, onchain: pd.DataFrame) -> None:
    run_sql_file(client, "models.sql")
    client.command("TRUNCATE TABLE anomaly_scores")
    cols = ["entity_type", "entity", "date", "zscore_score", "iforest_score", "lof_score",
            "zscore_flag", "iforest_flag", "lof_flag", "votes"]
    for kind, df, key, date in (("coin", market, "coin_id", "date"), ("token", onchain, "token", "day")):
        out = pd.DataFrame({
            "entity_type": kind, "entity": df[key].astype(str), "date": pd.to_datetime(df[date]).dt.date,
            **{f"{m}_score": df[f"{m}_score"].astype(float) for m in METHODS},
            **{f"{m}_flag": df[f"{m}_flag"].astype(bool) for m in METHODS},
            "votes": df[[f"{m}_flag" for m in METHODS]].sum(axis=1).astype("uint8")})
        client.insert_df("anomaly_scores", out[cols])


def main() -> None:
    client = get_client()
    t = time.time()
    df = client.query_df("""
        SELECT coin_id, date, volume, market_cap, log_return, residual_return, extreme_move,
               return_z, volume_z, turnover_z, residual_z
        FROM market_features ORDER BY coin_id, date""")
    df["date"] = pd.to_datetime(df["date"])
    df = robust_market_features(df)
    print(f"robust z-scores for {len(df):,} rows in {time.time() - t:.0f}s")

    df = model_rows(df)
    t = time.time()
    df = score(df, MARKET_BUDGET)
    print(f"scored {len(df):,} rows with 3 methods in {time.time() - t:.0f}s")
    df["date"] = df["date"].dt.date

    report_market(df)
    compare_standard_vs_robust(df)
    onchain = run_onchain(client)
    write_scores(client, df, onchain)
    print("\nwrote anomaly_scores to ClickHouse")


if __name__ == "__main__":
    main()
