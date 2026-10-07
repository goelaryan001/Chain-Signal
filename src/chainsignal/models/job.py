"""The Phase 4 scoring job as library functions, shared by scripts/run_models.py and Dagster."""
import pandas as pd
from clickhouse_connect.driver.client import Client

from chainsignal.db.load import run_sql_file
from chainsignal.models.detectors import flag_top, isolation_forest_score, lof_score, zscore_score
from chainsignal.models.pipeline import METHODS, model_rows, robust_market_features, score

MARKET_BUDGET = 0.01    # each method flags its top 1% of coin-days
ONCHAIN_BUDGET = 0.05   # ~225 token-days in total, so a larger share to get a handful per token
ONCHAIN_FEATURES = ["transfers_z", "addresses_z", "rt_share_z", "concentration_z",
                    "large_transfers_z", "tpa_z"]


def score_market(client: Client) -> pd.DataFrame:
    df = client.query_df("""
        SELECT coin_id, date, volume, market_cap, log_return, residual_return, extreme_move,
               return_z, volume_z, turnover_z, residual_z
        FROM market_features ORDER BY coin_id, date""")
    df["date"] = pd.to_datetime(df["date"])
    df = score(model_rows(robust_market_features(df)), MARKET_BUDGET)
    df["date"] = df["date"].dt.date
    return df


def score_onchain(client: Client) -> pd.DataFrame:
    od = client.query_df(f"SELECT token, day, {', '.join(ONCHAIN_FEATURES)}, transfers, unique_addresses, rt_share "
                         "FROM onchain_features WHERE transfers_z IS NOT NULL ORDER BY token, day")
    X = od[ONCHAIN_FEATURES].fillna(0).to_numpy(dtype=float)
    od["zscore_score"], od["iforest_score"], od["lof_score"] = (
        zscore_score(X), isolation_forest_score(X), lof_score(X, n_neighbors=10))
    for m in METHODS:
        od[f"{m}_flag"] = flag_top(od[f"{m}_score"].to_numpy(), ONCHAIN_BUDGET)
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


def run_scoring_job(client: Client) -> dict:
    """Score market and on-chain rows with all three methods and write anomaly_scores."""
    market, onchain = score_market(client), score_onchain(client)
    write_scores(client, market, onchain)
    votes = market[[f"{m}_flag" for m in METHODS]].sum(axis=1)
    return {"coin_days": len(market), "flags_per_method": int(market["zscore_flag"].sum()),
            "votes_2plus": int((votes >= 2).sum()), "votes_3": int((votes == 3).sum()), "token_days": len(onchain)}
