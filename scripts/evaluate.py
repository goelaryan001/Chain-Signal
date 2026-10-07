"""Phase 5: evaluate the three detectors.

A. Synthetic injection: plant known anomalies at 4 strengths, measure recall at the
   1% alert budget and precision@k, per method and anomaly type.
B. Backtest: the February 2026 market-wide crash, and three documented single-asset
   exploits whose coins are fetched separately (they fell out of the top 1000).

Run: .venv/bin/python scripts/evaluate.py      (~5 min; ~6 CoinGecko calls, cached)
Writes CSVs to docs/results/.
"""
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

from chainsignal.config import get_settings
from chainsignal.db.clickhouse import get_client
from chainsignal.evaluation.backtest import (ASSET_EVENTS, MARKET_EVENTS, MISSING_EVENTS,
                                             event_coin_rows, window_hits)
from chainsignal.evaluation.injection import choose_sites, inject, precision_at_k, recall_table
from chainsignal.ingest.coingecko import CoinGeckoClient
from chainsignal.models.detectors import robust_rolling_z
from chainsignal.models.pipeline import METHODS, model_rows, robust_market_features, score

BUDGET = 0.01
STRENGTHS = [3, 6, 12, 24]
SEED = 2026
EVENT_DIR = Path("data/raw/coingecko/events")
RESULTS = Path("docs/results")


def load_base(client) -> tuple[pd.DataFrame, pd.Series]:
    df = client.query_df("""SELECT coin_id, date, volume, market_cap, log_return, residual_return
                            FROM market_features ORDER BY coin_id, date""")
    df["date"] = pd.to_datetime(df["date"])
    mr = client.query_df("SELECT DISTINCT date, market_return FROM market_features WHERE market_return IS NOT NULL")
    market_return = pd.Series(mr["market_return"].to_numpy(), index=pd.to_datetime(mr["date"]))
    return df.reset_index(drop=True), market_return


def score_frame(df: pd.DataFrame) -> pd.DataFrame:
    """robust features -> model rows -> scores, keeping the original row id."""
    df = df.copy()
    df["row_id"] = np.arange(len(df))
    return score(model_rows(robust_market_features(df)), BUDGET)


def run_injection(base: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    sites = choose_sites(base, rng)
    print(f"A. synthetic injection: {len(sites)} sites (one per coin), "
          f"{sites.type.value_counts().to_dict()}, budget {BUDGET:.0%}")
    results = []
    for s in STRENGTHS:
        t = time.time()
        scored = score_frame(inject(base, sites, s, np.random.default_rng(SEED + s)))
        pos = pd.Series(np.arange(len(scored)), index=scored["row_id"].to_numpy())
        kept = sites[sites["row"].isin(pos.index)].copy()
        kept["row"] = pos.loc[kept["row"]].to_numpy()
        table = recall_table(scored, kept, METHODS)
        prec = precision_at_k(scored, kept, METHODS)
        print(f"\n  strength {s} sigma  ({len(kept)} scoreable sites, {time.time() - t:.0f}s)  recall at budget:")
        print((table * 100).round(1).to_string().replace("\n", "\n    ").join(["    ", ""]))
        print("    precision@k (lower bound): " + "  ".join(f"{m} {v:.0%}" for m, v in prec.items()))
        for t_name, row in table.iterrows():
            results.append({"strength": s, "type": t_name, **row.to_dict()})
        results.append({"strength": s, "type": "precision_at_k", **prec})
    return pd.DataFrame(results)


def fetch_event_payloads() -> dict[str, dict]:
    EVENT_DIR.mkdir(parents=True, exist_ok=True)
    cg = None
    payloads = {}
    for coin_id in ASSET_EVENTS:
        path = EVENT_DIR / f"{coin_id}.json"
        if not path.exists():
            cg = cg or CoinGeckoClient(get_settings().coingecko_api_key)
            path.write_text(json.dumps(cg.market_chart(coin_id, 365)))
        payloads[coin_id] = json.loads(path.read_text())
    return payloads


def run_backtest(base: pd.DataFrame, market_return: pd.Series) -> pd.DataFrame:
    events = [event_coin_rows(c, p, market_return) for c, p in fetch_event_payloads().items()]
    combined = pd.concat([base, *events], ignore_index=True).sort_values(["coin_id", "date"]).reset_index(drop=True)
    scored = score_frame(combined)
    out = []

    print("\nB1. market-wide event")
    daily = scored.groupby(scored.date.dt.date)
    for name, days in MARKET_EVENTS.items():
        for d in days:
            day = daily.get_group(d)
            shares = {m: day[f"{m}_flag"].mean() for m in METHODS}
            print(f"  {name} {d}: {len(day)} coins | share flagged "
                  + "  ".join(f"{m} {v:.1%}" for m, v in shares.items())
                  + f" | median |rz_return| {day.rz_return.abs().median():.1f}, "
                  f"median |rz_residual| {day.rz_residual.abs().median():.1f}, "
                  f"market return {np.expm1(market_return.get(pd.Timestamp(d), np.nan)):.1%}")
            out.append({"event": f"{name} {d}", "kind": "market-wide", **shares})
        typical = {m: daily[f"{m}_flag"].mean().median() for m in METHODS}
        print("  typical day share flagged (median over days): " + "  ".join(f"{m} {v:.1%}" for m, v in typical.items()))
        # a market-wide event belongs in the market series itself, not in per-coin residuals
        mz = robust_rolling_z(market_return.sort_index(), pd.Series(market_return.sort_index().index),
                              min_scale=0.005)
        mz.index = market_return.sort_index().index
        print("  market return robust z on crash days: "
              + "  ".join(f"{d} {mz.get(pd.Timestamp(d), np.nan):+.1f}" for d in days)
              + f"  (largest |z| of the whole year: {mz.abs().max():.1f} on {mz.abs().idxmax().date()})")
        for coin in ("bitcoin", "ethereum"):
            r = scored[(scored.coin_id == coin) & scored.date.dt.date.isin(days)]
            print(f"  {coin:<9} on crash days: rz_return {r.rz_return.round(1).tolist()}  "
                  f"flags {[[m for m in METHODS if x[f'{m}_flag']] for _, x in r.iterrows()]}")

    print("\nB2. single-asset exploits (coins appended: survivorship bias kept them out of the top 1000)")
    for coin_id, (d, desc, source) in ASSET_EVENTS.items():
        hits = window_hits(scored, coin_id, d, METHODS, days=1)
        print(f"  {coin_id:<24} {d}  {desc}")
        if hits["rows"] == 0:
            # no data is not a miss: report it separately so it can't count against a method
            last = combined.loc[combined.coin_id == coin_id, "date"].max()
            print(f"      NO DATA around the event: CoinGecko's history ends {last.date()}  [source: {source}]")
            out.append({"event": f"{coin_id} {d}", "kind": "single-asset (no data)",
                        **{m: np.nan for m in METHODS}})
            continue
        who = [m for m in METHODS if hits[m]] or ["none"]
        print(f"      +/-1 day: flagged by {', '.join(who)} | max |rz_return| {hits['max_abs_rz_return']:.1f}, "
              f"max rz_volume {hits['max_rz_volume']:.1f}  [source: {source}]")
        raw = combined[(combined.coin_id == coin_id)
                       & (combined.date.dt.date >= d - pd.Timedelta(days=2).to_pytimedelta())
                       & (combined.date.dt.date <= d + pd.Timedelta(days=2).to_pytimedelta())]
        for _, r in raw.iterrows():
            print(f"        {r.date.date()}  return {np.expm1(r.log_return) if pd.notna(r.log_return) else float('nan'):+7.1%}"
                  f"  volume ${r.volume:,.0f}")
        out.append({"event": f"{coin_id} {d}", "kind": "single-asset", **{m: float(hits[m]) for m in METHODS}})
    for name, why in MISSING_EVENTS.items():
        print(f"  {name}: skipped ({why})")
    return pd.DataFrame(out)


def main() -> None:
    client = get_client()
    base, market_return = load_base(client)
    RESULTS.mkdir(parents=True, exist_ok=True)
    run_injection(base).to_csv(RESULTS / "phase5_injection.csv", index=False)
    run_backtest(base, market_return).to_csv(RESULTS / "phase5_backtest.csv", index=False)
    print(f"\nresults written to {RESULTS}/")


if __name__ == "__main__":
    main()
