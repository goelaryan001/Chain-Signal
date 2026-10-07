"""Export everything the dashboard shows to docs/dashboard/data.json (shared by the script and Dagster).

The page itself is docs/dashboard/index.html; it only reads data.json.
"""
from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd

from chainsignal.evaluation.backtest import ASSET_EVENTS
from chainsignal.ingest.coingecko import parse_market_chart
from chainsignal.models.compare import anomaly_type
from chainsignal.models.detectors import robust_rolling_z
from chainsignal.models.pipeline import METHODS, model_rows, robust_market_features, score

OUT = Path("docs/dashboard/data.json")
RESULTS = Path("docs/results")
BUDGET = 0.01
TOP_COINS = 150
NOTABLE = ["celer-network", "pyth-network", "ordinals", "hyperlane", "just", "wojak-5", "novachargex-coin",
           "spark-2", "beldex", "handy", "jpycoin", "gold-park", "emerald-security-token", "medxt",
           "space-and-time", "hashkey-ecopoints", "a-meme-coin", "power-protocol", "bulla-3"]
QUALITY_EXAMPLES = {"pinksale": "bad prints", "bitmind": "TAO/USD unit switches", "bitcoin": "volume collapse"}
PEGGED_SQL = """SELECT coin_id FROM market_daily_clean WHERE log_return IS NOT NULL GROUP BY coin_id
                HAVING median(abs(log_return)) < 0.002"""


def sig(x, digits=5):
    """Round to significant digits for a compact JSON (None for missing)."""
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return None
    return float(f"{x:.{digits}g}")


def series(values) -> list:
    return [sig(v) for v in values]


def build_dashboard_data(client, out: Path = OUT) -> dict:
    """Write data.json and return a short summary."""
    q = lambda sql: client.query(sql).result_rows

    # ---------- scoring (deterministic, identical to Phase 4) ----------
    df = client.query_df("""SELECT coin_id, date, volume, market_cap, log_return, residual_return
                            FROM market_features ORDER BY coin_id, date""")
    df["date"] = pd.to_datetime(df["date"])
    scored = score(model_rows(robust_market_features(df)), BUDGET)
    scored["type"] = anomaly_type(scored.rz_return, scored.rz_volume,
                                  scored[["rz_turnover", "rz_residual"]].abs().max(axis=1))

    d0, d1 = q("SELECT min(date), max(date) FROM market_daily_raw")[0]
    days = pd.date_range(d0, d1, freq="D")
    day_index = {d.date(): i for i, d in enumerate(days)}

    # ---------- coins for the explorer ----------
    coins = client.query_df("SELECT coin_id, symbol, name, market_cap_rank FROM coins")
    modelled = set(scored.coin_id)
    ranked = coins[coins.coin_id.isin(modelled)].sort_values("market_cap_rank")
    chosen = list(dict.fromkeys(list(ranked.coin_id.head(TOP_COINS)) + [c for c in NOTABLE if c in modelled]))
    raw = client.query_df(f"""SELECT coin_id, date, price, volume, market_cap FROM market_daily_raw
                              WHERE coin_id IN ({",".join(f"'{c}'" for c in chosen)}) ORDER BY coin_id, date""")
    explorer = []
    meta = coins.set_index("coin_id")
    for coin in chosen:
        r = raw[raw.coin_id == coin]
        price, volume = [None] * len(days), [None] * len(days)
        for d, p, v in zip(r.date, r.price, r.volume):
            i = day_index[pd.Timestamp(d).date()]
            price[i], volume[i] = sig(p), sig(v)
        s = scored[scored.coin_id == coin]
        flagged = s[s[[f"{m}_flag" for m in METHODS]].any(axis=1)]
        explorer.append({
            "id": coin, "symbol": meta.loc[coin, "symbol"].upper(), "name": meta.loc[coin, "name"],
            "rank": int(meta.loc[coin, "market_cap_rank"]) if pd.notna(meta.loc[coin, "market_cap_rank"]) else None,
            "price": price, "volume": volume,
            "flags": {m: [day_index[d.date()] for d in s.loc[s[f"{m}_flag"], "date"]] for m in METHODS},
            "detail": {str(day_index[x.date.date()]): [sig(x.rz_return, 3), sig(x.rz_volume, 3), sig(x.rz_turnover, 3),
                                                       sig(x.rz_residual, 3), x.type, int(x.votes)]
                       for x in flagged.itertuples()},
        })

    # ---------- data quality ----------
    quality = {"status": dict(q("SELECT status, count() FROM coin_quality GROUP BY status")),
               "volume_glitches": q("""SELECT countIf(volume_glitch), uniqExactIf(coin_id, volume_glitch) FROM market_daily_flagged
                                       WHERE coin_id IN (SELECT coin_id FROM coin_quality WHERE status = 'ok')""")[0],
               "examples": []}
    for coin, label in QUALITY_EXAMPLES.items():
        r = client.query_df(f"""SELECT f.date, f.price, f.volume, f.extreme_move, f.volume_glitch FROM market_daily_flagged f
                                WHERE coin_id = '{coin}' ORDER BY date""")
        quality["examples"].append({
            "id": coin, "label": label,
            "status": q(f"SELECT status FROM coin_quality WHERE coin_id = '{coin}'")[0][0],
            "dates": [pd.Timestamp(d).date().isoformat() for d in r.date], "price": series(r.price), "volume": series(r.volume),
            "marked": [i for i, (e, g) in enumerate(zip(r.extreme_move, r.volume_glitch)) if e or g]})

    # ---------- model comparison ----------
    flags = {m: scored[f"{m}_flag"].to_numpy() for m in METHODS}
    jaccard = [[round(float((flags[a] & flags[b]).sum() / (flags[a] | flags[b]).sum()), 3) for b in METHODS]
               for a in METHODS]
    types = ["volume_only", "price_and_vol", "price_shock", "other_feature", "multivariate"]
    type_share = {m: [round(float((scored.loc[flags[m], "type"] == t).mean()), 4) for t in types] for m in METHODS}
    only = {m: int((flags[m] & (scored.votes == 1)).sum()) for m in METHODS}
    # keep the strongest agreed days by dollar volume: sorting by z-score size surfaced residual glitches
    top = scored[scored.votes >= 2].sort_values(["votes", "volume"], ascending=False).head(400)
    top_rows = [[x.coin_id, x.date.date().isoformat(), x.type, int(x.votes),
                 [m for m in METHODS if getattr(x, f"{m}_flag")],
                 sig(x.rz_return, 3), sig(x.rz_volume, 3), sig(x.volume, 4)] for x in top.itertuples()]

    # ---------- wash-trading candidates ----------
    wash = scored[(scored.votes >= 2) & (scored.type == "volume_only") & (scored.rz_volume > 0)]
    wash = wash.sort_values("volume", ascending=False).head(40)
    mcap = df.set_index(["coin_id", "date"])["market_cap"]
    wash_rows = [[x.coin_id, x.date.date().isoformat(), sig(x.volume, 4), sig(mcap.get((x.coin_id, x.date)), 4),
                  sig(x.rz_volume, 3), sig(x.rz_return, 3), [m for m in METHODS if getattr(x, f"{m}_flag")]]
                 for x in wash.itertuples()]
    turnover = q(f"""SELECT coin_id, toString(date), volume, market_cap, volume / market_cap, exp(log_return) - 1
                     FROM market_daily_clean
                     WHERE market_cap > 0 AND volume > 5 * market_cap AND abs(exp(log_return) - 1) < 0.05
                       AND coin_id NOT IN ({PEGGED_SQL})
                     ORDER BY volume / market_cap DESC LIMIT 40""")
    turnover_rows = [[c, d, sig(v, 4), sig(m, 4), sig(r, 3), sig(ret, 3)] for c, d, v, m, r, ret in turnover]
    turnover_summary = {x: q(f"""SELECT count(), uniqExact(coin_id) FROM market_daily_clean
                                 WHERE market_cap > 0 AND volume > {x} * market_cap AND abs(exp(log_return) - 1) < 0.05
                                   AND coin_id NOT IN ({PEGGED_SQL})""")[0] for x in (1, 5, 10)}

    # ---------- market-level series ----------
    mr = client.query_df("SELECT DISTINCT date, market_return FROM market_features WHERE market_return IS NOT NULL ORDER BY date")
    mr["date"] = pd.to_datetime(mr["date"])
    mz = robust_rolling_z(mr["market_return"], mr["date"], min_scale=0.005)
    market = {"dates": [d.date().isoformat() for d in mr.date], "ret": series(np.expm1(mr.market_return)),
              "z": series(mz)}

    # ---------- on-chain ----------
    od = client.query_df("""SELECT token, day, transfers, unique_addresses, new_addresses, rt_share,
                                   top10_sender_share, large_transfers, hub_share FROM onchain_features ORDER BY token, day""")
    tf = client.query_df("SELECT entity, date, zscore_flag, iforest_flag, lof_flag FROM anomaly_scores WHERE entity_type = 'token'")
    onchain = {}
    for token, g in od.groupby("token"):
        f = tf[tf.entity == token]
        onchain[token] = {
            "days": [pd.Timestamp(d).date().isoformat() for d in g.day],
            **{c: series(g[c]) for c in ["transfers", "unique_addresses", "new_addresses", "rt_share",
                                         "top10_sender_share", "large_transfers", "hub_share"]},
            "flags": {m: [pd.Timestamp(d).date().isoformat() for d in f.loc[f[f"{m}_flag"], "date"]] for m in METHODS}}
    contracts = q("""
        WITH pairs AS (
          SELECT token, least(from_address, to_address) a, greatest(from_address, to_address) b,
                 countIf(from_address = a) ab, countIf(from_address = b) ba
          FROM token_transfers_raw GROUP BY token, a, b HAVING ab >= 1 AND ba >= 1)
        SELECT token, count(), sum(ab + ba), sumIf(ab + ba, la.is_contract OR lb.is_contract)
        FROM pairs p LEFT JOIN address_labels la ON p.a = la.address LEFT JOIN address_labels lb ON p.b = lb.address
        GROUP BY token ORDER BY token""")
    token_totals = q("""SELECT token, count(), uniqExactArray([from_address, to_address]) FROM token_transfers_raw
                        GROUP BY token ORDER BY token""")

    # ---------- evaluation ----------
    inj = pd.read_csv(RESULTS / "phase5_injection.csv")
    injection = {t: {m: [sig(inj[(inj.strength == s) & (inj.type == t)][m].iloc[0], 3) for s in sorted(inj.strength.unique())]
                     for m in METHODS + ["consensus_2plus"]}
                 for t in ["all", "price_spike", "volume_spike", "combo"]}
    events = []
    for coin_id, (d, desc, source) in ASSET_EVENTS.items():
        rows = parse_market_chart(coin_id, json.loads(Path(f"data/raw/coingecko/events/{coin_id}.json").read_text()))
        e = pd.DataFrame(rows)
        e["date"] = pd.to_datetime(e["date"])
        win = e[(e.date >= pd.Timestamp(d) - pd.Timedelta(days=30)) & (e.date <= pd.Timestamp(d) + pd.Timedelta(days=30))]
        events.append({"id": coin_id, "date": d.isoformat(), "desc": desc, "source": source,
                       "last_date": e.date.max().date().isoformat(),
                       "dates": [x.date().isoformat() for x in win.date], "price": series(win.price),
                       "volume": series(win.volume)})

    totals = {
        "coins": int(q("SELECT count() FROM coins")[0][0]),
        "market_rows": int(q("SELECT count() FROM market_daily_raw")[0][0]),
        "clean_coins": int(quality["status"].get("ok", 0)),
        "modelled_rows": int(len(scored)), "modelled_coins": int(scored.coin_id.nunique()),
        "flags_per_method": int(flags["zscore"].sum()),
        "votes2": int((scored.votes >= 2).sum()), "votes3": int((scored.votes == 3).sum()),
        "transfers": int(sum(r[1] for r in token_totals)),
        "addresses": int(q("SELECT uniqExactArray([from_address, to_address]) FROM token_transfers_raw")[0][0]),
        "labelled": int(q("SELECT count() FROM address_labels")[0][0]),
        "contracts": int(q("SELECT countIf(is_contract) FROM address_labels")[0][0]),
        "cutoff_z": sig(float(scored.loc[flags["zscore"], "zscore_score"].min()), 3),
        "fat_tail_share": sig(q("SELECT countIf(abs(return_z) > 3) / count() FROM market_features WHERE return_z IS NOT NULL")[0][0], 3),
        "median_turnover": sig(q("SELECT median(volume / market_cap) FROM market_daily_clean WHERE market_cap > 0")[0][0], 3),
    }

    data = {
        "generated": date.today().isoformat(), "window": [d0.isoformat(), d1.isoformat()],
        "days": [d.date().isoformat() for d in days], "methods": METHODS, "totals": totals,
        "coins": explorer, "quality": quality,
        "models": {"jaccard": jaccard, "types": types, "type_share": type_share, "only": only, "top": top_rows},
        "wash": {"consensus": wash_rows, "turnover": turnover_rows,
                 "turnover_summary": {str(k): list(v) for k, v in turnover_summary.items()}},
        "market": market,
        "onchain": {"tokens": onchain, "contracts": [list(r) for r in contracts],
                    "totals": [list(r) for r in token_totals]},
        "evaluation": {"strengths": sorted(int(s) for s in inj.strength.unique()), "injection": injection,
                       "backtest": pd.read_csv(RESULTS / "phase5_backtest.csv").replace({np.nan: None}).to_dict("records"),
                       "events": events},
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, separators=(",", ":"), default=str))
    return {"path": str(out), "mb": round(out.stat().st_size / 1e6, 2), "coins": len(explorer),
            "consensus_rows": len(top_rows), "wash_rows": len(wash_rows), "turnover_rows": len(turnover_rows)}
