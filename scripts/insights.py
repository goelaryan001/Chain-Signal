"""Phase 6: compute every headline number in the README from the data, so each claim is reproducible.

Run: .venv/bin/python scripts/insights.py     (needs Phases 2-5 to have run)
Also writes the output to docs/results/insights.txt.
"""
import io
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd

from chainsignal.db.clickhouse import get_client

RESULTS = Path("docs/results")

# Low-volatility assets (median daily move < 0.2%): pegged and yield-bearing stablecoins,
# tokenized treasuries. "Price flat" is their design, so it says nothing about wash trading.
# A first version used "price within 2% of $1", which missed yield-bearing dollar tokens
# whose price drifts upward (Re Protocol reUSD: $1.00 -> $1.10).
STABLECOINS_SQL = """
SELECT coin_id FROM market_daily_clean WHERE log_return IS NOT NULL GROUP BY coin_id
HAVING median(abs(log_return)) < 0.002
"""


def section(title: str) -> None:
    print(f"\n## {title}")


def main(client) -> None:
    q = lambda sql: client.query(sql).result_rows

    section("Data")
    coins, rows, d0, d1 = q("SELECT uniqExact(coin_id), count(), min(date), max(date) FROM market_daily_raw")[0]
    print(f"market: {coins:,} coins, {rows:,} daily rows, {d0} -> {d1}")
    # uniqExactArray, not arrayJoin: arrayJoin would duplicate every row and double count()
    for token, n, addrs, t0, t1 in q("""SELECT token, count(), uniqExactArray([from_address, to_address]),
                                        min(block_time), max(block_time) FROM token_transfers_raw GROUP BY token ORDER BY token"""):
        print(f"on-chain {token:<5}: {n:,} transfers, {addrs:,} addresses, {t0:%Y-%m-%d} -> {t1:%Y-%m-%d}")

    section("Data quality")
    for status, n in q("SELECT status, count() FROM coin_quality GROUP BY status ORDER BY status"):
        print(f"coin_quality {status:<12} {n:>4} coins")
    vg = q("""SELECT countIf(volume_glitch), uniqExactIf(coin_id, volume_glitch) FROM market_daily_flagged
              WHERE coin_id IN (SELECT coin_id FROM coin_quality WHERE status = 'ok')""")[0]
    print(f"isolated one-day volume collapses removed: {vg[0]:,} days across {vg[1]} coins")

    section("Fat tails")
    share = q("SELECT countIf(abs(return_z) > 3) / count() FROM market_features WHERE return_z IS NOT NULL")[0][0]
    print(f"days with |return z| > 3: {share:.2%} (a normal distribution gives 0.27%: {share / 0.0027:.0f}x)")
    cutoff = q("SELECT minIf(zscore_score, zscore_flag) FROM anomaly_scores WHERE entity_type = 'coin'")[0][0]
    print(f"robust |z| needed to reach the top 1% of coin-days: {cutoff:.1f}")

    section("Impossible turnover (volume many times market cap, price flat)")
    print(f"median daily turnover: {q('SELECT median(volume / market_cap) FROM market_daily_clean WHERE market_cap > 0')[0][0]:.1%} of market cap")
    stables = [r[0] for r in q(STABLECOINS_SQL)]
    print(f"low-volatility (pegged) assets identified from prices: {len(stables)} (excluded below: flat by design)")
    for x in (1, 5, 10):
        n, k = q(f"""SELECT count(), uniqExact(coin_id) FROM market_daily_clean
                     WHERE market_cap > 0 AND volume > {x} * market_cap AND abs(exp(log_return) - 1) < 0.05
                       AND coin_id NOT IN ({STABLECOINS_SQL})""")[0]
        print(f"  volume > {x:>2}x market cap, |price move| < 5%: {n:,} coin-days, {k} coins")
    print("  most frequent (excluding pegged assets):")
    for coin, days, peak in q(f"""SELECT coin_id, count(), max(volume / market_cap) FROM market_daily_clean
                                  WHERE market_cap > 0 AND volume > 5 * market_cap AND abs(exp(log_return) - 1) < 0.05
                                    AND coin_id NOT IN ({STABLECOINS_SQL})
                                  GROUP BY coin_id ORDER BY count() DESC LIMIT 5"""):
        print(f"    {coin:<32} {days:>3} days, peak {peak:,.0f}x market cap")

    section("On-chain")
    for token, pairs, with_c, share_c in q("""
        WITH pairs AS (
          SELECT token, least(from_address, to_address) a, greatest(from_address, to_address) b,
                 countIf(from_address = a) ab, countIf(from_address = b) ba
          FROM token_transfers_raw GROUP BY token, a, b HAVING ab >= 1 AND ba >= 1)
        SELECT token, count(), countIf(la.is_contract OR lb.is_contract),
               sumIf(ab + ba, la.is_contract OR lb.is_contract) / sum(ab + ba)
        FROM pairs p LEFT JOIN address_labels la ON p.a = la.address LEFT JOIN address_labels lb ON p.b = lb.address
        GROUP BY token ORDER BY token"""):
        print(f"{token:<5} back-and-forth pairs {pairs:,}: {with_c:,} involve a contract, carrying {share_c:.0%} of their transfers")
    for token, rt, med in q("""SELECT token, avg(rt_share), median(transfers) FROM onchain_features GROUP BY token ORDER BY token"""):
        print(f"{token:<5} wallet-to-wallet round trips: {rt:.2%} of transfers on average; median {med:,.0f} transfers/day")
    for fn, share in q("""SELECT splitByChar('(', toString(function_name))[1] fn, count() / (SELECT count() FROM token_transfers_raw WHERE token = 'PEPE')
                         FROM token_transfers_raw WHERE token = 'PEPE' GROUP BY fn ORDER BY count() DESC LIMIT 4"""):
        print(f"PEPE transfers via {fn or '(none)':<16} {share:.1%}")

    section("Models (Phase 4) and evaluation (Phase 5)")
    flags = client.query_df("SELECT zscore_flag, iforest_flag, lof_flag, votes FROM anomaly_scores WHERE entity_type = 'coin'")
    for a, b in (("zscore", "iforest"), ("zscore", "lof"), ("iforest", "lof")):
        inter = (flags[f"{a}_flag"] & flags[f"{b}_flag"]).sum()
        union = (flags[f"{a}_flag"] | flags[f"{b}_flag"]).sum()
        print(f"Jaccard {a} vs {b}: {inter / union:.2f}")
    print(f"coin-days flagged by all 3: {(flags.votes == 3).sum():,}, by 2+: {(flags.votes >= 2).sum():,}")
    inj = pd.read_csv(RESULTS / "phase5_injection.csv")
    for s in sorted(inj.strength.unique()):
        r = inj[(inj.strength == s) & (inj.type.isin(["all", "combo"]))].set_index("type")
        print(f"injection {s:>2} sigma recall  all: z {r.loc['all', 'zscore']:.0%} IF {r.loc['all', 'iforest']:.0%} "
              f"LOF {r.loc['all', 'lof']:.0%}   combo: z {r.loc['combo', 'zscore']:.0%} IF {r.loc['combo', 'iforest']:.0%} "
              f"LOF {r.loc['combo', 'lof']:.0%}")
    bt = pd.read_csv(RESULTS / "phase5_backtest.csv")
    for _, r in bt[bt.kind != "market-wide"].iterrows():
        res = "no data" if pd.isna(r.zscore) else ", ".join(m for m in ("zscore", "iforest", "lof") if r[m] == 1) or "missed"
        print(f"backtest {r.event:<34} {res}")


if __name__ == "__main__":
    buf = io.StringIO()
    with redirect_stdout(buf):
        main(get_client())
    text = buf.getvalue()
    print(text)
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "insights.txt").write_text(text.lstrip())
