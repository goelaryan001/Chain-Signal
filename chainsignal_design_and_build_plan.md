# ChainSignal: Crypto Market & On-Chain Anomaly Detection — Design & Build Plan

**Provenance note**: original design, no existing code to start from. If using Claude Code: design and build from this document, don't search for or adapt an existing crypto-dashboard repo.

**Relationship to MarketPulse**: deliberately kept separate. MarketPulse is real-time streaming (Kafka, live ticks) on equities. This is large-scale **batch** analysis (scheduled historical pulls, heavier one-shot processing) on crypto markets plus on-chain data — different rhythm, different asset class, genuinely non-redundant.

---

## Part 1: What this project actually is

A pipeline that pulls real crypto market data at scale (thousands of coins) and real on-chain transaction data (a few specific tokens, traced deep), stores it in a database actually used for this purpose in industry, processes it with Spark, and runs a **properly designed and evaluated** anomaly-detection system on top — not just "ran Isolation Forest and called it done."

**The one-sentence pitch**: "I built a batch pipeline combining broad crypto market data with deep on-chain transaction data, stored in ClickHouse, processed with PySpark, and compared multiple anomaly-detection approaches with a real evaluation methodology — not just accuracy on data I can't verify."

## Part 2: Data sources — both verified real

- **CoinGecko Demo API** (free, confirmed): 21,000+ coins, 100 calls/min, 10,000 calls/month, real historical price/volume/market-cap data
- **Etherscan API** (free, confirmed): 5 calls/sec, 100,000 calls/day, real transaction/event data. Note: free-tier max records per request dropped from 10,000 to 1,000 as of July 2026 on several endpoints — design pagination around this from the start, don't discover it mid-build.

## Part 3: Database choice — ClickHouse, and why it's a real, defensible pick

**ClickHouse** — a columnar OLAP database, genuinely used across real crypto/fintech companies for exactly this workload: high-volume, append-heavy, time-series analytical queries. This gives you a *fifth* distinct database across your projects (Snowflake, DuckDB, TimescaleDB, MySQL, now ClickHouse), each chosen for a real, explainable reason — worth having that one-line "why ClickHouse over the others" answer ready: it's built specifically for extremely fast aggregation over huge append-only datasets, which is exactly this project's access pattern.

## Part 4: The pipeline

```
SOURCES: CoinGecko (market breadth, ~1000+ coins) + Etherscan (on-chain
          depth, a few specific tokens traced through transactions →
          internal transactions → decoded events)
   ↓
INGEST: scheduled batch pulls (not streaming), paginated correctly
         around Etherscan's real free-tier limits
   ↓
STORE: ClickHouse — raw tables first, then aggregated analytical tables
   ↓
PROCESS: PySpark for large-scale feature engineering across the full
          historical pull — rolling statistics, cross-asset correlation
          features, on-chain activity features per token
   ↓
MODEL DESIGN & COMPARISON (Part 5 — the real depth)
   ↓
EVALUATION (Part 6 — the real rigor)
   ↓
INSIGHTS: what did the data actually say
```

## Part 5: The ML design — comparing real candidates, not picking one blind

This is the part worth understanding deeply, since it's what you specifically asked to go deeper on. Anomaly detection has no single "correct" algorithm — a real data scientist compares candidates and justifies the choice with evidence, the same way your churn project compared S/T/X-learners instead of just picking one.

**Candidate 1 — Statistical baseline (rolling z-score)**: your established approach from MarketPulse and PCAP. Simple, fully interpretable, no training required. Weakness: only looks at one feature at a time, misses multivariate patterns (a coin whose price and volume are each individually normal, but the *combination* is unusual).

**Candidate 2 — Isolation Forest**: an ensemble method built specifically for anomaly detection, not adapted from classification. The core idea, worth being able to explain cleanly: it randomly partitions the data with a tree structure, and anomalies — being rare and different — get isolated into their own leaf in *fewer splits* than normal points do. Anomaly score is based on average path length across many random trees. Genuinely elegant, handles multivariate patterns the z-score baseline misses, no labels needed.

**Candidate 3 — Local Outlier Factor (LOF)**: compares a point's local density to its neighbors' local density, flagging points that sit in a sparser neighborhood than their surroundings. Complementary to Isolation Forest — LOF is better at catching *local* anomalies (unusual relative to nearby points) while Isolation Forest is better at *global* anomalies (unusual relative to the whole dataset). Worth running both and comparing where they agree and disagree.

**The actual design decision to make and defend**: don't just pick one. Run all three, compare their flagged anomalies, and use agreement across methods as a stronger signal than any single method alone — directly mirrors the "don't collect algorithms, but do compare deliberately and justify" discipline from your churn project's model comparison.

**A concrete, named thing to actually hunt for — wash-trading**: rather than leaving detection generic ("unusual patterns"), give it a real target. Wash-trading is a well-documented real problem in crypto markets — artificially inflating trading volume by trading with yourself or colluding wallets. It shows up as a genuine multivariate anomaly (volume spikes without the price movement or unique-participant growth you'd expect from real activity) — exactly the kind of pattern Isolation Forest and LOF are suited to catch that a single-feature z-score would miss. Naming this specifically turns "detected some anomalies" into "detected patterns consistent with wash-trading," a much sharper, more defensible interview answer.

## Part 6: Evaluation — the part most people skip, and the part that actually matters

Anomaly detection has the same fundamental problem your Solar project ran into: **no ground truth exists in real data.** Nobody can tell you with certainty which historical crypto moments were "true anomalies." The honest, rigorous answer combines two techniques you've already used successfully elsewhere:

**1. Synthetic injection (the churn project's trick, reused here)**: inject known, synthetic anomalies into a copy of the real data — an artificial price/volume spike you created yourself — then measure whether each candidate model actually recovers it. This gives you a real, provable precision/recall number, the same way embedding a known degradation signal let you validate the Solar project's detection logic.

**2. Backtesting against real known events (the Solar project's trick, reused here)**: pick 2-3 real, independently-documented crypto market events (a known historical flash crash, a known exchange incident) and check whether your models actually flag them. This is the "cross-check against independent real records" validation, same category as checking Solar anomalies against real weather data.

**Report both honestly** — if Isolation Forest catches the synthetic injections well but misses a real historical event LOF catches, say so. That kind of honest, specific result (not "the model works great") is what separates a rigorous project from one that just ran to completion.

## Part 7: Phased Claude Code build plan

Same rule as always — one phase at a time, verify before moving on.

### Phase 0 — Setup
```
Set up the project structure and a docker-compose.yml running a single
ClickHouse container. Requirements: pandas, numpy, pyspark, scikit-learn,
requests, pytest. Confirm ClickHouse is reachable before writing any logic.
```

### Phase 1 — Real data ingestion
```
Write ingestion for CoinGecko (pull ~500-1000 coins' historical daily data,
respecting the 100 calls/min free limit) and Etherscan (pick 2-3 real
tokens, pull real transaction history, paginated correctly around the
1,000-record free-tier limit as of July 2026). Print real samples from
both sources to confirm we're working with real, sane data before
building anything on top.
```

### Phase 2 — Load into ClickHouse
```
Design and load a ClickHouse schema: raw market data table, raw on-chain
transaction table. Explain the schema design choices given ClickHouse's
columnar, append-heavy access pattern.
```

### Phase 3 — PySpark feature engineering
```
Build a PySpark job computing: rolling statistics per coin (mean/std over
a window), cross-asset correlation features, and on-chain activity
features per token (transaction volume, unique addresses, large-transfer
counts). Write the engineered features back to ClickHouse.
```

### Phase 4 — Model comparison
```
Implement all three candidates from Part 5: rolling z-score baseline,
Isolation Forest, and Local Outlier Factor. Run all three on the engineered
features. Compare their flagged anomalies -- where do they agree, where do
they disagree, and what does that tell us about each method's strengths.
```

### Phase 5 — Evaluation
```
Implement both evaluation approaches from Part 6: inject synthetic known
anomalies and measure precision/recall per model, AND backtest against
2-3 real, independently-documented historical crypto events. Report
results honestly, including any model's failures, not just successes.
```

### Phase 6 — Insights + tests + README
```
Pull together the actual data-driven insights this analysis produced --
not just "the pipeline runs," but what did we learn about crypto market
anomalies and on-chain activity patterns. Write pytest tests for the pure
logic (feature engineering, each detection method, the evaluation
functions). Write a README covering the architecture, the ClickHouse
choice, the three-way model comparison, and the evaluation methodology --
written so it would hold up handed to an interviewer.
```

After each phase, stop and make sure you can explain what got built and why, out loud, before moving on.

---

## Part 8: What changed from the original plan (learned while building, Phases 0–6)

The plan above is kept as written. These are the places where reality differed, and what the build did instead. Details for each are in the build log (`docs/build-log.html`).

- **PySpark 4.0, not 3.5.** The machine has Java 21, which Spark 3.5 doesn't support.
- **Etherscan V2 has no `logIndex`.** Transfers are keyed by (tx_hash, position within the transaction) instead.
- **Pagination by block range, not by page.** Page numbers stop working past 10,000 records. A short or empty API answer is never trusted as "end of data": one silently ended a pull two days early.
- **A data-quality layer came before modelling.** It wasn't in the original pipeline. The largest raw "anomalies" were feed errors: bad prints, TAO/USD quote switches, dead feeds, one-day volume collapses.
- **CoinGecko's daily point stamped day D is day D−1's close.** Every market date was one day late until it was cross-checked against a second source.
- **Wallet vs contract labels.** 96–98% of back-and-forth transfers involve DEX pools, routers or bots, so round trips are counted between wallets only, after labelling 13,204 addresses.
- **Robust z-scores (median/MAD) with a scale floor** replaced plain rolling z-scores. Plain z-scores reached 201; median/MAD alone reached 74,309 on stablecoins.
- **LOF needed duplicate points collapsed before fitting.** Without that, it flagged ordinary days of flat assets.
- **The design's claim about Isolation Forest holds only for multi-feature anomalies.** On real data at a 1% budget it mostly mirrors the z-score (overlap 0.73). On planted combination anomalies it wins clearly (43% vs 11% recall at 12σ).
- **Survivorship bias in the universe.** "Top 1,000 by today's market cap" excludes coins that collapsed during the year, which are the events the backtest needs. They were fetched separately for Phase 5, and Phase 10 below addresses it properly.
- **Added after Phase 6:** an interactive dashboard (`docs/dashboard/`) generated from the analysis database.

## Part 9: Resume upgrades — Phases 7–11

The project is complete as designed. These five phases close the gaps an interviewer would probe, on both tracks. **Data engineering:** does the pipeline run itself, incrementally, with tested data contracts? **Data science:** how precise is the detector really, and is the sample unbiased? Same rule as before: one phase at a time, each on its own branch, merged only after CI passes.

### Phase 7 — Continuous integration (data engineering) ✅ done
```
Add a GitHub Actions workflow that runs on every push to main and every pull
request: Python 3.11 + Java 21 (for Spark), install the project, run every test
that doesn't need a loaded database (pytest -m "not integration"). Add the CI
badge to the README. Verify the run is green on the latest commit before merging.
```
Why: proves the project works on a clean machine, and keeps it that way. Result: 47 tests pass on GitHub in ~35 s (PR #1).

### Phase 8 — Orchestration and incremental loads (data engineering)
```
Put the pipeline under Dagster: each stage (ingest CoinGecko, ingest Etherscan,
load ClickHouse, build features, score, evaluate) becomes an asset with its
dependencies, so Dagster runs them in order, retries failures and shows the run
history in its UI. Add a daily schedule that ingests only the newest day(s):
CoinGecko by date, Etherscan from the last stored block. Make the daily load
idempotent: re-running a day replaces that day's partition instead of
appending duplicates. Prove it by running the same day twice and showing
identical row counts and checksums.
```
Why: today the pipeline is a list of scripts with full reloads. A scheduled, incremental, re-runnable pipeline is the core of real data engineering work, and the single biggest gap in the current version.

### Phase 9 — dbt models and tests (data engineering)
```
Move the SQL transformation layer (quality views, coin_quality, the clean view,
the token daily rollup) into dbt with the ClickHouse adapter. Express the data
contracts as dbt tests: unique keys, not-null columns, accepted status values,
"no excluded coin appears in the clean model", "rollup totals equal raw counts".
Generate the dbt docs site with the lineage graph. Run dbt build in the Dagster
pipeline and dbt tests in CI where possible.
```
Why: dbt is on most data engineering job descriptions. It also makes the cleaning rules into documented, tested contracts instead of hand-run SQL.

### Phase 10 — Fix survivorship bias (data science)
```
Rebuild the coin universe from market cap at the START of the window, not
today's. Pull daily history for coins currently ranked 1,001–3,500 (~2,500
CoinGecko calls; check the monthly budget first), then pick the top 1,000 by
market cap on the first day of the window. Rerun cleaning, features, models and
evaluation, and report what changed: how many collapsed coins entered the
universe, and whether detector results and backtest coverage moved. State the
remaining bias plainly: coins that fell below rank 3,500 are still missing.
```
Why: finding a bias in your own work and measuring the effect of fixing it is one of the strongest stories a data scientist can tell.

### Phase 11 — Precision study (data science)
```
Turn "candidates" into a measured precision. (1) Automated: check every day
flagged by 2+ methods against a second, independent market-data source, and
classify it as confirmed in both sources (a real market event), single-source
(likely feed error) or no second-source data. (2) Manual: hand-label a random
sample of about 100 flagged days with their cause (news, exploit, listing,
unlock, wash-trading shape, data error), using the same checks as for Celer
Network. Report precision with bootstrap confidence intervals, per method and
for consensus, and add 95% intervals to the Phase 5 recall numbers too.
```
Why: precision is the number every anomaly-detection discussion eventually asks for. Measuring it honestly, with intervals and a documented labelling method, is what separates analysis from a demo.

After each phase: update the build log (steps, results, bugs, corrections, "explain it out loud"), commit everything, and make sure you can explain what got built and why before moving on.
