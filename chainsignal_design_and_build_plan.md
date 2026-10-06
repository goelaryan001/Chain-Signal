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
