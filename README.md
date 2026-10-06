# ChainSignal

Batch pipeline for crypto market and on-chain anomaly detection: CoinGecko (market breadth) + Etherscan (on-chain depth) → ClickHouse → PySpark features → z-score / Isolation Forest / LOF comparison → synthetic-injection and historical-event evaluation.

Full design: [chainsignal_design_and_build_plan.md](chainsignal_design_and_build_plan.md). Build log: [docs/build-log.html](docs/build-log.html).

## Status

| Phase | What | State |
|---|---|---|
| 0 | Environment: ClickHouse, Python deps, Spark | done |
| 1 | Real data ingestion (CoinGecko, Etherscan) | done |
| 2 | ClickHouse schema + load | done |
| 3 | PySpark feature engineering | next |
| 4 | Model comparison | |
| 5 | Evaluation | |
| 6 | Insights, tests, final README | |

## Setup

Requires Docker, Python 3.11+, and Java 17 or 21 (for Spark).

```bash
cp .env.example .env              # then add your CoinGecko + Etherscan keys
docker compose up -d              # starts ClickHouse on :8123 / :9000
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt -e .
.venv/bin/python scripts/check_env.py   # verifies ClickHouse, Spark, API keys
.venv/bin/pytest

# Phase 1: pull raw data into data/raw/ (cached + resumable)
.venv/bin/python scripts/ingest_coingecko.py --coins 1000      # ~25 min, ~1,004 of 10k monthly calls
.venv/bin/python scripts/ingest_etherscan.py --days 90         # or one process per token: --tokens PEPE --rate 1.6
.venv/bin/python scripts/inspect_raw.py                        # samples + sanity + data-quality checks

# Phase 2: load into ClickHouse (drop/recreate, idempotent) and verify against the raw files
.venv/bin/python scripts/load_clickhouse.py
```

## ClickHouse tables

| Table / view | What it holds |
|---|---|
| `coins` | top-1000 coin list (id, symbol, name, rank) |
| `market_daily_raw` | daily price / market cap / volume per coin, untouched |
| `token_transfers_raw` | every LINK / PEPE / LOOKS transfer in the 90-day window |
| `token_daily` | per-token daily rollup: transfers, volume, unique senders/receivers, swaps |
| `market_daily_flagged` | raw market rows + log return, extreme-move, zero-volume, gap flags |
| `coin_quality` | per-coin verdict: `ok`, `feed_glitch` (2+ moves >10x), `stale_feed` (>20% zero-volume days) |
| `market_daily_clean` | the modelling input: `ok` coins only |

Schema and design notes: [src/chainsignal/db/schema.sql](src/chainsignal/db/schema.sql), [quality.sql](src/chainsignal/db/quality.sql).

## Layout

```
src/chainsignal/
  config.py        settings from .env
  spark.py         local SparkSession factory
  db/              ClickHouse client, schema/rollup/quality SQL, loader
  ingest/          CoinGecko + Etherscan pulls        (Phase 1)
  features/        PySpark feature jobs               (Phase 3)
  models/          z-score, Isolation Forest, LOF     (Phase 4)
  evaluation/      synthetic injection, backtests     (Phase 5)
scripts/           runnable entry points
tests/             pytest
```
