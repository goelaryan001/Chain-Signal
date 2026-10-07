"""Dagster definitions: the ChainSignal pipeline as a graph of assets, run on schedules.

    market_snapshot ─┐
    market_reconcile ┼─> transforms ─┐
    token_transfers ─┴─> address_labels ┴─> features ─> anomaly_scores ─> dashboard_data

Daily job (00:20 UTC, inside the post-midnight snapshot window): provisional market day
from a /coins/markets snapshot, new on-chain transfers, then everything downstream.
Weekly job (Monday 01:00 UTC): replace the past week's provisional market days with
authoritative market_chart values, then everything downstream.

Every ingest step stores its raw response before loading and replaces whole day
partitions, so any run can be repeated safely. Asset checks guard the data contracts.

Run locally:  dagster dev -m chainsignal.orchestration.definitions
"""
import json
from pathlib import Path

from dagster import (AssetCheckResult, AssetExecutionContext, AssetSelection, Backoff, Definitions, Failure,
                     MaterializeResult, MetadataValue, RetryPolicy, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from chainsignal.config import get_settings
from chainsignal.dashboard import build_dashboard_data
from chainsignal.db.clickhouse import get_client
from chainsignal.db.incremental import table_fingerprint
from chainsignal.db.load import MARKET_COLUMNS, TRANSFER_COLUMNS, insert_batched, label_rows, run_sql_file
from chainsignal.features.job import run_feature_job, verify_features
from chainsignal.ingest.coingecko import CoinGeckoClient
from chainsignal.ingest.etherscan import EtherscanClient
from chainsignal.ingest.labels import label_addresses, label_candidates
from chainsignal.models.job import MARKET_BUDGET, run_scoring_job
from chainsignal.pipeline.daily import run_market_reconcile, run_market_snapshot, run_transfer_update

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / "data" / "raw"
API_RETRY = RetryPolicy(max_retries=2, delay=60, backoff=Backoff.EXPONENTIAL)


def _md(d: dict) -> dict:
    return {k: (MetadataValue.json(v) if isinstance(v, (dict, list)) else v) for k, v in d.items()}


# ---------- ingest ----------

@asset(group_name="ingest", retry_policy=API_RETRY,
       description="Yesterday's market day for all tracked coins from one /coins/markets snapshot (4 calls), "
                   "loaded as provisional rows (source='snapshot') by replacing that day's partition.")
def market_snapshot(context: AssetExecutionContext) -> MaterializeResult:
    r = run_market_snapshot(get_client(), CoinGeckoClient(get_settings().coingecko_api_key), RAW)
    context.log.info(f"market day {r['day']}: {r['rows']} rows, fetched={r['fetched']}")
    return MaterializeResult(metadata=_md({k: r[k] for k in ("day", "rows", "missing", "fetched", "fetched_at")}))


@asset(group_name="ingest", retry_policy=API_RETRY,
       description="Replace the past week's provisional snapshot days with authoritative market_chart values "
                   "(source='reconciled'). ~1,000 CoinGecko calls, so it runs weekly.")
def market_reconcile(context: AssetExecutionContext) -> MaterializeResult:
    r = run_market_reconcile(get_client(), CoinGeckoClient(get_settings().coingecko_api_key), RAW, days=8)
    context.log.info(f"reconciled {r.get('reconciled', 0)} coin-days over {r['days']}")
    return MaterializeResult(metadata=_md(r))


@asset(group_name="ingest", retry_policy=API_RETRY,
       description="Every LINK / PEPE / LOOKS transfer from the start of the last stored day to the latest block, "
                   "replacing those day partitions (the last stored day is completed, never duplicated).")
def token_transfers(context: AssetExecutionContext) -> MaterializeResult:
    contracts = json.loads((RAW / "etherscan" / "contracts.json").read_text())
    r = run_transfer_update(get_client(), EtherscanClient(get_settings().etherscan_api_key, calls_per_second=3),
                            contracts, RAW)
    context.log.info(f"{r['rows']} transfers over {r['days']}, blocks {r['start_block']}-{r['end_block']}")
    return MaterializeResult(metadata=_md({k: r[k] for k in ("rows", "days", "start_block", "end_block",
                                                              "per_token", "fetched", "raw_file")}))


@asset(deps=[token_transfers], group_name="ingest", retry_policy=API_RETRY,
       description="Contract-or-wallet labels for any NEW address in a back-and-forth pair (cached; "
                   "already-labelled addresses cost nothing), then refresh the address_labels table.")
def address_labels(context: AssetExecutionContext) -> MaterializeResult:
    client = get_client()
    cache = RAW / "etherscan" / "address_labels.jsonl"
    stats = label_addresses(get_settings().etherscan_api_key, label_candidates(client), cache)
    client.command("TRUNCATE TABLE address_labels")
    n = insert_batched(client, "address_labels", label_rows(cache),
                       ["address", "is_contract", "creator", "factory", "created_block"])
    context.log.info(f"labels: {stats}, table rows {n}")
    return MaterializeResult(metadata=_md({**stats, "table_rows": n}))


# ---------- transform / features / models / serve ----------

@asset(deps=[market_snapshot, market_reconcile, token_transfers], group_name="transform",
       description="Rebuild the token_daily rollup and the data-quality views (flagged -> coin_quality -> clean).")
def transforms(context: AssetExecutionContext) -> MaterializeResult:
    client = get_client()
    run_sql_file(client, "rollup.sql")
    run_sql_file(client, "quality.sql")
    status = dict(client.query("SELECT status, count() FROM coin_quality GROUP BY status").result_rows)
    clean_rows = client.query("SELECT count() FROM market_daily_clean").result_rows[0][0]
    return MaterializeResult(metadata=_md({"coin_quality": status, "clean_rows": clean_rows}))


@asset(deps=[transforms, address_labels], group_name="features",
       description="ClickHouse -> Parquet -> PySpark market and on-chain features -> ClickHouse, then the "
                   "pipeline's own verification checks; any failed check fails the asset.")
def features(context: AssetExecutionContext) -> MaterializeResult:
    client = get_client()
    seconds = run_feature_job(client, log=context.log.info)
    checks = verify_features(client)
    failed = [f"{name}: {detail}" for name, ok, detail in checks if not ok]
    if failed:
        raise Failure(description="feature verification failed", metadata={"failed": MetadataValue.json(failed)})
    return MaterializeResult(metadata=_md({"seconds": round(seconds), "checks": {n: d for n, _, d in checks}}))


@asset(deps=[features], group_name="models",
       description="Robust z-score, Isolation Forest and LOF at an equal 1% budget; writes anomaly_scores.")
def anomaly_scores(context: AssetExecutionContext) -> MaterializeResult:
    return MaterializeResult(metadata=_md(run_scoring_job(get_client())))


@asset(deps=[anomaly_scores], group_name="serve",
       description="Export docs/dashboard/data.json for the interactive explorer.")
def dashboard_data(context: AssetExecutionContext) -> MaterializeResult:
    return MaterializeResult(metadata=_md(build_dashboard_data(get_client(), ROOT / "docs" / "dashboard" / "data.json")))


# ---------- asset checks: the data contracts ----------

@asset_check(asset=market_snapshot, description="One row per (coin, day) in market_daily_raw.")
def market_keys_unique() -> AssetCheckResult:
    n, u = get_client().query("SELECT count(), uniqExact(coin_id, date) FROM market_daily_raw").result_rows[0]
    return AssetCheckResult(passed=n == u, metadata={"rows": n, "unique_keys": u})


@asset_check(asset=token_transfers, description="One row per (token, tx_hash, tx_seq): re-runs never duplicate.")
def transfer_keys_unique() -> AssetCheckResult:
    client = get_client()
    n, u = client.query("SELECT count(), uniqExact(token, tx_hash, tx_seq) FROM token_transfers_raw").result_rows[0]
    rows, fp = table_fingerprint(client, "token_transfers_raw", TRANSFER_COLUMNS)
    return AssetCheckResult(passed=n == u, metadata={"rows": n, "unique_keys": u, "fingerprint": str(fp)})


@asset_check(asset=transforms, description="No coin excluded by coin_quality leaks into market_daily_clean.")
def clean_view_excludes_bad_coins() -> AssetCheckResult:
    leaked = get_client().query("""SELECT count() FROM market_daily_clean
        WHERE coin_id IN (SELECT coin_id FROM coin_quality WHERE status != 'ok')""").result_rows[0][0]
    return AssetCheckResult(passed=leaked == 0, metadata={"leaked_rows": leaked})


@asset_check(asset=anomaly_scores, description="Every method flags exactly its alert budget of coin-days.")
def alert_budget_exact() -> AssetCheckResult:
    n, z, i, l = get_client().query("""SELECT count(), countIf(zscore_flag), countIf(iforest_flag), countIf(lof_flag)
        FROM anomaly_scores WHERE entity_type = 'coin'""").result_rows[0]
    k = round(n * MARKET_BUDGET)
    return AssetCheckResult(passed=z == i == l == k, metadata={"coin_days": n, "budget": k, "zscore": z, "iforest": i, "lof": l})


# ---------- jobs and schedules ----------

DOWNSTREAM = AssetSelection.assets(transforms, features, anomaly_scores, dashboard_data)
daily_update = define_asset_job(
    "daily_update", selection=AssetSelection.assets(market_snapshot, token_transfers, address_labels) | DOWNSTREAM,
    description="Provisional market day + new transfers + labels, then everything downstream.")
weekly_reconcile = define_asset_job(
    "weekly_reconcile", selection=AssetSelection.assets(market_reconcile) | DOWNSTREAM,
    description="Replace provisional market days with authoritative values, then everything downstream.")

defs = Definitions(
    assets=[market_snapshot, market_reconcile, token_transfers, address_labels, transforms, features,
            anomaly_scores, dashboard_data],
    asset_checks=[market_keys_unique, transfer_keys_unique, clean_view_excludes_bad_coins, alert_budget_exact],
    jobs=[daily_update, weekly_reconcile],
    schedules=[
        ScheduleDefinition(job=daily_update, cron_schedule="20 0 * * *", execution_timezone="UTC"),
        ScheduleDefinition(job=weekly_reconcile, cron_schedule="0 1 * * 1", execution_timezone="UTC"),
    ],
)
