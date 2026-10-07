"""The Dagster pipeline graph loads and is wired as designed (runs in CI: no services needed)."""
from dagster import AssetKey

from chainsignal.orchestration.definitions import defs


def test_definitions_load_with_both_jobs_and_schedules():
    repo = defs.get_repository_def()
    assert {j.name for j in repo.get_all_jobs()} >= {"daily_update", "weekly_reconcile"}
    crons = {s.job_name: s.cron_schedule for s in repo.schedule_defs}
    assert crons == {"daily_update": "20 0 * * *", "weekly_reconcile": "0 1 * * 1"}


def test_daily_schedule_runs_inside_the_snapshot_window():
    """Read the REAL schedule: moving it past the window would make every daily snapshot fail."""
    from chainsignal.ingest.coingecko import SNAPSHOT_WINDOW_HOURS
    daily = next(s for s in defs.get_repository_def().schedule_defs if s.job_name == "daily_update")
    minute, hour, *_ = daily.cron_schedule.split()
    assert daily.execution_timezone == "UTC" and int(hour) < SNAPSHOT_WINDOW_HOURS


def test_asset_graph_dependencies():
    graph = defs.get_repository_def().asset_graph
    parents = lambda name: {k.path[-1] for k in graph.get(AssetKey(name)).parent_keys}
    assert parents("transforms") == {"market_snapshot", "market_reconcile", "token_transfers"}
    assert parents("features") == {"transforms", "address_labels"}
    assert parents("dashboard_data") == {"anomaly_scores"}
