"""Export everything the dashboard shows to docs/dashboard/data.json.

Run: .venv/bin/python scripts/build_dashboard.py   (needs Phases 2-5 to have run; ~1 min)
The logic lives in chainsignal.dashboard, shared with the Dagster pipeline.
"""
from chainsignal.dashboard import build_dashboard_data
from chainsignal.db.clickhouse import get_client

if __name__ == "__main__":
    s = build_dashboard_data(get_client())
    print(f"wrote {s['path']} ({s['mb']:.2f} MB): {s['coins']} coins in explorer, {s['consensus_rows']} consensus rows, "
          f"{s['wash_rows']} wash candidates, {s['turnover_rows']} turnover rows")
