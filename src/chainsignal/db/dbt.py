"""Run the dbt project (dbt/) from Python: used by the full loader and the Dagster transforms asset."""
from pathlib import Path

from chainsignal.config import get_settings  # noqa: F401  (loads .env so dbt's env_var() sees the connection)

DBT_DIR = Path(__file__).resolve().parents[3] / "dbt"


class DbtFailure(RuntimeError):
    pass


def run_dbt(args: list[str]) -> dict:
    """Invoke dbt; return counts per status, raise DbtFailure listing anything that failed."""
    from dbt.cli.main import dbtRunner   # imported lazily: dbt is heavy and only needed here

    res = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--quiet"])
    results = list(getattr(res.result, "results", []) or [])
    counts: dict[str, int] = {}
    failed = []
    for r in results:
        status = str(r.status).split(".")[-1].lower()
        counts[status] = counts.get(status, 0) + 1
        if status in ("error", "fail"):
            failed.append(f"{r.node.resource_type} {r.node.name}: {r.message}")
    if not res.success:
        raise DbtFailure(f"dbt {' '.join(args)} failed: " + ("; ".join(failed) or str(res.exception)))
    return {"command": " ".join(args), "nodes": len(results), **counts}
