"""The dbt project compiles without a database (runs in CI): models, refs, sources and tests resolve."""
from chainsignal.db.dbt import DBT_DIR


def test_dbt_project_parses_with_expected_models_and_contracts():
    from dbt.cli.main import dbtRunner
    res = dbtRunner().invoke(["parse", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--quiet"])
    assert res.success, res.exception
    nodes = res.result.nodes.values()
    models = {n.name for n in nodes if n.resource_type == "model"}
    tests = [n for n in nodes if n.resource_type == "test"]
    assert models == {"market_daily_flagged", "coin_quality", "market_daily_clean", "token_daily"}
    assert len(tests) >= 28
    # the contracts that guard the incremental loads must exist
    names = {t.name for t in tests}
    assert {"assert_market_keys_unique", "assert_transfer_keys_unique", "assert_token_daily_matches_raw"} <= names


def test_models_read_raw_tables_only_through_sources():
    """Lineage depends on source()/ref(): a hard-coded table name would hide a dependency."""
    raw = ["market_daily_raw", "token_transfers_raw", "address_labels", "coins"]
    for sql in (DBT_DIR / "models").rglob("*.sql"):
        text = sql.read_text()
        code = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
        for table in raw:
            assert f"FROM {table}" not in code and f"JOIN {table}" not in code, f"{sql.name} reads {table} directly"
