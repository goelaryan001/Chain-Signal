"""Smoke tests for the Phase 0 environment."""
import pytest

from chainsignal.config import get_settings


def test_settings_defaults_match_docker_compose():
    s = get_settings()
    assert s.clickhouse_port == 8123
    assert s.clickhouse_db == "chainsignal"


@pytest.mark.integration
def test_clickhouse_roundtrip():
    from chainsignal.db.clickhouse import get_client

    client = get_client()
    assert client.query("SELECT 1 + 1").result_rows[0][0] == 2


@pytest.mark.integration
def test_spark_local_job():
    from chainsignal.spark import get_spark

    spark = get_spark("chainsignal-test")
    assert spark.range(10).count() == 10
    spark.stop()
