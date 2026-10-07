"""ClickHouse connection helper (HTTP interface via clickhouse-connect)."""
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from chainsignal.config import Settings, get_settings


def get_client(settings: Settings | None = None) -> Client:
    s = settings or get_settings()
    return clickhouse_connect.get_client(
        host=s.clickhouse_host,
        port=s.clickhouse_port,
        username=s.clickhouse_user,
        password=s.clickhouse_password,
        database=s.clickhouse_db,
        # raw tables are partitioned by day, so a full reload's insert blocks span ~365
        # partitions; ClickHouse refuses more than 100 per insert by default
        settings={"max_partitions_per_insert_block": 1000},
    )
