"""Central settings, read from environment variables (and .env if present)."""
from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    coingecko_api_key: str
    etherscan_api_key: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str


def get_settings() -> Settings:
    return Settings(
        coingecko_api_key=os.getenv("COINGECKO_API_KEY", ""),
        etherscan_api_key=os.getenv("ETHERSCAN_API_KEY", ""),
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "chainsignal"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "chainsignal"),
        clickhouse_db=os.getenv("CLICKHOUSE_DB", "chainsignal"),
    )
