"""CoinGecko Demo API client: market breadth (top-N coins, 365 days of daily history).

Demo plan limits (docs, checked 2026-10): 100 calls/min, 10,000 calls/month,
1 year of daily history. The monthly cap is the real constraint, so every
per-coin response is cached to disk and never re-fetched.
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import requests

from chainsignal.ingest.http import RateLimiter, get_json

BASE_URL = "https://api.coingecko.com/api/v3"
MS_PER_DAY = 86_400_000


class CoinGeckoClient:
    def __init__(self, api_key: str, calls_per_minute: float = 92):
        self.session = requests.Session()
        self.headers = {"x-cg-demo-api-key": api_key, "accept": "application/json"}
        self.limiter = RateLimiter(calls_per_minute / 60)

    def _get(self, path: str, params: dict | None = None):
        return get_json(self.session, BASE_URL + path, self.limiter, params, self.headers, rate_limit_backoff=60)

    def top_coins(self, n: int) -> list[dict]:
        """Top-n coins by market cap (250 per page)."""
        coins: list[dict] = []
        page = 1
        while len(coins) < n:
            batch = self._get("/coins/markets", {
                "vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": page,
            })
            if not batch:
                break
            coins.extend(batch)
            page += 1
        return coins[:n]

    def market_chart(self, coin_id: str, days: int = 365) -> dict:
        return self._get(f"/coins/{coin_id}/market_chart", {
            "vs_currency": "usd", "days": days, "interval": "daily",
        })

    def coin_detail(self, coin_id: str) -> dict:
        return self._get(f"/coins/{coin_id}", {
            "localization": "false", "tickers": "false", "market_data": "false",
            "community_data": "false", "developer_data": "false",
        })


def parse_market_chart(coin_id: str, payload: dict) -> list[dict]:
    """Turn a market_chart payload into one row per UTC day.

    CoinGecko's daily series has a point at 00:00 UTC for each day plus a
    trailing "now" point mid-day. Only midnight points are complete daily
    snapshots, so the trailing partial point is dropped.

    The point stamped 00:00 UTC on day D carries the close and 24h volume of day
    D-1, so rows are labelled D-1. (Found in Phase 5: CoinRanking dates Celer's
    $1.5B volume day 2026-06-03; labelling by the stamp had put it on 06-04, which
    would misalign every backtest against real event dates.)
    """
    series = {}
    for field, key in (("price", "prices"), ("market_cap", "market_caps"), ("volume", "total_volumes")):
        for ts_ms, value in payload.get(key) or []:
            if ts_ms % MS_PER_DAY != 0:
                continue
            series.setdefault(ts_ms, {})[field] = value
    rows = []
    for ts_ms in sorted(series):
        values = series[ts_ms]
        rows.append({
            "coin_id": coin_id,
            "date": datetime.fromtimestamp(ts_ms / 1000 - 86_400, tz=timezone.utc).date().isoformat(),
            "price": values.get("price"),
            "market_cap": values.get("market_cap"),
            "volume": values.get("volume"),
        })
    return rows


def pull_market_data(client: CoinGeckoClient, n_coins: int, raw_dir: Path, days: int = 365) -> dict:
    """Fetch the top-n list and each coin's daily history, caching raw JSON per coin."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    chart_dir = raw_dir / "market_chart"
    chart_dir.mkdir(exist_ok=True)

    coins_path = raw_dir / "coins_markets.json"
    coins = json.loads(coins_path.read_text()) if coins_path.exists() else []
    if len(coins) < n_coins:  # a cached list from a smaller run must not cap a bigger one
        coins = client.top_coins(n_coins)
        coins_path.write_text(json.dumps(coins))
    coins = coins[:n_coins]

    stats = {"coins": len(coins), "fetched": 0, "cached": 0, "failed": []}
    for i, coin in enumerate(coins, 1):
        path = chart_dir / f"{coin['id']}.json"
        if path.exists():
            stats["cached"] += 1
            continue
        try:
            path.write_text(json.dumps(client.market_chart(coin["id"], days)))
            stats["fetched"] += 1
        except Exception as exc:
            stats["failed"].append((coin["id"], str(exc)))
        if i % 50 == 0:
            print(f"  coingecko: {i}/{len(coins)} coins ({stats['fetched']} fetched, {stats['cached']} cached)")
    return stats


# ---------- daily snapshots (Phase 8) ----------
# A daily per-coin market_chart update would cost ~1,000 calls/day (30k/month, 3x the
# Demo budget). /coins/markets returns 250 coins per call, so a snapshot of the whole
# universe costs 4 calls. Taken shortly after 00:00 UTC on day D, it approximates the
# close of day D-1, which is what the historical rows hold.
SNAPSHOT_WINDOW_HOURS = 3


def snapshot_day(fetched_at: datetime, window_hours: int = SNAPSHOT_WINDOW_HOURS):
    """The market day a snapshot stands for: the previous UTC day, if taken soon after midnight."""
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware (UTC)")
    utc = fetched_at.astimezone(timezone.utc)
    if utc.hour >= window_hours:
        raise ValueError(f"snapshot taken at {utc:%H:%M} UTC is a mid-day price, not yesterday's close: "
                         f"take it within {window_hours}h after 00:00 UTC")
    return (utc - timedelta(days=1)).date()


def parse_markets_snapshot(payload: list[dict], day, tracked: set[str] | None = None) -> list[dict]:
    """Rows shaped like parse_market_chart's, for one day, from a /coins/markets snapshot."""
    rows = []
    for c in payload:
        if tracked is not None and c["id"] not in tracked:
            continue
        if c.get("current_price") is None or c.get("total_volume") is None:
            continue
        rows.append({"coin_id": c["id"], "date": day.isoformat(), "price": c["current_price"],
                     "market_cap": c.get("market_cap"), "volume": c["total_volume"]})
    return rows
