"""Backtest against real, independently documented events.

Two kinds of event:
* market-wide (the February 2026 crash): every coin moves together. A good detector
  of coin-specific anomalies should NOT flag most coins' raw return as unusual once the
  market move is accounted for; we report how each method reacts.
* single-asset exploits (Resolv USR depeg, Kelp DAO rsETH hack, Maya Protocol CACAO):
  these coins had fallen out of the current top 1000 (survivorship bias), so their
  histories are fetched separately and appended before scoring.
"""
from datetime import date

import numpy as np
import pandas as pd

from chainsignal.ingest.coingecko import parse_market_chart

MARKET_EVENTS = {"February 2026 crash": [date(2026, 2, 4), date(2026, 2, 5), date(2026, 2, 6)]}

# coin id -> (event date, description, source)
ASSET_EVENTS = {
    "resolv-usr": (date(2026, 3, 22), "Resolv USR: ~80M unbacked USR minted, stablecoin depeg",
                   "crowdfundinsider.com, 2026-04"),
    "kelp-dao-restaked-eth": (date(2026, 4, 19), "Kelp DAO hack, ~$293M drained (largest DeFi exploit of 2026)",
                              "thestreet.com"),
    "cacao": (date(2026, 8, 18), "Maya Protocol exploit, 48.87M CACAO fabricated, token -88.7%",
              "shattered.io"),
}
MISSING_EVENTS = {"Balance Coin (BLC), 2026-07-22": "not listed on CoinGecko, so no market data"}


def event_coin_rows(coin_id: str, payload: dict, market_return: pd.Series) -> pd.DataFrame:
    """Market rows for an appended coin, shaped like market_features.

    residual_return uses beta = 1 (log return minus the market's median return): these
    coins are not in the Spark job, and a one-factor beta needs history we lack.
    """
    df = pd.DataFrame(parse_market_chart(coin_id, payload))
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    gap = df["date"].diff().dt.days
    df["log_return"] = np.where(gap == 1, np.log(df["price"] / df["price"].shift()), np.nan)
    df["residual_return"] = df["log_return"] - df["date"].map(market_return).astype(float)
    return df[["coin_id", "date", "volume", "market_cap", "log_return", "residual_return"]]


def window_hits(scored: pd.DataFrame, coin_id: str, event: date, methods: list[str], days: int = 1) -> dict:
    """Did each method flag the coin within +/- `days` of the event date? Plus the peak scores."""
    rows = scored[(scored.coin_id == coin_id)
                  & (scored.date.dt.date >= event - pd.Timedelta(days=days).to_pytimedelta())
                  & (scored.date.dt.date <= event + pd.Timedelta(days=days).to_pytimedelta())]
    return {"rows": len(rows), **{m: bool(rows[f"{m}_flag"].any()) for m in methods},
            "max_abs_rz_return": float(rows["rz_return"].abs().max()) if len(rows) else np.nan,
            "max_rz_volume": float(rows["rz_volume"].max()) if len(rows) else np.nan}
