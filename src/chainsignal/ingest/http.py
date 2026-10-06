"""Shared HTTP plumbing for the ingestion clients: pacing and retries."""
import time

import requests


class RateLimiter:
    """Enforces a minimum interval between calls (simple, deterministic pacing)."""

    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self._last = 0.0

    def wait(self) -> None:
        delay = self._last + self.min_interval - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._last = time.monotonic()


def get_json(
    session: requests.Session,
    url: str,
    limiter: RateLimiter,
    params: dict | None = None,
    headers: dict | None = None,
    max_retries: int = 5,
    rate_limit_backoff: float = 30.0,
):
    """GET a JSON payload, retrying on 429 and 5xx with backoff."""
    for attempt in range(max_retries):
        limiter.wait()
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)
        except requests.RequestException:
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429:
            time.sleep(rate_limit_backoff)
            continue
        if resp.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"giving up on {url} after {max_retries} attempts")
