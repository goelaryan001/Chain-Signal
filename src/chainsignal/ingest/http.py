"""Shared HTTP plumbing for the ingestion clients: pacing and retries."""
import threading
import time

import requests


class RateLimiter:
    """Enforces a minimum interval between calls; safe to share across threads."""

    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:  # sleeping inside the lock is what spaces out concurrent callers
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
    """GET a JSON payload, retrying on network errors, 429 and 5xx with capped backoff."""
    last_error = ""
    for attempt in range(max_retries):
        limiter.wait()
        try:
            resp = session.get(url, params=params, headers=headers, timeout=60)
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(min(2 ** attempt, 60))
            continue
        if resp.status_code == 429:
            last_error = "HTTP 429"
            time.sleep(rate_limit_backoff)
            continue
        if resp.status_code >= 500:
            last_error = f"HTTP {resp.status_code}"
            time.sleep(min(2 ** attempt, 60))
            continue
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:  # e.g. an HTML error page served with status 200
            last_error = f"non-JSON body: {resp.text[:80]!r}"
            time.sleep(min(2 ** attempt, 60))
    raise RuntimeError(f"giving up on {url} after {max_retries} attempts (last error: {last_error})")
