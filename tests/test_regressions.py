"""One test per bug fixed during the build, reproducing the conditions that caused it.

Bugs already covered elsewhere: 2 (no logIndex) test_ingest::number_within_tx, 4 (early
stop) test_ingest::spurious_empty / truncated, 9 (dates) test_ingest::parse_market_chart,
10 (no data != miss) test_evaluation::window_hits; corrections for z floor and LOF
duplicates in test_models, volume glitches in test_clickhouse_tables.
"""
import json
import os
import sys
import threading
import time

import pytest
import requests

from chainsignal.ingest import etherscan as es_mod
from chainsignal.ingest import http
from chainsignal.ingest.coingecko import pull_market_data


class FakeResponse:
    def __init__(self, status=200, payload=None, text="{}"):
        self.status_code, self._payload, self.text = status, payload, text

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(self.status_code)


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), 0

    def get(self, *a, **k):
        self.calls += 1
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)


def test_bug1_cached_small_coin_list_does_not_cap_a_bigger_run(tmp_path):
    class Client:
        def top_coins(self, n):
            return [{"id": f"c{i}"} for i in range(n)]

        def market_chart(self, coin_id, days):
            return {"prices": []}

    (tmp_path / "coins_markets.json").write_text(json.dumps([{"id": "c0"}] * 5))  # from a 5-coin trial
    stats = pull_market_data(Client(), 20, tmp_path)
    assert stats["coins"] == 20


def test_bug3_retries_report_the_last_cause():
    session = FakeSession([FakeResponse(503), requests.ConnectionError("reset"), FakeResponse(200, None, "<html>")])
    with pytest.raises(RuntimeError, match="last error: non-JSON body"):
        http.get_json(session, "https://x", http.RateLimiter(1000), max_retries=3)
    assert session.calls == 3


def test_bug3_recovers_after_transient_errors():
    session = FakeSession([FakeResponse(502), FakeResponse(429), FakeResponse(200, {"ok": 1})])
    assert http.get_json(session, "https://x", http.RateLimiter(1000), max_retries=5) == {"ok": 1}


def test_bug5_spark_workers_use_this_interpreter(monkeypatch):
    from pyspark.sql import SparkSession
    from chainsignal.spark import get_spark
    active = SparkSession.getActiveSession()
    if active:  # a session left by another test would hide the bug: start a fresh one
        active.stop()
    monkeypatch.setenv("PYSPARK_PYTHON", "python3")  # the PATH default that pointed at Python 3.8
    spark = get_spark("chainsignal-regression")
    assert os.environ["PYSPARK_PYTHON"] == sys.executable
    worker = spark.sparkContext.parallelize([0], 1).map(lambda _: __import__("sys").version_info[:2]).first()
    assert tuple(worker) == sys.version_info[:2]


def test_bug6_rate_limit_answers_are_retried_not_fatal(monkeypatch):
    answers = [{"status": "0", "message": "NOTOK", "result": "Max calls per sec rate limit reached (5/sec)"}] * 3
    answers.append({"status": "1", "message": "OK", "result": [{"contractAddress": "0xa"}]})
    monkeypatch.setattr(es_mod, "get_json", lambda *a, **k: answers.pop(0))
    client = es_mod.EtherscanClient("key", calls_per_second=1000)
    assert client.contract_creations(["0xa"]) == [{"contractAddress": "0xa"}]


def test_bug6_no_data_found_is_an_empty_answer(monkeypatch):
    monkeypatch.setattr(es_mod, "get_json", lambda *a, **k: {"status": "0", "message": "No data found", "result": []})
    assert es_mod.EtherscanClient("key", calls_per_second=1000).contract_creations(["0xa"]) == []


def test_rate_limiter_spaces_calls_across_threads(monkeypatch):
    monkeypatch.undo()  # real sleep for this one
    limiter, stamps = http.RateLimiter(50), []  # 20 ms apart
    lock = threading.Lock()

    def work():
        limiter.wait()
        with lock:
            stamps.append(time.monotonic())

    threads = [threading.Thread(target=work) for _ in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    gaps = [b - a for a, b in zip(sorted(stamps), sorted(stamps)[1:])]
    assert min(gaps) >= 0.018
