"""Label addresses as contract or wallet (EOA) with Etherscan's getcontractcreation.

Why: the busiest back-and-forth pairs in the data are DEX pools, routers and bots,
which trade with everyone by design. Wash-trading features must look at
wallet-to-wallet activity only, and an address's look gives no hint of what it
is (an "ordinary" sender in our Phase 1 sample was the Wrapped LOOKS contract).

The endpoint takes 5 addresses per call and returns entries only for contracts.
Results are appended to a JSON-lines cache, one line per address, so the job is
resumable and never asks about the same address twice.

Limitation: smart-contract wallets (Safe multisigs, ERC-4337 accounts) are
contracts too, so they are grouped with pools and routers here.
"""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading

from chainsignal.ingest.etherscan import EtherscanClient
from chainsignal.ingest.http import RateLimiter

BATCH = 5


def label_batch(addresses: list[str], creations: list[dict]) -> list[dict]:
    """One label row per address: contracts from the API answer, everything else a wallet."""
    by_address = {c["contractAddress"].lower(): c for c in creations}
    rows = []
    for a in addresses:
        c = by_address.get(a.lower())
        rows.append({
            "address": a.lower(),
            "is_contract": c is not None,
            "creator": (c or {}).get("contractCreator", "").lower(),
            "factory": (c or {}).get("contractFactory", "").lower(),
            "created_block": int((c or {}).get("blockNumber") or 0),
        })
    return rows


def label_addresses(api_key: str, addresses: list[str], cache_path: Path,
                    calls_per_second: float = 3, threads: int = 6) -> dict:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if cache_path.exists():
        done = {json.loads(line)["address"] for line in cache_path.open()}
    todo = sorted({a.lower() for a in addresses} - done)
    batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]

    limiter = RateLimiter(calls_per_second)  # one limiter shared by all threads
    local = threading.local()
    write_lock = threading.Lock()
    progress = {"batches": 0}

    def work(batch: list[str]) -> None:
        if not hasattr(local, "client"):
            local.client = EtherscanClient(api_key, limiter=limiter)
        rows = label_batch(batch, local.client.contract_creations(batch))
        with write_lock:
            with cache_path.open("a") as f:
                f.writelines(json.dumps(r) + "\n" for r in rows)
            progress["batches"] += 1
            if progress["batches"] % 200 == 0:
                print(f"  labels: {progress['batches']:,}/{len(batches):,} batches")

    with ThreadPoolExecutor(threads) as pool:
        list(pool.map(work, batches))  # list() re-raises any worker exception
    return {"requested": len(set(addresses)), "already_cached": len(done), "fetched": len(todo)}
