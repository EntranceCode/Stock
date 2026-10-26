"""Chunked acquisition supervisor: keep one serial connection at the provider.

BaoStock's public server cross-talks when several sessions share one IP, so the
acquisition runs one serial process at a time. This supervisor runs bounded
chunks, each starting at the first symbol no earlier run has captured, so a
terminated or failed chunk resumes exactly where the evidence stopped instead
of re-fetching covered symbols. A final retry pass re-attempts unresolved
symbols with fresh chunks.

Usage:  python -B .acquisition/supervise_acquisition.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
HISTORY = PROJECT / "data/external/baostock_history"
CHUNK = 150


def requests_list() -> list[dict]:
    """The full candidate list: every run records the slice it was asked for,
    and the offset-0 batch holds all of them."""
    lists = []
    for run in HISTORY.iterdir():
        path = run / "requests.json"
        if path.is_file():
            try:
                items = json.loads(path.read_text(encoding="utf-8"))
            except ValueError:
                continue
            if isinstance(items, list):
                lists.append(items)
    if not lists:
        raise SystemExit("no run records its requests")
    return max(lists, key=len)


def covered_symbols() -> set[str]:
    covered: set[str] = set()
    for run in HISTORY.iterdir():
        prices = run / "prices"
        if prices.is_dir():
            covered.update(path.name.split("_")[0] for path in prices.iterdir())
    return covered


def first_gap(requests: list[dict], covered: set[str]) -> int | None:
    return next((index for index, item in enumerate(requests)
                 if item["code"][3:] not in covered), None)


def run_chunk(offset: int, limit: int) -> int:
    # A raised consecutive-failure ceiling lets one chunk grind through the
    # provider's stall bursts instead of aborting after five and relaunching
    # into the same burst; the wire guard still rejects every mismatched reply.
    result = subprocess.run([sys.executable, "-B",
                             str(PROJECT / "scripts/fetch_baostock_history.py"),
                             "--mode", "all", "--offset", str(offset),
                             "--limit", str(limit),
                             "--max-consecutive-failures", "60"],
                            cwd=PROJECT, capture_output=True, text=True)
    print(result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "(no output)",
          flush=True)
    return result.returncode


def main() -> int:
    while True:
        requests = requests_list()
        gap = first_gap(requests, covered_symbols())
        if gap is None:
            print("every requested symbol is captured", flush=True)
            return 0
        remaining = len(requests) - gap
        print(f"resuming at index {gap} ({requests[gap]['code']}); "
              f"{remaining} symbol(s) remain", flush=True)
        run_chunk(gap, min(CHUNK, remaining))
        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
