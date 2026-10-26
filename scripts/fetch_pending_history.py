"""Plan pending price requests against the CURRENT evidence, then collect them.

``retry_baostock_history.py`` refuses to reuse an old request inventory once its
source manifests no longer hash to what they hashed when the plan was made, and
that refusal is right: a plan against a different inventory is not a plan for
this one. Recording session-gap causes rewrote the stock_daily manifest, so this
script plans fresh against the current registered evidence and lets the normal
collector write its own, separately evidenced batch.

Usage:
    python -B scripts/fetch_pending_history.py
"""
from __future__ import annotations

import json
from pathlib import Path

import fetch_baostock_history as collector

# The registered manifest carries no exchange column, so the venue comes from the
# code families the project already uses: 60/68 are SSE, 00/30 are SZSE. Anything
# else is left out of the plan rather than guessed.
SSE_PREFIXES = ("60", "68")
SZSE_PREFIXES = ("00", "30")
PREFIXED_ETF = ("15", "16", "18", "50", "51", "52", "56", "58")


def venue_prefix(symbol: str) -> str | None:
    if symbol.startswith(SSE_PREFIXES):
        return "sh"
    if symbol.startswith(SZSE_PREFIXES):
        return "sz"
    return None


def acquired_codes() -> set:
    """Codes already captured by any batch that still has a manifest."""
    codes = set()
    for manifest_path in collector.PROJECT.glob("data/external/baostock_history/*/manifest.json"):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for entry in manifest.get("files", []):
            code = (entry.get("source_request") or {}).get("code")
            if isinstance(code, str):
                codes.add(code)
    return codes


def requests_for(mode, start, end):
    """Pending registered stocks only; the plan and its sources are recorded."""
    registry_path = collector.PROJECT / "config" / "datasets.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    manifest_path = (collector.PROJECT / registry["datasets"]["stock_daily"]["manifest"]).resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    have = acquired_codes()
    items = []
    for entry in manifest.get("files", []):
        symbol = str(entry.get("symbol", ""))
        if not symbol or symbol.startswith(PREFIXED_ETF):
            continue
        prefix = venue_prefix(symbol)
        if prefix is None:
            continue
        code = prefix + "." + symbol
        if code in have:
            continue
        items.append({"code": code, "start": start, "end": end,
                      "selection": "pending_against_current_inventory"})
    print("pending requests: " + str(len(items)), flush=True)
    return items, [manifest_path, registry_path]


def main() -> int:
    collector.requests_for = requests_for
    return collector.main(["--mode", "all", "--start", "2018-01-01",
                           "--end", "2026-07-07", "--pause", "0.5"])


if __name__ == "__main__":
    raise SystemExit(main())
