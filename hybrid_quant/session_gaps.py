"""Answer the missing-days question from enumerated evidence instead of a re-read."""
from __future__ import annotations

from pathlib import Path
from typing import Mapping

DATASET = "session_gaps"


def summarise(registry, dataset: str, daily_manifest: Mapping, recorded: list,
              stock_dataset: str):
    """(passed, detail, evidence) from the enumerated product, or None.

    The product is trusted only when it reconciles with the registered inventory:
    the symbol sets must match and every symbol must satisfy
    bars + missing == expected. A stale product therefore makes the gate fall back
    to the slow enumeration, never to a wrong answer.
    """
    if dataset != stock_dataset:
        return None
    from . import readiness as gate

    gap_path, gap_manifest = gate._manifest(registry, DATASET)
    if gap_path is None or not isinstance(gap_manifest, Mapping):
        return None
    summary = None
    for entry in gate._file_entries(gap_manifest):
        if str(entry.get("path", "")).endswith("symbols.parquet"):
            summary = entry
            break
    if summary is None:
        return None
    try:
        rows = gate._rows(gate._entry_path(gap_path, summary),
                          ("symbol", "expected_sessions", "bar_rows", "missing_count",
                           "unexplained_count"))
        by_symbol = {str(row["symbol"]): row for row in rows}
        inventory = {str(entry["symbol"]): entry for entry in gate._file_entries(daily_manifest)}
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if set(by_symbol) != set(inventory):
        return None
    incomplete = [item for item in recorded
                  if not (isinstance(item, dict) and item.get("symbol")
                          and isinstance(item.get("dates"), list) and item.get("cause"))]
    missing_total, affected, unrecorded = 0, [], []
    for symbol in inventory:
        row = by_symbol[symbol]
        missing = int(row["missing_count"])
        unexplained = int(row["unexplained_count"])
        # A registered series may hold dates that were not scheduled sessions,
        # so bars + missing need not equal expected; the verdict only needs the
        # enumerated counts to be self-consistent and the symbol sets to match.
        if missing < 0 or unexplained < 0 or unexplained > missing:
            return None
        if missing:
            missing_total += missing
            affected.append(symbol)
            if unexplained == missing:
                unrecorded.append(symbol)
    passed = not unrecorded and not incomplete
    names = ", ".join(sorted(set(unrecorded))[:6])
    detail = (str(missing_total) + " missing session(s) across " + str(len(affected))
              + " securities (from registered " + DATASET + " evidence); "
              + str(len(recorded)) + " session_gap record(s) registered; "
              + str(len(unrecorded)) + " unexplained (" + (names or "none") + "); "
              + str(len(incomplete)) + " record(s) lack symbol/dates/cause")
    return passed, detail, str(gap_path)
