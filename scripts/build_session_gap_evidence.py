"""Enumerate every bar-missing session per security as a registered product.

The missing-days criterion re-reads all 4,272 registered series on every gate
run; profiling put that at 1,068 s of a 1,089 s evaluation. This builder does the
enumeration once and writes it as versioned evidence, so the criterion can verify
the registered inventory against it instead of recomputing it every time.

Only registered evidence is read: the daily series (which sessions it holds), the
registered calendars (which sessions were scheduled) and the manifest's own
session_gap_records (which gaps already carry a cause). Nothing is inferred; a
gap without a record stays unexplained and keeps the criterion failing.

Usage:
    python -B scripts/build_session_gap_evidence.py
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
from hybrid_quant.data_registry import DatasetRegistry  # noqa: E402
from hybrid_quant import readiness  # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    pq = readiness._parquet()
    import pyarrow as pa

    registry = DatasetRegistry(PROJECT / "config" / "datasets.json", PROJECT)
    path, manifest = readiness._manifest(registry, readiness.STOCK_DATASET)
    if path is None or manifest is None:
        raise SystemExit("stock_daily is not registered")
    exchange_of = readiness._security_exchange_map(registry)
    open_days_by_exchange: dict = {}
    recorded = {item.get("symbol"): set(item.get("dates") or [])
                for item in (manifest.get("session_gap_records") or [])
                if isinstance(item, dict) and item.get("symbol")}
    summaries, details = [], []
    for entry in readiness._file_entries(manifest):
        symbol = str(entry.get("symbol"))
        rows = readiness._rows(readiness._entry_path(path, entry), ("trade_date",))
        series = set()
        for row in rows:
            day = row.get("trade_date")
            day = day.date() if isinstance(day, datetime) else day
            if day.__class__.__name__ == "date":
                series.add(day)
        if not series:
            continue
        exchange = exchange_of.get(symbol)
        if exchange not in open_days_by_exchange:
            open_days_by_exchange[exchange] = readiness._open_days_for_exchange(registry, exchange)
        open_days = open_days_by_exchange[exchange]
        first, last = min(series), max(series)
        expected = [day for day in open_days if first <= day <= last]
        missing = [day for day in expected if day not in series]
        known = recorded.get(symbol, set())
        unexplained = 0
        for day in missing:
            if known and day.isoformat() in known:
                continue
            unexplained += 1
            details.append({"symbol": symbol, "trade_date": day})
        summaries.append({"symbol": symbol, "first_date": first, "last_date": last,
                          "expected_sessions": len(expected), "bar_rows": len(rows),
                          "missing_count": len(missing), "unexplained_count": unexplained})
    output = PROJECT / "data" / "external" / "session_gaps" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    files = []
    for name, rows, columns in (("symbols.parquet", summaries,
                                 ("symbol", "first_date", "last_date", "expected_sessions",
                                  "bar_rows", "missing_count", "unexplained_count")),
                                ("gaps.parquet", details, ("symbol", "trade_date"))):
        table = pa.Table.from_pylist(rows)
        if rows:
            table = table.select(columns)
        target = output / name
        pq.write_table(table, target, compression="zstd")
        files.append({"path": str(target), "rows": len(rows), "sha256": sha(target)})
    records_hash = hashlib.sha256(
        json.dumps(manifest.get("session_gap_records") or [], sort_keys=True,
                   separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    record = {"run_id": output.name, "output": str(output),
              "session_gap_records_sha256": records_hash,
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "backtest_ready": False,
              "purpose": "Enumerated bar-missing sessions so the gate verifies evidence instead of re-reading every series each run.",
              "source_manifests": [str(path)],
              "symbols": len(summaries), "missing_sessions": sum(item["missing_count"] for item in summaries),
              "unexplained_sessions": len(details), "files": files}
    (output / "manifest.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "symbols": len(summaries),
                      "missing_sessions": record["missing_sessions"],
                      "unexplained_sessions": record["unexplained_sessions"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
