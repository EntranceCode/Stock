"""Build append-safe backward-adjusted prices from raw bars and corporate actions.

Adjustment factors are anchored at each symbol first bar, so appending later
sessions or later events never changes an earlier adjusted value. Nothing here
admits data for backtesting: the corporate action layer still lacks original
announcement and payment-date validation, and events the source quarantined are
absent entirely, so the output is staged evidence with explicit exclusions.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))

PRICE_COLUMNS = ("open", "high", "low", "close")
LIMIT = 0.11  # A-share daily price limit, plus a margin for rounding


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def event_factor(previous_close: float, cash: float, multiplier: float) -> float:
    """Ratio of the theoretical ex-price to the previous close.

    Mirrors the exchange formula for cash dividends and share changes:
    ex = (previous_close - cash) / multiplier, so the factor is ex / previous_close.
    """
    if not all(math.isfinite(v) for v in (previous_close, cash, multiplier)):
        raise ValueError("Non-finite adjustment inputs")
    if previous_close <= 0 or multiplier <= 0 or cash < 0:
        raise ValueError("Invalid adjustment inputs")
    factor = (previous_close - cash) / (previous_close * multiplier)
    if not 0 < factor <= 1:
        raise ValueError("Implausible adjustment factor")
    return factor


def build_factors(dates, closes, events):
    """Return (factors, applied, skipped) for one symbol.

    dates ascending with parallel closes; events are (ex_date, cash, multiplier).
    A factor is the cumulative inverse ex-ratio of every event up to that bar.
    """
    ordered = sorted(events)
    factors, applied, skipped = [], [], []
    running, index = 1.0, 0
    for day in dates:
        while index < len(ordered) and ordered[index][0] <= day:
            ex_date, cash, multiplier = ordered[index]
            index += 1
            position = bisect_left(dates, ex_date)
            if position == 0:
                skipped.append({"ex_date": str(ex_date), "reason": "no bar precedes the ex-date"})
                continue
            try:
                factor = event_factor(closes[position - 1], cash, multiplier)
            except ValueError as exc:
                skipped.append({"ex_date": str(ex_date), "reason": str(exc)})
                continue
            running *= 1.0 / factor
            applied.append({"ex_date": str(ex_date), "previous_close": closes[position - 1],
                            "cash": cash, "multiplier": multiplier, "ex_ratio": factor})
        factors.append(running)
    return factors, applied, skipped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, help="Only process the first N symbols with events")
    args = parser.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq

    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    stock_manifest_path = PROJECT / registry["datasets"]["stock_daily"]["manifest"]
    stock = json.loads(stock_manifest_path.read_text(encoding="utf-8"))
    events_path = (PROJECT / registry["datasets"]["action_samples"]["manifest"]).parent / "events.parquet"
    events = pq.read_table(events_path).to_pylist()

    daily = {item["symbol"]: item for item in stock["files"]}
    by_symbol = {}
    unusable = []
    for event in events:
        symbol, ex_date = event.get("symbol"), event.get("ex_date")
        if ex_date is None:
            unusable.append({"symbol": symbol, "reason": "event has no ex-date",
                             "event_type": event.get("event_type")})
            continue
        if symbol not in daily:
            unusable.append({"symbol": symbol, "ex_date": str(ex_date),
                             "reason": "symbol absent from the daily dataset"})
            continue
        if str(ex_date) > str(daily[symbol]["end"]):
            unusable.append({"symbol": symbol, "ex_date": str(ex_date),
                             "reason": "ex-date is after the last daily bar"})
            continue
        multiplier = float(event.get("shares_multiplier") or 1.0)
        cash = float(event.get("cash_per_old_unit_gross") or 0.0)
        by_symbol.setdefault(symbol, []).append((ex_date, cash, multiplier))

    output = PROJECT / "data/external/adjusted_prices" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    (output / "stocks").mkdir(parents=True)
    files, applied_total, skipped_total = [], [], []
    beyond_before = beyond_after = events_checked = 0
    symbols = [item for item in stock["files"] if item["symbol"] in by_symbol]
    symbols.sort(key=lambda item: item["symbol"])
    if args.limit:
        symbols = symbols[:args.limit]
    for item in symbols:
        table = pq.read_table(item["output"], columns=["trade_date", *PRICE_COLUMNS])
        dates = table.column("trade_date").to_pylist()
        closes = [float(v) for v in table.column("close").to_pylist()]
        factors, applied, skipped = build_factors(dates, closes, by_symbol[item["symbol"]])
        applied_total.extend({"symbol": item["symbol"], **entry} for entry in applied)
        skipped_total.extend({"symbol": item["symbol"], **entry} for entry in skipped)
        for entry in applied:
            position = bisect_left(dates, __import__("datetime").date.fromisoformat(entry["ex_date"]))
            if position == 0 or position >= len(dates):
                continue
            events_checked += 1
            raw = closes[position] / closes[position - 1] - 1
            adjusted = (closes[position] * factors[position]) / (closes[position - 1] * factors[position - 1]) - 1
            beyond_before += abs(raw) > LIMIT
            beyond_after += abs(adjusted) > LIMIT
        frame = {"trade_date": dates, "symbol": [item["symbol"]] * len(dates), "adjustment_factor": factors,
                 "close_raw": closes}
        for name in PRICE_COLUMNS:
            frame[name + "_adjusted"] = [float(v) * f for v, f in zip(table.column(name).to_pylist(), factors)]
        path = output / "stocks" / (item["symbol"] + "_adjusted.parquet")
        provenance = {"symbol": item["symbol"], "price_basis": "backward_adjusted_for_cash_and_shares",
                      "backtest_ready": False, "events_applied": len(applied), "events_skipped": len(skipped),
                      "source": item["output"], "source_sha256": item["output_sha256"],
                      "anchor": "factor 1.0 at the first bar; later sessions cannot change earlier values"}
        out = pa.Table.from_pylist([{**row, "trade_date": row["trade_date"]} for row in
                                    [{k: frame[k][i] for k in frame} for i in range(len(dates))]])
        out = out.replace_schema_metadata({b"stock.provenance": json.dumps(provenance).encode()})
        pq.write_table(out, path, compression="zstd")
        files.append({"symbol": item["symbol"], "rows": len(dates), "path": str(path), "sha256": sha(path),
                      "events_applied": len(applied), "events_skipped": len(skipped)})
    manifest = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
                "backtest_ready": False, "price_basis": "backward_adjusted_for_cash_and_shares",
                "readiness_gaps": ["original_announcement_validation", "payment_dates_and_tax",
                                   "ratio_precision_and_effective_date", "quarantined_events_absent_from_source",
                                   "security_level_status"],
                "source_manifests": [{"path": str(stock_manifest_path), "sha256": sha(stock_manifest_path)},
                                     {"path": str(events_path), "sha256": sha(events_path)}],
                "symbols_written": len(files), "rows": sum(item["rows"] for item in files),
                "events_applied": len(applied_total), "events_skipped": len(skipped_total),
                "events_with_ex_date": sum(1 for event in events if event.get("ex_date") is not None),
                "events_unusable_count": len(unusable),
                "events_unusable_by_reason": {reason: sum(1 for item in unusable if item["reason"] == reason)
                                              for reason in sorted({item["reason"] for item in unusable})},
                "events_unusable": unusable[:200],
                "ex_dates_beyond_price_limit_before_adjustment": beyond_before,
                "ex_dates_beyond_price_limit_after_adjustment": beyond_after,
                "ex_dates_checked": events_checked,
                "files": files, "skipped_detail": skipped_total[:200]}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("output", "symbols_written", "rows", "events_applied",
                                               "events_skipped", "ex_dates_checked",
                                               "ex_dates_beyond_price_limit_before_adjustment",
                                               "ex_dates_beyond_price_limit_after_adjustment")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
