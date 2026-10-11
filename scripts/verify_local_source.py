"""Compare the saved public reference snapshot with local stock daily bars."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sys
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import pyarrow.parquet as pq
from fetch_http_reference import china_today, iso_date, new_snapshot_directory, save_json_once


def decode_daily_dates(raw: pd.Series, as_of: str | None = None) -> pd.Series:
    """Decode date only, requiring the documented 15:00 wall-clock bucket rule.

    This does not recover lost seconds or produce a genuine UTC timestamp.
    """
    as_of = iso_date(as_of) if as_of is not None else china_today()
    if raw.isna().any() or not pd.api.types.is_integer_dtype(raw):
        raise ValueError("Daily raw time must contain non-null integers")
    dates = pd.to_datetime(raw * 1000, unit="s", errors="coerce").dt.normalize()
    if dates.isna().any() or (dates < pd.Timestamp("1990-01-01")).any() or (dates > pd.Timestamp(as_of)).any():
        raise ValueError("Daily dates are outside supported bounds")
    expected = dates.map(lambda d: (d.value // 1_000_000_000 + 15 * 3600) // 1000)
    if not np.array_equal(raw.to_numpy(), expected.to_numpy()):
        raise ValueError("Daily timestamp does not match the 15:00 wall-clock floor bucket")
    if dates.duplicated().any() or not dates.is_monotonic_increasing:
        raise ValueError("Daily dates must be unique and increasing; never deduplicate silently")
    # Validate against a historical exchange calendar later. A modern weekday
    # rule is not sufficient for the early history present in these files.
    return dates


def main(argv=None):
    base = PROJECT / "reports" / "source-verification"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, default=base / "http-reference.json",
                        help="Legacy fixed snapshot or an explicit versioned http-reference.json")
    parser.add_argument("--source", type=Path, default=Path(r"D:\vscode\data\A股数据\parquet\stocks"))
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    args = parser.parse_args(argv)
    reference_path = args.reference.resolve()
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    if reference["errors"] or reference.get("complete") is False:
        raise ValueError("Reference snapshot contains errors")
    if reference["end"] > args.as_of:
        raise ValueError("Reference window extends beyond as-of")
    source_root = args.source.resolve()
    results, details = [], []
    for symbol, variants in reference["securities"].items():
        for variant in variants.values():
            if "source" in variant:
                raw_path = Path(variant["source"])
                if hashlib.sha256(raw_path.read_bytes()).hexdigest() != variant["source_sha256"]:
                    raise ValueError("Reference raw response hash does not match")
        local = pq.read_table(source_root / f"{symbol[2:]}_daily.parquet").to_pandas()
        local["trade_date"] = decode_daily_dates(local.time, args.as_of).dt.strftime("%Y-%m-%d")
        local = local.set_index("trade_date")
        reference_rows = variants["day"]["bars"]
        qfq = {row[0]: np.array([float(row[i]) for i in (1, 3, 4, 2)]) for row in variants["qfqday"]["bars"]}
        matches, volume_diffs, adjusted_different = 0, [], 0
        for row in reference_rows:
            date = row[0]
            if date not in local.index:
                raise ValueError(f"Reference date missing locally: {symbol} {date}")
            bar = local.loc[date]
            expected = np.array([float(row[i]) for i in (1, 3, 4, 2)])
            actual = bar[["open", "high", "low", "close"]].to_numpy(dtype=float)
            matched = bool(np.allclose(actual, expected, rtol=2e-6, atol=1e-4))
            matches += matched
            volume_diff = float(bar.tick_volume) - float(row[5])
            volume_diffs.append(volume_diff)
            qfq_different = date in qfq and not np.allclose(actual, qfq[date], rtol=2e-6, atol=1e-4)
            adjusted_different += bool(qfq_different)
            details.append({"symbol": symbol, "trade_date": date, "ohlc_match_unadjusted": matched,
                            "local_volume_raw": int(bar.tick_volume), "reference_volume_raw": float(row[5]),
                            "volume_difference": volume_diff, "differs_from_current_qfq": bool(qfq_different)})
        results.append({"symbol": symbol, "reference_rows": len(reference_rows), "ohlc_matches": matches,
                        "volume_max_abs_difference": max(abs(x) for x in volume_diffs),
                        "rows_different_from_current_qfq": adjusted_different})
    location = new_snapshot_directory(base, "verification_")
    summary = {"verified_at": args.as_of, "verification_executed_at": datetime.now(timezone.utc).isoformat(),
               "output": str(location), "source": str(source_root), "reference": str(reference_path),
               "reference_sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
               "reference_window": [reference["start"], reference["end"]], "symbols": results,
               "sample_check_passed": len(results) == 5 and all(x["ohlc_matches"] == x["reference_rows"] and x["volume_max_abs_difference"] <= 1 for x in results),
               "date_rule": "floor(naive wall-clock date 15:00 epoch seconds / 1000), date only",
               "price_basis": "Unadjusted OHLC matches on sampled dates only; no full-history corporate-action verification",
               "volume_rule": "Raw daily volumes match reference within 1; hands-to-shares multiplier 100 is a source-based inference, not a per-file historical proof"}
    pd.DataFrame(details).to_csv(location / "reference-comparison.csv", index=False, encoding="utf-8-sig")
    save_json_once(location / "verification.json", summary)
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    if not summary["sample_check_passed"]:
        raise ValueError("Reference checks failed; do not normalize the batch")
    return location


if __name__ == "__main__":
    main()
