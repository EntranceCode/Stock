"""Versioned, lossless daily staging. It never declares unverified data tradable."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from verify_local_source import decode_daily_dates

READINESS_GAPS = ["full_history_adjustment_and_corporate_actions", "historical_security_status",
                  "historical_exchange_calendar", "historical_volume_units", "turnover_amount"]


def normalize_frame(frame: pd.DataFrame, code: str, as_of: str) -> pd.DataFrame:
    if not (len(code) == 6 and code.isdigit() and code.startswith(("00", "30", "60"))):
        raise ValueError("Unsupported stock code; indexes and ETFs require separate adapters")
    dates = decode_daily_dates(frame.time, as_of)
    values = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("Invalid stock prices")
    o, h, lo, c = values.T
    tol = np.maximum(np.abs(values).max(axis=1) * 2e-6, 1e-6)
    if ((h + tol < np.maximum.reduce([o, lo, c])) | (lo - tol > np.minimum.reduce([o, h, c]))).any():
        raise ValueError("Invalid OHLC relationship")
    if frame.tick_volume.isna().any() or (frame.tick_volume < 0).any() or (frame.tick_volume > np.iinfo(np.int64).max // 100).any():
        raise ValueError("Invalid or overflowing volume")
    result = frame[["open", "high", "low", "close"]].copy()
    result.insert(0, "trade_date", dates.dt.date)
    result.insert(1, "symbol", code)
    result.insert(2, "exchange", "SSE" if code.startswith("60") else "SZSE")
    result["source_time"] = frame.time
    result["volume_raw"] = frame.tick_volume
    result["volume_shares_estimate"] = frame.tick_volume * 100
    return result


def require_backtest_ready(path: Path) -> None:
    """Mandatory gate for future strategy loaders; inspection staging is rejected."""
    metadata = pq.read_schema(path).metadata or {}
    evidence = json.loads(metadata.get(b"stock.provenance", b"{}"))
    if evidence.get("backtest_ready") is not True or evidence.get("readiness_gaps"):
        raise ValueError("Data is staged for inspection only; readiness gaps must be resolved before backtesting")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(r"D:\vscode\data\A股数据\parquet"))
    parser.add_argument("--limit", type=int, default=None, help="Optional smoke-check file limit")
    parser.add_argument("--verification", type=Path, default=PROJECT / "reports/source-verification/verification.json",
                        help="Saved verification report; old default remains available")
    args = parser.parse_args()
    evidence_path = args.verification.resolve()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    reference = Path(evidence["reference"])
    if not evidence.get("sample_check_passed") or hashlib.sha256(reference.read_bytes()).hexdigest() != evidence["reference_sha256"]:
        raise ValueError("Reference checks missing, failed or stale")
    checked_reference_rows = sum(item["reference_rows"] for item in evidence["symbols"])
    source = args.source.resolve()
    base = PROJECT / "data" / "normalized"
    if source == base.resolve() or source in base.resolve().parents:
        raise ValueError("Output must not be inside source")
    paths = sorted((source / "stocks").glob("*_daily.parquet"))
    if args.limit is not None:
        if args.limit <= 0:
            raise ValueError("Limit must be positive")
        paths = paths[:args.limit]
    if not paths:
        raise ValueError("No stock daily files found")
    if shutil.disk_usage(PROJECT).free < sum(p.stat().st_size for p in paths) * 5:
        raise ValueError("Insufficient free disk space for versioned output")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = base / run_id
    (output / "stocks").mkdir(parents=True)
    manifest = {"run_id": run_id, "source": str(source), "output": str(output),
                "verification": str(evidence_path), "verification_sha256": hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
                "date_rule": evidence["date_rule"], "source_reference_sha256": evidence["reference_sha256"],
                "backtest_ready": False, "readiness_gaps": READINESS_GAPS,
                "files": [], "quarantine": [], "source_rows": 0, "normalized_rows": 0,
                "complete": False, "limit": args.limit}
    for path in source.rglob("*.parquet"):
        if path.parent.name == "indices" or not path.stem.endswith("_daily"):
            manifest["quarantine"].append({"source": str(path), "reason": "index_corruption" if path.parent.name == "indices" else "intraday_time_precision_unverified"})
    for i, path in enumerate(paths, 1):
        try:
            source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            raw = pq.read_table(path).to_pandas()
            manifest["source_rows"] += len(raw)
            code = path.stem.split("_")[0]
            result = normalize_frame(raw, code, evidence["verified_at"])
            provenance = {"source": str(path), "sha256": source_hash, "source_rows": len(raw),
                          "date_rule": evidence["date_rule"], "date_confidence": "batch_rule_checked_against_saved_reference_sample",
                          "checked_reference_rows": checked_reference_rows,
                          "timestamp_precision": "calendar_date_only; original seconds not recoverable",
                          "price_basis": "unadjusted_consistent_on_reference_samples; full_history_unknown",
                          "volume_shares_estimate_rule": "volume_raw * 100, source-inferred; not a verified volume_shares field",
                          "backtest_ready": False, "readiness_gaps": READINESS_GAPS}
            table = pa.Table.from_pandas(result, preserve_index=False)
            table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps(provenance).encode()})
            destination = output / "stocks" / path.name
            pq.write_table(table, destination, compression="zstd")
            check = pq.read_table(destination).to_pandas()
            if len(check) != len(raw) or not np.array_equal(check.source_time, raw.time):
                raise ValueError("Read-back validation failed: row count or raw time changed")
            for field in ("open", "high", "low", "close"):
                if not np.array_equal(check[field], raw[field]):
                    raise ValueError(f"Read-back validation failed: {field} changed")
            if not np.array_equal(check.volume_raw, raw.tick_volume):
                raise ValueError("Read-back validation failed: raw volume changed")
            manifest["normalized_rows"] += len(check)
            manifest["files"].append({"symbol": code, "source": str(path), "source_sha256": source_hash,
                                      "output": str(destination), "output_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                                      "rows": len(check), "start": str(check.trade_date.min()), "end": str(check.trade_date.max())})
        except Exception as exc:
            manifest["quarantine"].append({"source": str(path), "reason": "daily_validation_failed", "error": str(exc)})
        if i % 500 == 0:
            print(f"Staged {i}/{len(paths)}; accepted {len(manifest['files'])}", flush=True)
    manifest["complete"] = len(manifest["files"]) == len(paths)
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {k: manifest[k] for k in ["run_id", "output", "source_rows", "normalized_rows", "complete", "backtest_ready"]}
    summary["normalized_files"] = len(manifest["files"])
    summary["quarantine_files"] = len(manifest["quarantine"])
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True, indent=2), flush=True)
    if not manifest["complete"]:
        raise RuntimeError("Some daily files failed validation; see manifest quarantine")


if __name__ == "__main__":
    main()
