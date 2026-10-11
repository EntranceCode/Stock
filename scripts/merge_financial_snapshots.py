"""Index financial snapshots without overwriting source records or filling unknown dates."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifests", nargs="+", type=Path)
    args = parser.parse_args()
    stock = json.loads((PROJECT / "data/normalized/20261007T082337Z_34eda55f/manifest.json").read_text(encoding="utf-8"))
    expected = {item["symbol"] for item in stock["files"]}
    files, sources, errors = {}, [], []
    unknown_dates = 0
    for path in args.manifests:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        sources.append(str(path.resolve()))
        errors.extend(snapshot.get("errors", []))
        for item in snapshot["files"]:
            symbol = item["symbol"]
            if symbol in files:
                raise ValueError(f"Overlapping snapshots need explicit version resolution: {symbol}")
            if symbol not in expected:
                raise ValueError(f"Unexpected stock outside source pool: {symbol}")
            metadata = pq.ParquetFile(item["path"]).metadata
            if metadata.num_rows != item["rows"]:
                raise ValueError(f"Financial file row count mismatch: {symbol}")
            unknown_dates += item.get("unknown_announcement_rows", 0)
            files[symbol] = item
            if len(files) % 1000 == 0:
                print("Verified financial file headers", len(files), flush=True)
    missing = sorted(expected - set(files))
    run = "merged_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = PROJECT / "data" / "external" / "financials" / run
    output.mkdir(parents=True)
    manifest = {"run_id": run, "output": str(output), "source_manifests": sources,
                "source_error_history": errors, "missing_symbols": missing,
                "complete_snapshot_coverage": not missing, "backtest_ready": False,
                "unknown_announcement_rows": unknown_dates,
                "readiness_gaps": ["original_announcement_vintages_and_restatements", "unknown_announcement_dates",
                                   "historical_share_counts_and_corporate_actions"],
                "files": [files[symbol] for symbol in sorted(files)], "errors": []}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {"output": str(output), "financial_symbols": len(files), "rows": sum(item["rows"] for item in files.values()),
               "unknown_announcement_rows": unknown_dates, "missing_symbols": missing, "backtest_ready": False}
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True, indent=2), flush=True)
    if missing:
        raise ValueError("Financial snapshot coverage incomplete")


if __name__ == "__main__":
    main()
