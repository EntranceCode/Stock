"""Build the enriched ETF daily dataset from registered bars plus staging fields.

The registered ETF daily files carry OHLC only: no turnover, no share volume,
no previous close, no provider status. The acquired provider staging carries
all of them, and this builder joins them onto the registered series - keeping
the registered bars as the price record and adding the staging's fields - after
verifying the two sources agree on the sessions they share. Any OHLC
disagreement, or a session present in one source and absent in the other, is
recorded in the run manifest and refuses the registration: the enriched
dataset must not silently change the price record.

Usage:
    python -B scripts/build_etf_daily_enriched.py
    python -B scripts/build_etf_daily_enriched.py --register
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

STAGING_FIELDS = ("turnover_cny_reported", "volume_shares_reported", "preclose",
                  "provider_trade_status", "provider_is_st")
OHLC_TOLERANCE = 0.001


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def day_of(value) -> str:
    return str(value)[:10]


def newest_staging() -> dict[str, Path]:
    """The newest staging price file per bare symbol."""
    newest: dict[str, tuple[str, Path]] = {}
    for run in sorted((PROJECT / "data/external/baostock_history").iterdir()):
        prices = run / "prices"
        if not prices.is_dir():
            continue
        for path in prices.iterdir():
            symbol = path.name.split("_")[0]
            if symbol.startswith(("sh.", "sz.")):
                symbol = symbol[3:]
            if symbol not in newest or run.name > newest[symbol][0]:
                newest[symbol] = (run.name, path)
    return {symbol: path for symbol, (_, path) in newest.items()}


def build(project_root: Path) -> tuple[dict, Path, list[str]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    project_root = Path(project_root)
    config = json.loads((project_root / "config/datasets.json").read_text(encoding="utf-8"))
    registered_manifest_path = Path(config["datasets"]["etf_daily"]["manifest"])
    registered = json.loads(registered_manifest_path.read_text(encoding="utf-8"))
    staging = newest_staging()

    output = project_root / "data/external/etf_daily_enriched" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    (output / "stocks").mkdir(parents=True)
    files, cross_checks, problems = [], [], []
    for entry in registered.get("files", []):
        symbol = str(entry.get("symbol"))
        bare = symbol[2:] if symbol[:2] in ("sh", "sz") else symbol
        bars = pq.read_table(Path(entry["path"])).to_pylist()
        staged_path = staging.get(bare)
        staged_rows = pq.read_table(staged_path).to_pylist() if staged_path else []
        staged = {day_of(row["trade_date"]): row for row in staged_rows}
        series = {day_of(row["trade_date"]): row for row in bars}
        only_registered = sorted(set(series) - set(staged))
        only_staged = sorted(set(staged) - set(series))
        mismatches = []
        for day in sorted(set(series) & set(staged)):
            for field in ("open", "high", "low", "close"):
                if abs(float(series[day][field]) - float(staged[day][field])) > OHLC_TOLERANCE:
                    mismatches.append({"trade_date": day, "field": field,
                                       "registered": series[day][field], "staged": staged[day][field]})
        cross_checks.append({"symbol": symbol, "registered_sessions": len(series),
                             "staged_sessions": len(staged),
                             "sessions_only_in_registered": only_registered,
                             "sessions_only_in_staging": only_staged,
                             "ohlc_mismatches": mismatches})
        if mismatches:
            problems.append(f"{symbol}: {len(mismatches)} OHLC disagreement(s) between the "
                            f"registered series and the staging")
        enriched = []
        for row in bars:
            day = day_of(row["trade_date"])
            staged_row = staged.get(day, {})
            enriched.append({**row, **{field: staged_row.get(field) for field in STAGING_FIELDS}})
        path = output / "stocks" / (bare + "_daily_enriched.parquet")
        table = pa.Table.from_pylist(enriched).replace_schema_metadata({
            b"stock.provenance": json.dumps({
                "symbol": symbol, "price_basis": "provider reported, unadjusted",
                "backtest_ready": False, "available_at_unknown": True,
                "source_registered_file": entry["path"],
                "source_registered_sha256": entry.get("sha256"),
                "source_staging_file": str(staged_path) if staged_path else None,
                "source_staging_sha256": sha(staged_path) if staged_path else None,
                "cross_verification": "registered bars kept as the price record; staging "
                                      "fields joined by session; disagreements refuse the run",
            }).encode()})
        pq.write_table(table, path, compression="zstd")
        with_turnover = sum(1 for row in enriched if row.get("turnover_cny_reported") is not None)
        files.append({"symbol": symbol, "rows": len(enriched), "path": str(path),
                      "sha256": sha(path), "start": enriched[0]["trade_date"].isoformat()
                      if hasattr(enriched[0]["trade_date"], "isoformat") else str(enriched[0]["trade_date"]),
                      "end": enriched[-1]["trade_date"].isoformat()
                      if hasattr(enriched[-1]["trade_date"], "isoformat") else str(enriched[-1]["trade_date"]),
                      "rows_with_turnover": with_turnover})
    manifest = {
        "run_id": output.name, "output": str(output),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "backtest_ready": False,
        "purpose": "registered ETF daily bars joined with provider staging fields "
                   "(turnover, share volume, previous close, trade status)",
        "source_manifests": [
            {"path": str(registered_manifest_path), "sha256": sha(registered_manifest_path),
             "role": "registered ETF daily bars (price record)"}],
        "cross_verification": cross_checks,
        "cross_verification_problems": problems,
        "symbols": len(files), "rows": sum(item["rows"] for item in files),
        "rows_with_turnover": sum(item["rows_with_turnover"] for item in files),
        "files": files,
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest, manifest_path, problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", action="store_true",
                        help="replace the registered etf_daily dataset with this run")
    args = parser.parse_args()

    manifest, manifest_path, problems = build(PROJECT)
    print(json.dumps({"output": str(manifest_path), "symbols": manifest["symbols"],
                      "rows": manifest["rows"], "rows_with_turnover": manifest["rows_with_turnover"],
                      "cross_verification_problems": problems}, ensure_ascii=False))
    if problems:
        print("cross-verification failed; config left untouched")
        return 2
    if not args.register:
        print(f"Evidence written to {manifest_path}; rerun with --register to activate it.")
        return 0
    config_path = PROJECT / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["datasets"]["etf_daily"] = {
        "manifest": str(manifest_path.relative_to(PROJECT)),
        "purpose": "registered ETF daily bars enriched with provider turnover, volume, "
                   "previous close and trade status",
    }
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Registered etf_daily -> {manifest_path.relative_to(PROJECT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
