"""Index source-reported daily status separately from account tradability."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))


def inside(path):
    path = path.resolve()
    if not path.is_relative_to(PROJECT):
        raise ValueError("Status evidence must stay inside the project")
    return path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifests", type=Path, nargs="+", required=True)
    args = parser.parse_args(argv)
    import pyarrow.parquet as pq
    files, sources, symbols, source_errors = [], [], set(), []
    halt_count = st_count = 0
    for manifest_path in args.manifests:
        manifest_path = inside(manifest_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        sources.append({"path": str(manifest_path), "sha256": sha(manifest_path)})
        source_errors.extend({"manifest": str(manifest_path), "error": error} for error in manifest.get("errors", []))
        for entry in manifest["observations"]:
            path = inside(Path(entry["path"]))
            if entry["symbol"] in symbols or sha(path) != entry["sha256"]:
                raise ValueError("Duplicate symbol or status-file hash mismatch")
            symbols.add(entry["symbol"])
            rows = pq.read_table(path, columns=["trade_date", "symbol", "provider_trade_status", "provider_is_st", "tradable"]).to_pylist()
            if len(rows) != entry["rows"] or not rows:
                raise ValueError("Status rows differ from source inventory")
            if any(row["symbol"] != entry["symbol"] or row["tradable"] is not None for row in rows):
                raise ValueError("Provider observations cannot grant execution eligibility")
            halt_count += sum(row["provider_trade_status"] == 0 for row in rows)
            st_count += sum(row["provider_is_st"] == 1 for row in rows)
            files.append({**entry, "start": str(rows[0]["trade_date"]), "end": str(rows[-1]["trade_date"]),
                          "source_inventory": str(manifest_path), "source_inventory_sha256": sources[-1]["sha256"]})
    output = PROJECT / "data/external/stock_status_reference" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    result = {"run_id": output.name, "generated_at": datetime.now(timezone.utc).isoformat(), "files": files,
              "source_manifests": sources, "normalized_rows": sum(entry["rows"] for entry in files),
              "as_of": max(entry["end"] for entry in files), "complete": True, "backtest_ready": False,
              "provider_reported_halt_observations": halt_count, "provider_reported_ST_observations": st_count,
              "errors": [], "source_error_history": source_errors,
              "readiness_gaps": ["full_historical_status_universe", "provider_status_independent_verification",
                                 "historical_price_limits_and_execution_eligibility", "point_in_time_availability"],
              "purpose": "source-reported status observations; never engine/account execution eligibility"}
    (output / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"manifest": str(output / "manifest.json"), "files": len(files), "rows": result["normalized_rows"],
                      "provider_reported_halts": halt_count, "provider_reported_ST": st_count}))


if __name__ == "__main__":
    main()
