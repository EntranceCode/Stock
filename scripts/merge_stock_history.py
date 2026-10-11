"""Create an immutable inventory of existing prices and accepted supplements.

Existing symbol files are never replaced. Multiple-listing supplements are
withheld from the merged view and preserved in their original evidence layer.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]


def inside(path, root):
    path = path.resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Price evidence must remain inside the independent project")
    return path


def file_sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(base_path, supplement_paths, root=PROJECT):
    paths = [inside(base_path, root), *(inside(path, root) for path in supplement_paths)]
    files, skipped, withheld, source_errors, sources = {}, [], [], [], []
    gaps = []
    for index, manifest_path in enumerate(paths):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        sources.append({"path": str(manifest_path), "sha256": file_sha(manifest_path)})
        for gap in manifest.get("readiness_gaps", []):
            if gap not in gaps:
                gaps.append(gap)
        source_errors.extend({"manifest": str(manifest_path), "error": error} for error in manifest.get("errors", []))
        if index == 0 and not manifest.get("complete"):
            raise ValueError("Existing base must have completed normalization")
        seen = set()
        for entry in manifest.get("files", []):
            symbol = entry["symbol"]
            if not isinstance(symbol, str) or len(symbol) != 6 or not symbol.isdigit() or symbol in seen:
                raise ValueError("Invalid or duplicate inventory symbol")
            seen.add(symbol)
            path = Path(entry.get("output") or entry["path"])
            if not path.is_absolute():
                path = manifest_path.parent / path
            path = inside(path, root)
            digest = entry.get("output_sha256") or entry.get("sha256")
            if not digest or file_sha(path) != digest:
                raise ValueError("Price output SHA-256 mismatch")
            if symbol in files:
                skipped.append({"symbol": symbol, "source_manifest": str(manifest_path), "reason": "existing price file retained"})
                continue
            if index and entry.get("source_request", {}).get("multiple_listing_records_require_review"):
                withheld.append({"symbol": symbol, "source_manifest": str(manifest_path), "reason": "multiple listing intervals require review"})
                continue
            files[symbol] = {**entry, "output": str(path), "output_sha256": digest,
                             "source_inventory": str(manifest_path), "source_inventory_sha256": sources[-1]["sha256"]}
    if not files:
        raise ValueError("Merged stock inventory cannot be empty")
    return {"files": [files[key] for key in sorted(files)], "normalized_rows": sum(entry["rows"] for entry in files.values()),
            "source_manifests": sources, "source_error_history": source_errors,
            "kept_original_duplicates": skipped, "withheld_multiple_listing_symbols": withheld,
            "readiness_gaps": list(dict.fromkeys([*gaps, "full_historical_universe_and_lifecycle_boundaries"]))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--supplements", nargs="+", type=Path, required=True)
    args = parser.parse_args(argv)
    result = inventory(args.base, args.supplements)
    output = PROJECT / "data/normalized/merged_history" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    result.update(run_id=output.name, output=str(output), generated_at=datetime.now(timezone.utc).isoformat(),
                  as_of=max(entry["end"] for entry in result["files"]), complete=True, backtest_ready=False,
                  errors=[], price_basis="unadjusted", purpose="merged staging inventory; original files retained")
    with (output / "manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({"manifest": str(output / "manifest.json"), "files": len(result["files"]), "rows": result["normalized_rows"],
                      "withheld": len(result["withheld_multiple_listing_symbols"]), "source_errors": len(result["source_error_history"])}))


if __name__ == "__main__":
    main()
