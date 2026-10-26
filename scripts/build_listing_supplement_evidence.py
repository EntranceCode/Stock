"""Supplement listing references from the registered provider universe (design §5.1).

The official SZSE/SSE snapshots registered in the security_metadata run cover
4,215 of the 4,272 local codes; 57 codes (600053, 600079, ...) have no official
listing reference although their bars run to the end of the research window.
The registered provider universe (baostock ``query_stock_basic``, 9,006
securities with listing dates, hash-inventoried) covers them.

This script builds a clearly labeled supplement: for every local code the
official candidates do not cover, it records the provider's listing and
termination dates with the exact source row, and flags any date that disagrees
with an official record (the official record stays authoritative). The
criterion then answers "every local code has a listing reference" from both
registered sources together.

Usage:
    python -B scripts/build_listing_supplement_evidence.py
    python -B scripts/build_listing_supplement_evidence.py --register
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(path: Path, project_root: Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(project_root) / resolved
    return resolved.resolve()


def as_day(value) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if value is None:
        return None
    text = str(value).strip()
    return text[:10] if text and text != "--" else None


def build(project_root: Path) -> tuple[dict, Path]:
    import pyarrow.parquet as pq

    project_root = Path(project_root)
    config = json.loads((project_root / "config/datasets.json").read_text(encoding="utf-8"))

    def manifest_of(dataset: str) -> tuple[Path, dict]:
        path = inside(Path(config["datasets"][dataset]["manifest"]), project_root)
        return path, json.loads(path.read_text(encoding="utf-8"))

    stock_path, stock = manifest_of("stock_daily")
    local_codes = sorted({str(entry["symbol"]) for entry in stock.get("files", [])
                          if entry.get("symbol")})

    meta_path, meta = manifest_of("security_metadata")
    official: dict[str, str] = {}
    for entry in meta.get("files", []):
        if entry.get("dataset") != "listing_candidates":
            continue
        for row in pq.read_table(inside(Path(entry["path"]), project_root),
                                 columns=["symbol", "source_listing_date"]).to_pylist():
            symbol, listed_on = row.get("symbol"), as_day(row.get("source_listing_date"))
            if symbol and listed_on:
                official[str(symbol)] = listed_on

    universe_path, universe = manifest_of("etf_universe")
    provider_file = next(entry for entry in universe.get("files", [])
                         if str(entry.get("path", "")).endswith("query_stock_basic.json"))
    payload = json.loads(inside(Path(provider_file["path"]), project_root).read_text(encoding="utf-8"))
    provider_rows = payload.get("rows", [])
    provider: dict[str, dict] = {}
    for index, record in enumerate(provider_rows):
        raw_code = str(record[0])
        # provider codes carry a venue prefix ("sh.600053"); local bars carry
        # the bare code ("600053"), so the numeric part is the join key.
        code = raw_code.split(".")[-1].lower()
        provider[code] = {"row": index, "provider_code": raw_code,
                          "provider_listing_date": as_day(record[2]),
                          "provider_out_date": as_day(record[3]),
                          "provider_type": record[4], "provider_status": record[5]}

    uncovered = [code for code in local_codes if code not in official]
    conflicts = []
    for symbol, listed_on in sorted(official.items()):
        record = provider.get(symbol)
        if record and record["provider_listing_date"] and record["provider_listing_date"] != listed_on:
            conflicts.append({"symbol": symbol, "official_listing_date": listed_on,
                              "provider_listing_date": record["provider_listing_date"],
                              "resolution": "official record stays authoritative; provider date recorded"})

    supplement = []
    still_missing = []
    for code in uncovered:
        record = provider.get(code)
        if record and record["provider_listing_date"]:
            supplement.append({"symbol": code, **record,
                               "reference_kind": "provider_listing_date",
                               "official_reference": False,
                               "source_file": str(inside(Path(provider_file["path"]), project_root)),
                               "source_sha256": provider_file.get("sha256")})
        else:
            still_missing.append(code)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + hashlib.sha256(
        json.dumps(uncovered).encode()).hexdigest()[:8]
    run_dir = project_root / "data/external/listing_supplement" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    supplement_path = run_dir / "listing_supplement.jsonl"
    supplement_path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n"
                                      for item in supplement), encoding="utf-8")
    manifest = {
        "run_id": run_id,
        "evidence_kind": "listing_supplement",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "purpose": "provider listing references for local codes the official snapshots "
                   "do not cover (design §5.1 listing coverage)",
        "source_manifests": [
            {"path": str(stock_path), "sha256": sha256(stock_path), "role": "local code universe"},
            {"path": str(meta_path), "sha256": sha256(meta_path), "role": "official listing candidates"},
            {"path": str(universe_path), "sha256": sha256(universe_path),
             "role": "provider security universe with listing dates"},
        ],
        "local_codes": len(local_codes),
        "official_references": len(official),
        "provider_references": len(supplement),
        "still_without_reference": still_missing,
        "date_conflicts_with_official": conflicts,
        "files": [{"path": str(supplement_path), "sha256": sha256(supplement_path),
                   "rows": len(supplement)}],
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest, manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", action="store_true",
                        help="also register the evidence dataset in config/datasets.json")
    args = parser.parse_args()

    manifest, manifest_path = build(PROJECT)
    print(f"local codes: {manifest['local_codes']}; official references: "
          f"{manifest['official_references']}; provider supplements: "
          f"{manifest['provider_references']}; still without a reference: "
          f"{len(manifest['still_without_reference'])}; date conflicts recorded: "
          f"{len(manifest['date_conflicts_with_official'])}")
    if not args.register:
        print(f"Evidence written to {manifest_path}; rerun with --register to activate it.")
        return 0
    config_path = PROJECT / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if "listing_supplement" in config["datasets"]:
        print("listing_supplement is already registered; config left unchanged.")
        return 0
    config["datasets"]["listing_supplement"] = {
        "manifest": str(manifest_path.relative_to(PROJECT)),
        "purpose": "provider listing references for codes the official snapshots do not cover",
    }
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Registered listing_supplement at {manifest_path.relative_to(PROJECT)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
