"""Compute and register output hashes for datasets whose manifests lack them.

Several normalization runs wrote row counts, source paths and source hashes,
but never recorded the hash of their own output files. The registry gate
(design §5.2) verifies the registered output hash of every file before
computing any readiness criterion, so such a dataset cannot be evaluated at
all: the refusal reason is the missing registration, not the data.

This script recomputes the SHA-256 and row count of every registered output
file, re-verifies retained sources, and writes the results into the manifest's
file inventory. Nothing is inferred: a hash, row count or security code that
disagrees with the file on disk stops the run with a non-zero exit status and
leaves the manifest untouched.

Usage:
    python -B scripts/finalize_dataset_registration.py --dataset financials --check
    python -B scripts/finalize_dataset_registration.py --dataset action_samples
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

SOURCE_SAMPLE_SIZE = 200


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(path: Path, project_root: Path) -> Path:
    """Resolve a registered path against the project root and enforce it."""
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(project_root) / resolved
    resolved = resolved.resolve()
    if not resolved.is_relative_to(Path(project_root).resolve()):
        raise ValueError(f"Registered evidence must stay inside the project: {path}")
    return resolved


def registered_manifest(project_root: Path, dataset: str) -> Path:
    config = json.loads((Path(project_root) / "config/datasets.json").read_text(encoding="utf-8"))
    return inside(Path(config["datasets"][dataset]["manifest"]), project_root)


def verify_financials(manifest_path: Path, project_root: Path) -> tuple[dict, list[dict], list[str]]:
    """Recompute per-symbol financial parquets; re-verify each retained source."""
    import pyarrow.parquet as pq

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    updates, problems = [], []
    for entry in manifest.get("files", []):
        symbol = entry.get("symbol")
        output = inside(Path(entry["path"]), project_root)
        if not output.is_file():
            problems.append(f"{symbol}: registered output file is missing")
            continue
        table = pq.read_table(output)
        codes = {str(row) for row in table.column("SECURITY_CODE").to_pylist()} if table.num_rows else set()
        if codes and codes != {str(symbol)}:
            problems.append(f"{symbol}: output file holds securities {sorted(codes)}")
        if entry.get("rows") != table.num_rows:
            problems.append(f"{symbol}: registered rows {entry.get('rows')} != file rows {table.num_rows}")
        digest = sha256(output)
        if entry.get("output_sha256") not in (None, digest):
            problems.append(f"{symbol}: registered output hash disagrees with the file")
        source = entry.get("source_original_file")
        if source:
            source_path = Path(source)
            if source_path.is_file():
                if sha256(source_path) != entry.get("source_sha256"):
                    problems.append(f"{symbol}: source file changed since registration")
            else:
                problems.append(f"{symbol}: registered source file is missing")
        updates.append({"symbol": symbol, "output_sha256": digest, "rows": table.num_rows})
    return manifest, updates, problems


def verify_action_samples(manifest_path: Path, project_root: Path) -> tuple[dict, list[dict], list[str]]:
    """Recompute the single events parquet; hash-check a sample of per-event sources."""
    import pyarrow.parquet as pq

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    events_path = inside(Path(manifest["output"]) / "events.parquet", project_root)
    problems: list[str] = []
    if not events_path.is_file():
        return manifest, [], ["events.parquet is missing from the registered run"]
    table = pq.read_table(events_path)
    if manifest.get("rows") != table.num_rows:
        problems.append(f"manifest rows {manifest.get('rows')} != file rows {table.num_rows}")
    symbols = table.column("symbol").to_pylist()
    if any(not symbol for symbol in symbols):
        problems.append("events.parquet holds rows without a symbol")
    rows = table.to_pylist()
    every_other = max(1, len(rows) // SOURCE_SAMPLE_SIZE)
    sampled = rows[::every_other][:SOURCE_SAMPLE_SIZE]
    source_checked = 0
    for row in sampled:
        source = row.get("source_file")
        recorded = row.get("source_sha256")
        if not source or not recorded:
            continue
        source_path = Path(source)
        if not source_path.is_file():
            problems.append(f"{row.get('symbol')} {row.get('ex_date')}: per-event source file is missing")
            continue
        if sha256(source_path) != recorded:
            problems.append(f"{row.get('symbol')} {row.get('ex_date')}: per-event source hash disagrees")
        source_checked += 1
    updates = [{"path": events_path, "sha256": sha256(events_path), "rows": table.num_rows,
                "per_event_sources_hash_checked": source_checked}]
    return manifest, updates, problems


VERIFIERS = {
    "financials": verify_financials,
    "action_samples": verify_action_samples,
}


def finalize(project_root: Path, dataset: str, *, write: bool) -> int:
    manifest_path = registered_manifest(project_root, dataset)
    manifest, updates, problems = VERIFIERS[dataset](manifest_path, project_root)
    if problems:
        print(f"{dataset}: registration problems found; manifest left untouched:")
        for problem in problems:
            print(" -", problem)
        return 2

    if dataset == "financials":
        entries = {entry["symbol"]: entry for entry in manifest["files"]}
        pending = [update for update in updates
                   if entries[update["symbol"]].get("output_sha256") != update["output_sha256"]]
        if not pending:
            print(f"All {len(updates)} registered financials files already carry verified output hashes.")
            return 0
        if not write:
            print(f"{len(pending)} financials file(s) lack output hashes; run without --check to register them.")
            return 1
        for update in pending:
            entries[update["symbol"]]["output_sha256"] = update["output_sha256"]
    else:
        existing = {entry.get("sha256") for entry in manifest.get("files", [])}
        if updates and updates[0]["sha256"] in existing:
            print("The action_samples events file already carries a verified output hash.")
            return 0
        if not write:
            print("action_samples lacks an output hash; run without --check to register it.")
            return 1
        manifest["files"] = [{"path": str(updates[0]["path"]), "sha256": updates[0]["sha256"],
                              "rows": updates[0]["rows"],
                              "per_event_sources_hash_checked": updates[0]["per_event_sources_hash_checked"]}]

    finalized_at = datetime.now(timezone.utc).isoformat()
    manifest["registration_finalized"] = {
        "at": finalized_at,
        "script": "scripts/finalize_dataset_registration.py",
        "files_hash_verified": len(updates),
        "note": "output sha256 and row counts recomputed from the registered files",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Registered output hashes for {len(updates)} {dataset} file(s) at {finalized_at}.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=sorted(VERIFIERS))
    parser.add_argument("--check", action="store_true", help="verify only; do not write the manifest")
    args = parser.parse_args()
    return finalize(PROJECT, args.dataset, write=not args.check)


if __name__ == "__main__":
    raise SystemExit(main())
