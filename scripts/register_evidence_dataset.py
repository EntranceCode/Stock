"""Register an already-archived evidence run into the dataset config.

Design §5.2 computes admission from registered evidence. Several runs built
between 2026-10-11 and 2026-10-19 (SZSE calendar, ETF universe, enriched
daily bars, stock status reference, adjusted prices) were archived with full
per-file hash inventories but never registered, so their criteria could not
even be evaluated - the gate was refusing on registration, not on data.

This script verifies the run's own inventory first - every registered file
exists, its SHA-256 matches, and its row count matches - then adds the dataset
entry to config/datasets.json. It refuses when anything disagrees; a dataset
whose manifest lacks a file inventory is refused with that reason. Already
registered datasets are reported and left untouched.

Usage:
    python -B scripts/register_evidence_dataset.py --dataset szse_calendar --run <run_dir> --check
    python -B scripts/register_evidence_dataset.py --dataset szse_calendar --run <run_dir>
    python -B scripts/register_evidence_dataset.py --list
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
    resolved = resolved.resolve()
    if not resolved.is_relative_to(Path(project_root).resolve()):
        raise ValueError(f"Registered evidence must stay inside the project: {path}")
    return resolved


def count_records(path: Path) -> int | None:
    """Row count of a registered evidence file, or None when not countable."""
    import pyarrow.parquet as pq

    if path.suffix == ".parquet":
        return pq.read_metadata(path).num_rows
    if path.suffix == ".json":
        value = json.loads(path.read_text(encoding="utf-8"))
        rows = value.get("rows") if isinstance(value, dict) else value
        if isinstance(rows, list):
            return len(rows)
    return None


def verify_run(manifest_path: Path, project_root: Path) -> list[str]:
    """Recompute every registered file hash and row count; return problems."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    problems: list[str] = []
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        return [f"manifest at {manifest_path} registers no file inventory"]
    for entry in entries:
        if not isinstance(entry, dict) or "path" not in entry:
            problems.append(f"inventory item without a path: {str(entry)[:80]}")
            continue
        path = inside(Path(entry["path"]), project_root)
        if not path.is_file():
            problems.append(f"registered file is missing: {path}")
            continue
        if "sha256" in entry and sha256(path) != entry["sha256"]:
            problems.append(f"hash mismatch: {path}")
        if "rows" in entry and entry["rows"] is not None:
            rows = count_records(path)
            if rows is not None and rows != entry["rows"]:
                problems.append(f"row count mismatch: {path} ({entry['rows']} registered, {rows} actual)")
    return problems


def register_dataset(project_root: Path, dataset: str, manifest_path: Path,
                     purpose: str | None = None, *, replace: bool = False) -> Path:
    """Add a verified run to the dataset config; refuse anything that disagrees.

    With ``replace`` the entry for an already registered dataset is repointed
    at the new run after it verifies; the older run directory is never touched,
    so the archive keeps both and the config records which one answers now.
    """
    project_root = Path(project_root)
    config_path = project_root / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if dataset in config["datasets"] and not replace:
        return config_path
    problems = verify_run(manifest_path, project_root)
    if problems:
        raise ValueError(f"{dataset}: run verification failed: " + "; ".join(problems))
    config["datasets"][dataset] = {
        "manifest": str(manifest_path.relative_to(project_root)),
        "purpose": purpose or f"registered evidence run {manifest_path.parent.name}",
    }
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", help="dataset name to register")
    parser.add_argument("--run", help="run directory inside data/external/<owner>/")
    parser.add_argument("--purpose", help="purpose note recorded in the config entry")
    parser.add_argument("--replace", action="store_true",
                        help="repoint an already registered dataset at a newer verified run")
    parser.add_argument("--check", action="store_true", help="verify only; do not write the config")
    parser.add_argument("--list", action="store_true", help="list registered datasets and exit")
    args = parser.parse_args()

    config_path = PROJECT / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if args.list:
        for name, entry in sorted(config["datasets"].items()):
            print(f"{name:24s} {entry.get('manifest')}")
        return 0
    if not args.dataset or not args.run:
        parser.error("--dataset and --run are required")
    if args.dataset in config["datasets"] and not args.replace:
        print(f"{args.dataset} is already registered at {config['datasets'][args.dataset]['manifest']}; "
              f"config left unchanged (use --replace to repoint it).")
        return 0

    run_dir = Path(args.run)
    if not run_dir.is_absolute():
        candidate = PROJECT / "data/external" / run_dir
        run_dir = candidate if candidate.is_dir() else PROJECT / run_dir
    manifest_path = inside(run_dir / "manifest.json", PROJECT)
    if not manifest_path.is_file():
        print(f"No manifest at {manifest_path}")
        return 2
    problems = verify_run(manifest_path, PROJECT)
    if problems:
        print(f"{args.dataset}: run verification failed; config left untouched:")
        for problem in problems:
            print(" -", problem)
        return 2
    if args.check:
        print(f"{args.dataset}: run verified; registering would add {manifest_path}.")
        return 0
    register_dataset(PROJECT, args.dataset, manifest_path, args.purpose, replace=args.replace)
    print(f"Registered {args.dataset} -> {manifest_path.relative_to(PROJECT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
