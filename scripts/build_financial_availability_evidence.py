"""Build the financial_availability evidence dataset (design §5.1).

The ``announced_at_sample_verified`` criterion compares announcement dates
computed by the normalization run against the provider's original responses.
This script draws a systematic sample across the whole normalized financials
dataset, resolves each sampled report row in the retained raw provider capture,
and records every comparison - matches and mismatches - in a JSONL evidence
file with counts in the run manifest.

Only retained evidence is read: the normalized parquets registered in the
financials manifest and the raw JSON captures registered as its cached
snapshot. A missing capture or a disagreeing row is recorded as a mismatch or
an unresolved row, never silently repaired. Usage:

    python -B scripts/build_financial_availability_evidence.py
    python -B scripts/build_financial_availability_evidence.py --register
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

SAMPLE_SECURITIES = 240


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


def notice_key(value) -> str | None:
    """The announcement date as a plain YYYY-MM-DD key, or None if unreadable."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:10]


def build(project_root: Path) -> tuple[dict, Path]:
    """Draw the sample against retained raw captures and write the evidence run."""
    import pyarrow.parquet as pq

    project_root = Path(project_root)
    config = json.loads((project_root / "config/datasets.json").read_text(encoding="utf-8"))
    financials_manifest_path = inside(Path(config["datasets"]["financials"]["manifest"]), project_root)
    financials = json.loads(financials_manifest_path.read_text(encoding="utf-8"))
    raw_root = inside(Path(financials["cached_snapshot"]) / "raw", project_root)
    entries = sorted(financials["files"], key=lambda entry: entry["symbol"])
    stride = max(1, len(entries) // SAMPLE_SECURITIES)
    sampled = entries[::stride][:SAMPLE_SECURITIES]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha256(financials_manifest_path.read_bytes()).hexdigest()[:8]
    base_id = f"{stamp}_{digest}"
    run_dir, suffix = project_root / "data/external/financial_availability" / base_id, 1
    while run_dir.exists():
        suffix += 1
        run_dir = project_root / "data/external/financial_availability" / f"{base_id}_{suffix}"
    run_dir.mkdir(parents=True)

    comparisons, unresolved = [], []
    for entry in sampled:
        symbol = entry["symbol"]
        capture = raw_root / f"{symbol}.json"
        if not capture.is_file():
            unresolved.append({"symbol": symbol, "reason": "raw provider capture is missing"})
            continue
        raw = json.loads(capture.read_text(encoding="utf-8"))
        original = {str(row.get("REPORT_DATE"))[:10]: notice_key(row.get("NOTICE_DATE"))
                    for row in raw.get("payload", {}).get("result", {}).get("data", [])}
        table = pq.read_table(entry["path"], columns=["REPORT_DATE", "NOTICE_DATE"])
        rows = table.to_pylist()
        if not rows:
            unresolved.append({"symbol": symbol, "reason": "normalized parquet holds no rows"})
            continue
        row = rows[0]  # the provider sorts by REPORT_DATE descending: the latest report
        report_date = str(row["REPORT_DATE"])[:10]
        record = {"symbol": symbol, "report_date": report_date,
                  "normalized_notice_date": notice_key(row["NOTICE_DATE"])}
        if report_date not in original:
            record.update(original_notice_date=None, matched=False)
            record["reason"] = "report period absent from the raw capture"
            unresolved.append({"symbol": symbol, "reason": record["reason"]})
        else:
            record["original_notice_date"] = original[report_date]
            record["matched"] = record["normalized_notice_date"] == original[report_date]
        comparisons.append(record)

    sample_path = run_dir / "sample_comparison.jsonl"
    sample_path.write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in comparisons) + "\n",
                           encoding="utf-8")
    matched = sum(1 for item in comparisons if item.get("matched"))
    manifest = {
        "run_id": run_dir.name,
        "evidence_kind": "financial_availability",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "purpose": "announcement dates compared between the normalized financials run "
                   "and the retained raw provider captures (design §5.1 sampling)",
        "provider": financials.get("provider"),
        "normalized_manifest": str(financials_manifest_path),
        "normalized_manifest_sha256": sha256(financials_manifest_path),
        "cached_snapshot": str(raw_root.parent),
        "sample_method": f"every {stride}th of {len(entries)} registered securities, latest report row each",
        "original_notice_dates_compared": len(comparisons),
        "original_notice_dates_matched": matched,
        "mismatches": [item for item in comparisons if not item.get("matched")],
        "unresolved_rows": unresolved,
        "files": [{"path": str(sample_path), "sha256": sha256(sample_path),
                   "rows": len(comparisons)}],
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest, manifest_path


def register(project_root: Path, manifest_path: Path) -> Path:
    """Register a built evidence run in the project's dataset config."""
    project_root = Path(project_root)
    config_path = project_root / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if "financial_availability" in config["datasets"]:
        return config_path
    config["datasets"]["financial_availability"] = {
        "manifest": str(manifest_path.relative_to(project_root)),
        "purpose": "announcement-date sampling against retained raw provider captures",
    }
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return config_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", action="store_true",
                        help="also register the new evidence dataset in config/datasets.json")
    args = parser.parse_args()

    manifest, manifest_path = build(PROJECT)
    share = (manifest["original_notice_dates_matched"] / manifest["original_notice_dates_compared"]
             if manifest["original_notice_dates_compared"] else 0.0)
    print(f"compared {manifest['original_notice_dates_compared']} announcement dates, "
          f"{manifest['original_notice_dates_matched']} matched ({share:.4f}); "
          f"{len(manifest['mismatches'])} mismatch(es), {len(manifest['unresolved_rows'])} unresolved")
    if args.register:
        register(PROJECT, manifest_path)
        print(f"Registered financial_availability at {manifest_path.relative_to(PROJECT)}.")
    else:
        print(f"Evidence written to {manifest_path}; rerun with --register to activate it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
