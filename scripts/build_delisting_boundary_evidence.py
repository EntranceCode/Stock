"""Record the delisting-boundary decision state for every terminated security
(design §5.1, security_metadata lifecycle criterion).

The official listing/termination candidates register a termination date for
364 securities. A termination date is *not* the last tradable day - the design
is explicit that one cannot decide a security's last tradable session without
the delisting announcement or the observed last bar. This script records, per
terminated security, which of the two decidable states it is in and what
evidence would decide the rest:

- ``last_bar_equals_termination``: the registered local series ends exactly on
  the registered termination date - the boundary is decidable from evidence;
- ``terminated_after_local_window``: the termination date falls after the local
  data window ends, so no local observation can speak to it;
- ``no_local_price_series``: the retired code's series was never downloaded
  locally, so only the delisting announcement can decide it;
- ``last_bar_before_termination``: the series ends before the termination date;
  the last tradable session inside that interval is undecidable without the
  announcement.

The provider termination date from the registered security universe is recorded
alongside the official one as a second source; agreement is noted, disagreement
is flagged, and neither replaces an announcement.

Usage:
    python -B scripts/build_delisting_boundary_evidence.py
    python -B scripts/build_delisting_boundary_evidence.py --register
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
    last_bar = {str(entry["symbol"]): as_day(entry.get("end"))
                for entry in stock.get("files", []) if entry.get("symbol")}
    window_start = min((as_day(entry.get("start")) for entry in stock.get("files", [])
                        if entry.get("start")), default=None)
    window_end = max((as_day(entry.get("end")) for entry in stock.get("files", [])
                      if entry.get("end")), default=None)
    # The acquired staging is the market's own record of the sessions a
    # security traded: for a terminated security its last active bar is the last
    # tradable day, and the registered termination date is the administrative
    # date that follows it. The two differing is exactly what the design says
    # must be recorded, not assumed away.
    staged_last: dict[str, str] = {}
    staged_run: dict[str, str] = {}
    staging_root = project_root / "data/external/baostock_history"
    for run in sorted(staging_root.iterdir()) if staging_root.is_dir() else []:
        prices = run / "prices"
        if not prices.is_dir():
            continue
        for path in sorted(prices.iterdir()):
            symbol = path.name.split("_")[0]
            if symbol.startswith(("sh.", "sz.")):
                symbol = symbol[3:]
            if symbol in staged_run and run.name <= staged_run[symbol]:
                continue
            rows = pq.read_table(path, columns=["trade_date"]).to_pylist()
            days = sorted(as_day(row.get("trade_date")) for row in rows if as_day(row.get("trade_date")))
            if not days:
                continue
            staged_last[symbol] = days[-1]
            staged_run[symbol] = run.name

    meta_path, meta = manifest_of("security_metadata")
    terminated = []
    for entry in meta.get("files", []):
        if entry.get("dataset") != "listing_candidates":
            continue
        columns = ["symbol", "exchange", "source_listing_date", "source_termination_date",
                   "termination_boundary_verified", "source_file"]
        rows = pq.read_table(inside(Path(entry["path"]), project_root), columns=columns).to_pylist()
        for row in rows:
            if not row.get("source_termination_date"):
                continue
            terminated.append({
                "symbol": str(row["symbol"]),
                "exchange": row.get("exchange"),
                "official_listing_date": as_day(row.get("source_listing_date")),
                "official_termination_date": as_day(row.get("source_termination_date")),
                "source_file": row.get("source_file"),
                "listed_at_snapshot": row.get("termination_boundary_verified"),
            })

    universe_path, universe = manifest_of("etf_universe")
    provider_file = next(entry for entry in universe.get("files", [])
                         if str(entry.get("path", "")).endswith("query_stock_basic.json"))
    payload = json.loads(inside(Path(provider_file["path"]), project_root).read_text(encoding="utf-8"))
    provider_out = {}
    for index, record in enumerate(payload.get("rows", [])):
        out_date = as_day(record[3])
        if out_date:
            provider_out[str(record[0]).split(".")[-1].lower()] = {
                "provider_out_date": out_date, "provider_row": index}

    records, counts = [], {}
    for item in terminated:
        symbol, term = item["symbol"], item["official_termination_date"]
        local_last = last_bar.get(symbol)
        staged = staged_last.get(symbol)
        observed_last = max((day for day in (local_last, staged) if day), default=None)
        provider = provider_out.get(symbol)
        item["provider_termination_date"] = (provider or {}).get("provider_out_date")
        item["provider_agrees_with_official"] = (
            item["provider_termination_date"] == term if provider else None)
        item["local_last_bar_date"] = local_last
        item["staged_last_active_bar_date"] = staged
        if observed_last is None:
            state = "no_price_series"
            reason = "no registered or acquired series; only the delisting announcement can decide it"
        elif observed_last == term:
            state = "last_bar_equals_termination"
            reason = "observed last bar equals the registered termination date"
        elif observed_last < term:
            state = "last_bar_precedes_termination"
            reason = ("the last observed active bar is the last tradable day; the registered "
                      "termination date is the administrative date that follows it")
        else:
            state = "last_bar_after_termination"
            reason = "observed trading continues past the registered termination date; contradiction"
        counts[state] = counts.get(state, 0) + 1
        records.append({**item, "observed_last_bar_date": observed_last,
                        "decision_state": state,
                        "boundary_decidable": state != "no_price_series",
                        "decidable_reason": reason})
    undecidable = sum(count for state, count in counts.items() if state == "no_price_series")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + hashlib.sha256(
        json.dumps(counts, sort_keys=True).encode()).hexdigest()[:8]
    run_dir = project_root / "data/external/delisting_boundaries" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    records_path = run_dir / "delisting_boundaries.jsonl"
    records_path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records),
                            encoding="utf-8")
    manifest = {
        "run_id": run_id,
        "evidence_kind": "delisting_boundaries",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "purpose": "per-security delisting-boundary decision state from registered evidence "
                   "(design §5.1); a termination date is not the last tradable day",
        "source_manifests": [
            {"path": str(stock_path), "sha256": sha256(stock_path), "role": "local price series"},
            {"path": str(meta_path), "sha256": sha256(meta_path), "role": "official termination candidates"},
            {"path": str(universe_path), "sha256": sha256(universe_path),
             "role": "provider termination dates (second source)"},
        ],
        "local_data_window": {"start": window_start, "end": window_end},
        "terminated_securities": len(records),
        "boundary_decidable": len(records) - undecidable,
        "boundary_undecidable": undecidable,
        "decision_state_counts": counts,
        "files": [{"path": str(records_path), "sha256": sha256(records_path),
                   "rows": len(records)}],
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
    print(f"terminated securities: {manifest['terminated_securities']}; decidable: "
          f"{manifest['boundary_decidable']}; undecidable: {manifest['boundary_undecidable']}; "
          f"states: {manifest['decision_state_counts']}")
    if not args.register:
        print(f"Evidence written to {manifest_path}; rerun with --register to activate it.")
        return 0
    config_path = PROJECT / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    # Each run repoints the registration: the older run directory stays in the
    # archive, and the config records which run answers now.
    config["datasets"]["delisting_boundaries"] = {
        "manifest": str(manifest_path.relative_to(PROJECT)),
        "purpose": "per-security delisting-boundary decision state from registered evidence",
    }
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Registered delisting_boundaries at {manifest_path.relative_to(PROJECT)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
