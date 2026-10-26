"""Attribute every bar-missing session to a recorded cause (design §5.1).

The missing-days criterion refuses a security whose registered series skips a
scheduled session without a ``session_gap_records`` entry naming the symbol,
its missing dates and the reason. This builder derives those records from
registered evidence only:

- the registered daily series (the exact session dates it holds),
- the registered exchange calendars (which sessions were scheduled, per the
  security's exchange),
- the acquired provider staging, whose rows carry the provider's trade status,
  so a session the provider reports as halted is distinguishable from one the
  provider simply never delivered.

Each missing session is classified into exactly one cause:

- ``provider_halt_observation``: the staging holds a row for the session whose
  trade status is not active - the provider reports the security was halted;
- ``no_provider_observation``: the staging holds no row for the session at all;
- ``active_provider_row_absent_from_series``: the staging holds an ACTIVE row
  the registered series does not - the local source dropped a live session,
  which is recorded rather than smoothed over.

A security whose staging is not yet acquired is reported as undecidable and
left unrecorded; the criterion keeps failing for it, which is the honest
answer until its evidence exists.

Usage:
    python -B scripts/build_session_gap_records.py                 # report only
    python -B scripts/build_session_gap_records.py --write         # write the records
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(Path(__file__).resolve().parent))


def as_day(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def read_series_dates(path: Path) -> set[date]:
    import pyarrow.parquet as pq
    days = set()
    for value in pq.read_table(path, columns=["trade_date"]).column("trade_date").to_pylist():
        day = as_day(value)
        if day is not None:
            days.add(day)
    return days


def staging_observations() -> dict[str, dict[date, int]]:
    """Per symbol, per session, the provider's trade status (1 = active)."""
    import pyarrow.parquet as pq

    observed: dict[str, dict[date, int]] = {}
    for run in sorted((PROJECT / "data/external/baostock_history").iterdir()):
        observations = run / "observations"
        if not observations.is_dir():
            continue
        for path in sorted(observations.glob("*_observations.parquet")):
            symbol = path.name.split("_")[0]
            if symbol.startswith(("sh.", "sz.")):
                symbol = symbol[3:]
            rows = pq.read_table(path, columns=["trade_date", "provider_trade_status"]).to_pylist()
            per_day = observed.setdefault(symbol, {})
            for row in rows:
                day, status = as_day(row.get("trade_date")), row.get("provider_trade_status")
                if day is not None and status is not None:
                    per_day[day] = int(status)
    return observed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true",
                        help="write the records into the registered daily manifests")
    args = parser.parse_args(argv)

    # The readiness helpers carry the calendar and exchange logic; importing
    # them keeps this builder and the criterion reading the same evidence.
    from hybrid_quant.data_registry import DatasetRegistry
    from hybrid_quant.readiness import (_open_days_for_exchange, _security_exchange_map,
                                        _registered_calendars, STOCK_DATASET, ETF_DATASET)

    registry = DatasetRegistry(PROJECT / "config/datasets.json", PROJECT)
    calendars = _registered_calendars(registry)
    if not calendars:
        print("no exchange calendar is registered; expected sessions cannot be counted")
        return 2
    exchange_of = _security_exchange_map(registry)
    observed = staging_observations()

    summaries = {}
    for dataset in (STOCK_DATASET, ETF_DATASET):
        if dataset not in registry.config["datasets"]:
            continue
        entry = registry.config["datasets"][dataset]["manifest"]
        manifest_path = registry._inside(registry.project_root / entry)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        records = []
        summary = {"securities_with_gaps": 0, "missing_sessions": 0, "recorded": 0,
                   "undecidable_securities": 0, "by_cause": {}, "securities_scanned": 0}
        for entry in manifest.get("files", []):
            symbol = str(entry.get("symbol"))
            source = entry.get("output") or entry.get("path")
            if not source:
                continue
            summary["securities_scanned"] += 1
            series_days = read_series_dates(Path(source))
            if not series_days:
                continue
            open_days = _open_days_for_exchange(registry, exchange_of.get(symbol))
            expected = [day for day in open_days if min(series_days) <= day <= max(series_days)]
            missing_days = [day for day in expected if day not in series_days]
            if not missing_days:
                continue
            summary["securities_with_gaps"] += 1
            summary["missing_sessions"] += len(missing_days)
            staging = observed.get(symbol[2:] if symbol[:2] in ("sh", "sz") else symbol)
            if staging is None:
                summary["undecidable_securities"] += 1
                continue
            halted = [day for day in missing_days if staging.get(day) not in (None, 1)]
            absent = [day for day in missing_days if staging.get(day) is None]
            dropped = [day for day in missing_days if staging.get(day) == 1]
            for cause, days in (("provider_halt_observation", halted),
                                ("no_provider_observation", absent),
                                ("active_provider_row_absent_from_series", dropped)):
                if days:
                    records.append({"symbol": symbol, "dates": [day.isoformat() for day in days],
                                    "cause": cause})
                    summary["by_cause"][cause] = summary["by_cause"].get(cause, 0) + len(days)
            summary["recorded"] += len(halted) + len(absent) + len(dropped)
        summaries[dataset] = {"summary": summary, "manifest_path": manifest_path,
                              "manifest": manifest, "records": records}
        if args.write and records:
            manifest["session_gap_records"] = records
            temporary = manifest_path.with_name(manifest_path.name + ".partial")
            temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(manifest_path)
    print(json.dumps({dataset: value["summary"] for dataset, value in summaries.items()},
                     ensure_ascii=False, indent=2))
    for dataset, value in summaries.items():
        if args.write and value["records"]:
            print(f"wrote {len(value['records'])} session_gap record group(s) into "
                  f"{value['manifest_path']} ({dataset})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
