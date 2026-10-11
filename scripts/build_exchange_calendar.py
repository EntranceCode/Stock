"""Build a date-only SSE calendar from official notices and explicit amendments.

This is not a security status table and does not establish execution eligibility.
"""
from __future__ import annotations
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class CalendarCoverageError(ValueError):
    pass


class ExchangeCalendar:
    def __init__(self, catalog: dict):
        self.catalog = catalog
        self.start, self.end = map(date.fromisoformat, (catalog["scope_start"], catalog["scope_end"]))
        self.years = {item["year"]: item for item in catalog["years"]}
        if len(self.years) != len(catalog["years"]) or not catalog["base_weekend_closed"]:
            raise CalendarCoverageError("Duplicate years or unsupported weekend rule")
        if set(self.years) != set(range(self.start.year, self.end.year + 1)):
            raise CalendarCoverageError("Calendar has missing years")
        for item in catalog["years"] + catalog["amendments"]:
            if item.get("verified") is not True:
                raise CalendarCoverageError("Unverified official closure source")
            for first, last, _ in item["closed_intervals"]:
                if date.fromisoformat(first) > date.fromisoformat(last):
                    raise CalendarCoverageError("Reversed closure interval")

    def is_scheduled_open(self, day: date, exchange: str = "SSE", knowledge_as_of: date | None = None) -> bool:
        if exchange != self.catalog["exchange"] or not self.start <= day <= self.end:
            raise CalendarCoverageError("Exchange/date outside verified source scope")
        annual = self.years[day.year]
        if knowledge_as_of is not None and date.fromisoformat(annual["announcement_date"]) > knowledge_as_of:
            raise CalendarCoverageError("Annual schedule had not yet been announced")
        if day.weekday() >= 5:
            return False
        # The following year's New Year notice may close dates in the previous
        # year (for example 2018-12-31 in the 2019 notice).
        for notice in self.catalog["years"] + self.catalog["amendments"]:
            if knowledge_as_of is not None and date.fromisoformat(notice["announcement_date"]) > knowledge_as_of:
                continue
            if any(date.fromisoformat(first) <= day <= date.fromisoformat(last) for first, last, _ in notice["closed_intervals"]):
                return False
        return True

    def next_session_after(self, day: date, exchange: str = "SSE", knowledge_as_of: date | None = None) -> date:
        if not self.start <= day <= self.end:
            raise CalendarCoverageError("Date outside calendar scope")
        candidate = day + timedelta(days=1)
        while candidate <= self.end:
            if self.is_scheduled_open(candidate, exchange, knowledge_as_of):
                return candidate
            candidate += timedelta(days=1)
        raise CalendarCoverageError("Next session outside calendar scope")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=PROJECT / "config/exchange-calendar-sources.json")
    parser.add_argument("--source-snapshot", type=Path, required=True, help="Complete archived official notices")
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(timezone(timedelta(hours=8))).date())
    args = parser.parse_args()
    catalog_bytes = args.catalog.read_bytes()
    catalog = json.loads(catalog_bytes)
    calendar = ExchangeCalendar(catalog)
    snapshot = json.loads((args.source_snapshot / "manifest.json").read_text(encoding="utf-8"))
    if not snapshot["acquisition_complete"] or snapshot["catalog_sha256"] != hashlib.sha256(catalog_bytes).hexdigest():
        raise CalendarCoverageError("Official source archive missing, incomplete, or catalog changed")
    expected_urls = {item["source_url"] for item in catalog["years"] + catalog["amendments"]}
    if {item["url"] for item in snapshot["files"]} != expected_urls:
        raise CalendarCoverageError("Source archive does not cover all notices")
    for item in snapshot["files"]:
        if hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest() != item["sha256"]:
            raise CalendarCoverageError("Official source bytes changed")
    cutoff = min(calendar.end, args.as_of)
    if cutoff < calendar.start:
        raise CalendarCoverageError("As-of predates calendar scope")
    dates = pd.date_range(calendar.start, cutoff).date
    frame = pd.DataFrame({"calendar_date": dates, "exchange": "SSE",
                          "is_scheduled_open": [calendar.is_scheduled_open(day) for day in dates]})
    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    etf_manifest_path = PROJECT / registry["datasets"]["etf_daily"]["manifest"]
    etfs = json.loads(etf_manifest_path.read_text(encoding="utf-8"))
    observations, comparisons = set(), []
    scheduled = set(frame.loc[frame.is_scheduled_open, "calendar_date"])
    for item in etfs["files"]:
        if not item["symbol"].startswith("sh"):
            continue
        bars = pq.read_table(item["path"], columns=["trade_date"]).to_pandas().trade_date
        observed = {day for day in bars if calendar.start <= day <= cutoff}
        if not observed:
            raise ValueError("No overlapping ETF comparison dates")
        within_window = {day for day in scheduled if min(observed) <= day <= max(observed)}
        comparisons.append({"symbol": item["symbol"], "observed": len(observed),
                            "bars_on_scheduled_closed_dates": sorted(map(str, observed - scheduled)),
                            "scheduled_sessions_without_bar": sorted(map(str, within_window - observed))})
        observations.update(observed)
    frame["observed_in_sse_etf_samples"] = frame.calendar_date.isin(observations)
    output = PROJECT / "data/external/calendar" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    provenance = {"exchange": "SSE", "as_of": str(cutoff), "source_catalog": str(args.catalog.resolve()),
                  "catalog_sha256": hashlib.sha256(catalog_bytes).hexdigest(), "source_snapshot": str(args.source_snapshot.resolve()),
                  "backtest_ready": False, "readiness_gaps": ["SZSE_calendar_independent_validation", "security_level_status"],
                  "meaning": "Scheduled exchange sessions only; individual securities may be halted or ineligible"}
    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps(provenance).encode()})
    path = output / "calendar.parquet"
    pq.write_table(table, path, compression="zstd")
    manifest = {**provenance, "output": str(output), "rows": len(frame), "scheduled_open_days": int(frame.is_scheduled_open.sum()),
                "files": [{"path": str(path), "rows": len(frame), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}],
                "etf_comparison_manifest": str(etf_manifest_path), "comparisons": comparisons,
                "observed_comparison_end": str(max(observations)) if observations else None,
                "calendar_source_scope_complete": True}
    manifest["all_sse_etf_comparisons_exact"] = bool(comparisons) and not any(item["bars_on_scheduled_closed_dates"] or item["scheduled_sessions_without_bar"] for item in comparisons)
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=True, indent=2))
    if any(item["bars_on_scheduled_closed_dates"] for item in comparisons):
        raise ValueError("Calendar/ETF evidence conflict; preserve output and investigate")


if __name__ == "__main__":
    main()
