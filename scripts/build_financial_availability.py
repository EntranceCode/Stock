"""Build a point-in-time availability layer over the financial snapshot.

The snapshot keeps one row per report period, so it can never expose a restated
vintage directly. This layer records the announcement date the snapshot itself
states, classifies every provider update against the next-year comparable, and
validates the announcement dates against archived original-announcement indexes.
Nothing here admits the data: values remain unverified against original notices.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))

PERIODS = ((r"(\d{4})年(?:第)?一季度报告", "-03-31"), (r"(\d{4})年半年度报告", "-06-30"),
           (r"(\d{4})年(?:第)?三季度报告", "-09-30"), (r"(\d{4})年年度报告", "-12-31"))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def plus_year(day: date) -> date:
    try:
        return day.replace(year=day.year + 1)
    except ValueError:
        return day.replace(year=day.year + 1, day=28)


def classify_update(notice, update, next_year_notice):
    """Explain why a provider update date differs from the announcement date."""
    if notice is None or update is None:
        return "unknown"
    if update <= notice:
        return "unrevised"
    if next_year_notice is not None and update == next_year_notice:
        return "next_year_comparable"
    return "unexplained"


def report_period(title: str) -> str | None:
    """Map an original-announcement title to the period it reports, or None."""
    if "补充" in title or "更正" in title:
        return None
    for pattern, suffix in PERIODS:
        match = re.search(pattern, title)
        if match:
            return match.group(1) + suffix
    return None


def beijing_day(epoch_ms) -> str | None:
    if epoch_ms is None:
        return None
    moment = datetime.fromtimestamp(int(epoch_ms) / 1000, tz=timezone(timedelta(hours=8)))
    return moment.date().isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq

    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    latest = max((PROJECT / "data/external/financial_originals").iterdir())
    index_path = latest / "announcement-index.json"
    original = json.loads(index_path.read_text(encoding="utf-8"))
    announced = {}
    for row in original["rows"]:
        period = report_period(str(row.get("announcementTitle") or ""))
        day = beijing_day(row.get("announcementTime"))
        if period and day:
            announced.setdefault(row["secCode"], {}).setdefault(period, set()).add(day)

    manifest_path = PROJECT / registry["datasets"]["financials"]["manifest"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest["files"][:args.limit] if args.limit else manifest["files"]
    records, counts, lag_days = [], {}, []
    validated = matched = 0
    for entry in files:
        symbol = entry["symbol"]
        table = pq.read_table(entry["path"], columns=["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"])
        periods = {}
        for rd, nd, ud in zip(table.column("REPORT_DATE").to_pylist(),
                              table.column("NOTICE_DATE").to_pylist(),
                              table.column("UPDATE_DATE").to_pylist()):
            if rd is None:
                continue
            periods[str(rd)[:10]] = (str(nd)[:10] if nd else None, str(ud)[:10] if ud else None)
        for period, (notice, update) in sorted(periods.items()):
            year, month, day = (int(part) for part in period.split("-"))
            following = periods.get(str(plus_year(date(year, month, day))))
            next_notice = following[0] if following else None
            state = classify_update(notice, update, next_notice)
            counts[state] = counts.get(state, 0) + 1
            if notice and update and update > notice:
                lag_days.append((date.fromisoformat(update) - date.fromisoformat(notice)).days)
            if notice and period in announced.get(symbol, {}):
                validated += 1
                matched += notice in announced[symbol][period]
            records.append({"symbol": symbol, "report_date": period, "available_at": notice,
                            "provider_update_date": update, "update_classification": state})
    lag_days.sort()
    output = PROJECT / "data/external/financial_availability" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    path = output / "availability.parquet"
    provenance = {"purpose": "point-in-time announcement dates for the financial snapshot",
                  "available_at_meaning": "the announcement date the snapshot states for that report period",
                  "backtest_ready": False,
                  "readiness_gaps": ["value_vintages_not_verified_against_original_notices",
                                     "restatement_values_not_captured", "full_stock_universe_coverage",
                                     "historical_share_counts_and_corporate_actions"],
                  "source_manifest": str(manifest_path), "source_manifest_sha256": sha(manifest_path),
                  "original_announcement_index": str(index_path),
                  "original_announcement_index_sha256": sha(index_path)}
    table = pa.Table.from_pylist(records)
    table = table.replace_schema_metadata({b"stock.provenance": json.dumps(provenance).encode()})
    pq.write_table(table, path, compression="zstd")
    summary = {**provenance, "run_id": output.name, "output": str(output), "symbols": len(files),
               "rows": len(records), "update_classification": counts,
               "update_lag_days": {"median": lag_days[len(lag_days) // 2] if lag_days else None,
                                  "p90": lag_days[int(len(lag_days) * 0.9)] if lag_days else None,
                                  "max": lag_days[-1] if lag_days else None},
               "original_notice_dates_compared": validated, "original_notice_dates_matched": matched,
               "files": [{"path": str(path), "rows": len(records), "sha256": sha(path)}]}
    (output / "manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("output", "symbols", "rows", "update_classification",
                                             "update_lag_days", "original_notice_dates_compared",
                                             "original_notice_dates_matched")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
