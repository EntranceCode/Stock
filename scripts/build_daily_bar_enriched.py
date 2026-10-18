"""Derive daily price limits and carry the reported amount and trade status.

DailyBar needs amount, halt status and the historical price limits, none of which
the local source carries. The BaoStock staging files do carry turnover, provider
trade status, previous close and an ST flag, so this layer carries those across
and derives the limits from the rules in force for the board and date.

Provider flags are staging observations: an ST flag here is what the provider
reported, not verified risk-warning status, and the limits are a rule model that
is checked against the observed high and low before being trusted.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))

CHINEXT_WIDE_LIMIT = date(2020, 8, 24)  # ChiNext moved from 10% to 20%
TOLERANCE = 0.011  # one cent plus float noise on a two-decimal limit price


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def limit_ratio(symbol: str, day: date, is_st: bool) -> float:
    """Price-limit ratio in force for this security on this date.

    STAR (688) and ChiNext (300/301) securities move 20%; ST keeps the tighter
    5% band only on the main boards. Special first-day and relisting windows are
    not modelled here and are reported as violations instead.
    """
    if symbol.startswith("688"):
        return 0.20
    if symbol.startswith(("300", "301")):
        return 0.20 if day >= CHINEXT_WIDE_LIMIT else 0.10
    return 0.05 if is_st else 0.10


def round_cent(value: float) -> float:
    """A-share limit prices round half up to one cent, not to even."""
    return math.floor(value * 100 + 0.5) / 100


def limit_prices(previous_close: float, ratio: float):
    """The upper and lower limit prices for a previous close and ratio."""
    return round_cent(previous_close * (1 + ratio)), round_cent(previous_close * (1 - ratio))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, help="Only process the first N securities")
    args = parser.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq

    staging = PROJECT / "data/external/baostock_history"
    # A symbol can appear in more than one acquisition run (an earlier subset and
    # a later whole-universe run). The newest run is the authoritative copy, so
    # keep one source per symbol instead of emitting duplicate rows for it.
    discovered = sorted(staging.glob("*/prices/*.parquet"))
    newest: dict[str, Path] = {}
    for path in discovered:
        newest[path.name.split("_")[0]] = path
    sources = [newest[symbol] for symbol in sorted(newest)]
    if args.limit:
        sources = sources[:args.limit]
    output = PROJECT / "data/external/daily_bar_enriched" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    (output / "stocks").mkdir(parents=True)
    files, violations, rows_total = [], [], 0
    for source in sources:
        symbol = source.name.split("_")[0]
        records = pq.read_table(source).to_pylist()
        enriched = []
        previous_day = None
        for index, row in enumerate(records):
            day = row["trade_date"]
            # The first bar of a series, or one after an interruption, follows the
            # exchange reopening rule rather than the ordinary band, and its base
            # close is stale, so the ordinary limits are not reported for it.
            resumption = previous_day is None or (day - previous_day).days > 7
            previous_day = day
            is_st = row.get("provider_is_st") == 1
            ratio = limit_ratio(symbol, day, is_st)
            previous = row.get("preclose")
            up = down = None
            if previous and not resumption:
                up, down = limit_prices(float(previous), ratio)
            high, low = row.get("high"), row.get("low")
            if previous and high is not None and low is not None and not resumption and index >= 5:
                if high > up + TOLERANCE or low < down - TOLERANCE:
                    violations.append({"symbol": symbol, "trade_date": str(day), "high": high, "low": low,
                                       "limit_up": up, "limit_down": down, "ratio": ratio,
                                       "is_st_reported": is_st})
            enriched.append({"trade_date": day, "symbol": symbol,
                             "open": row.get("open"), "high": high, "low": low, "close": row.get("close"),
                             "preclose": previous, "turnover_cny_reported": row.get("turnover_cny_reported"),
                             "provider_trade_status": row.get("provider_trade_status"),
                             "provider_is_st_reported": row.get("provider_is_st"),
                             "limit_ratio": ratio, "limit_up": up, "limit_down": down,
                             "limit_model_applicable": not resumption})
        rows_total += len(enriched)
        path = output / "stocks" / (symbol + "_daily_enriched.parquet")
        provenance = {"symbol": symbol, "backtest_ready": False,
                      "limit_model": "board and date rules with half-up cent rounding",
                      "provider_flags_are_staging": True,
                      "source": str(source), "source_sha256": sha(source)}
        table = pa.Table.from_pylist(enriched)
        table = table.replace_schema_metadata({b"stock.provenance": json.dumps(provenance).encode()})
        pq.write_table(table, path, compression="zstd")
        files.append({"symbol": symbol, "rows": len(enriched), "path": str(path), "sha256": sha(path)})
    manifest = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
                "backtest_ready": False,
                "readiness_gaps": ["full_universe_not_covered", "available_at_unknown", "tradable_flag_unknown",
                                   "ipo_and_relisting_limit_exemptions_not_modelled", "provider_flags_unverified"],
                "limit_rules": {"star_688": 0.20, "chinext_300_301": {"before_2020_08_24": 0.10, "from": 0.20},
                                "main_board": 0.10, "main_board_st": 0.05},
                "validation": "derived limits are checked against observed high and low, skipping each series first five rows and every bar that follows a break longer than seven days",
                "source_files_seen": len(discovered), "duplicate_source_files": len(discovered) - len(newest),
                "symbols": len(files), "rows": rows_total,
                "rows_breaching_derived_limits": len(violations), "violations": violations[:200], "files": files}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "symbols": len(files), "rows": rows_total,
                      "rows_breaching_derived_limits": len(violations)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
