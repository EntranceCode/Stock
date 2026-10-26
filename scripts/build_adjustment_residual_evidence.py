"""Audit the adjusted series against its events (design §5.1, adjustment criterion).

The earlier enumeration of this evidence compared each ex-date's adjusted move
with the nominal price limit plus one tick. That test conflates two questions
and produced 23 "residuals" that were actually raw price observations sitting a
few cents above a tick-rounded limit price - not adjustment errors.

This audit asks the precise question instead, in two parts:

1. **Adjustment exactness.** For every bar, the cumulative adjustment factor's
   day-over-day ratio must equal the ratio implied by that day's corporate
   action: ``multiplier * close_prev / (close_prev - cash_per_share)``, where
   the cash is the event's per-share amount (the provider's dividend field is
   per ten shares and the normalizer divided it, verified against the raw
   ``IMPL_PLAN_PROFILE`` text). A bar whose ratio disagrees is a genuine
   residual adjustment jump: the adjustment did not reproduce the ex-reference.
2. **Limit-band observations.** For every ex-date, the exchange's limit band
   around the event's ex-reference (board- and era-aware, cent rounding) is
   computed, and any observed raw high or low outside that band is enumerated
   separately - a raw price observation, not an adjustment residual.

Both lists are written per item with the numbers behind them, so the
criterion's answer - and the price-limit recheck's - can be audited item by
item. Usage:

    python -B scripts/build_adjustment_residual_evidence.py
    python -B scripts/build_adjustment_residual_evidence.py --register
"""
from __future__ import annotations

import argparse
import decimal
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

LIMIT_START = date(1996, 12, 16)
CHINEXT_CHANGE = date(2020, 8, 24)
FACTOR_TOLERANCE = 1e-6  # relative, for float arithmetic on cumulative factors
BAND_TOLERANCE = 0.005   # half a cent: the limit price is cent-rounded, prices are floats


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(path: Path, project_root: Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(project_root) / resolved
    return resolved.resolve()


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


def limit_ratio(code: str, day: date) -> float:
    """Price limit for the security's board and era."""
    if code.startswith("688"):
        return 0.2
    if code.startswith(("300", "301")):
        return 0.2 if day >= CHINEXT_CHANGE else 0.1
    return 0.1


def rounded_limit(reference: float, ratio: float) -> tuple[float, float, float]:
    """Exchange-style limit prices around an ex-reference.

    The exchange rounds the ex-reference itself to the cent first, then rounds
    each limit price to the cent; skipping the first rounding overstates the
    band by up to a cent and flags limit-hits as breaches.
    """
    reference_rounded = float(decimal.Decimal(str(reference))
                              .quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP))

    def limit(signed):
        return float(decimal.Decimal(str(reference_rounded * (1 + signed * ratio)))
                     .quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP))
    return reference_rounded, limit(1.0), limit(-1.0)


def build(project_root: Path) -> tuple[dict, Path]:
    import pyarrow.parquet as pq

    project_root = Path(project_root)
    config = json.loads((project_root / "config/datasets.json").read_text(encoding="utf-8"))

    def manifest_of(dataset: str) -> tuple[Path, dict]:
        path = inside(Path(config["datasets"][dataset]["manifest"]), project_root)
        return path, json.loads(path.read_text(encoding="utf-8"))

    adjusted_path, adjusted = manifest_of("adjusted_prices")
    stock_path, stock_manifest = manifest_of("stock_daily")
    events_path, events_manifest = manifest_of("action_samples")
    events_entry = next((entry for entry in events_manifest.get("files", [])
                         if str(entry.get("path", "")).endswith("events.parquet")), None)
    if events_entry is None:
        raise ValueError("action_samples registers no events parquet")
    events = pq.read_table(inside(Path(events_entry["path"]), project_root)).to_pylist()
    ex_by_symbol: dict[str, dict[date, dict]] = {}
    unusable_events = []
    for event in events:
        day = as_day(event.get("ex_date"))
        if not day or not event.get("symbol"):
            continue
        cash, multiplier = event.get("cash_per_old_unit_gross"), event.get("shares_multiplier")
        if not isinstance(multiplier, (int, float)) or multiplier <= 0:
            unusable_events.append({"symbol": event["symbol"], "ex_date": day.isoformat(),
                                    "reason": "event multiplier is unusable"})
            continue
        if cash is None:
            cash = 0.0
        if not isinstance(cash, (int, float)) or cash < 0:
            unusable_events.append({"symbol": event["symbol"], "ex_date": day.isoformat(),
                                    "reason": "event cash amount is unusable"})
            continue
        # A plan can arrive as several rows sharing one ex-date (cash and share
        # components separately): they act together, so cash adds and
        # multipliers multiply.
        existing = ex_by_symbol.setdefault(str(event["symbol"]), {}).get(day)
        if existing is None:
            ex_by_symbol[str(event["symbol"])][day] = {
                "cash_per_share": float(cash), "multiplier": float(multiplier),
                "event_type": event.get("event_type"),
                "source_file": event.get("source_file"),
                "components": 1}
        else:
            existing["cash_per_share"] += float(cash)
            existing["multiplier"] *= float(multiplier)
            existing["components"] += 1

    adjusted_files = {str(entry.get("symbol")): entry["path"] for entry in adjusted.get("files", [])
                      if entry.get("symbol")}
    stock_files = {str(entry.get("symbol")): (entry.get("path") or entry.get("output"))
                   for entry in stock_manifest.get("files", []) if entry.get("symbol")}

    genuine_residuals, limit_observations = [], []
    bars_audited, ex_dates_audited = 0, 0
    skipped, within_band = [], 0
    for symbol, ex_dates in sorted(ex_by_symbol.items()):
        path = adjusted_files.get(symbol)
        if not path:
            skipped.append({"symbol": symbol, "reason": "no adjusted series registered"})
            continue
        rows = pq.read_table(inside(Path(path), project_root),
                             columns=["trade_date", "adjustment_factor", "close_raw"]).to_pylist()
        raw_bars = None  # the raw high/low, read only when a close leaves the band
        previous = None
        for row in rows:
            day = as_day(row.get("trade_date"))
            factor = row.get("adjustment_factor")
            raw_close = row.get("close_raw")
            if day is None or factor is None:
                previous = None
                continue
            bars_audited += 1
            event = ex_dates.get(day)
            if event is not None and previous is not None:
                ex_dates_audited += 1
                prev_day, prev_factor, prev_raw = previous
                if prev_raw and prev_raw > event["cash_per_share"]:
                    reference = (prev_raw - event["cash_per_share"]) / event["multiplier"]
                    implied_ratio = prev_raw / reference
                    applied_ratio = factor / prev_factor if prev_factor else None
                    if applied_ratio is None or abs(applied_ratio - implied_ratio) > FACTOR_TOLERANCE:
                        genuine_residuals.append({
                            "symbol": symbol, "ex_date": day.isoformat(),
                            "applied_factor_ratio": applied_ratio,
                            "event_implied_ratio": round(implied_ratio, 9),
                            "raw_close_previous": prev_raw, "cash_per_share": event["cash_per_share"],
                            "multiplier": event["multiplier"], "event_components": event["components"],
                            "event_type": event["event_type"],
                            "source_file": event.get("source_file")})
                    if day >= LIMIT_START:
                        ratio = limit_ratio(symbol, day)
                        reference_rounded, up, down = rounded_limit(reference, ratio)
                        outside = []
                        raw_close_value = float(raw_close) if raw_close is not None else None
                        if raw_close_value is not None and not (
                                down - BAND_TOLERANCE <= raw_close_value <= up + BAND_TOLERANCE):
                            outside.append({"field": "close", "value": round(raw_close_value, 4)})
                            # The raw high/low decide whether this is a close-only or a
                            # session-wide observation; read them from the registered bars.
                            stock_path_file = stock_files.get(symbol)
                            if stock_path_file and raw_bars is None:
                                raw_bars = {as_day(item.get("trade_date")): item for item in
                                            pq.read_table(inside(Path(stock_path_file), project_root),
                                                          columns=["trade_date", "high", "low"]).to_pylist()}
                            bar = (raw_bars or {}).get(day)
                            for field, bound, direction in (("high", up, 1), ("low", down, -1)):
                                value = (bar or {}).get(field)
                                if value is None:
                                    continue
                                value = float(value)
                                beyond = (value > bound + BAND_TOLERANCE if direction > 0
                                          else value < bound - BAND_TOLERANCE)
                                if beyond:
                                    outside.append({"field": field, "value": round(value, 4)})
                        if outside:
                            limit_observations.append({
                                "symbol": symbol, "ex_date": day.isoformat(),
                                "ex_reference": round(reference, 6),
                                "ex_reference_rounded": reference_rounded,
                                "limit_up": up, "limit_down": down, "limit_ratio": ratio,
                                "outside": outside,
                                "raw_close": round(float(raw_close), 4) if raw_close else None,
                                "event_type": event["event_type"],
                                "cash_per_share": event["cash_per_share"],
                                "multiplier": event["multiplier"]})
                        else:
                            within_band += 1
            previous = (day, factor, raw_close)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + hashlib.sha256(
        adjusted_path.read_bytes()).hexdigest()[:8]
    run_dir = project_root / "data/external/adjustment_residuals" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    residual_path = run_dir / "residual_jumps.jsonl"
    residual_path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n"
                                    for item in genuine_residuals), encoding="utf-8")
    observations_path = run_dir / "limit_band_observations.jsonl"
    observations_path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n"
                                        for item in limit_observations), encoding="utf-8")
    manifest = {
        "run_id": run_id,
        "evidence_kind": "adjustment_residuals",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "purpose": "adjustment exactness audit: factor ratios versus event-implied "
                   "ex-references, with limit-band observations enumerated separately "
                   "(design §5.1)",
        "source_manifests": [
            {"path": str(adjusted_path), "sha256": sha256(adjusted_path),
             "role": "adjusted price series"},
            {"path": str(events_path), "sha256": sha256(events_path),
             "role": "ex-date events"},
        ],
        "methodology": {
            "adjustment_exactness": ("the cumulative factor's day-over-day ratio must equal "
                                     "multiplier * close_prev / (close_prev - cash_per_share); "
                                     f"relative tolerance {FACTOR_TOLERANCE}"),
            "cash_unit": "per share (provider dividend field is per ten shares; the normalizer "
                         "divided it; verified against the raw IMPL_PLAN_PROFILE text)",
            "limit_band": {"main_board": 0.1, "star_688": 0.2,
                           "chinext_300_301": {"before_2020_08_24": 0.1, "from": 0.2},
                           "limit_start_date": LIMIT_START.isoformat(),
                           "rounding": "limit price rounded to the cent around the ex-reference"},
            "previous_methodology": "replaced the nominal-limit comparison, which conflated "
                                    "adjustment errors with raw price observations",
        },
        "bars_audited": bars_audited,
        "ex_dates_audited": ex_dates_audited,
        "ex_dates_within_limit_band": within_band,
        "symbols_without_adjusted_series": skipped,
        "unusable_events": unusable_events,
        "ex_dates_beyond_price_limit_after_adjustment": len(genuine_residuals),
        "residual_jump_evidence": "residual_jumps.jsonl",
        "limit_band_observations": len(limit_observations),
        "limit_band_observations_evidence": "limit_band_observations.jsonl",
        "files": [{"path": str(residual_path), "sha256": sha256(residual_path),
                   "rows": len(genuine_residuals)},
                  {"path": str(observations_path), "sha256": sha256(observations_path),
                   "rows": len(limit_observations)}],
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest, manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", action="store_true",
                        help="replace the registered adjustment_residuals evidence with this run")
    args = parser.parse_args()

    manifest, manifest_path = build(PROJECT)
    print(f"bars audited: {manifest['bars_audited']}; ex-dates audited: {manifest['ex_dates_audited']}; "
          f"genuine adjustment residuals: {manifest['ex_dates_beyond_price_limit_after_adjustment']}; "
          f"limit-band observations: {manifest['limit_band_observations']}; "
          f"within band: {manifest['ex_dates_within_limit_band']}; "
          f"symbols without an adjusted series: {len(manifest['symbols_without_adjusted_series'])}")
    if not args.register:
        print(f"Evidence written to {manifest_path}; rerun with --register to activate it.")
        return 0
    config_path = PROJECT / "config/datasets.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if "adjustment_residuals" not in config["datasets"]:
        config["datasets"]["adjustment_residuals"] = {
            "manifest": str(manifest_path.relative_to(PROJECT)),
            "purpose": "adjustment exactness audit with limit-band observations",
        }
    else:
        config["datasets"]["adjustment_residuals"]["manifest"] = str(manifest_path.relative_to(PROJECT))
    config["updated_on"] = datetime.now(timezone.utc).date().isoformat()
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Registered adjustment_residuals at {manifest_path.relative_to(PROJECT)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
