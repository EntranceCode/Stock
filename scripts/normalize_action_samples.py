"""Normalize cached action records without inventing payment/announcement times."""
from __future__ import annotations
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps" / "parquet"))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup


def cash_per_unit(description: str) -> float:
    match = re.fullmatch(r"每10份派现金([0-9]+(?:\.[0-9]+)?)元", description.strip())
    if not match:
        raise ValueError(f"Unrecognized dividend description: {description}")
    value = float(match.group(1)) / 10
    if not math.isfinite(value):
        raise ValueError("Non-finite cash distribution")
    return value


def split_multiplier(description: str) -> float:
    match = re.fullmatch(r"1:([0-9]+(?:\.[0-9]+)?)", description.strip())
    if not match or not math.isfinite(float(match.group(1))) or float(match.group(1)) <= 0:
        raise ValueError(f"Unrecognized split ratio: {description}")
    return float(match.group(1))


def gross_return(previous_close: float, close: float, shares_multiplier: float = 1, cash: float = 0) -> float:
    """Gross ex-event return per pre-event unit, excluding taxes and reinvestment fees."""
    if not all(math.isfinite(value) for value in (previous_close, close, shares_multiplier, cash)):
        raise ValueError("Non-finite event return inputs")
    if previous_close <= 0 or close <= 0 or shares_multiplier <= 0 or cash < 0:
        raise ValueError("Invalid event return inputs")
    value = (close * shares_multiplier + cash) / previous_close - 1
    if not math.isfinite(value):
        raise ValueError("Event return overflow")
    return value


def stock_event_values(row: dict) -> tuple[float, float]:
    """Cross-check per-ten-share fields against the implementation description."""
    description = str(row.get("IMPL_PLAN_PROFILE") or "").strip()
    if not re.match(r"^(?:每)?10(?:股)?(?:送|转|派)", description):
        raise ValueError(f"Unverified per-ten-share description: {description}")
    cash_match = re.search(r"派([0-9]+(?:\.[0-9]+)?)元", description)
    cash_description = float(cash_match.group(1)) if cash_match else 0.0
    shares_description = sum(float(value) for value in re.findall(r"(?:送|转增|转)([0-9]+(?:\.[0-9]+)?)", description))
    cash_raw = float(row.get("PRETAX_BONUS_RMB") or 0)
    shares_raw = float(row.get("BONUS_IT_RATIO") or 0)
    if not all(math.isfinite(value) and value >= 0 for value in (cash_raw, shares_raw, cash_description, shares_description)):
        raise ValueError("Invalid stock event values")
    if not math.isclose(cash_raw, cash_description, rel_tol=1e-6, abs_tol=1e-6):
        raise ValueError("Cash field disagrees with implementation description")
    if not math.isclose(shares_raw, shares_description, rel_tol=1e-6, abs_tol=1e-6):
        raise ValueError("Share field disagrees with implementation description")
    return cash_raw / 10, 1 + shares_raw / 10


def apply_issuer_evidence(events: list[dict], source: Path, start_date: date, as_of: date) -> None:
    """Add official table observations and refine a split without guessing its ex session."""
    path = source / "511010-dividends.json"
    evidence = json.loads(path.read_text(encoding="utf-8"))
    headers = [cell.get_text(strip=True) for cell in BeautifulSoup(evidence["table_html"], "html.parser").select("thead th")]
    records = [[cell.get_text(strip=True) for cell in row.select("td")] for row in BeautifulSoup(evidence["table_html"], "html.parser").select("tbody tr")]
    if headers != ["除息日", "权益登记日", "红利分配日", "每份分红(元)"] or records != evidence["records"]:
        raise ValueError("Issuer table records disagree with saved DOM or unit headers")
    if evidence["fund_code"] != "511010" or evidence["cash_unit"] != "CNY_per_fund_unit":
        raise ValueError("Unexpected issuer fund or cash unit")
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    notice = evidence.get("verified_original_notice_example", {})
    for ex_text, record_text, pay_text, cash_text in records:
        ex_date, record_date, payment = map(date.fromisoformat, (ex_text, record_text, pay_text))
        cash = float(cash_text)
        if not math.isfinite(cash) or cash < 0 or not record_date <= ex_date <= payment:
            raise ValueError("Invalid issuer event values or date order")
        if not start_date <= ex_date <= as_of:
            continue
        matches_notice = notice.get("ex_date") == ex_text
        if matches_notice and (notice["record_date"] != record_text or notice["payment_date"] != pay_text or float(notice["cash_cny_per_unit"]) != cash):
            raise ValueError("Issuer table disagrees with original notice")
        events.append({"symbol": "511010", "asset_type": "etf", "ex_date": ex_date, "source_event_date": ex_date,
                       "event_type": "distribution", "effective_date_unknown": False, "cash_per_old_unit_gross": cash,
                       "shares_multiplier": 1.0, "record_date": record_date, "payment_date": payment,
                       "announcement_date": date.fromisoformat(notice["announcement_date"]) if matches_notice else None,
                       "source_file": str(path), "source_sha256": source_hash, "source_url": evidence["source_url"],
                       "original_announcement_url": notice["source_url"] if matches_notice else None,
                       "validation_status": "issuer_table_and_original_notice_match" if matches_notice else "current_issuer_table; original_notice_not_checked"})
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for proof in manifest["evidence"]:
        if proof.get("event_type") != "share_split":
            continue
        if not start_date <= date.fromisoformat(proof["effective_date"]) <= as_of:
            continue
        matching = [event for event in events if event["symbol"] == proof["symbol"] and event["event_type"] == "split"
                    and str(event["source_event_date"]) == proof["effective_date"]]
        if len(matching) != 1:
            raise ValueError("Official split evidence does not match exactly one cached event")
        multiplier = float(proof["shares_multiplier"])
        if not math.isfinite(multiplier) or multiplier <= 0:
            raise ValueError("Invalid official split multiplier")
        matching[0].update({"shares_multiplier": multiplier, "ex_date": None,
                            "announcement_date": date.fromisoformat(proof["announcement_date"]),
                            "effective_phase": proof["effective_phase"], "rounding_rule": proof["rounding_rule"],
                            "price_adjustment_date_candidate": date.fromisoformat(proof["price_adjustment_date_candidate"]),
                            "original_announcement_url": proof["source_url"], "issuer_evidence_file": str(manifest_path),
                            "issuer_evidence_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                            "validation_status": "official_ratio_and_end_of_day_phase; ex_trading_session_not_verified"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=PROJECT / "data" / "external" / "actions" / "20261007T084713Z_043d9ea1")
    parser.add_argument("--stock-source", type=Path, help="Versioned paginated stock-action snapshot; replaces sample stock records")
    parser.add_argument("--issuer-source", type=Path, help="Saved manager table and original split notice observations")
    parser.add_argument("--allow-quarantine", action="store_true", help="Accept a partial inspection output while retaining rejected source records")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(timezone(timedelta(hours=8))).date())
    args = parser.parse_args()
    if args.start_date > args.as_of:
        parser.error("start-date must not follow as-of")
    source = args.source.resolve()
    original = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    output = PROJECT / "data" / "external" / "normalized_actions" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    events, errors, excluded = [], [], []
    stock_manifest = None
    stock_paths = sorted(source.glob("stock_*.json"))
    universe = None
    if args.stock_source:
        stock_source = args.stock_source.resolve()
        stock_manifest = json.loads((stock_source / "manifest.json").read_text(encoding="utf-8"))
        if not stock_manifest.get("acquisition_complete"):
            raise ValueError("Incomplete paginated stock-action snapshot")
        if date.fromisoformat(stock_manifest["as_of"]) < args.as_of or date.fromisoformat(stock_manifest["start_date"]) > args.start_date:
            raise ValueError("Stock snapshot does not cover the requested date interval")
        stock_paths = [stock_source / item["path"] for item in stock_manifest["pages"]]
        universe = set(stock_manifest["requested_symbols"])
    for path in stock_paths:
        rows = json.loads(path.read_text(encoding="utf-8"))["payload"]["result"]["data"]
        source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        for row in rows:
            symbol = row["SECURITY_CODE"]
            if universe is not None and symbol not in universe:
                continue
            if row.get("ASSIGN_PROGRESS") != "实施分配" or not row.get("EX_DIVIDEND_DATE"):
                excluded.append({"symbol": symbol, "reason": "not_implemented_or_no_ex_date"})
                continue
            ex_date = pd.Timestamp(row["EX_DIVIDEND_DATE"]).date()
            if not args.start_date <= ex_date <= args.as_of:
                continue
            try:
                cash, shares = stock_event_values(row)
            except ValueError as exc:
                errors.append({"symbol": symbol, "ex_date": str(ex_date), "error": str(exc), "source_file": str(path), "source_record": row})
                continue
            if cash == 0 and shares == 1:
                continue
            events.append({"symbol": symbol, "asset_type": "stock", "ex_date": ex_date, "cash_per_old_unit_gross": cash,
                           "shares_multiplier": shares, "payment_date": None, "announcement_date": None,
                           "source_event_date": ex_date, "event_type": "distribution", "effective_date_unknown": False,
                           "source_file": str(path), "source_sha256": source_hash,
                           "validation_status": "per_ten_fields_match_description; original_announcement_and_payment_unknown"})
    for path in sorted(source.glob("etf_*_events_*.csv")):
        symbol = path.name.split("_")[1]
        table = pd.read_csv(path, dtype=str)
        for _, row in table.iterrows():
            try:
                if "除息日" in table.columns:
                    if "暂无分红信息" in str(row["除息日"]):
                        continue
                    ex_date = pd.Timestamp(row["除息日"]).date()
                    cash, shares = cash_per_unit(row["每10份分红"]), 1.0
                    payment = pd.Timestamp(row["分红发放日"]).date()
                    if payment < ex_date:
                        raise ValueError("Payment date precedes ex date")
                elif "拆分折算日" in table.columns:
                    if "暂无拆分" in str(row["拆分折算日"]):
                        continue
                    ex_date = pd.Timestamp(row["拆分折算日"]).date()
                    cash, shares = 0.0, split_multiplier(row["拆分折算比例"])
                    payment = None
                else:
                    raise ValueError("Unrecognized action table columns")
                if not args.start_date <= ex_date <= args.as_of:
                    continue
                is_split = "拆分折算日" in table.columns
                # A table's conversion date may refer to a post-close conversion,
                # rather than the first trading session quoted per new unit.
                events.append({"symbol": symbol, "asset_type": "etf", "ex_date": None if is_split else ex_date, "cash_per_old_unit_gross": cash,
                               "shares_multiplier": shares, "payment_date": payment, "announcement_date": None,
                               "source_event_date": ex_date, "event_type": "split" if is_split else "distribution",
                               "effective_date_unknown": is_split,
                               "source_file": str(path), "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                               "validation_status": "public_table_parsed; original_announcement_not_checked"})
            except Exception as exc:
                errors.append({"symbol": symbol, "source": str(path), "error": str(exc)})
    if args.issuer_source:
        apply_issuer_evidence(events, args.issuer_source.resolve(), args.start_date, args.as_of)
    frame = pd.DataFrame(events)
    if frame.empty:
        raise ValueError("No events were normalized")
    frame = frame.sort_values(["symbol", "source_event_date"])
    if frame.duplicated(["symbol", "source_event_date", "cash_per_old_unit_gross", "shares_multiplier"]).any():
        raise ValueError("Duplicate action records; do not apply them twice")
    provenance = {"source": str(source), "stock_source": str(args.stock_source.resolve()) if args.stock_source else None,
                  "issuer_source": str(args.issuer_source.resolve()) if args.issuer_source else None,
                  "as_of": str(args.as_of), "start_date": str(args.start_date), "backtest_ready": False,
                  "readiness_gaps": ["original_announcement_validation", "stock_payment_dates_and_tax",
                                     "full_event_type_and_security_coverage", "historical_issuer_event_completeness", "ratio_precision_and_effective_date"]}
    if errors:
        provenance["readiness_gaps"].append("quarantined_event_field_disagreements")
    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps(provenance).encode()})
    pq.write_table(table, output / "events.parquet", compression="zstd")
    manifest = {**provenance, "output": str(output), "rows": len(frame), "errors": errors,
                "source_download_errors": [item for item in original["errors"] if not (args.issuer_source and item.get("symbol") == "511010")],
                "source_error_history": original["errors"] + (stock_manifest.get("errors", []) if stock_manifest else []),
                "excluded_plans": len(excluded),
                "normalization_complete": not errors, "quarantined_events": len(errors),
                "stock_source_acquisition": stock_manifest and {key: stock_manifest[key] for key in ("requested_symbol_count", "symbols_with_records", "symbols_without_source_records", "acquisition_complete")},
                "counts_by_asset": frame.asset_type.value_counts().to_dict()}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: manifest[key] for key in ("output", "rows", "quarantined_events", "normalization_complete", "backtest_ready", "counts_by_asset")}, ensure_ascii=True, indent=2))
    if errors and not args.allow_quarantine:
        raise RuntimeError("Some cached event records could not be normalized")


if __name__ == "__main__":
    main()
