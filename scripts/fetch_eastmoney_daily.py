"""Capture public Eastmoney daily bars with volume and amount, wire-audited.

The registered local daily series carries neither turnover (成交额) nor share
volume: the earlier source provided only tick counts. The Eastmoney public
kline endpoint returns both alongside OHLC, and the project already acquires
other financial evidence from this provider. This collector captures each
reply once with its HTTP metadata, verifies the reply against the requested
security and window before using it, and records per-request outcomes in a
manifest rewritten after every request, so a terminated batch keeps its
inventory. Nothing here admits data for backtesting; the output is staging
evidence for the turnover and price-limit recheck criteria.

Usage:
    python -B scripts/fetch_eastmoney_daily.py --start 2018-01-01 --end 2026-07-07
    python -B scripts/fetch_eastmoney_daily.py --limit 200 --offset 400
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_http_reference import new_snapshot_directory, save_json_once, iso_date  # noqa: E402

KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
FIELDS1 = "f1,f2,f3,f4,f5,f6"
FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
WINDOW = {"start": "2018-01-01", "end": "2026-07-07"}
GAPS = ["turnover_unit_and_caliber_verification", "trade_status_and_suspensions_absent",
        "point_in_time_availability", "provider_values_independent_verification"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def secid_of(symbol: str) -> str:
    """Eastmoney market prefix: Shanghai codes trade on market 1, Shenzhen on 0."""
    return ("1." if symbol.startswith(("60", "68")) else "0.") + symbol


def candidate_symbols(start: str, end: str) -> tuple[list[dict], list[Path]]:
    """The same universe the price acquisition covers: registered daily symbols,
    registered ETFs, and the retired candidates the reference run observed."""
    config = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    sources: list[Path] = []
    items: dict[str, dict] = {}
    for dataset in ("stock_daily", "etf_daily"):
        manifest_path = Path(config["datasets"][dataset]["manifest"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        sources.append(manifest_path)
        for entry in manifest.get("files", []):
            symbol = str(entry.get("symbol", ""))
            code = symbol[2:] if symbol[:2] in ("sh", "sz") else symbol
            if code:
                items.setdefault(code, {"code": code, "start": start, "end": end,
                                        "selection": f"registered_{dataset}"})
    reference = Path(config["datasets"]["security_metadata"]["manifest"])
    reference_manifest = json.loads(reference.read_text(encoding="utf-8"))
    report = json.loads(Path(reference_manifest["report"]).read_text(encoding="utf-8"))
    sources.append(reference)
    # The report records the overlap as a count and lists the members whose
    # local series is missing; those are the retired codes whose observed
    # sessions a boundary check would need.
    retired = report.get("retired_A_codes_overlapping_research_missing_local_prices") or []
    if not isinstance(retired, list):
        retired = []
    for code in retired:
        items.setdefault(str(code), {"code": str(code), "start": start, "end": end,
                                     "selection": "retired_candidate_missing_local_prices"})
    return [items[code] for code in sorted(items)], sources


def verify_reply(payload: dict, item: dict) -> list[dict]:
    """Normalized rows, or a raised ValueError when the reply is not usable."""
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("klines"), list):
        raise ValueError("Reply carries no kline list")
    if data.get("code") != item["code"]:
        raise ValueError(f"Reply is for {data.get('code')}, not {item['code']}")
    rows, seen = [], set()
    for line in data["klines"]:
        parts = line.split(",")
        if len(parts) != 11:
            raise ValueError("Reply row width differs from the requested fields")
        day = parts[0]
        if not item["start"] <= day <= item["end"]:
            raise ValueError("Reply row outside the requested window")
        if day in seen:
            raise ValueError("Reply repeats a session")
        seen.add(day)
        rows.append({"trade_date": day, "symbol": item["code"], "open": float(parts[1]),
                     "close": float(parts[2]), "high": float(parts[3]), "low": float(parts[4]),
                     "volume_shares": float(parts[5]), "amount_cny": float(parts[6]),
                     "amplitude_percent": float(parts[7]), "pct_change_percent": float(parts[8]),
                     "change_amount": float(parts[9]), "turnover_rate": float(parts[10])})
    if any(float(row["high"]) < float(row["low"]) for row in rows):
        raise ValueError("Reply holds a bar whose high is below its low")
    return rows


def write_manifest(path: Path, manifest: dict) -> None:
    """Rewrite this run's manifest so an interrupted batch keeps its inventory."""
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
    temporary.replace(path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=iso_date, default=WINDOW["start"])
    parser.add_argument("--end", type=iso_date, default=WINDOW["end"])
    parser.add_argument("--limit", type=int, help="Only process the first N symbols")
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--pause", type=float, default=0.12)
    args = parser.parse_args(argv)
    if date.fromisoformat(args.start) > date.fromisoformat(args.end):
        raise ValueError("Invalid history interval")
    if args.offset < 0 or args.pause < 0.05 or args.pause > 5:
        raise ValueError("Invalid batch controls")
    import pyarrow as pa
    import pyarrow.parquet as pq
    import requests

    items, sources = candidate_symbols(args.start, args.end)
    selected = items[args.offset:args.offset + args.limit if args.limit else None]
    if not selected:
        raise ValueError("Empty acquisition batch")
    output = new_snapshot_directory(PROJECT / "data" / "external" / "eastmoney_daily")
    (output / "raw").mkdir(parents=True)
    (output / "observations").mkdir()
    manifest = {"run_id": output.name, "output": str(output), "provider": "Eastmoney public kline API",
                "as_of": args.end, "start": args.start, "end": args.end, "backtest_ready": False,
                "readiness_gaps": GAPS, "price_basis": "provider reported, unadjusted",
                "selection": "registered daily universe, registered ETFs and retired candidates",
                "requested_symbols": len(selected), "total_candidates": len(items),
                "offset": args.offset, "limit": args.limit,
                "source_manifests": [{"path": str(path), "sha256": sha(path)} for path in sources],
                "files": [], "outcomes": [], "errors": []}
    save_json_once(output / "requests.json", selected)
    write_manifest(output / "manifest.json", manifest)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"})
    import time
    for index, item in enumerate(selected, 1):
        try:
            params = {"secid": secid_of(item["code"]), "fields1": FIELDS1, "fields2": FIELDS2,
                      "klt": "101", "fqt": "0", "beg": item["start"].replace("-", ""),
                      "end": item["end"].replace("-", "")}
            response = session.get(KLINE, params=params, timeout=(10, 40))
            body = output / "raw" / f"{item['code']}.json"
            body.write_bytes(response.content)
            response.raise_for_status()
            payload = response.json()
            rows = verify_reply(payload, item)
            if not rows:
                raise ValueError("Empty history is unresolved, not proof of no trading")
            table = pa.Table.from_pylist(rows).replace_schema_metadata({
                b"stock.provenance": json.dumps({
                    "provider": manifest["provider"], "price_basis": "unadjusted",
                    "backtest_ready": False, "available_at_unknown": True,
                    "turnover_unit": "provider documented CNY",
                    "volume_unit": "provider documented shares",
                    "source_response": str(body), "source_response_sha256": sha(body),
                    "retrieved_at": datetime.now(timezone.utc).isoformat()}).encode()})
            observation = output / "observations" / f"{item['code']}_daily.parquet"
            pq.write_table(table, observation, compression="zstd")
            manifest["files"].append({"symbol": item["code"], "rows": len(rows),
                                      "start": rows[0]["trade_date"], "end": rows[-1]["trade_date"],
                                      "path": str(observation), "sha256": sha(observation),
                                      "source_request": item,
                                      "source_response": str(body),
                                      "source_response_sha256": sha(body)})
            manifest["outcomes"].append({**item, "status": "acquired", "rows": len(rows)})
            print(f"{index}/{len(selected)} {item['code']} rows={len(rows)}", flush=True)
        except Exception as exc:
            manifest["errors"].append({**item, "status": "unresolved", "error": str(exc)})
            manifest["outcomes"].append({**item, "status": "unresolved", "error": str(exc)})
            print(f"{index}/{len(selected)} {item['code']} unresolved: {exc}", flush=True)
        write_manifest(output / "manifest.json", manifest)
        time.sleep(args.pause)
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    manifest["total_rows"] = sum(item["rows"] for item in manifest["files"])
    manifest["complete"] = not manifest["errors"]
    write_manifest(output / "manifest.json", manifest)
    print(json.dumps({"manifest": str(output / "manifest.json"), "files": len(manifest["files"]),
                      "rows": manifest["total_rows"], "errors": len(manifest["errors"])}))
    return 0 if manifest["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
