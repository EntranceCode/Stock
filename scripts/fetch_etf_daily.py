"""Fetch versioned unadjusted ETF research samples, never a historical-universe proof."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import hashlib
import json
from pathlib import Path
import sys
import requests
from fetch_http_reference import capture_http_response, china_today, iso_date, new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps" / "parquet"))
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

SYMBOLS = {"sh510050": "上证50ETF", "sh510300": "沪深300ETF", "sh510500": "中证500ETF",
           "sh510880": "红利ETF", "sz159915": "创业板ETF", "sh511010": "国债ETF"}
URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"


def request_year(symbol, year, as_of, raw_path):
    start, end = f"{year}-01-01", min(f"{year}-12-31", as_of)
    if start > end:
        raise ValueError("Requested ETF year is after as-of")
    param = f"{symbol},day,{start},{end},320,"
    response = requests.get(URL, params={"param": param}, timeout=15)
    payload, evidence = capture_http_response(raw_path, response, {"param": param}, as_of, URL)
    rows = payload.get("data", {}).get(symbol, {}).get("day", [])
    if payload.get("code") != 0 or not rows:
        raise ValueError(f"Empty or failed ETF reference {symbol} {year}: {payload.get('msg')}")
    if len(rows) >= 320:
        raise ValueError("Potentially truncated annual history")
    rows = [r for r in rows if start <= r[0] <= end]
    if not rows:
        raise ValueError("Empty requested annual history")
    return {"symbol": symbol, "year": year, "param": param, "rows": rows,
            "raw_response": str(raw_path), "retrieved_at": evidence["retrieved_at"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    args = parser.parse_args(argv)
    if args.as_of < "2018-01-01":
        raise ValueError("ETF sample begins in 2018; as-of must be on or after 2018-01-01")
    output = new_snapshot_directory(PROJECT / "data" / "external" / "etf")
    (output / "raw").mkdir()
    manifest = {"run_id": output.name, "output": str(output), "provider": URL, "as_of": args.as_of,
                "selection": "Six explicit engineering samples; not point-in-time full ETF universe",
                "price_basis": "unadjusted", "backtest_ready": False,
                "readiness_gaps": ["corporate_actions_and_total_returns", "historical_etf_universe",
                                   "historical_rules_and_status", "verified_turnover_and_units"],
                "raw_response_format": "complete response_text with request and retrieval evidence",
                "raw_responses": [], "files": [], "errors": []}
    records = {symbol: [] for symbol in SYMBOLS}
    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks = {pool.submit(request_year, symbol, year, args.as_of, output / "raw" / f"{symbol}_{year}.json"): (symbol, year)
                 for symbol in SYMBOLS for year in range(2018, int(args.as_of[:4]) + 1)}
        for future in as_completed(tasks):
            symbol, year = tasks[future]
            try:
                response = future.result()
                records[symbol].extend(response["rows"])
                print(symbol, year, len(response["rows"]), flush=True)
            except Exception as exc:
                manifest["errors"].append({"symbol": symbol, "year": year, "error": str(exc)})
            finally:
                raw_path = output / "raw" / f"{symbol}_{year}.json"
                if raw_path.exists():
                    manifest["raw_responses"].append({"symbol": symbol, "year": year, "path": str(raw_path),
                                                      "sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest()})
    for symbol, rows in records.items():
        if not rows:
            continue
        # The provider's ordering is date, open, close, high, low, volume.
        frame = pd.DataFrame([r[:6] for r in rows], columns=["trade_date", "open", "close", "high", "low", "volume_raw"])
        frame = frame.sort_values("trade_date")
        if frame.trade_date.duplicated().any():
            manifest["errors"].append({"symbol": symbol, "error": "Duplicate dates across annual requests"})
            continue
        frame["trade_date"] = pd.to_datetime(frame.trade_date).dt.date
        for field in ["open", "high", "low", "close", "volume_raw"]:
            frame[field] = pd.to_numeric(frame[field], errors="raise")
        prices = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
        o, h, lo, c = prices.T
        if not np.isfinite(prices).all() or (prices <= 0).any() or (h < np.maximum.reduce([o, lo, c])).any() or (lo > np.minimum.reduce([o, h, c])).any() or not np.isfinite(frame.volume_raw).all() or (frame.volume_raw < 0).any():
            manifest["errors"].append({"symbol": symbol, "error": "Invalid ETF OHLC or volume"})
            continue
        frame["symbol"] = symbol[2:]
        frame["exchange"] = "SSE" if symbol.startswith("sh") else "SZSE"
        frame["asset_type"] = "government_bond_etf" if symbol == "sh511010" else "equity_etf"
        metadata = {"provider": URL, "price_basis": "unadjusted", "backtest_ready": False,
                    "readiness_gaps": manifest["readiness_gaps"], "volume_unit": "unverified_provider_units",
                    "as_of": args.as_of,
                    "raw_responses": [item for item in manifest["raw_responses"] if item["symbol"] == symbol]}
        table = pa.Table.from_pandas(frame, preserve_index=False)
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps(metadata).encode()})
        path = output / f"{symbol}_daily.parquet"
        pq.write_table(table, path, compression="zstd")
        manifest["files"].append({"symbol": symbol, "name": SYMBOLS[symbol], "rows": len(frame), "start": str(frame.trade_date.min()),
                                  "end": str(frame.trade_date.max()), "path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest["complete"] = not manifest["errors"] and len(manifest["files"]) == len(SYMBOLS)
    save_json_once(output / "manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=True, indent=2), flush=True)
    if not manifest["complete"]:
        raise RuntimeError("ETF sample download incomplete; consult manifest errors")
    return output


if __name__ == "__main__":
    main()
