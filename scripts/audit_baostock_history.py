"""Rebuild saved BaoStock observations from wire bytes and compare controls.

This offline audit verifies transformations and sample price agreement. It
does not verify the provider's full universe, account execution eligibility,
or the original publication time of historical observations.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from uuid import uuid4
import zlib

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
from fetch_baostock_history import FIELDS, normalize_rows


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(path):
    path = path.resolve()
    if not path.is_relative_to(PROJECT):
        raise ValueError("Audit evidence must stay inside the project")
    return path


def protocol_rows(capture):
    """Check the SDK 0.9.4 compressed-history frame without using its parser."""
    wire = base64.b64decode(capture["wire_response_base64"], validate=True)
    request = base64.b64decode(capture["wire_request_base64"], validate=True)
    if not request.endswith(b"\n") or not wire.endswith(b"<![CDATA[]]>\n"):
        raise ValueError("Incomplete request or response wire frame")
    header = wire[:21].decode("utf-8")
    parts = header.split("\1")
    if len(parts) != 3 or parts[1] != "96":
        raise ValueError("Unexpected compressed history response header")
    length = int(parts[2])
    compressed = wire[21:21 + length]
    if not re.fullmatch(rb"\x01[0-9]{1,10}\n<!\[CDATA\[\]\]>\n", wire[21 + length:]):
        raise ValueError("Wire frame length does not match response header")
    decoded = header + zlib.decompress(compressed).decode("utf-8")
    if decoded != capture["decoded_protocol_response"]:
        raise ValueError("Saved SDK response differs from independently decoded wire")
    body = decoded[decoded.index("{"):]
    payload, end = json.JSONDecoder().raw_decode(body)
    tail = body[end:].split("\1")
    params = capture["params"]
    if tail[1:7] != [params["code"], FIELDS, params["start"], params["end"], "d", "3"]:
        raise ValueError("Response parameters differ from the requested history")
    return payload["record"]


def audit(manifest_path, base):
    import pyarrow.parquet as pq
    manifest_path = inside(manifest_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for source in manifest["source_manifests"]:
        if sha(inside(Path(source["path"]))) != source["sha256"]:
            raise ValueError("Source inventory changed after acquisition")
    requests = json.loads((manifest_path.parent / "requests.json").read_text(encoding="utf-8"))
    raw_by_code = {}
    for item in manifest["raw_responses"]:
        path = inside(Path(item["path"]))
        if sha(path) != item["sha256"]:
            raise ValueError("Raw response SHA-256 mismatch")
        capture = json.loads(path.read_text(encoding="utf-8"))
        if capture["params"]["code"] != item["code"]:
            raise ValueError("Response inventory code mismatch")
        raw_by_code.setdefault(item["code"], []).append((item["page"], capture))
    observations = {item["symbol"]: item for item in manifest["observations"]}
    controls, results = [], []
    for entry in manifest["files"]:
        symbol = entry["symbol"]
        params = entry["source_request"]
        pages = sorted(raw_by_code[params["code"]])
        if [page for page, _ in pages] != list(range(1, len(pages) + 1)):
            raise ValueError("Response page sequence is incomplete")
        raw_rows = [row for _, capture in pages for row in protocol_rows(capture)]
        rebuilt = normalize_rows(FIELDS.split(","), raw_rows, params["code"], params["start"], params["end"])
        observed = observations[symbol]
        obs_path, price_path = inside(Path(observed["path"])), inside(Path(entry["output"]))
        if sha(obs_path) != observed["sha256"] or sha(price_path) != entry["output_sha256"]:
            raise ValueError("Normalized output SHA-256 mismatch")
        saved_observations = pq.read_table(obs_path).to_pylist()
        expected_prices = [{**row, "volume_raw": row["volume_shares_reported"], "turnover": row["turnover_cny_reported"]}
                           for row in rebuilt if row["provider_trade_status"] == 1]
        prices = pq.read_table(price_path).to_pylist()
        if rebuilt != saved_observations or prices != expected_prices:
            raise ValueError("Normalized records differ from saved source responses")
        if len(rebuilt) != observed["rows"] or len(prices) != entry["rows"]:
            raise ValueError("Saved row counts differ from inventory")
        unusual_amounts = []
        for row in prices:
            volume = row["volume_shares_reported"]
            if volume and not row["low"] - 0.01 <= row["turnover_cny_reported"] / volume <= row["high"] + 0.01:
                unusual_amounts.append(str(row["trade_date"]))
        results.append({"symbol": symbol, "source_pages": len(pages), "observations": len(rebuilt),
                        "active_price_rows": len(prices), "halted_observations": len(rebuilt) - len(prices),
                        "wire_decode_and_normalization_exact": True,
                        "amount_per_reported_share_outside_daily_range_dates": unusual_amounts})
        if symbol in base:
            old_entry = base[symbol]
            old_path = inside(Path(old_entry["output"]))
            if sha(old_path) != old_entry["output_sha256"]:
                raise ValueError("Original price file SHA-256 mismatch")
            old_rows = {row["trade_date"]: row for row in pq.read_table(old_path).to_pylist()}
            shared = [row for row in prices if row["trade_date"] in old_rows]
            mismatches, maximum, volume_differences = 0, 0.0, 0
            share_deltas = []
            for row in shared:
                old = old_rows[row["trade_date"]]
                for key in ("open", "high", "low", "close"):
                    difference = abs(old[key] - row[key])
                    maximum = max(maximum, difference)
                    mismatches += difference > 0.0001 + abs(old[key]) * (2 ** -23) * 2
                estimate = old.get("volume_shares_estimate")
                if estimate is not None and estimate != row["volume_shares_reported"]:
                    volume_differences += 1
                if estimate is not None:
                    share_deltas.append(estimate - row["volume_shares_reported"])
            original_window = {day for day in old_rows if params["start"] <= str(day) <= params["end"]}
            provider_active = {row["trade_date"] for row in prices}
            provider_halted = {row["trade_date"] for row in rebuilt if row["provider_trade_status"] == 0}
            controls.append({"symbol": symbol, "shared_active_days": len(shared), "OHLC_mismatched_cells": mismatches,
                             "maximum_absolute_price_difference": maximum,
                             "original_volume_estimate_differs_from_provider_days": volume_differences,
                             "original_estimate_minus_provider_shares_min": min(share_deltas) if share_deltas else None,
                             "original_estimate_minus_provider_shares_max": max(share_deltas) if share_deltas else None,
                             "share_differences_at_least_100": sum(abs(delta) >= 100 for delta in share_deltas),
                             "provider_active_dates_missing_from_original": sorted(str(day) for day in provider_active - original_window),
                             "original_price_dates_with_provider_halt_flag": sorted(str(day) for day in original_window & provider_halted),
                             "original_price_dates_absent_from_all_provider_observations": sorted(str(day) for day in original_window - provider_active - provider_halted),
                             "original_price_file": str(old_path), "original_price_sha256": old_entry["output_sha256"]})
    return {"manifest": str(manifest_path), "manifest_sha256": sha(manifest_path),
            "request_count": len(requests), "acquired_price_files": len(results), "errors": manifest["errors"],
            "all_raw_response_hashes_verified": True, "price_files": results, "existing_price_controls": controls}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifests", nargs="+", type=Path, required=True)
    parser.add_argument("--base", type=Path, required=True)
    args = parser.parse_args(argv)
    base_path = inside(args.base)
    base_manifest = json.loads(base_path.read_text(encoding="utf-8"))
    audits = [audit(path, {entry["symbol"]: entry for entry in base_manifest["files"]}) for path in args.manifests]
    output = PROJECT / "reports/history-audits" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True, exist_ok=False)
    report = {"evaluated_at": datetime.now(timezone.utc).isoformat(), "backtest_ready": False,
              "base_manifest": str(base_path), "base_manifest_sha256": sha(base_path),
              "price_tolerance": "0.0001 + abs(original) * float32_epsilon * 2",
              "limitations": ["source flags do not grant account execution eligibility",
                              "sample agreement does not validate the full original universe",
                              "original volume estimate is not promoted to verified shares"], "audits": audits}
    (output / "audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps({"audit": str(output / "audit.json"), "price_files_verified": sum(x["acquired_price_files"] for x in audits),
                      "controls": sum(len(x["existing_price_controls"]) for x in audits),
                      "OHLC_mismatches": sum(c["OHLC_mismatched_cells"] for x in audits for c in x["existing_price_controls"])}))


if __name__ == "__main__":
    main()
