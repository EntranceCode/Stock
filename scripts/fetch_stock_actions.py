"""Acquire immutable paginated dividend/bonus snapshots for the local stock pool.

Coverage of this provider's dividend report is not coverage of rights issues,
mergers, delistings, payment dates, or every historical corporate action.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time
from uuid import uuid4
import requests

PROJECT = Path(__file__).resolve().parents[1]
URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_page(payload: dict, page: int, page_size: int, expected: tuple[int, int] | None = None):
    result = payload.get("result") or {}
    if payload.get("success") is not True:
        raise ValueError(f"Provider reported failure: {payload.get('message')}")
    count, pages = int(result["count"]), int(result["pages"])
    rows = result.get("data") or []
    if count <= 0 or pages != (count + page_size - 1) // page_size:
        raise ValueError("Invalid pagination metadata")
    if expected is not None and (count, pages) != expected:
        raise ValueError("Provider row count changed during pagination; preserve snapshot and restart")
    expected_rows = page_size if page < pages else count - page_size * (pages - 1)
    if not 1 <= page <= pages or len(rows) != expected_rows:
        raise ValueError("Incomplete or unexpected page")
    for row in rows:
        if not row.get("SECURITY_CODE") or not row.get("EX_DIVIDEND_DATE") or not row.get("REPORT_DATE"):
            raise ValueError("Missing record identity fields")
    return rows, (count, pages)


def record_key(row: dict) -> tuple[str, str, str]:
    return row["EX_DIVIDEND_DATE"], row["SECURITY_CODE"], row["REPORT_DATE"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-manifest", type=Path, default=PROJECT / "data/normalized/20261007T082337Z_34eda55f/manifest.json")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(timezone(timedelta(hours=8))).date())
    parser.add_argument("--resume", type=Path, help="Reuse saved successful pages without modifying raw responses")
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()
    if args.start_date > args.as_of or args.delay < 0.25:
        parser.error("Invalid dates or delay below 0.25 seconds")
    stock_manifest_path = args.stock_manifest.resolve()
    stocks = json.loads(stock_manifest_path.read_text(encoding="utf-8"))
    symbols = sorted({item["symbol"] for item in stocks["files"]})
    params = {"reportName": "RPT_SHAREBONUS_DET", "columns": "ALL", "pageSize": 500,
              "sortColumns": "EX_DIVIDEND_DATE,SECURITY_CODE,REPORT_DATE", "sortTypes": "1,1,1",
              "filter": f"(EX_DIVIDEND_DATE>='{args.start_date}')(EX_DIVIDEND_DATE<='{args.as_of}')",
              "source": "WEB", "client": "WEB"}
    if args.resume:
        output = args.resume.resolve()
        if not output.is_relative_to(PROJECT / "data/external/stock_actions"):
            raise ValueError("Resume path must be a stock-action snapshot within the project")
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        if manifest["params"] != params or manifest["requested_symbols"] != symbols:
            raise ValueError("Resume query/universe differs from the saved snapshot")
        if manifest["acquisition_complete"]:
            raise ValueError("Snapshot already complete; do not reacquire it")
    else:
        run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
        output = PROJECT / "data/external/stock_actions" / run
        (output / "raw").mkdir(parents=True)
        manifest = {"run_id": run, "output": str(output), "source_url": URL, "params": params,
                    "start_date": str(args.start_date), "as_of": str(args.as_of), "started_at": now(),
                    "stock_manifest": str(stock_manifest_path), "stock_manifest_sha256": hashlib.sha256(stock_manifest_path.read_bytes()).hexdigest(),
                    "requested_symbols": symbols, "requested_symbol_count": len(symbols), "pages": [], "errors": [],
                    "acquisition_complete": False, "backtest_ready": False,
                    "readiness_gaps": ["payment_dates_and_original_announcements", "rights_issues_mergers_and_other_actions",
                                       "historical_security_universe", "provider_completeness_not_independently_verified"]}

    def checkpoint():
        (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    checkpoint()
    print("Output", output, flush=True)
    expected, all_rows, keys = None, [], set()
    session = requests.Session()
    try:
        page = 1
        while expected is None or page <= expected[1]:
            request_params = {**params, "pageNumber": page}
            if page <= len(manifest["pages"]):
                entry = manifest["pages"][page - 1]
                path = output / entry["path"]
                if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
                    raise ValueError("Saved raw page hash changed")
                envelope = json.loads(path.read_text(encoding="utf-8"))
                if envelope["params"] != request_params:
                    raise ValueError("Saved page query differs")
            else:
                retrieved_at = now()
                response = session.get(URL, params=request_params, timeout=30)
                # Keep complete returned JSON before any semantic validation.
                envelope = {"url": response.url, "params": request_params, "retrieved_at": retrieved_at,
                            "http_status": response.status_code, "response_text": response.text}
                try:
                    envelope["payload"] = response.json()
                except ValueError:
                    envelope["payload"] = None
                path = output / "raw" / f"page_{page:04d}_{uuid4().hex[:8]}.json"
                path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
                response.raise_for_status()
                if not isinstance(envelope["payload"], dict):
                    raise ValueError("Non-JSON source response preserved")
            rows, expected = validate_page(envelope["payload"], page, params["pageSize"], expected)
            if expected[1] > 1000:
                raise ValueError("Unexpected query size")
            ordered = [record_key(row) for row in rows]
            if ordered != sorted(ordered) or (all_rows and ordered[0] < record_key(all_rows[-1])):
                raise ValueError("Source page ordering is unstable")
            if any(key in keys for key in ordered) or len(set(ordered)) != len(ordered):
                raise ValueError("Duplicate record identity; do not silently deduplicate")
            keys.update(ordered)
            all_rows.extend(rows)
            if page > len(manifest["pages"]):
                manifest["pages"].append({"page": page, "rows": len(rows), "path": str(path.relative_to(output)),
                                          "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "retrieved_at": envelope["retrieved_at"]})
            manifest["provider_count"], manifest["provider_pages"] = expected
            checkpoint()
            if page == 1 or page % 10 == 0 or page == expected[1]:
                print("page", page, "of", expected[1], "records", len(all_rows), flush=True)
            page += 1
            if page <= expected[1]:
                time.sleep(args.delay)
        if len(all_rows) != expected[0]:
            raise ValueError("Final row count does not match source pagination")
        counts = Counter(row["SECURITY_CODE"] for row in all_rows if row["SECURITY_CODE"] in symbols)
        manifest.update({"acquisition_complete": True, "completed_at": now(), "provider_rows": len(all_rows),
                         "local_universe_rows": sum(counts.values()), "symbols_with_records": len(counts),
                         "symbols_without_source_records": sorted(set(symbols) - set(counts)),
                         "records_by_requested_symbol": {symbol: counts.get(symbol, 0) for symbol in symbols},
                         "coverage_note": "No returned records is not independently verified absence of corporate actions."})
    except Exception as exc:
        manifest["errors"].append({"at": now(), "error": str(exc)})
        checkpoint()
        raise
    checkpoint()
    print("Complete", manifest["local_universe_rows"], "local records;", len(counts), "symbols with source records", flush=True)


if __name__ == "__main__":
    main()
