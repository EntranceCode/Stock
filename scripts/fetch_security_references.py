"""Save official listing, delisting, and name-change exports as immutable evidence."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import requests
from fetch_http_reference import china_today, iso_date, new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]

# The SSE current-stock list is published only as a paged JSON query; the Excel
# export used for the delisted list does not cover it. One call returns every
# row, which is verified rather than assumed.
SSE_CURRENT_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_CURRENT_PARAMS = {"sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L", "isPagination": "true",
                      "pageHelp.pageSize": "500", "pageHelp.pageNo": "1",
                      "pageHelp.beginPage": "1", "pageHelp.endPage": "1",
                      "STOCK_TYPE": "1", "COMPANY_STATUS": "2"}


def decode_sse_payload(text: str) -> dict:
    """Parse the query payload, tolerating the JSONP-style wrapper."""
    body = text.strip()
    if body.startswith("(") and body.endswith(")"):
        body = body[1:-1]
    return json.loads(body)


def fetch_sse_current(session, output: Path, as_of) -> dict:
    response = session.get(SSE_CURRENT_URL, params=SSE_CURRENT_PARAMS,
                           headers={"Referer": "https://www.sse.com.cn/"}, timeout=40)
    response.raise_for_status()
    body = response.content
    payload = decode_sse_payload(body.decode("utf-8"))
    rows = (payload.get("pageHelp") or {}).get("data")
    if not isinstance(rows, list) or not rows:
        raise ValueError("SSE current-stock query returned no rows; preserved before parsing")
    codes = [str(row.get("A_STOCK_CODE") or "").strip() for row in rows]
    if not any(len(code) == 6 and code.isdigit() for code in codes):
        raise ValueError("SSE current-stock query returned no usable A-share codes")
    path = output / "sse_current.json"
    with path.open("xb") as stream:
        stream.write(body)
    return {"dataset": "sse_current", "url": SSE_CURRENT_URL, "response_url": response.url,
            "params": SSE_CURRENT_PARAMS, "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "http_status": response.status_code, "content_type": response.headers.get("Content-Type"),
            "bytes": len(body), "path": str(path), "sha256": hashlib.sha256(body).hexdigest(),
            "format_signature": ".json", "rows": len(rows),
            "a_share_codes": len([code for code in codes if len(code) == 6 and code.isdigit()])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    args = parser.parse_args()
    sources = [
        ("szse_current", "https://www.szse.cn/api/report/ShowReport", {"SHOWTYPE": "xlsx", "CATALOGID": "1110", "TABKEY": "tab1"}),
        ("szse_delisted", "https://www.szse.cn/api/report/ShowReport", {"SHOWTYPE": "xlsx", "CATALOGID": "1793_ssgs", "TABKEY": "tab2"}),
        ("szse_name_changes", "https://www.szse.cn/api/report/ShowReport", {"SHOWTYPE": "xlsx", "CATALOGID": "SSGSGMXX", "TABKEY": "tab2", "txtKsrq": "1990-01-01", "txtZzrq": args.as_of}),
        ("sse_delisted_main", "https://query.sse.com.cn/sseQuery/commonExcelDd.do", {"sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L", "type": "inParams", "STOCK_CODE": "", "REG_PROVINCE": "", "STOCK_TYPE": "1,2", "COMPANY_STATUS": "3"}),
    ]
    output = new_snapshot_directory(PROJECT / "data/external/security_references")
    manifest = {"output": str(output), "as_of": args.as_of,
                "as_of_scope": "Name-change date filter; current and delisted lists are snapshots, not historical versions",
                "source_method": "Official page download links verified in browser; response semantics require inspection",
                "files": [], "errors": [], "backtest_ready": False,
                "readiness_gaps": ["full_exchange_universe_and_relisting_intervals", "historical_risk_warning_and_suspension_status",
                                   "historical_industry_and_share_counts", "original_status_announcements"]}
    session = requests.Session()
    for name, url, params in sources:
        try:
            retrieved_at = datetime.now(timezone.utc).isoformat()
            response = session.get(url, params=params, headers={"Referer": "https://www.sse.com.cn/" if name.startswith("sse_") else "https://www.szse.cn/"}, timeout=40)
            body = response.content
            extension = ".xlsx" if body.startswith(b"PK\x03\x04") else ".xls" if body.startswith(bytes.fromhex("d0cf11e0a1b11ae1")) else ".response"
            path = output / (name + extension)
            with path.open("xb") as stream:
                stream.write(body)
            record = {"dataset": name, "url": url, "response_url": response.url, "params": params,
                      "retrieved_at": retrieved_at, "http_status": response.status_code,
                      "content_type": response.headers.get("Content-Type"), "bytes": len(body),
                      "path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "format_signature": extension}
            save_json_once(output / (name + ".json"), record)
            manifest["files"].append(record)
            response.raise_for_status()
            if len(body) < 100:
                raise ValueError("Empty or unexpectedly short export; preserved before parsing")
            print(name, len(body), extension, flush=True)
        except Exception as exc:
            manifest["errors"].append({"dataset": name, "error": str(exc)})
            if isinstance(exc, requests.exceptions.ConnectionError):
                break
        time.sleep(0.5)
    try:
        record = fetch_sse_current(session, output, args.as_of)
        manifest["files"].append(record)
        print(record["dataset"], record["rows"], record["format_signature"], flush=True)
    except Exception as exc:
        manifest["errors"].append({"dataset": "sse_current", "error": str(exc)})
    manifest["all_requests_succeeded"] = not manifest["errors"] and len(manifest["files"]) == len(sources) + 1
    save_json_once(output / "manifest.json", manifest)
    print("Output", output, "request_errors", len(manifest["errors"]), flush=True)
    if not manifest["all_requests_succeeded"]:
        raise RuntimeError("Some official exports failed; inspect saved responses rather than treating them as empty lists")


if __name__ == "__main__":
    main()
