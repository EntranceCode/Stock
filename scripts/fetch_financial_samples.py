"""Fetch public financial snapshots; current snapshots are not original historical vintages."""
from datetime import datetime, timezone
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
import pyarrow as pa
import pyarrow.parquet as pq

URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SYMBOLS = ["000001", "000002", "300750", "600000", "600519"]


def cached_response(path: Path):
    """Read new complete responses or legacy payloads without inventing collection times."""
    envelope = json.loads(path.read_text(encoding="utf-8"))
    payload = envelope["payload"] if "payload" in envelope else json.loads(envelope["response_text"])
    retrieved_at = envelope.get("source_retrieved_at", envelope.get("retrieved_at"))
    if retrieved_at is not None:
        timestamp = datetime.fromisoformat(retrieved_at)
        if timestamp.tzinfo is None:
            raise ValueError("Source retrieval timestamp must include its timezone")
    return envelope["params"], payload, retrieved_at


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    parser.add_argument("--stock-manifest", type=Path, help="Fetch every stock in a normalized daily manifest")
    parser.add_argument("--limit", type=int, help="Optional bounded pilot count")
    parser.add_argument("--symbols", nargs="+", help="Explicit codes for a bounded acquisition or cached repair")
    parser.add_argument("--cached-snapshot", type=Path, help="Normalize existing raw responses without networking")
    args = parser.parse_args(argv)
    if args.as_of < "2018-01-01":
        raise ValueError("Financial sample begins in 2018; as-of must be on or after 2018-01-01")
    symbols = SYMBOLS
    if args.stock_manifest:
        source_manifest = json.loads(args.stock_manifest.read_text(encoding="utf-8"))
        if not source_manifest.get("complete") or source_manifest.get("limit") is not None:
            raise ValueError("A complete full daily manifest is required")
        symbols = [entry["symbol"] for entry in source_manifest["files"]]
    if args.limit is not None:
        if args.limit <= 0:
            raise ValueError("Limit must be positive")
        symbols = symbols[:args.limit]
    if args.symbols:
        symbols = args.symbols
    if any(len(symbol) != 6 or not symbol.isdigit() for symbol in symbols):
        raise ValueError("Invalid stock code")
    if len(symbols) != len(set(symbols)):
        raise ValueError("Duplicate stock codes would overwrite evidence within the run")
    output = new_snapshot_directory(PROJECT / "data" / "external" / "financials")
    (output / "raw").mkdir()
    normalized_at = datetime.now(timezone.utc).isoformat()
    manifest = {"run_id": output.name, "output": str(output), "provider": URL, "as_of": args.as_of,
                "as_of_scope": "announcement-date filter only; current snapshot is not a historical vintage",
                "normalized_at": normalized_at,
                "report_name": "RPT_F10_FINANCE_MAINFINADATA", "backtest_ready": False,
                "requested_symbols": len(symbols), "stock_manifest": str(args.stock_manifest) if args.stock_manifest else None,
                "cached_snapshot": str(args.cached_snapshot) if args.cached_snapshot else None,
                "readiness_gaps": ["original_announcement_vintages_and_restatements", "full_stock_universe_coverage",
                                   "historical_share_counts_and_corporate_actions"], "files": [], "errors": []}
    session = requests.Session() if not args.cached_snapshot else None
    for index, symbol in enumerate(symbols, 1):
        params = {"reportName": manifest["report_name"], "columns": "ALL",
                  "filter": f'(SECURITY_CODE="{symbol}")(REPORT_DATE>=\'2018-01-01\')',
                  "pageSize": 200, "pageNumber": 1, "sortColumns": "REPORT_DATE", "sortTypes": -1,
                  "source": "WEB", "client": "WEB"}
        try:
            raw = output / "raw" / f"{symbol}.json"
            original_path = None
            if args.cached_snapshot:
                original_path = (args.cached_snapshot / "raw" / f"{symbol}.json").resolve()
                params, payload, retrieved_at = cached_response(original_path)
                # Preserve source bytes, hash and acquisition evidence on reprocessing.
                with raw.open("xb") as destination:
                    destination.write(original_path.read_bytes())
            else:
                response = session.get(URL, params=params, timeout=15)
                payload, acquisition = capture_http_response(raw, response, params, args.as_of, URL)
                retrieved_at = acquisition["retrieved_at"]
            result = payload.get("result") or {}
            rows = result.get("data") or []
            if not payload.get("success") or not rows:
                raise ValueError(f"No valid financial rows: {payload.get('message')}")
            if result.get("count", len(rows)) > len(rows):
                raise ValueError("Financial sample paginated; full requested window not returned")
            frame = pd.DataFrame(rows)
            required = ["SECURITY_CODE", "REPORT_DATE", "NOTICE_DATE"]
            if any(field not in frame for field in required):
                raise ValueError(f"Financial snapshot lacks announcement fields; columns: {list(frame)}")
            if not frame.SECURITY_CODE.eq(symbol).all():
                raise ValueError(f"Financial response contains a security other than requested {symbol}")
            notice = pd.to_datetime(frame.NOTICE_DATE, errors="raise")
            period = pd.to_datetime(frame.REPORT_DATE, errors="raise")
            if period.isna().any() or (period < pd.Timestamp("2018-01-01")).any() or (period.dt.date > pd.Timestamp(args.as_of).date()).any():
                raise ValueError("Financial report period is missing or outside the requested 2018-01-01 through as-of window")
            frame["report_period"] = period.dt.date
            frame["announcement_date"] = notice.dt.date
            frame["announcement_date_unknown"] = notice.isna()
            # Date-only announcements must not become available at midnight on
            # their announcement day. Exact session availability remains unverified.
            frame["earliest_candidate_available_date"] = (notice.dt.normalize() + pd.Timedelta(days=1)).dt.date
            frame["source_retrieved_at"] = pd.Series(retrieved_at, index=frame.index, dtype="string")
            frame["snapshot_retrieved_at"] = frame["source_retrieved_at"]
            frame["source_retrieval_time_unknown"] = retrieved_at is None
            frame["normalized_at"] = normalized_at
            frame["original_vintage_verified"] = False
            if (notice < period).any():
                raise ValueError("Announcement predates report period")
            frame = frame[(notice.dt.date <= pd.Timestamp(args.as_of).date()) | notice.isna()].copy()
            if frame.duplicated(["SECURITY_CODE", "REPORT_DATE"]).any():
                raise ValueError("Multiple financial rows per report; resolve versions explicitly")
            table = pa.Table.from_pandas(frame, preserve_index=False)
            table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps({
                "provider": URL, "backtest_ready": False, "readiness_gaps": manifest["readiness_gaps"],
                "as_of": args.as_of, "as_of_scope": manifest["as_of_scope"],
                "source_retrieved_at": retrieved_at, "source_retrieval_time_unknown": retrieved_at is None,
                "normalized_at": normalized_at, "source_original_file": str(original_path) if original_path else None,
                "source_sha256": hashlib.sha256(raw.read_bytes()).hexdigest()}).encode()})
            path = output / f"{symbol}_financials.parquet"
            pq.write_table(table, path, compression="zstd")
            fields = [field for field in ["EPSJB", "BPS", "ROEJQ", "ROEKCJQ", "MGJYXJJE", "TOTALOPERATEREVE", "PARENTNETPROFIT"] if field in frame]
            manifest["files"].append({"symbol": symbol, "rows": len(frame), "path": str(path),
                                      "columns": list(frame), "candidate_factor_fields": fields,
                                      "unknown_announcement_rows": int(frame.announcement_date_unknown.sum()),
                                      "source_retrieved_at": retrieved_at, "source_retrieval_time_unknown": retrieved_at is None,
                                      "source_original_file": str(original_path) if original_path else None,
                                      "earliest_report": str(frame.report_period.min()), "latest_report": str(frame.report_period.max()),
                                      "source_sha256": hashlib.sha256(raw.read_bytes()).hexdigest()})
            if len(symbols) <= 10 or index % 100 == 0:
                print("Financials", index, "/", len(symbols), "accepted", len(manifest["files"]), flush=True)
        except Exception as exc:
            manifest["errors"].append({"symbol": symbol, "error": str(exc)})
            print(symbol, "failed", str(exc), flush=True)
    if session is not None:
        session.close()
    manifest["complete_sample"] = len(manifest["files"]) == len(symbols) and not manifest["errors"]
    save_json_once(output / "manifest.json", manifest)
    print("Output", str(output), "complete_sample", manifest["complete_sample"], flush=True)
    if not manifest["complete_sample"]:
        raise RuntimeError("Financial sample not ready; inspect raw response and manifest")
    return output


if __name__ == "__main__":
    main()
