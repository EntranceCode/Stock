"""Create a versioned exclusion review for financial rows with no notice date.

This is an offline derivative. It never fills missing dates, changes snapshots,
or interprets the current API share fields as historical share counts.
"""
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import sys

from fetch_http_reference import new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps" / "parquet"))
import pandas as pd
import pyarrow.parquet as pq

REQUIRED = ["CURRENCY", "EPSJB", "BPS", "ROEJQ", "TOTALOPERATEREVE", "PARENTNETPROFIT", "NETCASH_OPERATE_PK", "TOTAL_ASSETS_PK", "TOTAL_EQUITY_PK"]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=PROJECT / "data/external/financials/20261008T235336Z_a4fbe81c/manifest.json")
    parser.add_argument("--symbol", default="601187")
    args = parser.parse_args(argv)
    path = args.manifest.resolve()
    if not path.is_relative_to(PROJECT):
        raise ValueError("Source must be in the project")
    manifest = json.loads(path.read_bytes())
    entries = [x for x in manifest["files"] if x["symbol"] == args.symbol]
    if len(entries) != 1:
        raise ValueError("Expected one source file for the requested symbol")
    parquet = Path(entries[0]["path"]).resolve()
    if not parquet.is_relative_to(PROJECT):
        raise ValueError("Parquet must be in the project")
    source_hash = sha(parquet)
    frame = pq.read_table(parquet).to_pandas()
    if len(frame) != entries[0]["rows"] or not frame.SECURITY_CODE.eq(args.symbol).all():
        raise ValueError("Source rows or symbol differs from manifest")
    rows = []
    for row in frame[frame.NOTICE_DATE.isna()].to_dict("records"):
        missing = [field for field in REQUIRED if pd.isna(row.get(field))]
        known_numbers = {name: row[name].item() if hasattr(row[name], "item") else row[name]
                         for name in ["TOTALDEPOSITS", "GROSSLOANS", "FIRST_ADEQUACY_RATIO", "TOTAL_SHARE", "A_FREE_SHARE"]
                         if name in row and not pd.isna(row[name])}
        rows.append({"symbol": args.symbol, "report_period": str(row["report_period"]),
                     "source_announcement_date": None, "corrected_announcement_date": None,
                     "available_at": None, "original_vintage_verified": False,
                     "notice_date_resolved": False, "historical_strategy_eligible": False,
                     "exclusion_reasons": ["notice_date_unknown", "original_source_values_and_versions_unknown", "required_quality_value_fields_missing"],
                     "missing_required_fields": missing,
                     "nonempty_current_api_fields": known_numbers,
                     "nonempty_field_warning": "Nonempty current API bank/share fields are not evidence of historical publication or original values.",
                     "listing_boundary_observation": {"listing_date_candidate": "2020-10-27", "listing_date_verified_in_this_derivative": False,
                         "reference_url": "https://money.finance.sina.com.cn/corp/view/vISSUE_MarketBulletinDetail.php?id=6673320&stockid=601187",
                         "meaning": "Two periods precede the candidate A-share listing date. Official original listing PDF is still required for verification."},
                     "resolution_policy": "Preserve null; acquire actual original disclosure evidence. An IPO report disclosing earlier figures is usable only under its own publication time, never backdated to this report period."})
    if sha(parquet) != source_hash:
        raise ValueError("Source changed during review")
    output = new_snapshot_directory(PROJECT / "data/external/financial_notice_review")
    review = {"generated_at": datetime.now(timezone.utc).isoformat(), "rows": rows, "source_modified": False,
              "source_manifest": str(path), "source_manifest_sha256": sha(path),
              "source_parquet": str(parquet), "source_parquet_sha256": source_hash}
    review_path = output / "review.json"
    save_json_once(review_path, review)
    result = {"run_id": output.name, "output": str(output), "source_kind": "offline_financial_notice_exclusion_review",
              "as_of": manifest["as_of"], "generated_at": review["generated_at"], "backtest_ready": False,
              "source_manifest": str(path), "source_manifest_sha256": review["source_manifest_sha256"],
              "files": [{"symbol": args.symbol, "path": str(review_path), "sha256": sha(review_path), "rows": len(rows)}],
              "statistics": {"unknown_notice_rows": len(rows), "dates_filled": 0, "excluded_from_historical_strategy": len(rows)},
              "readiness_gaps": ["original_disclosure_date_evidence", "original_financial_values_and_revision_chain"], "errors": []}
    save_json_once(output / "manifest.json", result)
    print("Review", output / "manifest.json", "excluded", len(rows), "dates_filled", 0, flush=True)
    return output


if __name__ == "__main__":
    main()
