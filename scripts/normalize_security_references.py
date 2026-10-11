"""Stage official listing dates and name events without inventing historical status."""
from __future__ import annotations
import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import unicodedata
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def security_code(value) -> str:
    text = str(value).strip()
    if not re.fullmatch(r"[0-9]{6}", text):
        raise ValueError(f"Invalid security code; do not silently pad or repair: {text}")
    return text


def source_date(value, as_of: date) -> date:
    text = str(value).strip()
    if re.fullmatch(r"[0-9]{8}", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    parsed = date.fromisoformat(text)
    if not date(1990, 1, 1) <= parsed <= as_of:
        raise ValueError(f"Source date outside supported snapshot bounds: {parsed}")
    return parsed


def name_marker(value) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    text = re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(value))).upper()
    if not text or text == "NAN":
        return "unknown"
    for prefix, marker in [("*ST", "star_ST_name"), ("ST", "ST_name"), ("PT", "PT_name"), ("N", "N_name"), ("C", "C_name")]:
        if text.startswith(prefix):
            return marker
    return "unmarked_name_not_proof_of_normal_status"


def listing_record(row, code_column, name_column, listed_column, terminated_column, exchange, as_of):
    listed = source_date(row[listed_column], as_of)
    terminated = source_date(row[terminated_column], as_of) if terminated_column else None
    if terminated is not None and terminated < listed:
        raise ValueError("Termination precedes listing")
    return {"symbol": security_code(row[code_column]), "exchange": exchange, "source_listing_date": listed,
            "source_termination_date": terminated, "display_name_at_collection": str(row[name_column]),
            "listed_at_snapshot": terminated is None, "termination_boundary_verified": False,
            "all_listing_intervals_verified": False}


def name_event(row, as_of):
    before, after = row["变更前简称"], row["变更后简称"]
    if pd.isna(before) or pd.isna(after) or not str(before).strip() or not str(after).strip():
        raise ValueError("Name event lacks its historical before/after names")
    return {"symbol": security_code(row["证券代码"]), "exchange": "SZSE", "change_date": source_date(row["变更日期"], as_of),
            "name_before": str(before), "name_after": str(after),
            "name_marker_before": name_marker(before), "name_marker_after": name_marker(after),
            "display_name_at_collection": str(row["证券简称"]),
            "regulatory_status_verified": False, "announcement_time_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--excel-library-path", type=Path, help="Bundled pure-Python openpyxl package directory")
    args = parser.parse_args()
    if args.excel_library_path:
        sys.path.append(str(args.excel_library_path.resolve()))
    source = args.source.resolve()
    original = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    as_of = date.fromisoformat(original["as_of"])
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = PROJECT / "data/external/security_metadata" / run
    output.mkdir(parents=True)
    listings, events, current_snapshots, errors, sheets = [], [], [], [], []
    for entry in original["files"]:
        path = Path(entry["path"])
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ValueError("Source export hash changed")
        dataset = entry["dataset"]
        try:
            frames = pd.read_excel(path, sheet_name=None, dtype=str, engine="xlrd" if path.suffix == ".xls" else "openpyxl")
            if len(frames) != 1:
                raise ValueError("Unexpected multiple sheets; inspect mapping explicitly")
            sheet_name, frame = next(iter(frames.items()))
            sheets.append({"dataset": dataset, "sheet": sheet_name, "rows": len(frame), "columns": list(frame.columns)})
            for source_row, (_, row) in enumerate(frame.iterrows(), 2):
                identity = {"source_file": str(path), "source_sha256": entry["sha256"], "source_row": source_row,
                            "source_retrieved_at": entry["retrieved_at"], "snapshot_as_of": str(as_of), "source_dataset": dataset}
                try:
                    if dataset == "szse_current":
                        if pd.isna(row["A股代码"]) or not str(row["A股代码"]).strip():
                            errors.append({**identity, "reason": "No A-share code; raw row retained", "raw_record": row.fillna("").to_dict()})
                            continue
                        record = listing_record(row, "A股代码", "A股简称", "A股上市日期", None, "SZSE", as_of)
                        listings.append({**record, **identity})
                        current_snapshots.append({**identity, "symbol": record["symbol"], "exchange": "SZSE",
                                                  "industry_current_snapshot_only": row.get("所属行业"),
                                                  "total_A_shares_current_snapshot_only": row.get("A股总股本"),
                                                  "float_A_shares_current_snapshot_only": row.get("A股流通股本"),
                                                  "name_current_snapshot_only": record["display_name_at_collection"],
                                                  "historical_values_verified": False})
                    elif dataset in ("szse_delisted", "sse_delisted_main"):
                        code_col, name_col = ("证券代码", "证券简称") if dataset.startswith("szse") else ("原公司代码", "原公司简称")
                        listings.append({**listing_record(row, code_col, name_col, "上市日期", "终止上市日期", "SZSE" if dataset.startswith("szse") else "SSE", as_of), **identity})
                    elif dataset == "szse_name_changes":
                        events.append({**name_event(row, as_of), **identity})
                    else:
                        raise ValueError("Unexpected source dataset")
                except (ValueError, KeyError) as exc:
                    errors.append({**identity, "reason": str(exc), "raw_record": row.fillna("").to_dict()})
        except Exception as exc:
            errors.append({"source_file": str(path), "dataset": dataset, "reason": str(exc)})
    listing_frame, event_frame, snapshot_frame = map(pd.DataFrame, (listings, events, current_snapshots))
    if listing_frame.empty or event_frame.empty:
        raise ValueError("No listing/name event data staged; inspect source and dependencies")
    listing_frame["multiple_listing_records_require_review"] = listing_frame.duplicated(["exchange", "symbol"], keep=False)
    event_frame["multiple_events_same_day_require_review"] = event_frame.duplicated(["exchange", "symbol", "change_date"], keep=False)
    provenance = {"source_snapshot": str(source), "as_of": str(as_of), "backtest_ready": False,
                  "readiness_gaps": ["full_exchange_and_lifecycle_coverage", "delisting_date_boundary_and_relisting",
                                     "historical_initial_status_and_risk_warning", "historical_suspensions_and_execution_eligibility",
                                     "historical_industry_and_share_counts", "original_status_announcement_time"],
                  "usage": "Listing dates are candidates; name prefixes are observations, not verified regulatory or trading status"}
    files = []
    for name, frame in [("listing_candidates", listing_frame), ("name_events", event_frame), ("current_metadata_snapshot", snapshot_frame)]:
        table = pa.Table.from_pandas(frame, preserve_index=False)
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"stock.provenance": json.dumps(provenance).encode()})
        destination = output / f"{name}.parquet"
        pq.write_table(table, destination, compression="zstd")
        files.append({"dataset": name, "rows": len(frame), "path": str(destination), "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()})
    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    local_manifest = json.loads((PROJECT / registry["datasets"]["stock_daily"]["manifest"]).read_text(encoding="utf-8"))
    local = {item["symbol"]: item for item in local_manifest["files"]}
    codes = set(listing_frame.symbol)
    retired_a = set(listing_frame.loc[listing_frame.source_termination_date.notna() & listing_frame.symbol.str.startswith(("00", "30", "60", "68")), "symbol"])
    research_start = date(2018, 1, 1)
    research_end = min(as_of, max(date.fromisoformat(item["end"]) for item in local.values()))
    retired_during_research = {row["symbol"] for row in listings if row["symbol"] in retired_a
                              and row["source_termination_date"] is not None
                              and row["source_termination_date"] >= research_start and row["source_listing_date"] <= research_end}
    conflicts = []
    for row in listings:
        if row["symbol"] in local:
            start, end = map(date.fromisoformat, (local[row["symbol"]]["start"], local[row["symbol"]]["end"]))
            if start < row["source_listing_date"] or (row["source_termination_date"] and end > row["source_termination_date"]):
                conflicts.append({"symbol": row["symbol"], "local_first": str(start), "local_last": str(end),
                                  "source_listing": str(row["source_listing_date"]), "source_termination": str(row["source_termination_date"]) if row["source_termination_date"] else None,
                                  "reason": "Dates disagree; source/venue/prelisting/relisting semantics require review, no bars removed"})
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "output": str(output), "as_of": str(as_of), "backtest_ready": False,
              "source_sheets": sheets, "staged_counts": {item["dataset"]: item["rows"] for item in files},
              "name_event_start": str(event_frame.change_date.min()), "name_event_end": str(event_frame.change_date.max()),
              "local_codes_with_listing_reference": len(set(local) & codes), "local_codes_without_listing_reference": sorted(set(local) - codes),
              "delisted_A_codes_in_sources": len(retired_a), "delisted_A_codes_missing_local_prices": sorted(retired_a - set(local)),
              "research_window_candidate": [str(research_start), str(research_end)],
              "retired_A_codes_overlapping_research": len(retired_during_research),
              "retired_A_codes_overlapping_research_missing_local_prices": sorted(retired_during_research - set(local)),
              "multiple_listing_record_rows": int(listing_frame.multiple_listing_records_require_review.sum()),
              "multiple_name_events_same_day_rows": int(event_frame.multiple_events_same_day_require_review.sum()),
              "date_conflicts": conflicts, "quarantined_rows": errors, "readiness_gaps": provenance["readiness_gaps"]}
    # Each version keeps its own audit; the reports directory is only a latest-view copy.
    audit_path = output / "audit.json"
    audit_text = json.dumps(report, ensure_ascii=False, indent=2)
    audit_path.write_text(audit_text, encoding="utf-8")
    coverage = {
        "local_codes_with_listing_reference": report["local_codes_with_listing_reference"],
        "local_codes_without_listing_reference": len(report["local_codes_without_listing_reference"]),
        "research_window_candidate": report["research_window_candidate"],
        "retired_A_codes_overlapping_research": report["retired_A_codes_overlapping_research"],
        "retired_A_codes_overlapping_research_missing_local_prices": len(report["retired_A_codes_overlapping_research_missing_local_prices"]),
        "listing_date_conflicts": len(conflicts),
        "multiple_listing_record_rows": report["multiple_listing_record_rows"],
    }
    manifest = {**provenance, "output": str(output), "files": files, "errors": errors,
                "coverage": coverage, "report": str(audit_path),
                "report_sha256": hashlib.sha256(audit_path.read_bytes()).hexdigest()}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    (PROJECT / "reports/security-reference-audit.json").write_text(audit_text, encoding="utf-8")
    markdown = f"""# 交易所证券元信息接入审计

生成时间：{report['generated_at']}；源筛选日期：{as_of}。仅用于数据检查，未开放策略回测。

| 产物 | 行数 |
|---|---:|
| 上市/终止日期候选 | {len(listing_frame):,} |
| 历史名称事件 | {len(event_frame):,} |
| 当前行业/股本等快照 | {len(snapshot_frame):,} |

历史名称区间为{report['name_event_start']}至{report['name_event_end']}。源当前名称只作为采集时展示字段，历史名称使用变更前/后字段；ST/PT等前缀仅名称观察，不证明完整风险警示、涨跌幅或成交资格。没有名称事件的初始状态保持未知。

本地{len(local):,}个股票代码中，{report['local_codes_with_listing_reference']:,}个取得候选上市日期；{len(report['local_codes_without_listing_reference']):,}个未匹配。源退市A股代码{len(retired_a):,}个，其中{len(report['delisted_A_codes_missing_local_prices']):,}个没有本地价格文件，不能将当前本地池称作无幸存者偏差的完整历史股票池。

将区间限定为{research_start}至{research_end}后，候选挂牌区间与研究期有重叠的退市A股代码为{len(retired_during_research):,}个，其中{len(retired_during_research - set(local)):,}个没有本地价格文件。这是源范围内的缺价观察；源仍没有覆盖所有板块及多次挂牌完整性，不能用当前名单代替历史股票池。

发现{len(conflicts):,}条价格日期与候选挂牌区间不符、{report['multiple_listing_record_rows']:,}条重复证券挂牌记录、{report['multiple_name_events_same_day_rows']:,}条同日名称事件。保留所有来源记录，不强行合并重新上市或同日事件，不删除价格。终止日边界与最后可交易日需要原公告，上市区间不能直接替代可成交状态。

当前行业、股本与名称单独存为snapshot，禁止回填历史。源包含非A股代码时亦保留并待分类；只有明确A股代码前缀用于本次退市缺价观察。异常行{len(errors):,}条详见JSON。来源URL、原文件SHA-256与行号保留在快照及Parquet元数据中。

下一步核验上市日期冲突、重新上市、多段挂牌、完整停复牌与风险警示原公告，并补历史价格及原始财务版本。
"""
    (output / "audit.md").write_text(markdown, encoding="utf-8")
    (PROJECT / "reports/security-reference-audit.md").write_text(markdown, encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("output", "staged_counts", "local_codes_with_listing_reference", "delisted_A_codes_in_sources", "multiple_listing_record_rows", "multiple_name_events_same_day_rows")}, ensure_ascii=True, indent=2))
    print("Quarantined", len(errors), "date conflicts", len(conflicts), "missing retired A-share prices", len(retired_a - set(local)))


if __name__ == "__main__":
    main()
