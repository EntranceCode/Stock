"""Build a reproducible missing-price acquisition list from staged lifecycle evidence."""
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))
import pyarrow.parquet as pq


def checked_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    registry = checked_json(PROJECT / "config/datasets.json")
    stock_path = PROJECT / registry["datasets"]["stock_daily"]["manifest"]
    security_path = PROJECT / registry["datasets"]["security_metadata"]["manifest"]
    stock, security = checked_json(stock_path), checked_json(security_path)
    audit_path = Path(security["report"])
    if hashlib.sha256(audit_path.read_bytes()).hexdigest() != security["report_sha256"]:
        raise ValueError("Versioned security audit hash changed")
    audit = checked_json(audit_path)
    listing_entry = next(item for item in security["files"] if item["dataset"] == "listing_candidates")
    listing_path = Path(listing_entry["path"])
    if hashlib.sha256(listing_path.read_bytes()).hexdigest() != listing_entry["sha256"]:
        raise ValueError("Listing evidence hash changed")
    listings = pq.read_table(listing_path).to_pylist()
    local_codes = {item["symbol"] for item in stock["files"]}
    start, end = map(date.fromisoformat, audit["research_window_candidate"])
    candidates = {}
    for row in listings:
        terminated, listed = row["source_termination_date"], row["source_listing_date"]
        code = row["symbol"]
        if (terminated is None or not code.startswith(("00", "30", "60", "68"))
                or terminated < start or listed > end):
            continue
        candidates.setdefault(code, []).append(row)
    missing = sorted(set(candidates) - local_codes)
    if missing != audit["retired_A_codes_overlapping_research_missing_local_prices"]:
        raise ValueError("Missing-price reconstruction disagrees with versioned audit")
    requirements = []
    for code in missing:
        records = []
        for row in candidates[code]:
            records.append({
                "exchange": row["exchange"], "source_listing_date": str(row["source_listing_date"]),
                "source_termination_date": str(row["source_termination_date"]),
                "acquisition_window_candidate": [str(max(start, row["source_listing_date"])),
                                                  str(min(end, row["source_termination_date"]))],
                "display_name_at_collection": row["display_name_at_collection"],
                "source_dataset": row["source_dataset"], "source_file": row["source_file"],
                "source_sha256": row["source_sha256"], "source_row": row["source_row"],
                "multiple_listing_records_require_review": row["multiple_listing_records_require_review"],
                "termination_boundary_verified": False,
            })
        requirements.append({"symbol": code, "local_price_file_present": False,
                             "candidate_records": records,
                             "acquisition_status": "not_attempted_by_this_report"})
    year_counts = Counter(min(row["source_termination_date"].year for row in rows) for code, rows in candidates.items() if code in missing)
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = PROJECT / "data/external/history_requirements" / run
    output.mkdir(parents=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "output": str(output),
        "research_window_candidate": [str(start), str(end)], "backtest_ready": False,
        "source_manifests": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                             for path in (stock_path, security_path)],
        "source_audit": {"path": str(audit_path), "sha256": security["report_sha256"]},
        "scope": "Observed terminated A-share codes in acquired exchange exports; not a complete historical universe",
        "candidate_retired_codes": len(candidates), "missing_local_price_codes": len(missing),
        "already_present_codes": sorted(set(candidates) & local_codes),
        "missing_codes_by_first_candidate_termination_year": dict(sorted(year_counts.items())),
        "requirements": requirements,
        "acceptance_requirements": [
            "Save complete source responses, request parameters, collection timestamps, and hashes before parsing",
            "Keep unadjusted prices and corporate actions; do not splice prices from relisting or different venues",
            "Resolve all listing intervals and last tradable date from original announcements",
            "Classify absent sessions using suspension or lifecycle evidence; never fabricate bars",
            "Verify date encoding, currency, price precision, volume units, OHLC relationships, and duplicate dates",
            "Unavailable responses are acquisition failures, not evidence that a security did not exist",
            "Keep outputs backtest_ready=false until historical pool, corporate actions, and execution eligibility pass",
        ],
    }
    report_text = json.dumps(report, ensure_ascii=False, indent=2)
    (output / "requirements.json").write_text(report_text, encoding="utf-8")
    table = "\n".join(f"| {year} | {count} |" for year, count in sorted(year_counts.items()))
    markdown = f"""# 退市证券行情补采清单

生成时间：{report['generated_at']}。研究窗口候选为{start}至{end}。

取得的交易所导出中有{len(candidates)}个退市A股代码的候选挂牌区间与研究期重叠，其中{len(missing)}个没有本地行情文件。另{len(report['already_present_codes'])}个已有文件：{', '.join(report['already_present_codes'])}，仍需检查退市前停复牌及最后可交易日。

JSON逐代码保留来源文件、行号、挂牌日期候选和补采窗口候选；补采窗口只限定请求范围，不能用来认定当日可成交。重上市等多记录不合并，采集时名称不回填历史。

| 最早候选终止年 | 缺行情代码数 |
|---|---:|
{table}

清单仅覆盖本次取得的交易所来源，尚未覆盖完整历史股票池、所有板块和所有挂牌区间。不能以补齐清单推定消除幸存者偏差。

采集先保存完整响应与来源证据，再验证未复权价格、日期编码、币种、成交量单位和OHLC；缺日需与停复牌、退市及重上市公告对照，不补造K线。接口返回空或不可用时记录失败。补采价格仍须通过公司行动及成交资格核验，才能参与正式回测。

当前先做跨年份、跨交易所和多挂牌情形的小样本源验证；单个来源验证通过后再分批补采。完整验收要求见JSON中的acceptance_requirements。
"""
    (output / "requirements.md").write_text(markdown, encoding="utf-8")
    (PROJECT / "reports/missing-retired-prices.json").write_text(report_text, encoding="utf-8")
    (PROJECT / "reports/missing-retired-prices.md").write_text(markdown, encoding="utf-8")
    print(json.dumps({"output": str(output), "candidate_retired_codes": len(candidates),
                      "missing_local_price_codes": len(missing), "years": dict(sorted(year_counts.items()))}, indent=2))


if __name__ == "__main__":
    main()
