"""Audit current financial snapshots without treating them as historical vintages.

The report is a reproducible derivative. It never modifies registered source
files, creates factors, or changes any dataset's backtest admission status.
"""
from collections import Counter
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


FIELD_RULES = {
    "EPSJB": {"kind": "per_share_period", "meaning": "基本每股收益", "candidate_unit": "报告币种/股", "restriction": "不可直接季度年度化，也不可在股数变化时机械相加为TTM"},
    "BPS": {"kind": "per_share_balance", "meaning": "每股净资产", "candidate_unit": "报告币种/股", "restriction": "期末时点值；非正值不得用于常规正PB估值"},
    "MGJYXJJE": {"kind": "per_share_period", "meaning": "每股经营活动现金流", "candidate_unit": "报告币种/股", "restriction": "每股分母口径需核验，不按总额流量直接相加"},
    "TOTALOPERATEREVE": {"kind": "cumulative_flow", "meaning": "营业总收入", "candidate_unit": "报告币种元", "restriction": "中期候选TTM=本期累计+上年全年-上年同期累计，先核验口径与历史版本"},
    "PARENTNETPROFIT": {"kind": "cumulative_flow", "meaning": "归母净利润", "candidate_unit": "报告币种元", "restriction": "非正TTM不得用于常规正PE；总市值还需同一时点全部股权价值"},
    "KCFJCXSYJLR": {"kind": "cumulative_flow", "meaning": "扣非归母净利润", "candidate_unit": "报告币种元", "restriction": "须按累计总额拼接TTM，不能混用每股分母"},
    "NETCASH_OPERATE_PK": {"kind": "cumulative_flow", "meaning": "经营活动现金流净额", "candidate_unit": "报告币种元", "restriction": "累计流量口径需核验；不要对每股现金流套相同公式"},
    "ROEJQ": {"kind": "period_ratio", "meaning": "加权净资产收益率", "candidate_unit": "%", "restriction": "报告期比率，不能简单相加/乘四得到年度ROE；单位需原始报告复核"},
    "ROEKCJQ": {"kind": "period_ratio", "meaning": "扣非加权净资产收益率", "candidate_unit": "%", "restriction": "报告期比率，不能简单相加/乘四得到年度ROE"},
    "TOTAL_ASSETS_PK": {"kind": "balance", "meaning": "总资产", "candidate_unit": "报告币种元", "restriction": "期末时点值，不滚动求和"},
    "TOTAL_EQUITY_PK": {"kind": "balance", "meaning": "权益总额", "candidate_unit": "报告币种元", "restriction": "期末时点值；是否归母口径须独立核验"},
    "TOTAL_SHARE": {"kind": "share_count", "meaning": "总股本", "candidate_unit": "股", "restriction": "仅为报表记录，不能假定每日可用或全部股份以A股同价计值"},
    "A_FREE_SHARE": {"kind": "share_count", "meaning": "流通A股", "candidate_unit": "股", "restriction": "仅为报表记录，不能替代历史自由流通股本或总股本"},
    "ZCFZL": {"kind": "balance_ratio", "meaning": "资产负债率", "candidate_unit": "%", "restriction": "单位与行业可比性需核验；金融与非金融企业不直接统一排名"},
}
DATE_FIELDS = ["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE", "report_period", "announcement_date", "earliest_candidate_available_date"]
SELECTED_COLUMNS = list(FIELD_RULES) + DATE_FIELDS + ["SECURITY_CODE", "REPORT_TYPE", "CURRENCY", "ORG_TYPE", "original_vintage_verified"]
FLOW_FIELDS = [field for field, rule in FIELD_RULES.items() if rule["kind"] == "cumulative_flow"]


def parsed_dates(frame, field):
    """Normalize dates for comparison; retain separate missing/invalid counts."""
    source = frame[field] if field in frame else pd.Series(None, index=frame.index, dtype=object)
    missing = source.isna()
    result = pd.to_datetime(source, errors="coerce").dt.normalize()
    return result, {"missing": int(missing.sum()), "invalid_nonmissing": int((~missing & result.isna()).sum())}


def numeric_stats(source):
    converted = pd.to_numeric(source, errors="coerce")
    values = converted.to_numpy(dtype=float, na_value=np.nan)
    missing = source.isna().to_numpy()
    invalid = ~missing & np.isnan(values)
    finite = np.isfinite(values)
    valid = values[finite]
    return {
        "missing": int(missing.sum()), "invalid_nonmissing": int(invalid.sum()),
        "nonfinite_nonmissing": int((~missing & ~finite & ~invalid).sum()),
        "finite": int(finite.sum()), "negative": int((valid < 0).sum()),
        "zero": int((valid == 0).sum()), "positive": int((valid > 0).sum()),
        "min": float(valid.min()) if valid.size else None,
        "median": float(np.median(valid)) if valid.size else None,
        "max": float(valid.max()) if valid.size else None,
    }


def finite_number(value):
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def ttm_coverage(frame):
    """Count arithmetic dependencies only; do not expose TTM values or signals.

    EPS and all ratios are intentionally excluded. A dependency being present
    does not establish its original publication value, comparable accounting
    basis, or availability at a past trading decision.
    """
    periods, _ = parsed_dates(frame, "REPORT_DATE")
    working = frame.copy()
    working["_period"] = periods
    known = working[working._period.notna()]
    duplicates = known.duplicated(["SECURITY_CODE", "_period"], keep=False)
    if duplicates.any():
        raise ValueError("Duplicate symbol/report periods must be resolved explicitly before TTM coverage auditing")
    grouped = known.groupby("SECURITY_CODE", sort=False)
    coverage = {field: Counter() for field in FLOW_FIELDS}
    for _, group in grouped:
        rows = {row["_period"]: row for row in group.to_dict("records")}
        for period, row in rows.items():
            annual = period.month == 12 and period.day == 31
            interim = (period.month, period.day) in ((3, 31), (6, 30), (9, 30))
            if annual:
                dependencies = [row]
            elif interim:
                annual_row = rows.get(pd.Timestamp(period.year - 1, 12, 31))
                comparable_row = rows.get(pd.Timestamp(period.year - 1, period.month, period.day))
                dependencies = [row, annual_row, comparable_row]
            else:
                dependencies = []
            for field, counters in coverage.items():
                if not dependencies:
                    counters["nonstandard_report_period"] += 1
                elif any(item is None for item in dependencies):
                    counters["missing_required_report"] += 1
                elif any(pd.isna(item.get("CURRENCY")) for item in dependencies):
                    counters["currency_unknown"] += 1
                elif len({item.get("CURRENCY") for item in dependencies}) != 1:
                    counters["currency_mismatch"] += 1
                elif not all(finite_number(item.get(field)) for item in dependencies):
                    counters["missing_or_nonfinite_amount"] += 1
                else:
                    counters["annual_arithmetic_candidate" if annual else "interim_arithmetic_candidate"] += 1
    categories = ["annual_arithmetic_candidate", "interim_arithmetic_candidate", "missing_required_report", "currency_unknown", "currency_mismatch", "missing_or_nonfinite_amount", "nonstandard_report_period"]
    return {field: {name: int(counters[name]) for name in categories} for field, counters in coverage.items()}


def audit_frame(frame, as_of):
    if "SECURITY_CODE" not in frame:
        raise ValueError("Financial audit requires SECURITY_CODE")
    dates, date_quality = {}, {}
    for field in DATE_FIELDS:
        dates[field], date_quality[field] = parsed_dates(frame, field)
    period, notice, update = (dates[field] for field in ("REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"))
    delay = (update - notice).dt.total_seconds() / 86400
    known_delay = delay.dropna()
    announcement_lag = (notice - period).dt.days
    period_class = period.dt.strftime("%m-%d").map({"03-31": "Q1_cumulative", "06-30": "H1_cumulative", "09-30": "Q3_cumulative", "12-31": "annual"}).fillna("unknown_or_nonstandard")
    summary = {
        "rows": len(frame), "symbols": int(frame.SECURITY_CODE.nunique()),
        "date_quality": date_quality,
        "report_period_classes": {str(k): int(v) for k, v in period_class.value_counts().items()},
        "report_type_labels": {str(k): int(v) for k, v in frame.get("REPORT_TYPE", pd.Series(dtype=object)).fillna("[missing]").value_counts().items()},
        "currency_counts": {str(k): int(v) for k, v in frame.get("CURRENCY", pd.Series(dtype=object)).fillna("[missing]").value_counts().items()},
        "organization_type_counts": {str(k): int(v) for k, v in frame.get("ORG_TYPE", pd.Series(dtype=object)).fillna("[missing]").value_counts().items()},
        "date_relationships": {
            "announcement_before_period": int((notice < period).sum()),
            "announcement_after_snapshot_as_of": int((notice > pd.Timestamp(as_of)).sum()),
            "update_after_snapshot_as_of": int((update > pd.Timestamp(as_of)).sum()),
            "update_after_notice": int((delay > 0).sum()), "update_same_as_notice": int((delay == 0).sum()),
            "update_before_notice": int((delay < 0).sum()), "update_notice_relation_unknown": int(delay.isna().sum()),
            "update_minus_notice_days_quantiles": {str(q): float(known_delay.quantile(q)) if len(known_delay) else None for q in (.5, .9, .99)},
            "update_minus_notice_days_buckets": {
                "negative": int((delay < 0).sum()), "zero": int((delay == 0).sum()),
                "1_to_30": int(((delay > 0) & (delay <= 30)).sum()),
                "31_to_365": int(((delay > 30) & (delay <= 365)).sum()), "above_365": int((delay > 365).sum()),
            },
            "announcement_lag_days_min": int(announcement_lag.min()) if announcement_lag.notna().any() else None,
            "announcement_lag_days_max": int(announcement_lag.max()) if announcement_lag.notna().any() else None,
            "normalized_report_period_mismatch": int((period.notna() & dates["report_period"].notna() & (period != dates["report_period"])).sum()),
            "normalized_announcement_date_mismatch": int((notice.notna() & dates["announcement_date"].notna() & (notice != dates["announcement_date"])).sum()),
            "candidate_not_day_after_notice": int((notice.notna() & (dates["earliest_candidate_available_date"] != notice + pd.Timedelta(days=1))).sum()),
            "candidate_date_present_without_notice": int((notice.isna() & dates["earliest_candidate_available_date"].notna()).sum()),
        },
        "fields": {field: {**rule, "present_in_loaded_frame": field in frame, **numeric_stats(frame[field] if field in frame else pd.Series(None, index=frame.index, dtype=object))} for field, rule in FIELD_RULES.items()},
        "original_vintage_verified_true": int(frame.get("original_vintage_verified", pd.Series(False, index=frame.index)).eq(True).sum()),
        "ttm_dependency_coverage": ttm_coverage(frame),
    }
    # Location evidence is deliberately bounded; counts above always use all rows.
    missing_numeric = pd.Series(np.nan, index=frame.index)
    anomaly_masks = {"missing_notice_date": notice.isna(), "nonpositive_EPSJB": pd.to_numeric(frame.get("EPSJB", missing_numeric), errors="coerce") <= 0, "nonpositive_BPS": pd.to_numeric(frame.get("BPS", missing_numeric), errors="coerce") <= 0}
    summary["bounded_examples"] = {}
    for name, mask in anomaly_masks.items():
        selected = frame.loc[mask, [column for column in ("SECURITY_CODE", "REPORT_DATE", "NOTICE_DATE", "EPSJB", "BPS") if column in frame]].head(10)
        summary["bounded_examples"][name] = json.loads(selected.to_json(orient="records", force_ascii=False))
    return summary


def markdown_report(report):
    audit = report["audit"]
    relations = audit["date_relationships"]
    lines = ["# 财务字段口径审计", "", f"审计生成时间：{report['generated_at']}。注册数据时点：{report['snapshot_as_of']}。",
             f"完整读取 {report['files_read']:,} 个财务文件，共 {audit['rows']:,} 行、{audit['symbols']:,} 个股票代码；未修改来源文件。", "",
             "本报告检查当前快照的字段和依赖覆盖。字段中文含义、单位和累计口径是待原始财报核验的映射，不构成历史原始版本或投资可用性的确认。所有记录继续禁止进入正式历史回测。", "",
             "## 字段检查", "", "| 字段 | 候选口径 | 缺失 | 非法字符串 | 非有限 | 负值 | 零值 |", "|---|---|---:|---:|---:|---:|---:|"]
    for field, stats in audit["fields"].items():
        lines.append(f"| {field} | {stats['meaning']}，{stats['candidate_unit']} | {stats['missing']:,} | {stats['invalid_nonmissing']:,} | {stats['nonfinite_nonmissing']:,} | {stats['negative']:,} | {stats['zero']:,} |")
    lines += ["", "每股收益、每股现金流、ROE 属于报告期指标；总资产、权益、每股净资产、股本属于时点值。营业总收入、归母/扣非利润和经营现金流总额可作为累计流量候选，但累计口径及单位仍需与原始财报核对。季度 EPS 不能直接乘四；每股指标在股数变化时不能机械相加。ROE 不能按季度直接加总，金融和非金融行业的负债指标也需要分别处理。", "",
              "EPS 或净利润为负不自动说明数据错误，但不能生成常规正 PE。每股净资产非正值不能生成常规正 PB；缺失、非法字符串和无穷值均不能填零参加因子排名。报表股本不能当作已核验的每日股本，A/B/H 等股份也不能默认使用同一个 A 股价格计值。", "",
              "## 公告与更新日期", "",
              f"缺失公告日期 {audit['date_quality']['NOTICE_DATE']['missing']:,} 行；公告早于报告期 {relations['announcement_before_period']:,} 行；公告超过注册时点 {relations['announcement_after_snapshot_as_of']:,} 行。",
              f"更新日晚于公告日 {relations['update_after_notice']:,} 行，相同 {relations['update_same_as_notice']:,} 行，早于 {relations['update_before_notice']:,} 行，无法比较 {relations['update_notice_relation_unknown']:,} 行。",
              f"公告延迟候选日期与“公告日后一个自然日”不一致 {relations['candidate_not_day_after_notice']:,} 行；没有公告日却存在候选可用日期 {relations['candidate_date_present_without_notice']:,} 行。", "",
              "更新日可能反映维护、补录或版本变化，单凭其晚于公告日不能断言更正。当前快照不能把现在看到的数值回填到原公告日；公告日后一个自然日也不是已确认的下一个交易日或精确披露时刻。原始版本与历史修订缺口仍未解决。", "",
              "## TTM 依赖覆盖", "",
              "年报总额可作全年算术候选；中期候选需要本期累计、上年全年、上年同报告期累计三条记录，并要求币种相同、各金额有限。候选公式为：本期累计 + 上年全年 − 上年同期累计。只统计依赖，不生成因子或收益，也不把相同币种视为相同会计口径的证明。EPS、ROE、余额、每股现金流不进入该拼接。", "",
              "| 总额字段 | 全年算术候选 | 中期算术候选 | 缺少历史报告 | 金额缺失/非有限 | 币种未知/不同 |", "|---|---:|---:|---:|---:|---:|"]
    for field, counts in audit["ttm_dependency_coverage"].items():
        lines.append(f"| {field} | {counts['annual_arithmetic_candidate']:,} | {counts['interim_arithmetic_candidate']:,} | {counts['missing_required_report']:,} | {counts['missing_or_nonfinite_amount']:,} | {counts['currency_unknown'] + counts['currency_mismatch']:,} |")
    lines += ["", "各依赖必须有当时已披露的原始数值及版本、准确公告时点，且满足口径一致、修订可追溯、会计主体与币种可比，才可以继续实现交易日层面的 TTM。若引入后续修订，必须按修订公开时点更新，而不是覆盖以前的决策输入。", "",
              "## 报告期分布", "", "| 报告期类别 | 行数 |", "|---|---:|"]
    for name, count in audit["report_period_classes"].items():
        lines.append(f"| {name} | {count:,} |")
    lines += ["", "机器可读结果：[financial-field-audit.json](financial-field-audit.json)。来源清单及 SHA256 记录在该 JSON 中；完整统计和前十条定位样例均可用注册清单重新计算。", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=PROJECT / "config" / "datasets.json")
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    manifest_path = (PROJECT / config["datasets"]["financials"]["manifest"]).resolve()
    if not manifest_path.is_relative_to(PROJECT):
        raise ValueError("Financial manifest must be inside the project")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    frames, files_read, seen_paths = [], 0, set()
    for entry in manifest["files"]:
        path = Path(entry["path"]).resolve()
        if not path.is_relative_to(PROJECT) or path in seen_paths:
            raise ValueError("Source path must be unique and inside the project")
        seen_paths.add(path)
        parquet = pq.ParquetFile(path)
        columns = [name for name in SELECTED_COLUMNS if name in parquet.schema_arrow.names]
        frame = parquet.read(columns=columns).to_pandas()
        if len(frame) != entry["rows"]:
            raise ValueError(f"Manifest row count differs: {path}")
        if "SECURITY_CODE" not in frame or not frame.SECURITY_CODE.eq(entry["symbol"]).all():
            raise ValueError(f"Manifest symbol differs: {path}")
        # Preserve scalar source values while giving all-null columns the same
        # dtype across files. Numeric conversion is audited explicitly later.
        frames.append(frame.astype(object))
        files_read += 1
        if files_read % 1000 == 0:
            print(f"Read {files_read} financial files", flush=True)
    frame = pd.concat(frames, ignore_index=True)
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "snapshot_as_of": config["as_of"],
              "manifest": str(manifest_path), "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
              "files_read": files_read, "source_backtest_ready": manifest.get("backtest_ready", False),
              "backtest_ready": False, "semantics_verified_against_original_reports": False,
              "readiness_gaps": manifest.get("readiness_gaps", []), "audit": audit_frame(frame, config["as_of"])}
    output = PROJECT / "reports"
    output.mkdir(exist_ok=True)
    (output / "financial-field-audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    (output / "financial-field-audit.md").write_text(markdown_report(report), encoding="utf-8")
    print(json.dumps({"files": files_read, "rows": len(frame), "backtest_ready": False,
                      "missing_notice_date": report["audit"]["date_quality"]["NOTICE_DATE"]["missing"],
                      "date_relationships": report["audit"]["date_relationships"]}, ensure_ascii=True, indent=2), flush=True)


if __name__ == "__main__":
    main()
