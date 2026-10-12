"""Check large financial amounts against the original reports, including units.

Headline amounts are printed in 元, 千元, 万元, 百万元 or 亿元 depending on the
issuer, so a value comparison also has to establish the unit. Instead of reading
a unit header, this asks which standard unit scale relates the printed cell to
the stored value, and reports the scales it actually observed.

KNOWN LIMITATION: the label row is taken from the first page carrying the summary
marker, which for several issuers is the quarterly-breakdown table rather than the
headline table. Those cells sum to the headline figure, so they look like
mismatches. A reported mismatch therefore needs review before it means anything.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(PROJECT / "scripts"))

from verify_financial_vintages import label_row, page_spans, row_values  # noqa: E402

FIGURES = {
    "TOTALOPERATEREVE": ("revenue", re.compile(r"营业(?:总)?收入")),
    "PARENTNETPROFIT": ("net_profit", re.compile(r"归属于(?:上市公司股东|母公司(?:股东|所有者)|本公司股东)的净利润")),
}
UNIT_SCALES = {"元": 1.0, "千元": 1e3, "万元": 1e4, "百万元": 1e6, "亿元": 1e8}
# The headline table lives under this section heading; the quarterly breakdown
# and segment tables are separate sections further on.
SECTION = "主要会计数据和财务指标"
SUMMARY_MARKER = "加权平均净资产收益率"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def implied_unit(stored, printed, tolerance=0.01):
    """The unit that relates a printed cell to the stored value, or None."""
    if not printed or stored is None:
        return None
    scale = stored / printed
    for name, value in UNIT_SCALES.items():
        if abs(scale - value) <= value * tolerance:
            return name
    return None


def classify(stored, printed_values):
    """Match when one printed cell, in a standard unit, equals the stored value."""
    if stored is None:
        return "no_snapshot_row", None
    if not printed_values:
        return "not_extracted", None
    for value in printed_values:
        unit = implied_unit(stored, value)
        if unit:
            return "match", unit
    return "mismatch", None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--max-pages", type=int, default=60)
    args = parser.parse_args()
    import fitz
    import pyarrow.parquet as pq

    manifest_path = args.manifest.resolve()
    if not manifest_path.is_relative_to(PROJECT):
        raise ValueError("Evidence must stay inside the project")
    acquired = json.loads(manifest_path.read_text(encoding="utf-8"))
    if acquired.get("source_kind") != "official_announcement_documents":
        raise ValueError("Expected an official announcement manifest")

    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    financials = json.loads((PROJECT / registry["datasets"]["financials"]["manifest"]).read_text(encoding="utf-8"))
    stored = {}
    for entry in financials["files"]:
        table = pq.read_table(entry["path"], columns=["REPORT_DATE", *FIGURES]).to_pylist()
        for row in table:
            if row["REPORT_DATE"] is not None:
                stored[(entry["symbol"], str(row["REPORT_DATE"])[:10])] = row

    chosen = {}
    for entry in acquired["files"]:
        if entry.get("document_kind") != "original_report_candidate":
            continue
        key = (entry["symbol"], entry["report_period_candidate"])
        if key not in chosen or entry["bytes"] > chosen[key]["bytes"]:
            chosen[key] = entry

    results, counts, units = [], {}, {}
    for (symbol, period), entry in sorted(chosen.items()):
        pdf = Path(entry["path"])
        if sha(pdf) != entry["sha256"]:
            raise ValueError("Original document changed after acquisition")
        rows_found, selection = {}, None
        with fitz.open(pdf) as document:
            section_pages, marker_pages = [], []
            for index in range(min(len(document), args.max_pages)):
                page = document[index]
                compact = re.sub(r"\s+", "", page.get_text("text", sort=True))
                if SECTION in compact:
                    section_pages.append(page)
                elif SUMMARY_MARKER in compact:
                    marker_pages.append(page)
            # The section page decides the table; the marker scan is only a
            # fallback, and which path produced a row is recorded with it.
            for path, candidates in (("section", section_pages), ("marker_fallback", marker_pages)):
                if rows_found or not candidates:
                    continue
                for page in candidates:
                    spans = page_spans(page)
                    for column, (name, pattern) in FIGURES.items():
                        if name in rows_found:
                            continue
                        tokens = [t for t in label_row(spans, pattern) if "扣除" not in t]
                        values = row_values(tokens)
                        if values:
                            rows_found[name] = values
                if rows_found:
                    selection = path
        snapshot = stored.get((symbol, period), {})
        for column, (name, _) in FIGURES.items():
            outcome, unit = classify(snapshot.get(column), rows_found.get(name))
            counts[f"{name}:{outcome}"] = counts.get(f"{name}:{outcome}", 0) + 1
            if unit:
                units[f"{name}:{unit}"] = units.get(f"{name}:{unit}", 0) + 1
            results.append({"symbol": symbol, "report_period": period, "figure": name,
                            "stored": snapshot.get(column), "printed_values": rows_found.get(name),
                            "implied_unit": unit, "outcome": outcome, "page_selection": selection})

    output = PROJECT / "data/external/financial_amount_checks" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    report = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
              "figures": {k: v[0] for k, v in FIGURES.items()}, "unit_scales_tested": UNIT_SCALES,
              "method": "a printed cell matches when stored / printed equals a standard unit scale",
              "source_manifest": str(manifest_path), "source_manifest_sha256": sha(manifest_path),
              "backtest_ready": False, "outcome_counts": counts, "implied_units": units,
              "comparisons": results}
    (output / "comparison.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "outcome_counts": counts, "implied_units": units}, ensure_ascii=False))


if __name__ == "__main__":
    main()
