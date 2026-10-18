"""Check large financial amounts against the original reports, including units.

Headline amounts are printed in 元, 千元, 万元, 百万元 or 亿元 depending on the
issuer, so a value comparison also has to establish the unit. Instead of reading
a unit header, this asks which standard unit scale relates the printed cell to
the stored value, and reports the scales it actually observed.

The stored revenue column is total operating revenue, which equals operating
revenue plus financial-sector income for issuers that have a finance arm, so the
total-revenue row is preferred and the label actually used is reported.

KNOWN LIMITATION: the label row is taken from the first page carrying the summary
marker, which for some issuers is the quarterly-breakdown table rather than the
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
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(PROJECT / "scripts"))

from verify_financial_vintages import label_row, page_spans, row_values  # noqa: E402

# Candidate labels per stored column, tried in order; the one used is recorded.
FIGURES = {
    "TOTALOPERATEREVE": (("营业总收入", re.compile(r"营业总收入")), ("营业收入", re.compile(r"营业收入"))),
    "PARENTNETPROFIT": (("归母净利润", re.compile(r"归属于(?:上市公司股东|母公司(?:股东|所有者)|本公司股东)的净利润")),),
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
    parser.add_argument("--whole-document", action="store_true",
                        help="Also scan non-summary pages; verified for revenue only, and it adds mismatches elsewhere")
    args = parser.parse_args()
    try:
        import pymupdf
    except ImportError:  # older PyMuPDF releases only expose the legacy name
        import fitz as pymupdf
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

    results, counts, units, labels = [], {}, {}, {}
    for (symbol, period), entry in sorted(chosen.items()):
        pdf = Path(entry["path"])
        if sha(pdf) != entry["sha256"]:
            raise ValueError("Original document changed after acquisition")
        rows_found, labels_used, selection = {}, {}, {}
        with pymupdf.open(pdf) as document:
            pages = [document[i] for i in range(min(len(document), args.max_pages))]
            section_pages, marker_pages = [], []
            for page in pages:
                compact = re.sub(r"\s+", "", page.get_text("text", sort=True))
                if SECTION in compact:
                    section_pages.append(page)
                elif SUMMARY_MARKER in compact:
                    marker_pages.append(page)
            # The headline table decides first. Some issuers print only operating
            # revenue there while the stored column is total operating revenue, so
            # the whole document is a final fallback; the path used is recorded.
            paths = [("section", section_pages), ("marker_fallback", marker_pages)]
            if args.whole_document:
                paths.append(("whole_document", pages))
            for path, candidates in paths:
                if not candidates:
                    continue
                # The whole-document scan is enabled only for the revenue column,
                # where the total-versus-operating definition difference was
                # verified; elsewhere it reaches unrelated statements.
                allowed = FIGURES if path != "whole_document" else {"TOTALOPERATEREVE": FIGURES["TOTALOPERATEREVE"]}
                for page in candidates:
                    spans = page_spans(page)
                    for column, options in allowed.items():
                        if column in rows_found:
                            continue
                        for option, pattern in options:
                            tokens = [t for t in label_row(spans, pattern) if "扣除" not in t]
                            values = row_values(tokens)
                            if values:
                                rows_found[column] = values
                                labels_used[column] = option
                                selection[column] = path
                                break
                if len(rows_found) == len(FIGURES):
                    break
        snapshot = stored.get((symbol, period), {})
        for column, options in FIGURES.items():
            outcome, unit = classify(snapshot.get(column), rows_found.get(column))
            key = column + ":" + outcome
            counts[key] = counts.get(key, 0) + 1
            if unit:
                units[column + ":" + unit] = units.get(column + ":" + unit, 0) + 1
            if labels_used.get(column):
                k = column + ":" + labels_used[column]
                labels[k] = labels.get(k, 0) + 1
            results.append({"symbol": symbol, "report_period": period, "column": column,
                            "stored": snapshot.get(column), "printed_values": rows_found.get(column),
                            "label_used": labels_used.get(column), "implied_unit": unit,
                            "outcome": outcome, "page_selection": selection.get(column)})

    output = PROJECT / "data/external/financial_amount_checks" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    report = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
              "columns": {k: [o for o, _ in v] for k, v in FIGURES.items()},
              "unit_scales_tested": UNIT_SCALES,
              "method": "a printed cell matches when stored / printed equals a standard unit scale",
              "source_manifest": str(manifest_path), "source_manifest_sha256": sha(manifest_path),
              "backtest_ready": False, "outcome_counts": counts, "implied_units": units,
              "labels_used": labels, "comparisons": results}
    (output / "comparison.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "outcome_counts": counts, "labels_used": labels}, ensure_ascii=False))


if __name__ == "__main__":
    main()
