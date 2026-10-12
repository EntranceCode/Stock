"""Compare snapshot financial values against the original published reports.

The snapshot keeps one row per report period, so a restated value silently
replaces the published one. This compares a unit-unambiguous figure (basic
earnings per share, which the reports state in yuan per share) between the
archived original documents and the snapshot, and classifies every comparison
instead of resolving a disagreement on its own.
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

NUMBER = re.compile(r"^-?[0-9][0-9,]*(?:\.[0-9]+)?$")
# Some issuers print basic and diluted earnings in one row as 基本/稀释每股收益.
EPS_LABEL = re.compile(r"基本(?:[/／]?稀释)?每股收益")
SUMMARY_MARKERS = ("加权平均净资产收益率",)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def page_spans(page):
    """Text spans with their boxes, so table rows and columns stay separable."""
    spans = []
    for block in page.get_text("dict")["blocks"]:
        for line in block.get("lines", []):
            for span in line["spans"]:
                text = span["text"].strip()
                if text:
                    spans.append((span["bbox"][0], span["bbox"][1], span["bbox"][2], text))
    return spans


def _lines(spans, tolerance=4.0):
    """Cluster spans into visual lines, each sorted left to right."""
    lines = []
    for span in sorted(spans, key=lambda item: (item[1], item[0])):
        for line in lines:
            if abs(line[0][1] - span[1]) <= tolerance:
                line.append(span)
                break
        else:
            lines.append([span])
    return [sorted(line, key=lambda item: item[0]) for line in lines]


def rejoined_label_row(spans, pattern, window=4):
    """Fallback for a label split across spans, such as 基本每股收 + 益."""
    for line in _lines(spans):
        for start in range(len(line)):
            joined = ""
            for end in range(start, min(start + window, len(line))):
                joined += re.sub(r"\s+", "", line[end][3])
                match = pattern.search(joined)
                if match:
                    if "扣除" in joined:
                        break
                    remainder = joined[match.end():]
                    tail = ([remainder] if remainder else []) + [piece[3] for piece in line[end + 1:]]
                    return tail
    return []


def label_row(spans, label):
    """Spans to the right of a label on the same visual line, left to right.

    The deducted-profit variant also contains the plain label, so it is excluded;
    among the rest the shortest text is the plain label itself. A label the PDF
    splits across spans is only retried through the joined fallback, so every
    case the span-level rule already resolves keeps its previous answer.
    """
    anchors = [span for span in spans
               if label.search(re.sub(r"\s+", "", span[3])) and "扣除" not in span[3]]
    if anchors:
        anchor = min(anchors, key=lambda span: (len(span[3]), span[0]))
        row = sorted((span for span in spans if abs(span[1] - anchor[1]) <= 3), key=lambda span: span[0])
        return [span[3] for span in row if span[0] > anchor[0] + 1]
    return rejoined_label_row(spans, label)


def row_values(tokens):
    """Every numeric cell printed on the label row, in printed order.

    Issuers order the single-quarter and period-end columns differently, so the
    comparison asks whether the stored value appears among the printed cells
    instead of guessing which column the snapshot chose.
    """
    values = []
    for token in tokens:
        if not NUMBER.match(token):
            continue
        try:
            values.append(float(token.replace(",", "")))
        except ValueError:
            continue
    return values


def compare(published_values, stored, tolerance=0.005):
    """Classify one comparison without silently reconciling a difference."""
    if stored is None:
        return "no_snapshot_row"
    if not published_values:
        return "not_extracted"
    if any(abs(value - stored) <= tolerance for value in published_values):
        return "match"
    return "mismatch"


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
        table = pq.read_table(entry["path"], columns=["REPORT_DATE", "EPSJB"]).to_pylist()
        for row in table:
            if row["REPORT_DATE"] is not None:
                stored[(entry["symbol"], str(row["REPORT_DATE"])[:10])] = row["EPSJB"]

    # Prefer the full report when a period also has a shorter body version.
    chosen = {}
    for entry in acquired["files"]:
        if entry.get("document_kind") != "original_report_candidate":
            continue
        key = (entry["symbol"], entry["report_period_candidate"])
        if key not in chosen or entry["bytes"] > chosen[key]["bytes"]:
            chosen[key] = entry

    results, counts = [], {}
    for (symbol, period), entry in sorted(chosen.items()):
        pdf = Path(entry["path"])
        if sha(pdf) != entry["sha256"]:
            raise ValueError("Original document changed after acquisition")
        published, page_number = None, None
        with fitz.open(pdf) as document:
            for index in range(min(len(document), args.max_pages)):
                content = document[index].get_text("text", sort=True)
                compact = re.sub(r"\s+", "", content)
                if not (EPS_LABEL.search(compact) and all(marker in compact for marker in SUMMARY_MARKERS)):
                    continue
                values = row_values(label_row(page_spans(document[index]), EPS_LABEL))
                if values:
                    published, page_number = values, index + 1
                    break
        value_stored = stored.get((symbol, period))
        outcome = compare(published, value_stored)
        counts[outcome] = counts.get(outcome, 0) + 1
        results.append({"symbol": symbol, "report_period": period, "title": entry["title"],
                        "publication_date": entry["publication_date"], "pdf": str(pdf),
                        "eps_values_on_label_row": published, "eps_stored": value_stored,
                        "pdf_page_1based": page_number, "outcome": outcome})

    output = PROJECT / "data/external/financial_vintage_checks" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    output.mkdir(parents=True)
    report = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
              "figure": "basic earnings per share (yuan per share)",
              "why_this_figure": "the reports state it per share, so no 元/万元 unit inference is needed",
              "tolerance": args.tolerance if hasattr(args, "tolerance") else 0.005,
              "source_manifest": str(manifest_path), "source_manifest_sha256": sha(manifest_path),
              "financials_manifest": registry["datasets"]["financials"]["manifest"],
              "backtest_ready": False, "financial_values_verified": 0,
              "readiness_gaps": ["tolerance_and_rounding_review", "restatement_attribution",
                                 "non_headline_figures_unchecked", "full_stock_universe_coverage"],
              "outcome_counts": counts, "comparisons": results}
    (output / "comparison.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "comparisons": len(results), "outcome_counts": counts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
