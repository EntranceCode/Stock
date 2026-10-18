"""Extract evidence pages from an acquired official-report manifest.

This creates a new derivative version. It does not infer table columns, amounts,
units, historical availability or restatement relations. Full page text and
renders are for explicit review before any numerical observation is accepted.
"""
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import re

try:
    import pymupdf
except ImportError:  # older PyMuPDF releases only expose the legacy name
    import fitz as pymupdf

from fetch_http_reference import new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]
LABELS = ["营业收入", "营业总收入", "归属于", "净利润", "经营活动", "总资产", "资产总额", "股东权益", "净资产", "基本每股收益", "加权平均净资产收益率"]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evidence_path(value):
    path = Path(value).resolve()
    if not path.is_relative_to(PROJECT) or not path.is_file():
        raise ValueError("Evidence file must exist inside this project")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--render-limit", type=int, default=2, help="Bounded financial-summary pages per document")
    args = parser.parse_args(argv)
    if not 1 <= args.max_pages <= 100 or not 0 <= args.render_limit <= 10:
        raise ValueError("Invalid evidence extraction limit")
    source = evidence_path(args.manifest)
    source_bytes = source.read_bytes()
    acquired = json.loads(source_bytes)
    if acquired.get("source_kind") != "official_announcement_documents":
        raise ValueError("Expected official announcement manifest, not current numerical API snapshots")
    output = new_snapshot_directory(PROJECT / "data" / "external" / "financial_original_pages")
    (output / "pages").mkdir()
    (output / "renders").mkdir()
    manifest = {"run_id": output.name, "output": str(output), "generated_at": datetime.now(timezone.utc).isoformat(),
                "source_kind": "original_document_page_evidence", "source_manifest": str(source),
                "source_manifest_sha256": hashlib.sha256(source_bytes).hexdigest(), "backtest_ready": False,
                "readiness_gaps": ["manual_table_column_unit_and_amount_verification", "historical_availability_and_restatements"],
                "numeric_values_verified": 0, "files": [], "errors": []}
    for entry in acquired["files"]:
        if args.symbols and entry["symbol"] not in args.symbols:
            continue
        if entry["document_kind"] not in ("original_report_candidate", "correction_or_supplement_candidate"):
            continue
        try:
            pdf_path = evidence_path(entry["path"])
            if sha(pdf_path) != entry["sha256"]:
                raise ValueError("Original PDF hash differs")
            document = pymupdf.open(pdf_path)
            if document.is_encrypted:
                raise ValueError("Encrypted financial document requires a separate workflow")
            key = entry["symbol"] + "_" + entry["announcement_id"]
            records, renders = [], []
            for i in range(min(len(document), args.max_pages)):
                page = document[i]
                content = page.get_text("text", sort=True)
                compact = re.sub(r"\s+", "", content)
                labels = [label for label in LABELS if label in compact]
                if len(labels) < 2:
                    continue
                page_record = {"pdf_page_1based": i + 1, "labels_present": labels, "text": content,
                               "text_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                               "values_verified": False}
                records.append(page_record)
                # Render only potential primary summary pages, not tables of
                # contents that happen to list report section titles.
                if len(renders) < args.render_limit and ("基本每股收益" in labels or "加权平均净资产收益率" in labels):
                    image = output / "renders" / f"{key}_p{i+1:03}.png"
                    page.get_pixmap(matrix=pymupdf.Matrix(1.5, 1.5), alpha=False).save(image)
                    renders.append({"path": str(image), "sha256": sha(image), "pdf_page_1based": i + 1})
            path = output / "pages" / (key + ".json")
            save_json_once(path, {"symbol": entry["symbol"], "announcement_id": entry["announcement_id"],
                                 "title": entry["title"], "original_pdf": str(pdf_path), "original_pdf_sha256": entry["sha256"],
                                 "publication_date": entry["publication_date"], "available_at": None,
                                 "report_period_candidate": entry["report_period_candidate"],
                                 "original_vintage_verified": False, "pdf_pages": len(document),
                                 "inspected_pages": min(len(document), args.max_pages), "pages": records})
            document.close()
            manifest["files"].append({"symbol": entry["symbol"], "announcement_id": entry["announcement_id"],
                                      "path": str(path), "sha256": sha(path), "candidate_pages": len(records),
                                      "original_pdf": str(pdf_path), "original_pdf_sha256": entry["sha256"], "renders": renders})
        except Exception as exc:
            manifest["errors"].append({"symbol": entry["symbol"], "announcement_id": entry["announcement_id"], "error": str(exc)})
    save_json_once(output / "manifest.json", manifest)
    print("Page evidence", output / "manifest.json", "files", len(manifest["files"]), "errors", len(manifest["errors"]), flush=True)
    return output


if __name__ == "__main__":
    main()
