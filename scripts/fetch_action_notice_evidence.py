"""Capture complete, immutable original-company notices and public status pages.

This collector does not infer tradability or turn preliminary search results into
verified cash events. Human-checked event semantics are saved separately.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

import requests

try:
    import pymupdf
except ImportError:  # older PyMuPDF releases only expose the legacy name
    import fitz as pymupdf

PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_TARGETS = [
    {"id": "600025_20190705_sse", "symbol": "600025", "ex_date": "2019-07-05", "announcement_date": "2019-06-29", "url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/2019-06-29/600025_20190629_1.pdf"},
    {"id": "600025_20200619_sse", "symbol": "600025", "ex_date": "2020-06-19", "announcement_date": "2020-06-12", "url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/2020-06-12/600025_20200612_1.pdf"},
    {"id": "600989_20190927_sse", "symbol": "600989", "ex_date": "2019-09-27", "announcement_date": "2019-09-23", "url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/2019-09-23/600989_20190923_1.pdf"},
    {"id": "600989_20200604_sse", "symbol": "600989", "ex_date": "2020-06-04", "announcement_date": "2020-05-29", "url": "https://static.sse.com.cn/disclosure/listedinfo/announcement/c/2020-05-29/600989_20200529_1.pdf"},
    {"id": "000650_20260717_company_notice_mirror", "symbol": "000650", "ex_date": "2026-07-17", "announcement_date": "2026-07-09", "url": "https://file.finance.sina.com.cn/211.154.219.97%3A9494/MRGG/CNSESZ_STOCK/2026/2026-7/2026-07-09/12436518.PDF"},
    {"id": "sse_suspension_stock_page", "url": "https://www.sse.com.cn/disclosure/dealinstruc/suspension/stock/"},
]


def now():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def notice_text(response):
    """Reduce a disclosure page to readable text, honouring an explicit charset.

    Designated disclosure newspapers serve UTF-8 pages but often send no charset,
    so requests falls back to ISO-8859-1 and the text would decode to mojibake.
    A charset the server states explicitly is still respected.
    """
    declared = (response.encoding or "").lower()
    encoding = declared if declared and declared not in ("iso-8859-1", "latin-1") else "utf-8"
    text = response.content.decode(encoding, errors="replace")
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, help="JSON array of explicitly identified URLs; defaults to five conflict notices and SSE status page")
    args = parser.parse_args()
    targets = json.loads(args.targets.read_text(encoding="utf-8")) if args.targets else DEFAULT_TARGETS
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = PROJECT / "data/external/action_notice_evidence" / run
    (output / "raw").mkdir(parents=True)
    (output / "text").mkdir()
    manifest = {"run_id": run, "output": str(output), "started_at": now(), "backtest_ready": False,
                "readiness_gaps": ["manual_notice_semantic_validation", "full_stock_payment_date_coverage", "full_corporate_action_type_coverage", "historical_security_status_and_price_limits"],
                "sources": [], "errors": []}
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://www.sse.com.cn/"})
    for target in targets:
        entry = {**target, "request_method": "GET", "request_params": {}, "retrieved_at": now()}
        try:
            response = session.get(target["url"], timeout=25)
            pdf = response.content.startswith(b"%PDF")
            path = output / "raw" / (target["id"] + (".pdf" if pdf else ".html"))
            path.write_bytes(response.content)
            entry.update({"final_url": response.url, "http_status": response.status_code, "headers": dict(response.headers), "path": str(path.relative_to(output)), "sha256": sha(path), "bytes": len(response.content), "is_pdf": pdf})
            response.raise_for_status()
            if pdf:
                with pymupdf.open(stream=response.content, filetype="pdf") as document:
                    pages = [{"page": i + 1, "text": page.get_text()} for i, page in enumerate(document)]
                text_path = output / "text" / (target["id"] + ".json")
                text_path.write_text(json.dumps(pages, ensure_ascii=False, indent=2), encoding="utf-8")
                entry.update({"page_count": len(pages), "text_path": str(text_path.relative_to(output)), "text_sha256": sha(text_path)})
            elif target.get("notice_text"):
                text = notice_text(response)
                if not text:
                    raise ValueError("Disclosure page produced no readable text; raw response retained")
                text_path = output / "text" / (target["id"] + ".txt")
                text_path.write_text(text, encoding="utf-8")
                entry.update({"text_path": str(text_path.relative_to(output)), "text_sha256": sha(text_path), "text_chars": len(text)})
            elif "symbol" in target:
                raise ValueError("Expected company notice PDF; full non-PDF response retained")
            entry["download_complete"] = True
        except Exception as exc:
            entry["download_complete"] = False
            entry["error"] = str(exc)
            manifest["errors"].append({"id": target["id"], "error": str(exc)})
        manifest["sources"].append(entry)
        (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(target["id"], entry["download_complete"], entry.get("page_count"), entry.get("bytes"), entry.get("error", ""), flush=True)
    manifest["completed_at"] = now()
    manifest["downloaded_pdf_count"] = sum(item.get("download_complete") and item.get("is_pdf") for item in manifest["sources"])
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Output", output, flush=True)
    if any("10013" in item["error"] for item in manifest["errors"]):
        sys.exit(13)


if __name__ == "__main__":
    main()
