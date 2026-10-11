"""Capture CNINFO original report evidence without rewriting current API values.

Every response body and its HTTP metadata is written once in a unique directory.
Publication dates belong to announcement IDs; midnight API epochs are date-only,
not verified disclosure times. Corrections and associateAnnouncement links are
retained as evidence, never automatically assumed to be financial restatements.
The default is a bounded pilot. This does not grant historical backtest admission.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import urljoin, urlparse

import requests

from fetch_http_reference import new_snapshot_directory, save_json_once, iso_date

PROJECT = Path(__file__).resolve().parents[1]
CHINA = timezone(timedelta(hours=8))
QUERY = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
STATIC = "https://static.cninfo.com.cn/"
CATEGORIES = ";".join("category_" + name + "_szsh" for name in ("ndbg", "bndbg", "yjdbg", "sjdbg"))
DEFAULT_SYMBOLS = ["000001", "000002", "300750", "600000", "600519", "601187"]
FINANCIAL_TITLE = re.compile(r"年度报告|季度报告|半年度报告|会计差错|财务报表|财务报告|招股|上市公告书")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def clean_title(title):
    return re.sub(r"<[^>]+>", "", title).strip()


def period_from_title(title):
    """Parse only explicit report years and periods, not correction dates."""
    title = clean_title(title)
    match = re.search(r"(20\d{2})\s*年?\s*(第一季度|一季度|第三季度|三季度|半年度|年度)", title)
    if not match:
        return None
    ending = {"第一季度": "03-31", "一季度": "03-31", "第三季度": "09-30", "三季度": "09-30", "半年度": "06-30", "年度": "12-31"}[match[2]]
    return match[1] + "-" + ending


def classify_title(title):
    title = clean_title(title)
    if re.search(r"更正|修订|更新|补充", title):
        return "correction_or_supplement_candidate"
    if "招股" in title:
        return "ipo_disclosure"
    if "上市公告书" in title:
        return "listing_announcement"
    if re.search(r"摘要|英文|[Aa]nnual|[Ii]nterim", title):
        return "summary_or_translation"
    if re.search(r"年度报告|季度报告|半年度报告", title):
        return "original_report_candidate"
    return "other"


def publication(row):
    """Confirm date with two fields; retain raw epoch and unknown exact time."""
    epoch = row.get("announcementTime")
    if type(epoch) is not int or epoch < 0:
        raise ValueError("Missing announcement epoch")
    timestamp = datetime.fromtimestamp(epoch / 1000, CHINA)
    day = timestamp.date().isoformat()
    path_match = re.search(r"finalpage/(\d{4}-\d{2}-\d{2})/", row.get("adjunctUrl", ""))
    if not path_match or path_match[1] != day:
        raise ValueError("Publication date differs from official attachment directory")
    return {"publication_date": day, "announcement_time_raw_ms": epoch,
            "publication_date_verified": True, "publication_time_precision": "date_only",
            "exact_publication_time": None, "available_at": None,
            "availability_verified": False,
            "availability_restriction": "Do not use the midnight API epoch as a same-day availability timestamp."}


class Capture:
    def __init__(self, output):
        self.output = output
        (output / "raw").mkdir()
        (output / "pdf").mkdir()

    def request(self, key, method, url, *, params=None, data=None, destination=None):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", key):
            raise ValueError("Invalid response evidence key")
        # No secrets, authentication, browser cookies or licensed data endpoints.
        response = requests.request(method, url, params=params, data=data, timeout=(15, 50),
                                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/"})
        body = destination or self.output / "raw" / (key + ".body")
        with body.open("xb") as stream:
            stream.write(response.content)
        evidence = {"method": method, "url": url, "response_url": response.url,
                    "request_params": params, "request_form": data,
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "status_code": response.status_code, "response_headers": dict(response.headers),
                    "response_body_path": str(body), "response_body_sha256": sha(body),
                    "response_body_bytes": len(response.content),
                    "body_encoding": "requests-decoded complete HTTP entity body; no truncation"}
        evidence_path = self.output / "raw" / (key + ".http.json")
        save_json_once(evidence_path, evidence)
        response.raise_for_status()
        return response, {"path": str(evidence_path), "sha256": sha(evidence_path), **evidence}

    def pages(self, symbol, org_id, column, start, end, mode):
        result, seen, page, advertised = [], set(), 1, None
        while True:
            form = {"stock": f"{symbol},{org_id}", "tabName": "fulltext", "pageSize": 30,
                    "pageNum": page, "column": column, "seDate": f"{start}~{end}",
                    "sortName": "time", "sortType": "asc", "isHLtitle": "false"}
            if mode == "reports":
                form["category"] = CATEGORIES
            else:
                form["searchkey"] = mode
            response, evidence = self.request(f"{symbol}_{'reports' if mode == 'reports' else {'更正':'corrections', '补充':'supplements', '招股':'ipo', '上市公告书':'listing'}[mode]}_{page:03}", "POST", QUERY, data=form)
            payload = response.json()
            total = payload.get("totalAnnouncement")
            if type(total) is not int or total < 0 or total > 10000:
                raise ValueError("Invalid announcement total; refuse unbounded acquisition")
            if advertised is not None and advertised != total:
                raise ValueError("Directory changed during pagination; recapture in a new run")
            advertised = total
            rows = payload.get("announcements") or []
            for row in rows:
                if row.get("secCode") != symbol:
                    raise ValueError("Directory includes an unrequested security")
                announcement_id = row.get("announcementId")
                if not isinstance(announcement_id, str) or not announcement_id.isdigit() or announcement_id in seen:
                    raise ValueError("Missing or duplicated announcement ID")
                seen.add(announcement_id)
                result.append({**row, "title_clean": clean_title(row["announcementTitle"]),
                               "directory_evidence": {"path": evidence["path"], "sha256": evidence["sha256"]}})
            if not payload.get("hasMore"):
                break
            if not rows or page >= 400:
                raise ValueError("Pagination has not completed")
            page += 1
        if len(result) != advertised:
            raise ValueError("Captured rows differ from full announcement count")
        return result, {"query": mode, "symbol": symbol, "rows": len(result), "pages": page,
                        "complete_directory_query": True, "start": start, "end": end}

    def pdf(self, row):
        attachment = row.get("adjunctUrl", "")
        if not re.fullmatch(r"finalpage/\d{4}-\d{2}-\d{2}/[A-Za-z0-9._-]+\.[Pp][Dd][Ff]", attachment):
            raise ValueError("Attachment is not a canonical CNINFO PDF")
        url = urljoin(STATIC, attachment)
        if urlparse(url).hostname != "static.cninfo.com.cn":
            raise ValueError("Attachment origin differs")
        key = row["secCode"] + "_" + row["announcementId"]
        destination = self.output / "pdf" / (key + ".pdf")
        response, evidence = self.request(key, "GET", url, destination=destination)
        if not response.content.startswith(b"%PDF-"):
            raise ValueError("Attachment response is not PDF bytes")
        return {"symbol": row["secCode"], "announcement_id": row["announcementId"],
                "title": row["title_clean"], "document_kind": classify_title(row["title_clean"]),
                "report_period_candidate": period_from_title(row["title_clean"]), **publication(row),
                "url": url, "path": str(destination), "sha256": sha(destination),
                "bytes": len(response.content), "http_evidence": {"path": evidence["path"], "sha256": evidence["sha256"]},
                "directory_evidence": row["directory_evidence"],
                "original_document_id_verified": True, "original_values_extracted_verified": False,
                "original_vintage_verified": False, "restatement_chain_verified": False,
                "associate_announcement_raw": row.get("associateAnnouncement"),
                "association_semantics": "Raw CNINFO association; not assumed to mean correction or replacement."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--start", type=iso_date, default="2018-01-01")
    parser.add_argument("--end", type=iso_date, default="2026-10-07")
    parser.add_argument("--download-years", nargs="+", type=int, default=[2017, 2018, 2019])
    parser.add_argument("--workers", type=int, choices=(1, 2), default=2)
    args = parser.parse_args(argv)
    if len(set(args.symbols)) != len(args.symbols) or any(not re.fullmatch(r"\d{6}", x) for x in args.symbols):
        raise ValueError("Symbols must be unique six-digit stock codes")
    if args.start > args.end or date.fromisoformat(args.end) > datetime.now(CHINA).date():
        raise ValueError("Invalid acquisition date window")
    output = new_snapshot_directory(PROJECT / "data" / "external" / "financial_originals")
    print("Evidence directory", output, flush=True)
    capture = Capture(output)
    manifest = {"run_id": output.name, "output": str(output), "source_kind": "official_announcement_documents",
                "provider": "CNINFO public official disclosure archive", "as_of": args.end,
                "retrieved_at": datetime.now(timezone.utc).isoformat(), "backtest_ready": False,
                "readiness_gaps": ["full_universe_original_values_and_restatements", "field_extraction_and_accounting_basis_verification",
                                   "historical_availability_policy_and_verified_session_mapping"],
                "requested_symbols": args.symbols, "download_report_years": args.download_years,
                "coverage_scope": "Bounded official document pilot; publication directory is not a complete point-in-time financial factor database.",
                "directory_queries": [], "files": [], "errors": []}
    directory = {}
    for exchange in ("szse", "sse"):
        try:
            response, evidence = capture.request("stock_directory_" + exchange, "GET", f"https://www.cninfo.com.cn/new/data/{exchange}_stock.json")
            for item in response.json().get("stockList", []):
                code = item.get("code")
                if code in directory and directory[code]["orgId"] != item["orgId"]:
                    raise ValueError("Conflicting official organization IDs")
                directory[code] = {**item, "directory_source": {"path": evidence["path"], "sha256": evidence["sha256"]}}
        except Exception as exc:
            manifest["errors"].append({"stage": "stock_directory", "exchange": exchange, "error": str(exc)})
    all_rows = {}
    for symbol in args.symbols:
        if symbol not in directory:
            manifest["errors"].append({"stage": "resolve", "symbol": symbol, "error": "Official orgId not found; never guess an identifier"})
            continue
        org_id = directory[symbol]["orgId"]
        column = "szse" if symbol.startswith(("0", "3")) else "sse"
        modes = ["reports", "更正", "补充"] + (["招股", "上市公告书"] if symbol == "601187" else [])
        for mode in modes:
            try:
                rows, query_info = capture.pages(symbol, org_id, column, args.start, args.end, mode)
                manifest["directory_queries"].append(query_info)
                for row in rows:
                    key = (symbol, row["announcementId"])
                    if key in all_rows and any(all_rows[key].get(field) != row.get(field) for field in ("announcementTime", "adjunctUrl", "title_clean")):
                        raise ValueError("Same announcement ID changed across queries")
                    all_rows.setdefault(key, row)
                print(symbol, mode, len(rows), flush=True)
            except Exception as exc:
                manifest["errors"].append({"stage": "announcement_query", "symbol": symbol, "query": mode, "error": str(exc)})
    rows = sorted(all_rows.values(), key=lambda x: (x["secCode"], x["announcementTime"], x["announcementId"]))
    index_path = output / "announcement-index.json"
    save_json_once(index_path, {"rows": rows, "source_kind": "complete_query_responses_retained", "numeric_values": "none"})
    manifest["announcement_index"] = {"path": str(index_path), "sha256": sha(index_path), "rows": len(rows)}
    selected = []
    for row in rows:
        title = row["title_clean"]
        kind = classify_title(title)
        period = period_from_title(title)
        report_selected = period and int(period[:4]) in args.download_years and kind in ("original_report_candidate", "correction_or_supplement_candidate")
        ipo_selected = row["secCode"] == "601187" and kind in ("ipo_disclosure", "listing_announcement") and FINANCIAL_TITLE.search(title)
        if report_selected or ipo_selected:
            selected.append(row)
    manifest["pdf_requested"] = len(selected)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(capture.pdf, row): row for row in selected}
        for future in as_completed(pending):
            row = pending[future]
            try:
                item = future.result()
                manifest["files"].append(item)
                print("PDF", item["symbol"], item["announcement_id"], len(manifest["files"]), "/", len(selected), flush=True)
            except Exception as exc:
                manifest["errors"].append({"stage": "pdf", "symbol": row["secCode"], "announcement_id": row["announcementId"], "error": str(exc)})
    manifest["files"].sort(key=lambda x: (x["symbol"], x["publication_date"], x["announcement_id"]))
    manifest["complete_requested_acquisition"] = not manifest["errors"] and len(manifest["files"]) == len(selected)
    manifest["statistics"] = {"symbols_with_documents": len({x["symbol"] for x in manifest["files"]}),
                              "documents": len(manifest["files"]), "document_bytes": sum(x["bytes"] for x in manifest["files"]),
                              "date_verified_documents": sum(x["publication_date_verified"] for x in manifest["files"]),
                              "exact_time_verified_documents": 0, "financial_values_verified": 0}
    save_json_once(output / "manifest.json", manifest)
    print("Manifest", output / "manifest.json", "documents", len(manifest["files"]), "errors", len(manifest["errors"]), flush=True)
    return output


if __name__ == "__main__":
    main()
