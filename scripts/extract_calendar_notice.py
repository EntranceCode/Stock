"""Extract the closure intervals of one archived official holiday notice.

Catalog entries are reviewed by hand, but transcribing nine notices by eye invites
errors. This reads an archived notice, removes the spacing that inline markup leaves
inside dates, and prints the intervals the notice actually states, so a catalog can
be checked against the raw bytes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from bs4 import BeautifulSoup

ITEM = re.compile(r"（[一二三四五六七八九十]+）([^：]{2,12})：")
DATE = re.compile(r"(?:(\d{4})年)?(\d{1,2})月(\d{1,2})日")


def notice_text(path: Path) -> str:
    raw = Path(path).read_bytes().decode("utf-8", errors="replace")
    return re.sub(r"\s+", "", BeautifulSoup(raw, "html.parser").get_text(" ", strip=True))


def closure_section(text: str) -> str:
    start = text.find("休市安排")
    if start < 0:
        raise ValueError("Notice has no closure section")
    end = text.find("二、", start)
    return text[start:end if end > 0 else len(text)]


def intervals(text: str, year: int):
    """Return (holiday, first, last) for every item in the closure section."""
    body = closure_section(text)
    marks = list(ITEM.finditer(body))
    if not marks:
        raise ValueError("Closure section has no holiday items")
    output = []
    for index, mark in enumerate(marks):
        stop = marks[index + 1].start() if index + 1 < len(marks) else len(body)
        closed = body[mark.end():stop].split("休市")[0]
        found = DATE.findall(closed)
        if not found:
            raise ValueError("No date in closure item: " + mark.group(1))
        first_year, first_month, first_day = found[0]
        if len(found) > 1 and "至" in closed:
            last_year, last_month, last_day = found[1]
        else:
            last_year, last_month, last_day = first_year, first_month, first_day
        start_year = int(first_year) if first_year else year
        if last_year:
            end_year = int(last_year)
        elif int(last_month) < int(first_month):
            end_year = start_year + 1
        else:
            end_year = start_year
        output.append((mark.group(1),
                       "%04d-%02d-%02d" % (start_year, int(first_month), int(first_day)),
                       "%04d-%02d-%02d" % (end_year, int(last_month), int(last_day))))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("notice", type=Path)
    parser.add_argument("--year", type=int, required=True, help="Year the notice governs")
    args = parser.parse_args()
    for name, first, last in intervals(notice_text(args.notice), args.year):
        print(json.dumps({"holiday": name, "first": first, "last": last}, ensure_ascii=False))


if __name__ == "__main__":
    main()
