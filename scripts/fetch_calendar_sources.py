"""Archive the official notices behind one exchange calendar catalog."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import requests
from bs4 import BeautifulSoup
from fetch_http_reference import new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=PROJECT / "config/exchange-calendar-sources.json")
    parser.add_argument("--output-root", type=Path, default=PROJECT / "data/external/calendar_sources")
    args = parser.parse_args()
    catalog = json.loads(args.catalog.read_text(encoding="utf-8"))
    output = new_snapshot_directory(args.output_root)
    manifest = {"output": str(output), "catalog": str(args.catalog.resolve()),
                "catalog_sha256": hashlib.sha256(args.catalog.read_bytes()).hexdigest(),
                "source_kind": "official_exchange_html_response", "files": [], "errors": [],
                "acquisition_complete": False, "backtest_ready": False}
    session = requests.Session()
    sources = catalog["years"] + catalog["amendments"]
    for index, source in enumerate(sources, 1):
        try:
            response = session.get(source["source_url"], timeout=30)
            path = output / f"{index:02d}_{source['year']}.html"
            with path.open("xb") as stream:
                stream.write(response.content)
            metadata = {"url": source["source_url"], "response_url": response.url,
                        "retrieved_at": datetime.now(timezone.utc).isoformat(), "http_status": response.status_code,
                        "path": str(path), "sha256": hashlib.sha256(response.content).hexdigest()}
            save_json_once(path.with_suffix(".json"), metadata)
            response.raise_for_status()
            text = BeautifulSoup(response.content.decode("utf-8"), "html.parser").get_text(" ", strip=True)
            marker = catalog.get("issuer_marker")
            if marker is None and catalog["exchange"] == "SSE":
                # The archived SSE catalog predates issuer_marker; keep its check as it was.
                marker = "上海证券交易所"
            if str(source["year"]) not in text or "休市" not in text or (marker and marker not in text):
                raise ValueError("Unexpected official notice response; raw bytes preserved")
            manifest["files"].append({**metadata, "year": source["year"], "announcement_date": source["announcement_date"]})
            print("Archived", index, "/", len(sources), "year", source["year"], flush=True)
        except Exception as exc:
            manifest["errors"].append({"url": source["source_url"], "error": str(exc)})
            # Stop on network failure or source limit instead of repeatedly hitting it.
            break
        if index < len(sources):
            time.sleep(0.3)
    manifest["acquisition_complete"] = len(manifest["files"]) == len(sources) and not manifest["errors"]
    save_json_once(output / "manifest.json", manifest)
    print("Output", output, "complete", manifest["acquisition_complete"], flush=True)
    if not manifest["acquisition_complete"]:
        raise RuntimeError("Official calendar archive incomplete; preserve evidence and do not build an admitted calendar")


if __name__ == "__main__":
    main()
