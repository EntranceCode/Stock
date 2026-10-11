"""Retry only unresolved requests in a fresh, separately evidenced connection."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import fetch_baostock_history as collector


def pending_requests(manifest_paths):
    pending, sources, acquired = {}, [], set()
    for manifest_path in manifest_paths:
        manifest_path = manifest_path.resolve()
        if not manifest_path.is_relative_to(collector.PROJECT):
            raise ValueError("Retry evidence must stay inside the independent project")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        request_path = manifest_path.parent / "requests.json"
        requests = json.loads(request_path.read_text(encoding="utf-8"))
        acquired.update(entry["source_request"]["code"] for entry in manifest["files"])
        for source in manifest.get("source_manifests", []):
            path = Path(source["path"]).resolve()
            if not path.is_relative_to(collector.PROJECT) or hashlib.sha256(path.read_bytes()).hexdigest() != source["sha256"]:
                raise ValueError("Retry source inventory has changed")
        for item in requests:
            code = item["code"]
            if code in pending and pending[code] != item:
                raise ValueError("Conflicting request windows require explicit review")
            pending[code] = item
        sources.extend([manifest_path, request_path])
    return [pending[code] for code in sorted(pending) if code not in acquired], list(dict.fromkeys(sources))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-manifests", type=Path, nargs="+", required=True)
    args = parser.parse_args(argv)
    items, sources = pending_requests(args.from_manifests)
    if not items:
        print("No unresolved price requests remain in these acquisition batches.")
        return 0
    collector.requests_for = lambda mode, start, end: (items, sources)
    return collector.main(["--mode", "retired", "--start", min(item["start"] for item in items),
                           "--end", max(item["end"] for item in items), "--pause", "0.5"])


if __name__ == "__main__":
    raise SystemExit(main())
