"""Rebuild a run manifest from retained outputs after an interrupted acquisition.

fetch_baostock_history now rewrites manifest.json after every request, so a
terminated batch keeps its inventory. Batches captured before that change left
verified price and observation files plus wire captures with no run inventory at
all. This rebuilds such an inventory from what is actually on disk and recomputes
every hash, so the retained outputs can be audited and indexed like any other
batch.

Only retained evidence is used. Fields the terminated process never wrote are
either omitted or listed under inferred_fields; per-request failure messages that
were never written are described, not invented.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps/parquet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_baostock_history import GAPS  # noqa: E402


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def inside(path):
    path = Path(path).resolve()
    if not path.is_relative_to(PROJECT):
        raise ValueError("Recovery evidence must stay inside the project")
    return path


def reply_security(capture):
    """Security named by a decoded reply parameter tail, or None if undecodable."""
    decoded = capture.get("decoded_protocol_response")
    if not isinstance(decoded, str) or "{" not in decoded:
        return None
    body = decoded[decoded.index("{"):]
    try:
        _, end = json.JSONDecoder().raw_decode(body)
    except ValueError:
        return None
    tail = body[end:].split("\1")
    return tail[1] if len(tail) > 1 else None


def recover(run, source_manifests):
    import pyarrow.parquet as pq

    run = inside(run)
    if (run / "manifest.json").exists():
        raise ValueError("Run already has a manifest; recovery would overwrite recorded evidence")
    requests = json.loads((run / "requests.json").read_text(encoding="utf-8"))
    by_code = {item["code"]: item for item in requests}
    if len(by_code) != len(requests):
        raise ValueError("Duplicate request codes in requests.json")

    sources, seen = [], set()
    for manifest_path in source_manifests:
        manifest = json.loads(inside(manifest_path).read_text(encoding="utf-8"))
        for source in manifest["source_manifests"]:
            resolved = inside(source["path"])
            if sha(resolved) != source["sha256"]:
                raise ValueError("Source inventory changed: " + str(resolved))
            if source["path"] not in seen:
                seen.add(source["path"])
                sources.append({"path": source["path"], "sha256": source["sha256"]})
    if not sources:
        raise ValueError("No source inventory was provided")

    captures = {}
    for path in sorted((run / "raw").glob("*.json")):
        code, _, page = path.stem.rpartition("_")
        if code not in by_code or not page.isdigit():
            raise ValueError("Unrecognised wire capture name: " + path.name)
        captures.setdefault(code, []).append((int(page), path))
    for code, pages in captures.items():
        if sorted(page for page, _ in pages) != list(range(1, len(pages) + 1)):
            raise ValueError("Wire capture pages are incomplete for " + code)

    prices = {path.name[:-len("_daily.parquet")]: path for path in (run / "prices").glob("*_daily.parquet")}
    observations = {path.name[:-len("_observations.parquet")]: path for path in (run / "observations").glob("*_observations.parquet")}
    symbols = [item["code"][3:] for item in requests]
    if len(set(symbols)) != len(symbols):
        raise ValueError("Request list repeats a security symbol across exchanges")
    if set(observations) - set(by_code) or {code[3:] for code in observations} != set(prices):
        raise ValueError("Price and observation outputs do not cover the same securities")

    files, observation_entries, raw_entries, outcomes, errors = [], [], [], [], []
    started_at, scalars = None, {}
    for item in requests:
        code, symbol = item["code"], item["code"][3:]
        for page, path in sorted(captures.get(code, [])):
            capture = json.loads(path.read_text(encoding="utf-8"))
            if capture["params"]["code"] != code:
                raise ValueError("Wire capture " + path.name + " records a different request")
            scalars.setdefault("provider", capture.get("provider"))
            scalars.setdefault("endpoint", capture.get("endpoint"))
            scalars.setdefault("client_version", capture.get("client_version"))
            retrieved = capture.get("retrieved_at")
            if retrieved and (started_at is None or retrieved < started_at):
                started_at = retrieved
            raw_entries.append({"path": str(path), "sha256": sha(path), "code": code, "page": page})
        observation = observations.get(code)
        if symbol in prices:
            if observation is None:
                raise ValueError("Price output has no matching observation file for " + code)
            rows = pq.read_table(prices[symbol]).to_pylist()
            status = pq.read_table(observation, columns=["provider_trade_status"]).to_pylist()
            if not rows or len(rows) != sum(row["provider_trade_status"] == 1 for row in status):
                raise ValueError("Active rows disagree with status observations for " + symbol)
            dates = [str(row["trade_date"]) for row in rows]
            if dates != sorted(dates) or len(set(dates)) != len(dates):
                raise ValueError("Price dates are not unique and ascending for " + symbol)
            files.append({"symbol": symbol, "rows": len(rows), "start": dates[0], "end": dates[-1],
                          "output": str(prices[symbol]), "output_sha256": sha(prices[symbol]),
                          "price_basis": "unadjusted", "source_request": item,
                          "halted_observations": len(status) - len(rows), "historical_status_verified": False})
            observation_entries.append({"symbol": symbol, "rows": len(status), "path": str(observation),
                                        "sha256": sha(observation)})
            outcomes.append({**item, "status": "acquired_candidate", "rows": len(status), "active_price_rows": len(rows)})
        else:
            pages = captures.get(code, [])
            if not pages:
                detail = "Batch terminated before this request was sent"
            elif reply_security(json.loads(pages[0][1].read_text(encoding="utf-8"))) != code:
                detail = "Only an undecodable or misdirected wire reply is retained; the batch failure message was never written"
            else:
                detail = "Wire reply decoded but was never normalised; the batch failure message was never written"
            failure = {**item, "status": "unresolved", "error": detail,
                       "recovered_from_wire_capture": bool(pages)}
            errors.append(failure)
            outcomes.append(failure)

    if len(outcomes) != len(requests):
        raise ValueError("Outcome count differs from the request list")
    windows = [(item["start"], item["end"]) for item in requests]
    selections = {item["selection"] for item in requests}
    manifest = {
        "run_id": run.name, "output": str(run),
        "started_at": started_at if started_at else "",
        "provider": scalars.get("provider"), "client_version": scalars.get("client_version"),
        "endpoint": scalars.get("endpoint"),
        "mode": "retired" if selections == {"missing_retired_candidate"} else None,
        "price_basis": "unadjusted", "backtest_ready": False, "readiness_gaps": GAPS,
        "selection": "observed missing retired candidates; not a full historical universe",
        "start": min(start for start, _ in windows), "end": max(end for _, end in windows),
        "requested_symbols": len(requests), "total_candidates": len(requests),
        "offset": 0, "limit": None,
        "source_manifests": sources, "files": files, "observations": observation_entries,
        "raw_responses": raw_entries, "outcomes": outcomes, "errors": errors,
        "total_rows": sum(entry["rows"] for entry in files),
        "complete": not errors and len(outcomes) == len(requests),
        "recovered": {
            "recovered_at": datetime.now(timezone.utc).isoformat(),
            "reason": "the batch was terminated before this collector rewrote the manifest after every request, so the run kept its outputs but no inventory",
            "rebuilt_from": ["requests.json", "raw/*.json", "observations/*.parquet", "prices/*.parquet"],
            "inferred_fields": {
                "started_at": "earliest retained wire response timestamp; the true batch start is earlier",
                "start": "minimum requested window start in requests.json, not the original CLI argument",
                "end": "maximum requested window end in requests.json, not the original CLI argument",
                "limit": "the batch selected every retired candidate, so no limit is recorded",
            },
            "unrecoverable": "per-request failure messages for the batch final attempts were never written",
        },
    }
    (run / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="Interrupted run directory with requests.json but no manifest.json")
    parser.add_argument("--source-manifest", type=Path, nargs="+", required=True,
                        help="An intact manifest from the same acquisition series, used only for its source inventory")
    args = parser.parse_args(argv)
    manifest = recover(args.run, args.source_manifest)
    print(json.dumps({"manifest": str(Path(manifest["output"]) / "manifest.json"), "files": len(manifest["files"]),
                      "rows": manifest["total_rows"], "errors": len(manifest["errors"]),
                      "complete": manifest["complete"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
