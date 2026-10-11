"""Save immutable public Tencent reference snapshots for local data verification."""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4
import requests

PROJECT = Path(__file__).resolve().parents[1]
URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
SYMBOLS = ["sz000001", "sz000002", "sz300750", "sh600000", "sh600519"]


def china_today() -> str:
    return datetime.now(timezone(timedelta(hours=8))).date().isoformat()


def iso_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("Expected YYYY-MM-DD")
    return value


def new_snapshot_directory(base: Path, prefix: str = "") -> Path:
    run = prefix + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = base / run
    output.mkdir(parents=True, exist_ok=False)
    return output


def save_json_once(path: Path, value: dict) -> None:
    """Fail on an existing evidence file, including accidental same-run retries."""
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)


def capture_http_response(path: Path, response, params: dict, as_of: str, provider: str):
    """Persist the full response before HTTP/JSON validation or row filtering."""
    evidence = {"params": params, "provider": provider, "as_of": as_of,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "status_code": response.status_code, "response_text": response.text}
    save_json_once(path, evidence)
    response.raise_for_status()
    return json.loads(evidence["response_text"]), evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    parser.add_argument("--start", type=iso_date, default="2026-06-01")
    parser.add_argument("--end", type=iso_date, default="2026-07-07")
    args = parser.parse_args(argv)
    if not args.start <= args.end <= args.as_of:
        raise ValueError("Reference window must satisfy start <= end <= as-of")
    output = new_snapshot_directory(PROJECT / "reports" / "source-verification", "http_")
    (output / "raw").mkdir()
    session = requests.Session()
    result = {"run_id": output.name, "output": str(output), "as_of": args.as_of,
              "retrieved_at": datetime.now(timezone.utc).isoformat(), "provider": "Tencent public quote service",
              "url": URL, "start": args.start, "end": args.end, "securities": {}, "errors": []}
    try:
        for symbol in SYMBOLS:
            variants = {}
            for adjustment in ("", "qfq"):
                key = "qfqday" if adjustment else "day"
                param = f"{symbol},day,{args.start},{args.end},80,{adjustment}"
                raw_path = output / "raw" / f"{symbol}_{key}.json"
                try:
                    response = session.get(URL, params={"param": param}, timeout=15)
                    payload, evidence = capture_http_response(raw_path, response, {"param": param}, args.as_of, URL)
                    bars = payload.get("data", {}).get(symbol, {}).get(key, [])
                    if payload.get("code") != 0 or not bars:
                        raise ValueError(f"No valid {key} reference: {payload.get('msg')}")
                    bars = [row for row in bars if args.start <= row[0] <= args.end]
                    if not bars:
                        raise ValueError("Reference window contains no bars")
                    variants[key] = {"param": param, "bars": bars, "source": str(raw_path),
                                     "source_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
                                     "retrieved_at": evidence["retrieved_at"]}
                    print(symbol, key, len(bars), flush=True)
                except Exception as exc:
                    result["errors"].append({"symbol": symbol, "adjustment": adjustment,
                                             "raw_response": str(raw_path) if raw_path.exists() else None,
                                             "error": str(exc)})
            result["securities"][symbol] = variants
    finally:
        session.close()
    result["complete"] = not result["errors"]
    save_json_once(output / "http-reference.json", result)
    print("Reference", str(output / "http-reference.json"), flush=True)
    if result["errors"]:
        raise RuntimeError(f"Incomplete reference snapshot: {result['errors']}")
    return output


if __name__ == "__main__":
    main()
