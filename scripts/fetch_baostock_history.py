"""Capture public BaoStock daily history, wire responses, and provider status.

Unadjusted observations and provider flags remain staging evidence. No missing
session is fabricated, no listing boundary is inferred, and no history is
admitted to the strategy engine by this collector.

The batch manifest is rewritten after every request, so a terminated process
still leaves an inventory of what it actually saved. Each decoded reply is also
checked against the requested security, schema and window before its rows are
used, because a socket timeout can hand back the previous reply.
"""
from __future__ import annotations

import argparse
import base64
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import socket
import sys
import time
from uuid import uuid4

PROJECT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT / ".deps/parquet"))
sys.path.append(str(PROJECT / ".deps/baostock"))
FIELDS = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
GAPS = ["full_historical_universe_and_lifecycle_boundaries", "original_corporate_actions_and_total_returns",
        "historical_price_limits_and_execution_eligibility", "provider_status_independent_verification",
        "point_in_time_availability_and_historical_industry"]


def now():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_once(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)


def write_manifest(path, manifest):
    """Rewrite this run's own manifest so an interrupted batch keeps its inventory.

    The run directory is unique per batch, so this only replaces the current
    run's file; evidence recorded by earlier batches is never touched.
    """
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2, allow_nan=False)
    temporary.replace(path)


def reply_matches(response, request):
    """Check a decoded reply against the security, schema and window requested.

    A socket timeout can leave a late reply in the buffer, so the SDK can hand
    back the previous security's payload. Accepting it would attach another
    security's rows to this request.
    """
    if not isinstance(response, str) or "{" not in response:
        return False
    body = response[response.index("{"):]
    try:
        _, end = json.JSONDecoder().raw_decode(body)
    except ValueError:
        return False
    tail = body[end:].split("\1")
    return tail[1:7] == [request["code"], FIELDS, request["start"], request["end"], "d", "3"]


def number(value, field, nullable=True):
    if value == "" and nullable:
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"Non-finite {field}")
    return parsed


def normalize_rows(fields, rows, code, start, end):
    """Keep explicit suspension observations; reject malformed active OHLC."""
    if fields != FIELDS.split(","):
        raise ValueError("Provider returned unexpected fields/order")
    output, last = [], None
    for index, row in enumerate(rows, 1):
        if len(row) != len(fields):
            raise ValueError("Provider row width differs from fields")
        item = dict(zip(fields, row))
        day = date.fromisoformat(item["date"])
        if day.isoformat() != item["date"] or not start <= item["date"] <= end:
            raise ValueError("Date outside requested window")
        if last is not None and day <= last:
            raise ValueError("Dates must be unique and ascending")
        last = day
        if item["code"] != code or item["adjustflag"] != "3":
            raise ValueError("Unexpected security or adjustment")
        if item["tradestatus"] not in ("0", "1") or item["isST"] not in ("", "0", "1"):
            raise ValueError("Unknown provider status encoding")
        prices = {key: number(item[key], key) for key in ("open", "high", "low", "close", "preclose")}
        for key, value in prices.items():
            if value is not None and value < 0:
                raise ValueError(f"Negative {key}")
        active = item["tradestatus"] == "1"
        o, h, lo, c = (prices[key] for key in ("open", "high", "low", "close"))
        if active and (any(value is None or value <= 0 for value in (o, h, lo, c)) or h < max(o, lo, c) or lo > min(o, h, c)):
            raise ValueError("Invalid active-day OHLC")
        volume, amount = number(item["volume"], "volume"), number(item["amount"], "amount")
        if any(value is not None and value < 0 for value in (volume, amount)):
            raise ValueError("Negative volume or turnover")
        if volume is not None and not volume.is_integer():
            raise ValueError("Provider share volume must be integer")
        if active and (volume is None or amount is None):
            raise ValueError("Active day lacks explicit volume/turnover")
        output.append({"trade_date": day, "symbol": code[3:], "exchange": "SSE" if code.startswith("sh.") else "SZSE",
                       **prices, "volume_shares_reported": int(volume) if volume is not None else None,
                       "turnover_cny_reported": amount, "provider_trade_status": int(item["tradestatus"]),
                       "provider_is_st": int(item["isST"]) if item["isST"] else None,
                       "turn_percent_reported": number(item["turn"], "turn"),
                       "pct_change_percent_reported": number(item["pctChg"], "pctChg"),
                       "source_row": index, "adjusted_close": None, "available_at": None,
                       "tradable": None, "buyable": None, "sellable": None})
    return output


class WireSocket:
    def __init__(self, connection):
        self.connection, self.sent, self.received = connection, [], []

    def send(self, value):
        count = self.connection.send(value)
        self.sent.append(value[:count])
        return count

    def recv(self, count):
        value = self.connection.recv(count)
        if not value:
            raise ConnectionError("Provider closed the connection before a complete reply")
        self.received.append(value)
        return value

    def __getattr__(self, key):
        return getattr(self.connection, key)


def requests_for(mode, start, end):
    registry = json.loads((PROJECT / "config/datasets.json").read_text(encoding="utf-8"))
    stock_path = PROJECT / registry["datasets"]["stock_daily"]["manifest"]
    requirements_path = PROJECT / "reports/missing-retired-prices.json"
    requirements = json.loads(requirements_path.read_text(encoding="utf-8"))
    sources = [stock_path, requirements_path]
    items = {}
    if mode != "etf":
        for item in requirements["requirements"]:
            records = item["candidate_records"]
            exchange = {record["exchange"] for record in records}
            if len(exchange) != 1:
                raise ValueError("Ambiguous exchange requires separate listing review")
            code = ("sh." if next(iter(exchange)) == "SSE" else "sz.") + item["symbol"]
            windows = [record["acquisition_window_candidate"] for record in records]
            a, b = max(start, min(w[0] for w in windows)), min(end, max(w[1] for w in windows))
            if a <= b:
                items[code] = {"code": code, "start": a, "end": b, "selection": "missing_retired_candidate",
                               "multiple_listing_records_require_review": len(records) != 1,
                               "candidate_records": records}
    if mode == "all":
        stock = json.loads(stock_path.read_text(encoding="utf-8"))
        for entry in stock["files"]:
            symbol = entry["symbol"]
            code = ("sh." if symbol.startswith(("60", "68")) else "sz.") + symbol
            items.setdefault(code, {"code": code, "start": start, "end": end, "selection": "registered_local_stock"})
    if mode == "etf":
        # The provider security-basic snapshot is the ETF universe evidence; the
        # window keeps ETFs listed no later than the window end and not
        # terminated before its start, so a terminated fund still contributes
        # the sessions it traded.
        import pyarrow.parquet as pq
        universe_dir = sorted((PROJECT / "data/external/etf_universe").iterdir())[-1]
        universe = json.loads((universe_dir / "manifest.json").read_text(encoding="utf-8"))
        sources.append(universe_dir / "manifest.json")
        table = pq.read_table(universe_dir / "etf_universe.parquet").to_pylist()
        for row in table:
            listed, terminated = row["ipo_date"], row["out_date"]
            if listed > end or (terminated is not None and terminated < start):
                continue
            code = row["code"]
            a, b = max(start, listed), min(end, terminated or end)
            items[code] = {"code": code, "start": a, "end": b, "selection": "provider_etf_universe",
                           "provider_name": row["code_name"]}
    return [items[key] for key in sorted(items)], sources


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("retired", "all", "etf"), default="retired")
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="2026-07-07")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--pause", type=float, default=0.15)
    parser.add_argument("--max-consecutive-failures", type=int, default=5,
                        help="stop the batch after this many unresolved symbols in a row")
    args = parser.parse_args(argv)
    if date.fromisoformat(args.start) > date.fromisoformat(args.end) or date.fromisoformat(args.end) > datetime.now(timezone.utc).date():
        raise ValueError("Invalid history interval")
    if args.offset < 0 or args.limit is not None and args.limit <= 0 or not 0.05 <= args.pause <= 10 or args.max_consecutive_failures < 1:
        raise ValueError("Invalid batch controls")
    items, sources = requests_for(args.mode, args.start, args.end)
    selected = items[args.offset:args.offset + args.limit if args.limit else None]
    if not selected:
        raise ValueError("Empty acquisition batch")
    output = PROJECT / "data/external/baostock_history" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8])
    (output / "raw").mkdir(parents=True)
    (output / "observations").mkdir()
    (output / "prices").mkdir()
    manifest = {"run_id": output.name, "output": str(output), "started_at": now(), "provider": "https://www.baostock.com/",
                "client_version": "0.9.4", "endpoint": "public-api.baostock.com:10030", "mode": args.mode,
                "price_basis": "unadjusted", "backtest_ready": False, "readiness_gaps": GAPS,
                "selection": "registered local universe and observed missing retired candidates; not a full historical universe",
                "start": args.start, "end": args.end, "requested_symbols": len(selected), "total_candidates": len(items),
                "offset": args.offset, "limit": args.limit, "source_manifests": [{"path": str(p), "sha256": sha(p)} for p in sources],
                "files": [], "observations": [], "raw_responses": [], "outcomes": [], "errors": []}
    save_once(output / "requests.json", selected)
    import baostock as bs
    import baostock.common.context as context
    import baostock.util.socketutil as sockets
    import pyarrow as pa
    import pyarrow.parquet as pq
    # The login handshake must not inherit the query timeout: the provider's
    # login server can take far longer than a query under load, and a socket
    # timeout there surfaces as a spurious network-receive failure. The timeout
    # is applied after login, where it guards against stale replies.
    login = bs.login()
    if login.error_code != "0":
        manifest["errors"].append({"stage": "login", "code": login.error_code, "message": login.error_msg})
        save_once(output / "manifest.json", manifest)
        raise RuntimeError("Public BaoStock login failed; original failure evidence retained")
    # The live connection was opened during login, so the default timeout set
    # here cannot reach it: setdefaulttimeout applies only to sockets created
    # afterwards. A stalled reply on the login-time socket blocks forever,
    # which was observed as a 21-hour hang with the manifest unchanged. The
    # timeout is therefore applied to the connection itself as well. It must
    # stay well above a healthy query: the provider answers a full-window
    # query in 13-15 seconds under load, and a timeout that tight turns one
    # slow reply into a late reply that the next query then rejects as a
    # mismatched wire answer, burning consecutive failures in bursts.
    socket.setdefaulttimeout(60)
    context.default_socket.settimeout(60)
    wire = WireSocket(context.default_socket)
    context.default_socket = wire
    original_send = sockets.send_msg
    active = {"request": None, "page": 0, "failed": False, "failure": None}

    def capture_send(message):
        wire.sent, wire.received = [], []
        response = original_send(message)
        if active["request"] is not None:
            active["page"] += 1
            path = output / "raw" / f"{active['request']['code']}_{active['page']:03d}.json"
            save_once(path, {"provider": manifest["provider"], "endpoint": manifest["endpoint"], "client_version": "0.9.4",
                             "params": {**active["request"], "fields": FIELDS, "frequency": "d", "adjustflag": "3"},
                             "retrieved_at": now(), "wire_request_base64": base64.b64encode(b"".join(wire.sent)).decode(),
                             "wire_response_base64": base64.b64encode(b"".join(wire.received)).decode(),
                             "decoded_protocol_response": response,
                             "format": "complete wire bytes plus SDK-decoded protocol response before row parsing"})
            manifest["raw_responses"].append({"path": str(path), "sha256": sha(path), "code": active["request"]["code"], "page": active["page"]})
            if not reply_matches(response, active["request"]):
                active["failed"] = True
                active["failure"] = "Provider reply does not match the requested security, schema and window"
        return response

    sockets.send_msg = capture_send
    consecutive_failures = 0
    try:
        for index, item in enumerate(selected, 1):
            active.update(request=item, page=0, failed=False, failure=None)
            try:
                result = bs.query_history_k_data_plus(item["code"], FIELDS, start_date=item["start"], end_date=item["end"], frequency="d", adjustflag="3")
                rows = []
                while result.error_code == "0" and result.next():
                    rows.append(result.get_row_data())
                if result.error_code != "0" or active["failed"]:
                    reason = active["failure"] or f"{result.error_code} {result.error_msg}"
                    raise ValueError(f"Provider query failed: {reason}")
                if not rows:
                    raise ValueError("Empty history is unresolved, not proof of no trading")
                normalized = normalize_rows(result.fields, rows, item["code"], item["start"], item["end"])
                table = pa.Table.from_pylist(normalized)
                metadata = {"provider": manifest["provider"], "price_basis": "unadjusted", "backtest_ready": False,
                            "volume_unit": "provider_documented_shares", "amount_unit": "provider_documented_CNY",
                            "available_at_unknown": True, "readiness_gaps": GAPS,
                            "source_responses": [r for r in manifest["raw_responses"] if r["code"] == item["code"]]}
                table = table.replace_schema_metadata({b"stock.provenance": json.dumps(metadata).encode()})
                observation = output / "observations" / f"{item['code']}_observations.parquet"
                pq.write_table(table, observation, compression="zstd")
                manifest["observations"].append({"symbol": item["code"][3:], "rows": len(normalized), "path": str(observation), "sha256": sha(observation)})
                prices = [{**record, "volume_raw": record["volume_shares_reported"], "turnover": record["turnover_cny_reported"]}
                          for record in normalized if record["provider_trade_status"] == 1]
                if not prices:
                    raise ValueError("No active price observations; status evidence preserved")
                price_path = output / "prices" / f"{item['code'][3:]}_daily.parquet"
                pq.write_table(pa.Table.from_pylist(prices).replace_schema_metadata({b"stock.provenance": json.dumps(metadata).encode()}), price_path, compression="zstd")
                manifest["files"].append({"symbol": item["code"][3:], "rows": len(prices), "start": str(prices[0]["trade_date"]),
                                          "end": str(prices[-1]["trade_date"]), "output": str(price_path), "output_sha256": sha(price_path),
                                          "price_basis": "unadjusted", "source_request": item,
                                          "halted_observations": len(normalized) - len(prices), "historical_status_verified": False})
                manifest["outcomes"].append({**item, "status": "acquired_candidate", "rows": len(normalized), "active_price_rows": len(prices)})
                consecutive_failures = 0
                print(f"{index}/{len(selected)} {item['code']} rows={len(normalized)} active={len(prices)}", flush=True)
            except Exception as exc:
                consecutive_failures += 1
                failure = {**item, "status": "unresolved", "error": str(exc)}
                manifest["errors"].append(failure)
                manifest["outcomes"].append(failure)
                print(f"{index}/{len(selected)} {item['code']} unresolved: {exc}", flush=True)
                if consecutive_failures >= args.max_consecutive_failures:
                    manifest["stopped_after_consecutive_failures"] = True
                    break
            finally:
                active["request"] = None
                (output / "progress.json").write_text(json.dumps({"completed": len(manifest["outcomes"]), "requested": len(selected), "saved_price_files": len(manifest["files"]), "errors": len(manifest["errors"]), "updated_at": now()}), encoding="utf-8")
                write_manifest(output / "manifest.json", manifest)
            time.sleep(args.pause)
    finally:
        active["request"] = None
        sockets.send_msg = original_send
        bs.logout()
        manifest.update(finished_at=now(), total_rows=sum(item["rows"] for item in manifest["files"]),
                        complete=len(manifest["outcomes"]) == len(selected) and not manifest["errors"])
        write_manifest(output / "manifest.json", manifest)
        print(json.dumps({"manifest": str(output / "manifest.json"), "files": len(manifest["files"]), "rows": manifest["total_rows"], "errors": len(manifest["errors"]), "complete": manifest["complete"]}), flush=True)
    return 0 if manifest["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
