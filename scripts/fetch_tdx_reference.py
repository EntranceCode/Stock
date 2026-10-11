"""Fetch public reference bars for provenance checks, without importing AlphaMaster."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
from fetch_http_reference import china_today, iso_date, new_snapshot_directory, save_json_once

PROJECT = Path(__file__).resolve().parents[1]
SERVERS = [("115.238.90.165", 7709), ("180.153.18.170", 7709), ("119.147.212.81", 7709)]
TARGETS = [(0, "000001"), (0, "000002"), (0, "300750"), (1, "600000"), (1, "600519")]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=iso_date, default=china_today())
    args = parser.parse_args(argv)
    from pytdx.hq import TdxHq_API

    output = new_snapshot_directory(PROJECT / "reports" / "source-verification", "tdx_")
    (output / "raw").mkdir()
    errors = []
    api = TdxHq_API(raise_exception=True)
    connected = None
    for host, port in SERVERS:
        try:
            if api.connect(host, port, time_out=3):
                connected = [host, port]
                break
        except Exception as exc:
            errors.append({"host": host, "error": str(exc)})
    result = {"run_id": output.name, "output": str(output), "as_of": args.as_of,
              "retrieved_at": datetime.now(timezone.utc).isoformat(), "server": connected,
              "source": "pytdx get_security_bars(category=9), unadjusted reference",
              "securities": {}, "errors": errors}
    if connected:
        try:
            for market, code in TARGETS:
                try:
                    bars = api.get_security_bars(9, market, code, 0, 800)
                    # Preserve this response even when the subsequent action request fails.
                    save_json_once(output / "raw" / f"{code}_daily.json", {
                        "market": market, "symbol": code, "server": connected, "as_of": args.as_of,
                        "retrieved_at": datetime.now(timezone.utc).isoformat(), "daily_bars": bars})
                    actions = api.get_xdxr_info(market, code)
                    save_json_once(output / "raw" / f"{code}_actions.json", {
                        "market": market, "symbol": code, "server": connected, "as_of": args.as_of,
                        "retrieved_at": datetime.now(timezone.utc).isoformat(), "corporate_actions": actions})
                    if not bars:
                        raise ValueError("No reference daily bars returned")
                    selected = [bar for bar in bars if f"{bar['year']:04}-{bar['month']:02}-{bar['day']:02}" <= args.as_of]
                    if not selected:
                        raise ValueError("No bars before as-of; endpoint returns only its latest 800 bars")
                    selected_actions = [action for action in (actions or [])
                                        if f"{action['year']:04}-{action['month']:02}-{action['day']:02}" <= args.as_of]
                    result["securities"][code] = {"market": market, "daily_bars": selected,
                                                 "corporate_actions": selected_actions}
                    print(code, "reference_bars", len(selected), "actions", len(selected_actions), flush=True)
                except Exception as exc:
                    errors.append({"symbol": code, "error": str(exc)})
        finally:
            api.disconnect()
    result["complete"] = bool(connected) and len(result["securities"]) == len(TARGETS)
    save_json_once(output / "tdx-reference.json", result)
    if not result["complete"]:
        raise RuntimeError(f"Reference download failed or incomplete: {errors}")
    print("Saved reference data", str(output / "tdx-reference.json"), flush=True)
    return output


if __name__ == "__main__":
    main()
