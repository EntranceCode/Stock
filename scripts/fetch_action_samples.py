"""Save public dividend/split source snapshots; event semantics remain under review."""
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
from uuid import uuid4
import requests
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
STOCKS = ["000001", "000002", "300750", "600000", "600519"]
ETFS = ["510050", "510300", "510500", "510880", "159915", "511010"]


def main():
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = PROJECT / "data" / "external" / "actions" / run
    output.mkdir(parents=True)
    manifest = {"run_id": run, "output": str(output), "backtest_ready": False,
                "readiness_gaps": ["event_units_and_payment_dates", "original_announcements", "full_stock_coverage"],
                "stocks": [], "etfs": [], "errors": []}
    session = requests.Session()
    for symbol in STOCKS:
        params = {"reportName": "RPT_SHAREBONUS_DET", "columns": "ALL", "filter": f'(SECURITY_CODE="{symbol}")',
                  "pageSize": 500, "pageNumber": 1, "sortColumns": "REPORT_DATE", "sortTypes": -1,
                  "source": "WEB", "client": "WEB"}
        try:
            response = session.get(URL, params=params, timeout=15)
            response.raise_for_status()
            payload = response.json()
            (output / f"stock_{symbol}.json").write_text(json.dumps({"params": params, "payload": payload}, ensure_ascii=False, indent=2), encoding="utf-8")
            result = payload.get("result") or {}
            rows = result.get("data") or []
            if not payload.get("success") or not rows or result.get("count", len(rows)) > len(rows):
                raise ValueError("Action source empty, failed or truncated")
            manifest["stocks"].append({"symbol": symbol, "rows": len(rows), "columns": list(rows[0])})
            print(symbol, "stock_action_records", len(rows), "columns", list(rows[0]), flush=True)
        except Exception as exc:
            manifest["errors"].append({"symbol": symbol, "error": str(exc)})
    for symbol in ETFS:
        url = f"https://fundf10.eastmoney.com/fhsp_{symbol}.html"
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            (output / f"etf_{symbol}.html").write_text(response.text, encoding="utf-8")
            tables = pd.read_html(StringIO(response.text))
            relevant = [table for table in tables if any("除息" in str(c) or "折算" in str(c) for c in table.columns)]
            if not relevant:
                raise ValueError("Dividend/split table structure not recognized")
            entries = []
            for i, table in enumerate(relevant):
                csv = output / f"etf_{symbol}_events_{i}.csv"
                table.to_csv(csv, index=False, encoding="utf-8-sig")
                entries.append({"table": i, "rows": len(table), "columns": [str(c) for c in table.columns], "path": str(csv)})
            manifest["etfs"].append({"symbol": symbol, "provider": url, "tables": entries})
            print(symbol, "ETF_event_tables", len(entries), flush=True)
        except Exception as exc:
            manifest["errors"].append({"symbol": symbol, "error": str(exc)})
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Output", str(output), "errors", len(manifest["errors"]), flush=True)
    if manifest["errors"]:
        raise RuntimeError("Some action sources failed; consult saved raw snapshots")


if __name__ == "__main__":
    main()
