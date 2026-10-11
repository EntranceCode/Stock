"""Show acquisition coverage separately from permission to enter a backtest."""
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


def main():
    config = json.loads((PROJECT / "config" / "datasets.json").read_text(encoding="utf-8"))
    statuses = {}
    for name, entry in config["datasets"].items():
        path = (PROJECT / entry["manifest"]).resolve()
        if not path.is_relative_to(PROJECT):
            raise ValueError("Dataset manifests must stay inside the project")
        if not path.exists():
            statuses[name] = {"state": "manifest_missing_or_acquisition_in_progress", "backtest_ready": False}
            continue
        manifest = json.loads(path.read_text(encoding="utf-8"))
        files = manifest.get("files", [])
        rows = manifest.get("normalized_rows", manifest.get("rows", sum(item.get("rows", 0) for item in files)))
        file_count = len(files) if "files" in manifest else int((path.parent / "events.parquet").exists())
        statuses[name] = {"manifest": str(path), "state": "downloaded_or_staged", "file_count": file_count,
                          "snapshot_cutoff": manifest.get("as_of"),
                          "rows": rows, "backtest_ready": manifest.get("backtest_ready", False),
                          "readiness_gaps": manifest.get("readiness_gaps", []),
                          "errors": len(manifest.get("errors", [])),
                          "source_download_errors": len(manifest.get("source_download_errors", [])),
                          "source_error_history": len(manifest.get("source_error_history", [])),
                          "unknown_announcement_rows": sum(item.get("unknown_announcement_rows", 0) for item in files),
                          "files_with_unknown_source_retrieval_time": sum(item.get("source_retrieval_time_unknown", False) for item in files),
                          "quarantined_events": manifest.get("quarantined_events", 0)}
        if any("dataset" in item for item in files):
            statuses[name]["rows_by_dataset"] = {item["dataset"]: item.get("rows", 0) for item in files if "dataset" in item}
        if "coverage" in manifest:
            statuses[name]["coverage"] = manifest["coverage"]
        if "scheduled_open_days" in manifest:
            statuses[name]["scheduled_open_days"] = manifest["scheduled_open_days"]
            statuses[name]["observed_comparison_end"] = manifest.get("observed_comparison_end")
            statuses[name]["all_sse_etf_comparisons_exact"] = manifest.get("all_sse_etf_comparisons_exact", False)
    report = {"registry_baseline_as_of": config["as_of"], "registry_updated_on": config.get("updated_on"),
              "evaluated_at": datetime.now(timezone.utc).isoformat(), "datasets": statuses,
              "mixed_strategy_backtest_ready": False,
              "remaining": ["delisted_and_full_historical_universe_prices", "complete_exchange_calendars",
                            "original_financial_versions", "full_corporate_actions_and_payments",
                            "historical_security_status_industry_and_etf_constituents", "accurate_turnover_and_historical_rules"]}
    output = PROJECT / "reports" / "data-readiness.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
