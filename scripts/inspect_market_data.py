"""Read-only local Parquet audit; outputs go to the project, never the data source."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
LOCAL_DEPS = PROJECT / ".deps" / "parquet"
if LOCAL_DEPS.exists():
    sys.path.append(str(LOCAL_DEPS))

import numpy as np
import pandas as pd
import pyarrow
import pyarrow.parquet as pq

FIELDS = ("time", "open", "high", "low", "close", "tick_volume")


def candidate_date(value: int | None) -> str | None:
    """Hypothesis only: value equals epoch seconds divided by 1000."""
    if value is None or not 315532 <= value <= 1893456:
        return None
    return datetime.fromtimestamp(value * 1000, timezone.utc).date().isoformat()


def audit(path: Path, root: Path) -> dict:
    frame = pq.read_table(path).to_pandas()
    missing = [field for field in FIELDS if field not in frame]
    result = {"file": str(path.relative_to(root)), "group": path.parent.name,
              "period": path.stem.rsplit("_", 1)[-1], "rows": len(frame),
              "missing_fields": ",".join(missing)}
    if missing:
        return result
    time = frame["time"]
    prices = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
    finite = np.isfinite(prices).all(axis=1)
    positive = (prices > 0).all(axis=1)
    tolerance = np.maximum(np.abs(prices).max(axis=1) * 2e-6, 1e-6)
    o, h, lo, c = prices.T
    bad_ohlc = finite & ((h + tolerance < np.maximum.reduce([o, lo, c])) |
                         (lo - tolerance > np.minimum.reduce([o, h, c])))
    volume = frame["tick_volume"]
    raw_min = int(time.min()) if time.notna().any() else None
    raw_max = int(time.max()) if time.notna().any() else None
    close = frame["close"].astype(float)
    jumps = close.pct_change(fill_method=None).abs() > 0.30
    result.update({
        "time_min_raw": raw_min, "time_max_raw": raw_max,
        "candidate_start": candidate_date(raw_min), "candidate_end": candidate_date(raw_max),
        "time_null_rows": int(time.isna().sum()),
        "duplicate_time_extra_rows": int(time.duplicated().sum()),
        "time_reversal_rows": int((time.diff() < 0).sum()),
        "nonfinite_price_rows": int((~finite).sum()),
        "nonpositive_price_rows": int((finite & ~positive).sum()),
        "ohlc_invalid_rows": int(bad_ohlc.sum()),
        "volume_null_rows": int(volume.isna().sum()),
        "negative_volume_rows": int((volume < 0).sum()),
        "zero_volume_rows": int((volume == 0).sum()),
        "int64_min_volume_rows": int((volume == np.iinfo(np.int64).min).sum()),
        "close_jump_gt_30pct_rows": int(jumps.sum()),
        "min_close": float(close.min()), "max_close": float(close.max()),
    })
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(r"D:\vscode\data\A股数据\parquet"))
    parser.add_argument("--output", type=Path, default=PROJECT / "reports" / "local-data-audit")
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output.resolve()
    if output == root or root in output.parents:
        raise ValueError("Audit output must be outside the source data directory")
    paths = sorted(root.rglob("*.parquet"))
    if not paths:
        raise ValueError(f"No Parquet files found: {root}")
    output.mkdir(parents=True, exist_ok=True)
    inventory = []
    errors = []
    schemas = Counter()
    codes = defaultdict(set)
    for i, path in enumerate(paths, 1):
        period = path.stem.rsplit("_", 1)[-1]
        code = path.stem.rsplit("_", 1)[0]
        codes[(path.parent.name, code)].add(period)
        try:
            meta = pq.ParquetFile(path).metadata
            schema = str(meta.schema.to_arrow_schema().remove_metadata())
            schemas[schema] += 1
            inventory.append({"file": str(path.relative_to(root)), "group": path.parent.name,
                              "period": period, "bytes": path.stat().st_size,
                              "rows": meta.num_rows, "row_groups": meta.num_row_groups})
        except Exception as exc:
            errors.append({"file": str(path.relative_to(root)), "stage": "metadata", "error": str(exc)})
        if i % 4000 == 0:
            print(f"Metadata {i}/{len(paths)}", flush=True)
    pd.DataFrame(inventory).to_csv(output / "file-inventory.csv", index=False, encoding="utf-8-sig")

    daily_paths = [path for path in paths if path.stem.endswith("_daily")]
    daily = []
    for i, path in enumerate(daily_paths, 1):
        try:
            daily.append(audit(path, root))
        except Exception as exc:
            errors.append({"file": str(path.relative_to(root)), "stage": "daily_rows", "error": str(exc)})
        if i % 1000 == 0:
            print(f"Daily rows {i}/{len(daily_paths)}", flush=True)
    df = pd.DataFrame(daily)
    df.to_csv(output / "daily-file-quality.csv", index=False, encoding="utf-8-sig")

    # Deterministic sampling across groups; this is not a complete intraday row audit.
    intraday_paths = []
    for group in ("stocks", "indices"):
        for period in ("5min", "15min", "60min"):
            candidates = [p for p in paths if p.parent.name == group and p.stem.endswith("_" + period)]
            selected = np.linspace(0, len(candidates) - 1, min(10, len(candidates)), dtype=int) if candidates else []
            intraday_paths.extend(candidates[int(i)] for i in selected)
    intraday = []
    for path in intraday_paths:
        try:
            intraday.append(audit(path, root))
        except Exception as exc:
            errors.append({"file": str(path.relative_to(root)), "stage": "intraday_sample", "error": str(exc)})
    pd.DataFrame(intraday).to_csv(output / "intraday-sample-quality.csv", index=False, encoding="utf-8-sig")

    metrics = ["time_null_rows", "duplicate_time_extra_rows", "time_reversal_rows",
               "nonfinite_price_rows", "nonpositive_price_rows", "ohlc_invalid_rows",
               "volume_null_rows", "negative_volume_rows", "zero_volume_rows",
               "int64_min_volume_rows", "close_jump_gt_30pct_rows"]
    by_group = {}
    for group, sub in df.groupby("group"):
        end_counts = Counter(sub["candidate_end"].dropna())
        starts = sorted(sub["candidate_start"].dropna())
        ends = sorted(sub["candidate_end"].dropna())
        by_group[group] = {
            "files": len(sub), "rows": int(sub["rows"].sum()),
            "candidate_earliest": starts[0] if starts else None,
            "candidate_latest": ends[-1] if ends else None,
            "candidate_end_counts": dict(end_counts.most_common(12)),
            "metric_rows": {m: int(sub[m].sum()) for m in metrics},
            "metric_files": {m: int((sub[m] > 0).sum()) for m in metrics},
        }
    summary = {
        "audit_date": "2026-10-07", "source": str(root),
        "python": sys.version.split()[0], "pyarrow": pyarrow.__version__,
        "pandas": pd.__version__, "numpy": np.__version__,
        "file_count": len(paths), "bytes": sum(p.stat().st_size for p in paths),
        "metadata_rows": sum(x["rows"] for x in inventory),
        "counts_by_group_period": dict(Counter(f'{x["group"]}/{x["period"]}' for x in inventory)),
        "security_counts": dict(Counter(group for group, code in codes)),
        "schemas": dict(schemas), "daily_by_group": by_group,
        "intraday_sample_files": len(intraday),
        "intraday_sample_metrics": {m: sum(x.get(m, 0) for x in intraday) for m in metrics},
        "missing_daily": [f"{group}/{code}" for (group, code), periods in codes.items() if "daily" not in periods],
        "date_decoding": "Unverified hypothesis: time * 1000 as epoch seconds, UTC calendar date; source timezone and precision unknown.",
        "errors": errors,
    }
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True, indent=2), flush=True)


if __name__ == "__main__":
    main()
