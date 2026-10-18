"""Build a candidate point-in-time snapshot series from registered evidence.

The builder never opens the admission gate: it records the registry state at
build time, writes bundles that carry every declared assumption as a readiness
gap, and stops. Symbols whose contract-mandated fields cannot be produced from
evidence are excluded with a recorded reason; that is the normal outcome while
the §8.7 field gaps are open.

Usage (from the project root)::

    python -B scripts/build_snapshot_series.py --start 2024-01-02 --end 2024-01-31 \
        --adjusted data/external/adjusted_prices/<version> \
        --enriched data/external/daily_bar_enriched/<version> \
        --max-symbols 10 \
        --assume session_close_availability --assume buy_sell_equal_to_tradable \
        --assume turnover_provider_reported --assume lot_size_conventional_default \
        --assume settlement_conventional_default

Without any --assume the builder still runs and reports exactly which fields
block each symbol, which is the honest quantification of the open gaps.
"""
from __future__ import annotations

import argparse
from datetime import date, time
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from hybrid_quant.snapshot_series import ASSUMPTIONS, SeriesError, SeriesRequest, build_series

DEFAULT_OUTPUT = PROJECT / "data/external/snapshot_series"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, required=True, help="窗口首日 YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, required=True, help="窗口末日 YYYY-MM-DD")
    parser.add_argument("--adjusted", type=Path, required=True, help="后复权价格产品目录（含 manifest.json）")
    parser.add_argument("--enriched", type=Path, required=True, help="富化日线产品目录（含 manifest.json）")
    parser.add_argument("--factors", type=Path, default=None,
                        help="因子产品目录（build_factors.py 输出）；提供后载荷携带时点因子观测")
    parser.add_argument("--etf", type=Path, default=None,
                        help="ETF 证据产品目录（build_etf_evidence 输出）；提供后序列包含 ETF 证券")
    parser.add_argument("--calendar", type=Path, default=None,
                        help="交易日历 Parquet；默认取注册表 sse_calendar")
    parser.add_argument("--symbols", default="", help="逗号分隔的证券代码；默认全部")
    parser.add_argument("--max-symbols", type=int, default=None, help="按代码排序取前 N 只")
    parser.add_argument("--assume", action="append", default=[], choices=sorted(ASSUMPTIONS),
                        help="声明的推导假设；每加一项就在每个 bundle 登记一条 readiness gap，可重复")
    parser.add_argument("--decision-time", default="16:30", help="决策时点 HH:MM（默认 16:30，+08:00）")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="输出根目录")
    parser.add_argument("--no-verify-hashes", action="store_true", help="跳过已登记文件哈希校验")
    args = parser.parse_args(argv)

    symbols = tuple(sorted({item.strip() for item in args.symbols.split(",") if item.strip()}))
    hour, minute = (int(part) for part in args.decision_time.split(":"))
    request = SeriesRequest(
        start=args.start, end=args.end,
        adjusted_dir=args.adjusted, enriched_dir=args.enriched,
        calendar_path=args.calendar, symbols=symbols, max_symbols=args.max_symbols,
        assume=tuple(sorted(set(args.assume))),
        decision_time=time(hour, minute),
        verify_hashes=not args.no_verify_hashes,
        factors_dir=args.factors,
        etf_dir=args.etf,
    )
    try:
        index = build_series(PROJECT, request, args.output)
    except (SeriesError, OSError, ValueError) as exc:
        print(f"系统拒绝构建：{exc}", file=sys.stderr)
        return 2
    print(json.dumps({
        "series_id": index["series_id"],
        "output": str(Path(args.output).resolve() / index["series_id"]),
        "sessions": len(index["sessions"]),
        "symbols_included": len(index["symbols"]["included"]),
        "symbols_excluded": index["symbols"]["excluded_count"],
        "excluded_reasons": index["symbols"]["excluded_reasons"],
        "readiness_gaps": index["readiness_gaps"],
        "historical_data_admitted": index["historical_data_admitted"],
        "self_check": index["self_check"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
