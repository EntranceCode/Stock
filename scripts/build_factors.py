"""Build point-in-time quality/value factor observations from registered evidence.

Reads the registered financial snapshot and daily prices plus the named
availability layer, and writes one Parquet file per symbol with a manifest.
Each observation carries ``original_version_verified`` decided per row from the
availability layer's update classification (design D2-b): the classes explained by
a next-year comparison or by the row never being revised are treated as verified,
and every other class stays unverified so the engine keeps excluding it. Missing
components produce no score and an explicit reason instead of an estimate.

Usage (from the project root)::

    python -B scripts/build_factors.py \
        --availability data/external/financial_availability/<version> \
        --max-symbols 50

Output lands in ``data/external/factors/<run_id>``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from hybrid_quant.factors import FactorError, FactorRequest, build_factors

DEFAULT_OUTPUT = PROJECT / "data/external/factors"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--availability", type=Path, required=True,
                        help="财务可用时间层目录（含 manifest.json）")
    parser.add_argument("--symbols", default="", help="逗号分隔的证券代码；默认全部")
    parser.add_argument("--max-symbols", type=int, default=None, help="按代码排序取前 N 只")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="输出根目录")
    parser.add_argument("--no-verify-hashes", action="store_true", help="跳过已登记文件哈希校验")
    args = parser.parse_args(argv)

    symbols = tuple(sorted({item.strip() for item in args.symbols.split(",") if item.strip()}))
    request = FactorRequest(availability_dir=args.availability, symbols=symbols,
                            max_symbols=args.max_symbols,
                            verify_hashes=not args.no_verify_hashes)
    try:
        index = build_factors(PROJECT, request, args.output)
    except (FactorError, OSError, ValueError) as exc:
        print(f"系统拒绝构建：{exc}", file=sys.stderr)
        return 2
    print(json.dumps({
        "run_id": index["run_id"],
        "output": str(Path(args.output).resolve() / index["run_id"]),
        "symbols": index["symbols"],
        "observations": index["observations"],
        "quality_known": index["quality_known"],
        "value_known": index["value_known"],
        "requested_symbols_not_found": index["requested_symbols_not_found"],
        "exclusion_reasons": index["exclusion_reasons"],
        "readiness_gaps": index["readiness_gaps"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
