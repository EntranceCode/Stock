"""Local commands for the independent research system."""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4

from . import __version__
from .data_registry import DataNotReady, DatasetRegistry

PROJECT = Path(__file__).resolve().parents[1]


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return float(value)
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Unsupported output type: {type(value).__name__}")


def write_json(path: Path, value) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=json_value, allow_nan=False)


def registry_status(args) -> int:
    registry = DatasetRegistry(args.registry, PROJECT)
    statuses = registry.statuses()
    try:
        registry.validate_for_backtest()
        ready, reason = True, None
    except DataNotReady as exc:
        ready, reason = False, str(exc)
    # The printed readiness is the computed §5.2 conjunction behind the gate:
    # per-criterion results with the evidence each answer came from.
    readiness = registry.readiness()
    report = {"project": str(PROJECT), "version": __version__,
              "independent_system": True, "datasets": [asdict(item) for item in statuses],
              "historical_data_admitted": ready, "admission_reason": reason,
              "readiness": readiness.as_dict()}
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if args.command == "status" or ready else 2


def create_demo_report(config_path: Path, output_root: Path) -> dict:
    from .backtest import run_backtest
    from .demo import synthetic_snapshots
    from .models import FeeConfig, RiskConfig
    source = config_path.resolve()
    config_bytes = source.read_bytes()
    config = json.loads(config_bytes.decode("utf-8"))
    snapshots = synthetic_snapshots(config)
    result = run_backtest(snapshots, initial_cash=config["initial_cash"],
                          fee_config=FeeConfig(**config["fees"]), risk_config=RiskConfig(**config["risk"]),
                          budgets=config["budgets"])
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    output = output_root.resolve() / run_id
    if not output.is_relative_to(PROJECT):
        raise ValueError("Demo reports must stay inside this project")
    output.mkdir(parents=True)
    write_json(output / "config.json", config)
    for name in ("orders", "fills", "execution_events", "dividend_events", "decisions"):
        with (output / f"{name}.jsonl").open("x", encoding="utf-8") as stream:
            for record in result.get(name, []):
                stream.write(json.dumps(record, ensure_ascii=False, default=json_value, allow_nan=False) + "\n")
    equity = result["equity"]
    with (output / "equity.csv").open("x", newline="", encoding="utf-8") as stream:
        fields = ("trade_date", "cash", "receivables", "nav", "holdings", "stale_symbols", "drawdown", "actual_limit_violations")
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for record in equity:
            row = dict(record)
            for field in ("holdings", "stale_symbols", "actual_limit_violations"):
                row[field] = json.dumps(row.get(field), ensure_ascii=False)
            writer.writerow(row)
    package = Path(__file__).resolve().parent
    manifest = {
        "run_id": run_id, "generated_at": datetime.now(timezone.utc).isoformat(), "system_version": __version__,
        "purpose": "synthetic_engineering_demo", "validation_only": True, "investment_performance": False,
        "uses_alpha_master_runtime": False, "historical_data_admitted": False,
        "config_source": str(source), "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "code_hashes": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(package.glob("*.py"))},
        "evaluation_sessions": len(equity), "orders": len(result.get("orders", [])), "fills": len(result.get("fills", [])),
        "assumptions": config["assumptions"], "limitations": result.get("limitations", []),
    }
    report = f"""# 独立混合量化系统：合成数据工程演示

运行ID：{run_id}；系统版本：{__version__}。

已执行{len(equity)}个合成交易日，生成{manifest['orders']}笔订单意向、{manifest['fills']}笔模拟成交。输出包含逐日账户、策略/风控决策、订单、成交与拒绝原因。

该场景只检查程序链路。证券、行情、因子、交易日历、费用和交易规则均为合成设定，账户数值不能作为投资收益或策略有效性的证据。没有使用AlphaMaster运行代码，也没有连接券商。

配置、输出文件和代码哈希保存在manifest.json。真实历史数据仍须独立通过准入检查。当前实现范围与待办见docs/development-status.md。
"""
    (output / "report.md").write_text(report, encoding="utf-8")
    manifest["files"] = [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                         for path in sorted(output.iterdir()) if path.is_file()]
    write_json(output / "manifest.json", manifest)
    return {"run_id": run_id, "purpose": manifest["purpose"], "validation_only": True, "output": str(output),
            "evaluation_sessions": len(equity), "orders": manifest["orders"], "fills": manifest["fills"]}


def run_demo(args) -> int:
    print(json.dumps(create_demo_report(args.config, args.output_root), ensure_ascii=False, indent=2))
    return 0


def serve_dashboard(args) -> int:
    from .server import serve
    serve(PROJECT, port=args.port)
    return 0


def main(argv=None) -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="独立的A股个股与ETF混合策略研究系统")
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("status", "check-data"):
        sub = subparsers.add_parser(command, help="显示数据状态" if command == "status" else "验证真实数据是否具备正式回测资格")
        sub.add_argument("--registry", type=Path, default=PROJECT / "config/datasets.json")
        sub.set_defaults(handler=registry_status)
    sub = subparsers.add_parser("demo", help="用合成数据验证独立系统的策略、风控与账户链路")
    sub.add_argument("--config", type=Path, default=PROJECT / "config/demo.json")
    sub.add_argument("--output-root", type=Path, default=PROJECT / "reports/demo")
    sub.set_defaults(handler=run_demo)
    sub = subparsers.add_parser("serve", help="启动本地量化研究工作台")
    sub.add_argument("--port", type=int, default=8765)
    sub.set_defaults(handler=serve_dashboard)
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(2, f"系统拒绝运行：{exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
