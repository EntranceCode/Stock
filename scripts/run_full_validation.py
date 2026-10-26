"""Full-validation harness for a snapshot series (section 10.2 acceptance).

Runs the engine on one series in several configurations and compares against a
risk-matched benchmark, then reports accounting red lines and the honesty
caveats that bound every number:

* benchmarks - equal-weight buy-and-hold of the same candidate universe inside
  the same budget sleeves, entry costs applied once, valued on adjusted closes;
* ablations - each budget sleeve alone and without the stock sleeve;
* slippage sensitivity - 0/5/10/20 basis points through the engine cost model;
* out-of-sample - 60/20/20 time split with the last fifth reported sealed;
* red lines - negative cash, ledger reconstruction, unexecutable-reduction
  ratio, and actual limit violations, per run.

``--relax-gates`` relaxes the three verification gates for a mechanics check and
is recorded in every output row; without it the engine holds cash by design and
the run only proves the gates work.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from hybrid_quant.backtest import run_backtest
from hybrid_quant.models import (AssetType, DailyBar, DataSnapshot, FeeConfig,
                                 RiskConfig, Security, StockFactorObservation)

BUDGETS = {"stock": 0.40, "etf_rotation": 0.35, "defensive": 0.25}
ABLATIONS = {
    "hybrid_full": BUDGETS,
    "stock_only": {"stock": 1.00, "etf_rotation": 0.0, "defensive": 0.0},
    "etf_only": {"stock": 0.0, "etf_rotation": 1.00, "defensive": 0.0},
    "defensive_only": {"stock": 0.0, "etf_rotation": 0.0, "defensive": 1.00},
    "no_stock": {"stock": 0.0, "etf_rotation": 0.50, "defensive": 0.50},
}
SLIPPAGE_STEPS = (0.0, 5.0, 10.0, 20.0)


def load_series(series_dir: Path, relaxed: bool, start: date | None = None,
                end: date | None = None) -> tuple[DataSnapshot, ...]:
    """Rebuild engine snapshots from a written series, optionally gated open."""
    index = json.loads((series_dir / "series.json").read_text(encoding="utf-8"))
    snapshots = []
    for entry in index["sessions"]:
        day = date.fromisoformat(entry["trade_date"])
        if start is not None and day < start:
            continue
        if end is not None and day > end:
            continue
        payload = json.loads((series_dir / entry["manifest"]).parent.joinpath("payload.json").read_text(encoding="utf-8"))
        securities = {}
        for record in payload["securities"]:
            securities[record["symbol"]] = Security(
                symbol=record["symbol"], asset_type=AssetType(record["asset_type"]),
                listed_on=date.fromisoformat(record["listed_on"]),
                exchange=record["exchange"],
                delist_date=(date.fromisoformat(record["delist_date"])
                             if record.get("delist_date") else None),
                buy_lot_size=record["buy_lot_size"], settlement_sessions=record["settlement_sessions"],
                industry_exposure={"unclassified": 1.0} if relaxed else dict(record["industry_exposure"]),
                tracking_index=record["tracking_index"],
                historical_metadata_verified=True if relaxed else record["historical_metadata_verified"],
                lookthrough_verified=True if relaxed else record["lookthrough_verified"],
                eligible_for_new_positions=True if relaxed else record["eligible_for_new_positions"])
        histories = {symbol: [] for symbol in securities}
        for record in payload["bars"]:
            histories[record["symbol"]].append(DailyBar(
                symbol=record["symbol"], trade_date=date.fromisoformat(record["trade_date"]),
                open=record["open"], high=record["high"], low=record["low"], close=record["close"],
                turnover=record["turnover"], available_at=datetime.fromisoformat(record["available_at"]),
                adjusted_close=record["adjusted_close"], tradable=record["tradable"],
                buyable=record["buyable"], sellable=record["sellable"]))
        factors = {symbol: [] for symbol in securities}
        for record in payload["factors"]:
            factors[record["symbol"]].append(StockFactorObservation(
                symbol=record["symbol"], quality=record["quality"], value=record["value"],
                available_at=datetime.fromisoformat(record["available_at"]),
                original_version_verified=True if relaxed else record["original_version_verified"]))
        snapshots.append(DataSnapshot(
            datetime.fromisoformat(payload["decision_at"]), securities,
            {symbol: tuple(items) for symbol, items in histories.items()},
            {symbol: tuple(items) for symbol, items in factors.items()},
            payload["listing_age_sessions"]))
    return tuple(snapshots)


def engine_run(snapshots, budgets, slippage_bps):
    return run_backtest(snapshots, initial_cash=1_000_000, fee_config=FeeConfig(slippage_bps=slippage_bps),
                        risk_config=RiskConfig(), budgets=budgets)


def summarize(result) -> dict:
    equity = result["equity"]
    navs = [item["nav"] for item in equity]
    peak, max_drawdown = navs[0], 0.0
    for nav in navs:
        peak = max(peak, nav)
        max_drawdown = max(max_drawdown, 1 - nav / peak)
    attempted = sum(len(item["unexecutable_reductions"]) for item in result["decisions"])
    reductions = sum(1 for item in result["decisions"]
                     for order in item.get("unexecutable_reductions", []))
    return {
        "sessions": len(equity),
        "orders": len(result["orders"]),
        "fills": len(result["fills"]),
        "final_nav": round(navs[-1], 2),
        "total_return": round(navs[-1] / navs[0] - 1, 6),
        "max_drawdown": round(max_drawdown, 6),
        "fees_paid": round(sum(fill["commission"] + fill["tax"] for fill in result["fills"]), 2),
        "distinct_symbols_bought": len({fill["symbol"] for fill in result["fills"] if fill["side"] == "buy"}),
        "unexecutable_reductions": reductions,
        "unexecutable_ratio": round(reductions / attempted, 6) if attempted else 0.0,
        "actual_limit_violations": sum(len(item["actual_limit_violations"]) for item in result["decisions"]),
        "negative_cash_sessions": len([item for item in equity if item["cash"] < -1e-6]),
        "execution_statuses": {status: sum(1 for event in result["execution_events"] if event["status"] == status)
                               for status in sorted({event["status"] for event in result["execution_events"]})},
    }


def reconcile(result) -> dict:
    """Independent cash and holdings reconstruction from the fills."""
    cash, holdings = 1_000_000.0, {}
    for fill in result["fills"]:
        fee = fill["commission"] + fill["tax"]
        if fill["side"] == "buy":
            cash -= fill["notional"] + fee
            holdings[fill["symbol"]] = holdings.get(fill["symbol"], 0) + fill["quantity"]
        else:
            cash += fill["notional"] - fee
            holdings[fill["symbol"]] -= fill["quantity"]
    holdings = {symbol: quantity for symbol, quantity in holdings.items() if quantity}
    final = result["equity"][-1]
    return {"cash_difference": round(abs(cash - final["cash"]), 9),
            "holdings_match": holdings == final["holdings"]}


def benchmark(snapshots, budgets) -> dict:
    """Equal-weight buy-and-hold of the same candidate universe per sleeve.

    Entry costs are applied once at the first session on the engine's cost
    model; valuation uses adjusted closes, so the path is a total-return proxy
    for the same asset mix without any selection.
    """
    fees = FeeConfig()
    equity_records, cash, holdings = [], 1_000_000.0, {}
    first = snapshots[0]
    for sleeve, budget in budgets.items():
        if budget <= 0:
            continue
        wanted = {"stock": AssetType.STOCK, "etf_rotation": AssetType.EQUITY_ETF,
                  "defensive": AssetType.BOND_ETF}[sleeve]
        symbols = sorted(symbol for symbol, security in first.securities.items()
                         if security.asset_type == wanted)
        if not symbols:
            continue
        allocation = 1_000_000.0 * budget / len(symbols)
        for symbol in symbols:
            history = first.known_history(symbol)
            if not history:
                continue
            bar = history[-1]
            price = bar.adjusted_close if bar.adjusted_close is not None else bar.close
            if price is None or price <= 0:
                continue
            quantity = int(allocation // (price * (1 + fees.slippage_bps / 10000)))
            if quantity <= 0:
                continue
            cost = quantity * price * (1 + fees.slippage_bps / 10000)
            commission = max(cost * fees.commission_rate, fees.minimum_commission)
            cash -= cost + commission
            holdings[symbol] = quantity
    for snapshot in snapshots:
        nav = cash
        for symbol, quantity in holdings.items():
            history = snapshot.known_history(symbol)
            if not history:
                continue
            bar = history[-1]
            price = bar.adjusted_close if bar.adjusted_close is not None else bar.close
            nav += quantity * (price if price is not None else 0.0)
        equity_records.append(nav)
    navs = equity_records
    peak, max_drawdown = navs[0], 0.0
    for nav in navs:
        peak = max(peak, nav)
        max_drawdown = max(max_drawdown, 1 - nav / peak)
    return {"sessions": len(navs), "final_nav": round(navs[-1], 2),
            "total_return": round(navs[-1] / navs[0] - 1, 6),
            "max_drawdown": round(max_drawdown, 6),
            "symbols_held": len(holdings),
            "note": "equal-weight buy-and-hold of the same candidate universe per sleeve; entry costs once"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--series", type=Path, required=True, help="快照序列目录")
    parser.add_argument("--out", type=Path, default=PROJECT / "reports/full-validation", help="报告输出目录")
    parser.add_argument("--relax-gates", action="store_true",
                        help="放开三门做机制检查（记录在案；不加则引擎按证据原状全现金）")
    args = parser.parse_args(argv)

    series_dir = args.series.resolve()
    index = json.loads((series_dir / "series.json").read_text(encoding="utf-8"))
    days = [date.fromisoformat(item["trade_date"]) for item in index["sessions"]]
    relaxed = args.relax_gates
    print("series:", index["series_id"], "| sessions:", len(days), "| symbols:",
          len(index["symbols"]["included"]), "| gates:", "relaxed" if relaxed else "as evidence leaves them",
          flush=True)

    snapshots = load_series(series_dir, relaxed)
    runs = {}
    for name, budgets in ABLATIONS.items():
        result = engine_run(snapshots, budgets, 5.0)
        runs[name] = {**summarize(result), **reconcile(result), "budgets": budgets}
        print("ablation", name, "->", runs[name]["final_nav"], flush=True)
    sensitivity = {}
    for slippage in SLIPPAGE_STEPS:
        result = engine_run(snapshots, BUDGETS, slippage)
        sensitivity[f"{slippage:g}bps"] = {**summarize(result), **reconcile(result)}
        print("slippage", slippage, "->", sensitivity[f"{slippage:g}bps"]["final_nav"], flush=True)

    split = {}
    if len(days) >= 10:
        train_end = days[int(len(days) * 0.6) - 1]
        oos_start = days[int(len(days) * 0.8)]
        for name, (start, end) in {"full": (None, None),
                                   "train_60": (None, train_end),
                                   "sealed_oos_20": (oos_start, None)}.items():
            part = load_series(series_dir, relaxed, start, end)
            if len(part) < 2:
                split[name] = {"sessions": len(part), "note": "too few sessions to replay"}
                continue
            result = engine_run(part, BUDGETS, 5.0)
            split[name] = {**summarize(result), **reconcile(result), "budgets": BUDGETS}
            print("split", name, "->", split[name]["final_nav"], flush=True)

    report = {
        "generated_at": datetime.now().isoformat(),
        "series": {"id": index["series_id"], "path": str(series_dir),
                   "sessions": len(days), "symbols": len(index["symbols"]["included"]),
                   "first_session": days[0].isoformat(), "last_session": days[-1].isoformat(),
                   "self_check": index["self_check"]},
        "gates": "relaxed_for_mechanics_check" if relaxed else "as_evidence_leaves_them",
        "relaxation_scope": (["listing dates from official exchange lists and the provider security-basic snapshot",
                              "factor values from the sampled-verified provider snapshot",
                              "industry exposure as a single declared unclassified bucket"]
                             if relaxed else []),
        "caveats": [
            "Not evidence of strategy effectiveness: industry simplification binds the industry cap, "
            "ETF tracking indices are name-derived, the universe is a fetched subset, and factor versions are sampled-verified only.",
            "The benchmark shares the universe and budgets but not the strategy logic.",
        ],
        "ablations": runs,
        "slippage_sensitivity": sensitivity,
        "time_split": split,
        "benchmark": benchmark(snapshots, BUDGETS),
    }
    args.out.mkdir(parents=True, exist_ok=True)
    destination = args.out / f"{index['series_id']}.json"
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nreport:", destination)
    print(json.dumps({"benchmark": report["benchmark"],
                      "hybrid_full": {k: report["ablations"]["hybrid_full"][k]
                                      for k in ("final_nav", "total_return", "max_drawdown", "fees_paid")},
                      "gates": report["gates"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
