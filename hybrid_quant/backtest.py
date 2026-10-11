"""Deterministic daily replay with a shared account and next-session orders."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
import math
from typing import Mapping

from .accounting import Account, plan_orders
from .models import AssetType, CashDividend, DataSnapshot, FeeConfig, Fill, Order, RiskConfig, RiskState, StrategyTarget
from .portfolio import apply_risk, merge_targets, update_drawdown
from .strategies import defensive_target, etf_target, stock_target


def _order_record(order: Order) -> dict:
    return {"order_id": order.order_id, "symbol": order.symbol, "side": order.side.value,
            "quantity": order.quantity, "decision_at": order.decision_at.isoformat(),
            "execute_on": order.execute_on.isoformat(), "reference_price": order.reference_price}


def _fill_record(fill: Fill) -> dict:
    return {"order_id": fill.order_id, "symbol": fill.symbol, "side": fill.side.value,
            "trade_date": fill.trade_date.isoformat(), "quantity": fill.quantity, "price": fill.price,
            "notional": fill.notional, "commission": fill.commission, "tax": fill.tax}


def _actual_violations(account: Account, snapshot: DataSnapshot, prices: Mapping[str, float],
                       nav: float, config: RiskConfig) -> list[dict]:
    """Observe actual holdings independently of the risk module's desired weights."""
    violations = []
    equity = 0.0
    industries: dict[str, float] = {}
    for symbol, quantity in account.holdings().items():
        security = snapshot.securities.get(symbol)
        weight = quantity * prices[symbol] / nav
        if security is None or not security.historical_metadata_verified:
            violations.append({"constraint": "historical_metadata_unverified", "symbol": symbol, "actual": weight})
            continue
        cap = {AssetType.STOCK: config.stock_cap, AssetType.EQUITY_ETF: config.etf_cap,
               AssetType.BOND_ETF: config.bond_etf_cap}[security.asset_type]
        if weight > cap + 1e-9:
            violations.append({"constraint": "security_cap", "symbol": symbol, "actual": weight, "limit": cap})
        if security.equity_exposure:
            equity += weight
            if (not security.lookthrough_verified or not security.industry_exposure
                    or not math.isclose(sum(security.industry_exposure.values()), 1.0, abs_tol=1e-6)):
                violations.append({"constraint": "industry_lookthrough_unverified", "symbol": symbol, "actual": weight})
            for industry, fraction in security.industry_exposure.items():
                industries[industry] = industries.get(industry, 0.0) + weight * fraction
    if equity > config.equity_cap + 1e-9:
        violations.append({"constraint": "equity_cap", "actual": equity, "limit": config.equity_cap})
    for industry, weight in sorted(industries.items()):
        if weight > config.industry_cap + 1e-9:
            violations.append({"constraint": "industry_cap", "industry": industry, "actual": weight, "limit": config.industry_cap})
    cash_weight = account.cash / nav
    if cash_weight + 1e-9 < config.min_cash:
        violations.append({"constraint": "minimum_available_cash", "actual": cash_weight, "limit": config.min_cash})
    return violations


def run_backtest(snapshots: tuple[DataSnapshot, ...], initial_cash: float = 1e6,
                 fee_config: FeeConfig = FeeConfig(), risk_config: RiskConfig = RiskConfig(),
                 dividends: tuple[CashDividend, ...] = (), *, budgets: Mapping[str, float] | None = None) -> dict:
    """Replay supplied actual-session snapshots without reading future prices.

    Future dates are an explicit fixture calendar and determine monthly/weekly
    boundaries only. Orders are fixed at a close, attempted at the next supplied
    session's open, and expire after that attempt. The first close creates the
    first signals; the final close creates no order beyond the fixture calendar.
    """
    if len(snapshots) < 2:
        raise ValueError("At least two actual-session snapshots are required")
    days = tuple(snapshot.decision_at.date() for snapshot in snapshots)
    if any(current >= following for current, following in zip(days, days[1:])):
        raise ValueError("Snapshot actual-session dates must be strictly increasing")
    for snapshot, day in zip(snapshots, days):
        if not any(history and history[-1].trade_date == day
                   for symbol in snapshot.histories for history in (snapshot.known_history(symbol),)):
            raise ValueError(f"Snapshot {day.isoformat()} has no known current-session bar")
    for event in dividends:
        if days[0] <= event.ex_date <= days[-1] and event.ex_date not in days:
            raise ValueError("A dividend ex-date within the replay must be in its explicit session calendar")
    selected_budgets = dict(budgets if budgets is not None else {"stock": 0.40, "etf_rotation": 0.35, "defensive": 0.25})
    # Validate all budgets before changing the account, even if a first strategy
    # has no eligible security. The same budget map is reused every session.
    merge_targets((), selected_budgets, snapshots[0].decision_at)
    for required in ("stock", "etf_rotation", "defensive"):
        if required not in selected_budgets:
            raise ValueError(f"Missing strategy budget: {required}")
    account = Account(initial_cash, snapshots[0].securities, fee_config)
    risk_state = RiskState(high_watermark=initial_cash)
    targets: dict[str, StrategyTarget] = {}
    pending: tuple[Order, ...] = ()
    order_records: list[dict] = []
    fill_records: list[dict] = []
    decision_records: list[dict] = []
    equity_records: list[dict] = []
    valuation_dates: dict[str, date] = {}
    for session_index, (snapshot, day) in enumerate(zip(snapshots, days)):
        # Metadata supplied for this session replaces only current records. The
        # execution adapter refuses an order if today's record is unavailable.
        for order in pending:
            if order.symbol not in snapshot.securities:
                account.securities.pop(order.symbol, None)
        account.securities.update(snapshot.securities)
        account.process_dividends(day, dividends)
        current_bars = {}
        for symbol in snapshot.histories:
            history = snapshot.known_history(symbol)
            if history and history[-1].trade_date == day:
                current_bars[symbol] = history[-1]
        for order in sorted(pending, key=lambda item: (item.side.value == "buy", item.symbol)):
            fill = account.execute(order, current_bars.get(order.symbol), session_index)
            if fill is not None:
                fill_records.append(_fill_record(fill))
                valuation_dates.setdefault(fill.symbol, day)
        prices = account.last_prices
        stale = []
        for symbol in account.holdings():
            history = snapshot.known_history(symbol)
            if history:
                bar = history[-1]
                if bar.trade_date >= valuation_dates.get(symbol, date.min):
                    prices[symbol] = bar.close
                    valuation_dates[symbol] = bar.trade_date
                if bar.trade_date != day:
                    stale.append(symbol)
            else:
                stale.append(symbol)
            if symbol not in prices:
                raise ValueError(f"Held security {symbol} has no known valuation price")
        nav = account.nav(prices)
        if not math.isfinite(nav) or nav <= 0:
            raise ValueError("The replay requires positive finite account NAV")
        drawdown = update_drawdown(risk_state, nav, risk_config)
        last = session_index == len(snapshots) - 1
        next_day = None if last else days[session_index + 1]
        month_end = last or (day.year, day.month) != (next_day.year, next_day.month)
        week_end = last or day.isocalendar()[:2] != next_day.isocalendar()[:2]
        refreshed = []
        if session_index == 0 or month_end:
            held_stocks = tuple(symbol for symbol in account.holdings()
                                if snapshot.securities.get(symbol) is not None
                                and snapshot.securities[symbol].asset_type == AssetType.STOCK)
            targets["stock"] = stock_target(snapshot, current_symbols=held_stocks)
            targets["defensive"] = defensive_target(snapshot)
            refreshed.extend(("stock", "defensive"))
        if session_index == 0 or week_end:
            targets["etf_rotation"] = etf_target(snapshot)
            refreshed.append("etf_rotation")
        proposed = merge_targets(tuple(targets.values()), selected_budgets, snapshot.decision_at)
        risk = apply_risk(proposed, snapshot, risk_state, risk_config)
        violations = _actual_violations(account, snapshot, prices, nav, risk_config)
        approved = dict(risk.approved_weights)
        # An actual concentration/cash breach can persist when a sale is blocked
        # or capacity-limited. Do not increase the exposed asset while it persists.
        actual_weights = {symbol: quantity * prices[symbol] / nav for symbol, quantity in account.holdings().items()}
        freeze_all_additions = any(violation["constraint"] == "historical_metadata_unverified" for violation in violations)
        freeze_equity_additions = any(violation["constraint"] == "industry_lookthrough_unverified" for violation in violations)
        extra_reasons = []
        if freeze_all_additions or freeze_equity_additions:
            extra_reasons.append("unknown_actual_exposure_blocks_all_additions" if freeze_all_additions
                                 else "unknown_actual_industry_exposure_blocks_equity_additions")
            for symbol in tuple(approved):
                security = snapshot.securities.get(symbol)
                if freeze_all_additions or (security is not None and security.equity_exposure):
                    approved[symbol] = min(approved[symbol], actual_weights.get(symbol, 0))
        for violation in violations:
            if "symbol" in violation:
                symbol = violation["symbol"]
                approved[symbol] = min(approved.get(symbol, 0), actual_weights.get(symbol, 0))
            elif violation["constraint"] in ("equity_cap", "industry_cap", "minimum_available_cash"):
                for symbol in tuple(approved):
                    security = snapshot.securities.get(symbol)
                    affected = (violation["constraint"] == "minimum_available_cash"
                                or (security is not None and security.equity_exposure and
                                    (violation["constraint"] == "equity_cap" or security.industry_exposure.get(violation["industry"], 0) > 0)))
                    if affected:
                        approved[symbol] = min(approved[symbol], actual_weights.get(symbol, 0))
        pending = () if last else plan_orders(approved, snapshot, account, next_day, session_index)
        order_records.extend(_order_record(order) for order in pending)
        requested_exits = {order.symbol: order.quantity for order in pending if order.side.value == "sell"}
        unexecutable_reductions = []
        for symbol, weight in actual_weights.items():
            desired_reduction_value = max(0.0, weight - approved.get(symbol, 0.0)) * nav
            planned_reduction_value = requested_exits.get(symbol, 0) * prices[symbol]
            if desired_reduction_value > planned_reduction_value + 1e-7:
                unexecutable_reductions.append({"symbol": symbol, "actual_weight": weight,
                                               "approved_weight": approved.get(symbol, 0.0),
                                               "planned_sell_quantity": requested_exits.get(symbol, 0),
                                               "uncovered_reduction_value": desired_reduction_value - planned_reduction_value,
                                               "reason": "reduction_limited_by_rules_capacity_or_lots" if not last else "fixture_calendar_ended"})
        decision_records.append({"decision_at": snapshot.decision_at.isoformat(), "refreshed_strategies": refreshed,
                                 "strategy_targets": {name: {"weights": dict(target.weights), "reasons": list(target.reasons),
                                                              "signal_at": target.decision_at.isoformat()} for name, target in targets.items()},
                                 "proposed_weights": dict(risk.proposed_weights), "approved_weights": approved,
                                 "risk_scale": risk.risk_scale, "estimated_annual_vol": risk.estimated_annual_vol,
                                 "drawdown": drawdown, "locked": risk.locked, "reasons": list(risk.reasons) + extra_reasons,
                                 "actual_limit_violations": violations, "unexecutable_reductions": unexecutable_reductions,
                                 "filtered_future_factor_observations": sum(item.available_at > snapshot.decision_at
                                                                            for observations in snapshot.stock_factors.values() for item in observations)})
        equity_records.append({"trade_date": day.isoformat(), "cash": account.cash, "receivables": account.receivables,
                               "nav": nav, "holdings": account.holdings(), "stale_symbols": sorted(stale),
                               "drawdown": drawdown, "actual_limit_violations": violations})
    return {"equity": equity_records, "orders": order_records, "fills": fill_records,
            "execution_events": account.execution_events, "dividend_events": account.dividend_events,
            "decisions": decision_records, "config": {"initial_cash": initial_cash, "budgets": selected_budgets,
                                                         "fees": asdict(fee_config), "risk": asdict(risk_config)},
            "limitations": ["This is an explicit daily fixture replay, not evidence of investment performance.",
                            "Published fixture dates determine rebalance boundaries; no next-session price or factor determines original order quantity.",
                            "Daily open plus configured slippage approximates execution; opening auctions, queues and intraday liquidity are not reconstructed.",
                            "Only verified supplied execution metadata and tradability flags are used; these cannot establish real historical tradeability.",
                            "Cash dividend fixtures accrue before ex-date orders and become available on the first supplied session on/after pay-date.",
                            "Splits, rights offerings, mergers, precise historical fees and dividend taxes are not implemented.",
                            "Per-strategy virtual-account contribution attribution is not implemented.",
                            "Stale holdings remain in NAV and desired limits may be unachievable; actual violations are recorded."]}
