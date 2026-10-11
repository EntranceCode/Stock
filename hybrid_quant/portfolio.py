"""Merge strategy budgets and produce conservative account risk targets.

These functions create desired weights. They cannot guarantee an actual exit
when the execution adapter cannot sell a holding.
"""
from __future__ import annotations

from datetime import datetime
import math
from typing import Mapping

from .models import (
    AssetType, DataSnapshot, RiskConfig, RiskDecision, RiskState,
    StrategyTarget, aware, finite,
)


_TRIGGERS = (0.0, 0.08, 0.12, 0.16, 0.20)
_DRAWDOWN_SCALES = (1.0, 0.80, 0.50, 0.25, 0.0)
_EPSILON = 1e-12


def merge_targets(
    targets: tuple[StrategyTarget, ...],
    budgets: Mapping[str, float],
    decision_at: datetime,
) -> dict[str, float]:
    """Apply fixed budgets; unspent and absent strategy budgets stay in cash."""
    aware(decision_at, "decision_at")
    for strategy_id, budget in budgets.items():
        finite(budget, "strategy budget")
        if not strategy_id or budget < 0:
            raise ValueError("Strategy budgets need a name and nonnegative weight")
    if math.fsum(budgets.values()) > 1 + _EPSILON:
        raise ValueError("Strategy budgets exceed account equity")
    seen: set[str] = set()
    contributions: dict[str, list[float]] = {}
    for target in targets:
        if target.strategy_id in seen:
            raise ValueError("A strategy may provide only one target per merge")
        seen.add(target.strategy_id)
        if target.strategy_id not in budgets:
            raise ValueError(f"Unconfigured strategy: {target.strategy_id}")
        if target.decision_at > decision_at:
            raise ValueError("A strategy target is not yet available")
        for symbol, weight in target.weights.items():
            # Targets are frozen, but the Mapping they contain can be mutable.
            finite(weight, "target weight")
            if not symbol or weight < 0:
                raise ValueError("Strategy targets must be long-only")
            contributions.setdefault(symbol, []).append(
                budgets[target.strategy_id] * weight
            )
        if math.fsum(target.weights.values()) > 1 + _EPSILON:
            raise ValueError("Strategy target exceeds its internal budget")
    return {
        symbol: math.fsum(values)
        for symbol, values in sorted(contributions.items())
        if math.fsum(values) > 0
    }


def _check_state(state: RiskState) -> None:
    finite(state.high_watermark, "high watermark")
    finite(state.current_drawdown, "current drawdown")
    if state.high_watermark < 0 or not 0 <= state.current_drawdown <= 1:
        raise ValueError("Invalid risk account state")
    if state.drawdown_level not in range(5) or state.recovery_count < 0:
        raise ValueError("Invalid drawdown recovery state")


def update_drawdown(state: RiskState, nav: float, config: RiskConfig) -> float:
    """Advance drawdown once per session, without resetting the high watermark."""
    finite(nav, "nav", positive=True)
    _check_state(state)
    state.high_watermark = max(state.high_watermark, nav)
    drawdown = max(0.0, 1.0 - nav / state.high_watermark)
    state.current_drawdown = drawdown
    if state.locked or state.drawdown_level == 4:
        state.locked = True
        state.drawdown_level = 4
        state.recovery_count = 0
        return drawdown

    observed_level = max(
        level for level, trigger in enumerate(_TRIGGERS)
        if drawdown + _EPSILON >= trigger
    )
    if observed_level > state.drawdown_level:
        state.drawdown_level = observed_level
        state.recovery_count = 0
        if observed_level == 4:
            state.locked = True
    elif state.drawdown_level:
        recovery_line = _TRIGGERS[state.drawdown_level] - 0.02
        if drawdown < recovery_line - _EPSILON:
            state.recovery_count += 1
            if state.recovery_count >= config.recovery_sessions:
                state.drawdown_level -= 1
                state.recovery_count = 0
        else:
            state.recovery_count = 0
    else:
        state.recovery_count = 0
    return drawdown


def _estimate_annual_vol(
    weights: Mapping[str, float], snapshot: DataSnapshot, config: RiskConfig,
) -> tuple[float | None, str | None]:
    """Compute w'Cov(r)w using date-matched genuine adjusted price returns.

    Date sets and their predecessors must match, so a missing session cannot
    become a multi-session return paired with another security's daily return.
    No prices or suspension returns are forward-filled.
    """
    series: dict[str, tuple[float, ...]] = {}
    reference_dates = None
    for symbol in sorted(weights):
        history = snapshot.known_history(symbol)
        if len(history) < config.vol_window + 1:
            return None, f"volatility_history_insufficient:{symbol}"
        window = history[-(config.vol_window + 1):]
        if window[-1].trade_date != snapshot.decision_at.date():
            return None, f"volatility_history_stale:{symbol}"
        if any(not bar.tradable or bar.adjusted_close is None for bar in window):
            return None, f"volatility_adjusted_history_unavailable:{symbol}"
        dates = tuple(bar.trade_date for bar in window)
        if reference_dates is None:
            reference_dates = dates
        elif dates != reference_dates:
            return None, "volatility_return_dates_mismatch"
        prices = tuple(bar.adjusted_close for bar in window)
        if any(not math.isfinite(price) or price <= 0 for price in prices):
            return None, f"volatility_price_invalid:{symbol}"
        returns = tuple(prices[i] / prices[i - 1] - 1 for i in range(1, len(prices)))
        if any(not math.isfinite(value) for value in returns):
            return None, f"volatility_return_invalid:{symbol}"
        series[symbol] = returns
    portfolio_returns = tuple(
        math.fsum(weights[symbol] * series[symbol][i] for symbol in series)
        for i in range(config.vol_window)
    )
    mean = math.fsum(value / config.vol_window for value in portfolio_returns)
    # hypot avoids overflowing intermediate squared deviations; an unusable
    # final estimate is rejected rather than letting extreme finite inputs fail.
    dispersion = math.hypot(*(value - mean for value in portfolio_returns))
    annual_vol = dispersion * math.sqrt(config.annualization / (config.vol_window - 1))
    if not math.isfinite(annual_vol):
        return None, "volatility_estimate_invalid"
    return annual_vol, None


def apply_risk(
    weights: Mapping[str, float],
    snapshot: DataSnapshot,
    state: RiskState,
    config: RiskConfig = RiskConfig(),
) -> RiskDecision:
    """Only reduce proposed holdings; all removed weight becomes account cash."""
    _check_state(state)
    proposed = dict(weights)
    for symbol, weight in proposed.items():
        finite(weight, "account target weight")
        if not symbol or weight < 0:
            raise ValueError("Account target weights must be long-only")
    if math.fsum(proposed.values()) > 1 + _EPSILON:
        raise ValueError("Account target weights exceed account equity")
    reasons: list[str] = []
    approved: dict[str, float] = {}
    equities: dict[str, float] = {}
    for symbol, weight in sorted(proposed.items()):
        if not weight:
            continue
        security = snapshot.securities.get(symbol)
        if security is None or not security.historical_metadata_verified:
            reasons.append(f"historical_metadata_unverified:{symbol}")
            continue
        if not security.eligible_for_new_positions or security.listed_on > snapshot.decision_at.date():
            reasons.append(f"security_ineligible:{symbol}")
            continue
        history = snapshot.known_history(symbol)
        if (not history or history[-1].trade_date != snapshot.decision_at.date()
                or not history[-1].tradable or not history[-1].buyable):
            reasons.append(f"new_position_unavailable:{symbol}")
            continue
        if security.equity_exposure:
            exposures = security.industry_exposure
            if (not security.lookthrough_verified or not exposures
                    or any(not isinstance(key, str) or not key.strip() for key in exposures)
                    or any(not math.isfinite(value) or value < 0 for value in exposures.values())
                    or not math.isclose(math.fsum(exposures.values()), 1.0, rel_tol=0.0, abs_tol=1e-6)):
                reasons.append(f"industry_lookthrough_unverified:{symbol}")
                continue
            equities[symbol] = weight
        approved[symbol] = weight

    annual_vol = None
    vol_scale = 1.0
    if equities:
        annual_vol, error = _estimate_annual_vol(equities, snapshot, config)
        if error is not None:
            vol_scale = 0.0
            reasons.append(error)
        elif annual_vol > 0:
            vol_scale = min(1.0, config.target_annual_vol / annual_vol)
    effective_level = max(
        state.drawdown_level,
        max(level for level, trigger in enumerate(_TRIGGERS)
            if state.current_drawdown + _EPSILON >= trigger),
    )
    drawdown_scale = 0.0 if state.locked else _DRAWDOWN_SCALES[effective_level]
    risk_scale = min(vol_scale, drawdown_scale)
    if vol_scale < 1:
        reasons.append("volatility_target_reduction")
    if drawdown_scale < 1:
        reasons.append(f"drawdown_level:{effective_level}")
    if state.locked:
        reasons.append("drawdown_lock_requires_separate_research_decision")
    for symbol in approved:
        security = snapshot.securities[symbol]
        if symbol in equities:
            approved[symbol] *= risk_scale
        cap = {
            AssetType.STOCK: config.stock_cap,
            AssetType.EQUITY_ETF: config.etf_cap,
            AssetType.BOND_ETF: config.bond_etf_cap,
        }[security.asset_type]
        if approved[symbol] > cap:
            approved[symbol] = cap
            reasons.append(f"security_cap:{symbol}")

    equity_weight = math.fsum(approved[symbol] for symbol in equities)
    if equity_weight > config.equity_cap:
        scale = config.equity_cap / equity_weight
        for symbol in equities:
            approved[symbol] *= scale
        reasons.append("equity_cap")
    industries = sorted({
        industry for symbol in equities
        for industry in snapshot.securities[symbol].industry_exposure
    })
    for industry in industries:
        participants = tuple(
            symbol for symbol in equities
            if snapshot.securities[symbol].industry_exposure.get(industry, 0) > 0
        )
        exposure = math.fsum(
            approved[symbol] * snapshot.securities[symbol].industry_exposure[industry]
            for symbol in participants
        )
        if exposure > config.industry_cap:
            scale = config.industry_cap / exposure
            for symbol in participants:
                approved[symbol] *= scale
            reasons.append(f"industry_cap:{industry}")
    invested = math.fsum(approved.values())
    if invested > 1 - config.min_cash:
        scale = (1 - config.min_cash) / invested
        approved = {symbol: weight * scale for symbol, weight in approved.items()}
        reasons.append("minimum_cash")
    if state.locked or effective_level == 4:
        approved = {}
        reasons.append("all_assets_cash_exit_target")
    approved = {symbol: weight for symbol, weight in approved.items() if weight > 0}
    return RiskDecision(
        decision_at=snapshot.decision_at,
        proposed_weights=proposed,
        approved_weights=approved,
        risk_scale=risk_scale,
        estimated_annual_vol=annual_vol,
        drawdown=state.current_drawdown,
        locked=state.locked or effective_level == 4,
        reasons=tuple(reasons),
    )
