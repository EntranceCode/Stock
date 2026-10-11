"""Independent daily strategy rules operating only on point-in-time snapshots.

Targets describe fractions of each strategy's own budget. Unused fractions are
cash; the portfolio layer applies the separate 40/35/25 account budgets.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import pstdev

from .models import AssetType, DataSnapshot, Security, StrategyTarget


@dataclass(frozen=True)
class _PriceInputs:
    prices: tuple[float, ...]
    turnover_20: float
    volatility_60: float

    @property
    def momentum_20(self) -> float:
        return self.prices[-1] / self.prices[-21] - 1.0

    @property
    def momentum_60(self) -> float:
        return self.prices[-1] / self.prices[-61] - 1.0

    @property
    def trending(self) -> bool:
        return (self.prices[-1] > math.fsum(price / 60 for price in self.prices[-60:])
                and self.momentum_60 > 0)


def _price_inputs(snapshot: DataSnapshot, security: Security,
                  required_bars: int) -> tuple[_PriceInputs | None, str | None]:
    """Reject gaps instead of replacing adjusted prices or filling halted days.

    The provider must supply verified session history. This layer cannot infer
    missing exchange sessions from weekdays, so calendar validation remains a
    data-adapter responsibility.
    """
    if not security.historical_metadata_verified:
        return None, "historical security metadata is unverified"
    if not security.eligible_for_new_positions:
        return None, "security is ineligible for new positions"
    if security.listed_on > snapshot.decision_at.date():
        return None, "listing date is in the future"
    history = snapshot.known_history(security.symbol)
    if not history or history[-1].trade_date != snapshot.decision_at.date():
        return None, "no available bar for the decision date"
    if not history[-1].tradable:
        return None, "security is not tradable on the decision date"
    if len(history) < required_bars:
        return None, "insufficient known price history"
    window = history[-required_bars:]
    if any(bar.trade_date < security.listed_on for bar in window):
        return None, "price history precedes the verified listing date"
    if any(not bar.tradable for bar in window):
        return None, "history contains an untradable session"
    if any(bar.adjusted_close is None for bar in window):
        return None, "adjusted price history is incomplete"
    prices = tuple(float(bar.adjusted_close) for bar in window)
    daily_returns = tuple(prices[i] / prices[i - 1] - 1
                          for i in range(len(prices) - 60, len(prices)))
    if (any(not math.isfinite(value) for value in daily_returns)
            or not math.isfinite(prices[-1] / prices[-21] - 1)
            or not math.isfinite(prices[-1] / prices[-61] - 1)):
        return None, "adjusted price ratios are not finite"
    turnover = math.fsum(bar.turnover / 20 for bar in window[-20:])
    if turnover <= 0:
        return None, "known 20-session turnover is not positive"
    return _PriceInputs(prices, turnover, pstdev(daily_returns)), None


def _quantile(sorted_values: list[float], proportion: float) -> float:
    position = (len(sorted_values) - 1) * proportion
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def _winsorized_zscores(values: dict[str, float]) -> dict[str, float]:
    """Clip to interpolated 5th/95th percentiles, then population z-scores."""
    ordered = sorted(values.values())
    lower, upper = _quantile(ordered, 0.05), _quantile(ordered, 0.95)
    clipped = {symbol: min(upper, max(lower, value))
               for symbol, value in values.items()}
    # Positive rescaling leaves z-scores unchanged and avoids overflowing a
    # sum or variance when finite provider fields have extreme magnitudes.
    scale = max(abs(value) for value in clipped.values())
    scaled = {symbol: value / scale if scale else 0.0 for symbol, value in clipped.items()}
    mean = math.fsum(scaled.values()) / len(scaled)
    deviation = pstdev(scaled.values())
    if deviation == 0:
        return {symbol: 0.0 for symbol in clipped}
    return {symbol: (value - mean) / deviation
            for symbol, value in scaled.items()}


def _percentile_ranks(values: dict[str, float]) -> dict[str, float]:
    """Average tied one-based ranks divided by the eligible cross-section size."""
    ordered = sorted(values.items(), key=lambda item: (item[1], item[0]))
    result: dict[str, float] = {}
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        percentile = ((start + 1 + end) / 2) / len(ordered)
        for symbol, _ in ordered[start:end]:
            result[symbol] = percentile
        start = end
    return result


def stock_target(snapshot: DataSnapshot, current_symbols: tuple[str, ...] = (),
                 *, target_count: int = 25, buffer_count: int = 40) -> StrategyTarget:
    """Quality/value/momentum/low-volatility selection with a retention buffer.

    The latest *known* financial observation must be verified. An unverified
    known revision blocks the security, rather than silently falling back to
    an older observation or using the revised value at its old report date.
    """
    if target_count <= 0 or buffer_count < target_count:
        raise ValueError("target_count must be positive and buffer_count at least target_count")
    candidates: dict[str, tuple[float, float, float, float]] = {}
    reasons: list[str] = []
    for symbol, security in sorted(snapshot.securities.items()):
        if security.asset_type != AssetType.STOCK:
            continue
        prices, problem = _price_inputs(snapshot, security, 121)
        if problem is None and snapshot.listing_age_sessions.get(symbol, 0) < 250:
            problem = "fewer than 250 verified listing sessions"
        observations = tuple(item for item in snapshot.stock_factors.get(symbol, ())
                             if item.available_at <= snapshot.decision_at)
        factor = None
        if problem is None:
            if not observations:
                problem = "no known financial factor observation"
            else:
                latest_at = max(item.available_at for item in observations)
                latest = tuple(item for item in observations if item.available_at == latest_at)
                if len(latest) != 1:
                    problem = "ambiguous latest financial factor observation"
                else:
                    factor = latest[0]
                    if not factor.original_version_verified:
                        problem = "latest known financial version is unverified"
                    elif factor.quality is None or factor.value is None or factor.value <= 0:
                        problem = "quality/value factors are incomplete or value is not positive"
        if problem is not None:
            reasons.append(f"Excluded {symbol}: {problem}")
            continue
        assert factor is not None and prices is not None
        momentum = prices.prices[-21] / prices.prices[-121] - 1.0
        if not math.isfinite(momentum):
            reasons.append(f"Excluded {symbol}: adjusted momentum is not finite")
            continue
        candidates[symbol] = (factor.quality, factor.value, momentum, -prices.volatility_60)
    if not candidates:
        return StrategyTarget("stock", snapshot.decision_at, {},
                              tuple(reasons + ["No eligible stocks; the strategy budget stays in cash"]))
    factors = tuple(_winsorized_zscores({symbol: inputs[index]
                                         for symbol, inputs in candidates.items()})
                    for index in range(4))
    coefficients = (0.35, 0.25, 0.25, 0.15)
    scores = {symbol: math.fsum(coefficient * factor[symbol]
                               for coefficient, factor in zip(coefficients, factors))
              for symbol in candidates}
    ranked = sorted(scores, key=lambda symbol: (-scores[symbol], symbol))
    current = set(current_symbols)
    retained = [symbol for symbol in ranked[:buffer_count] if symbol in current]
    selected = retained[:target_count]
    for symbol in ranked[:target_count]:
        if len(selected) >= target_count:
            break
        if symbol not in selected:
            selected.append(symbol)
    weights = {symbol: 1.0 / target_count for symbol in selected}
    reasons.append(f"Selected {len(selected)}/{target_count} stocks; each uses 1/{target_count} of the strategy budget")
    reasons.append(f"Retained {len(set(selected) & set(retained))} eligible holdings inside the top-{buffer_count} buffer")
    return StrategyTarget("stock", snapshot.decision_at, weights, tuple(reasons))


def etf_target(snapshot: DataSnapshot, *, top_n: int = 3) -> StrategyTarget:
    """Trend rotation with index deduplication and inverse daily volatility."""
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    eligible: dict[str, _PriceInputs] = {}
    reasons: list[str] = []
    by_index: dict[str, list[str]] = {}
    for symbol, security in sorted(snapshot.securities.items()):
        if security.asset_type != AssetType.EQUITY_ETF:
            continue
        inputs, problem = _price_inputs(snapshot, security, 120)
        if problem is None and not security.tracking_index:
            problem = "tracking index is unknown"
        if problem is None and not security.tracking_index.strip():
            problem = "tracking index is unknown"
        if problem is None and not inputs.trending:
            problem = "60-session trend conditions are not met"
        if problem is not None:
            reasons.append(f"Excluded {symbol}: {problem}")
            continue
        assert inputs is not None and security.tracking_index is not None
        eligible[symbol] = inputs
        by_index.setdefault(security.tracking_index.strip(), []).append(symbol)
    # Keep the product with the greatest known turnover for each index before
    # ranking: duplicate products must not increase an index's rank weight.
    unique: dict[str, _PriceInputs] = {}
    for _, symbols in sorted(by_index.items()):
        chosen = min(symbols, key=lambda symbol: (-eligible[symbol].turnover_20, symbol))
        unique[chosen] = eligible[chosen]
        for symbol in symbols:
            if symbol != chosen:
                reasons.append(f"Excluded {symbol}: another ETF for the same index has higher known turnover")
    if not unique:
        return StrategyTarget("etf_rotation", snapshot.decision_at, {},
                              tuple(reasons + ["No eligible ETFs; the strategy budget stays in cash"]))
    rank_20 = _percentile_ranks({symbol: item.momentum_20 for symbol, item in unique.items()})
    rank_60 = _percentile_ranks({symbol: item.momentum_60 for symbol, item in unique.items()})
    scores = {symbol: 0.5 * rank_20[symbol] + 0.5 * rank_60[symbol] for symbol in unique}
    selected = sorted(unique, key=lambda symbol: (-scores[symbol], -unique[symbol].turnover_20, symbol))[:top_n]
    inverses = {symbol: 1 / max(unique[symbol].volatility_60, 0.005)
                for symbol in selected}
    invested_budget = len(selected) / top_n
    inverse_sum = math.fsum(inverses.values())
    weights = {symbol: invested_budget * inverse / inverse_sum
               for symbol, inverse in inverses.items()}
    reasons.append(f"Selected {len(selected)}/{top_n} independent indices; unused slots remain cash")
    return StrategyTarget("etf_rotation", snapshot.decision_at, weights, tuple(reasons))


def defensive_target(snapshot: DataSnapshot) -> StrategyTarget:
    """Hold at most 60% of the defensive budget in one qualified bond ETF."""
    eligible: dict[str, _PriceInputs] = {}
    reasons: list[str] = []
    for symbol, security in sorted(snapshot.securities.items()):
        if security.asset_type != AssetType.BOND_ETF:
            continue
        inputs, problem = _price_inputs(snapshot, security, 120)
        if problem is None and not inputs.trending:
            problem = "60-session trend conditions are not met"
        if problem is not None:
            reasons.append(f"Excluded {symbol}: {problem}")
            continue
        assert inputs is not None
        eligible[symbol] = inputs
    if not eligible:
        return StrategyTarget("defensive", snapshot.decision_at, {},
                              tuple(reasons + ["No eligible bond ETF; the defensive budget stays in cash"]))
    chosen = min(eligible, key=lambda symbol: (-eligible[symbol].turnover_20, symbol))
    reasons.append(f"Selected {chosen} by known turnover; 40% of the defensive budget remains cash")
    return StrategyTarget("defensive", snapshot.decision_at, {chosen: 0.6}, tuple(reasons))
