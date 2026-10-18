"""Shared contracts for decisions, risk, and the simulated account."""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
import math
from typing import Mapping


def finite(value: float, name: str, *, positive: bool = False) -> None:
    if not math.isfinite(value) or (positive and value <= 0):
        raise ValueError(f"{name} must be finite" + (" and positive" if positive else ""))


# The engine reads at most this many trailing sessions per security: the stock
# strategy's 121-session lookback plus headroom. Snapshot builders may supply a
# trailing window instead of a whole growing history without changing any
# engine-visible value; the whole prefix costs quadratic time and memory.
ENGINE_TRAILING_WINDOW_SESSIONS = 260


def aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")


def integer(value: int, name: str, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer at least {minimum}")


def boolean(value: bool, name: str) -> None:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be an explicit boolean")


class AssetType(str, Enum):
    STOCK = "stock"
    EQUITY_ETF = "equity_etf"
    BOND_ETF = "bond_etf"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class Security:
    symbol: str
    asset_type: AssetType
    listed_on: date
    buy_lot_size: int = 100
    settlement_sessions: int = 1
    industry_exposure: Mapping[str, float] = field(default_factory=dict)
    tracking_index: str | None = None
    historical_metadata_verified: bool = False
    lookthrough_verified: bool = False
    eligible_for_new_positions: bool = False

    def __post_init__(self):
        if not self.symbol:
            raise ValueError("Invalid security")
        integer(self.buy_lot_size, "buy_lot_size", 1)
        integer(self.settlement_sessions, "settlement_sessions")
        for name in ("historical_metadata_verified", "lookthrough_verified", "eligible_for_new_positions"):
            boolean(getattr(self, name), name)
        if not isinstance(self.asset_type, AssetType):
            raise ValueError("Unknown asset type")
        for value in self.industry_exposure.values():
            finite(value, "industry exposure")
            if value < 0:
                raise ValueError("Industry exposures must be nonnegative")
        if sum(self.industry_exposure.values()) > 1 + 1e-9:
            raise ValueError("Industry exposures exceed one")

    @property
    def equity_exposure(self) -> float:
        return 0.0 if self.asset_type == AssetType.BOND_ETF else 1.0


@dataclass(frozen=True)
class DailyBar:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    turnover: float
    available_at: datetime
    adjusted_close: float | None = None
    tradable: bool = False
    buyable: bool = False
    sellable: bool = False

    def __post_init__(self):
        aware(self.available_at, "bar available_at")
        for name in ("tradable", "buyable", "sellable"):
            boolean(getattr(self, name), name)
        for name in ("open", "high", "low", "close"):
            finite(getattr(self, name), name, positive=True)
        finite(self.turnover, "turnover")
        if self.turnover < 0 or self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("Invalid OHLC or turnover")
        if self.adjusted_close is not None:
            finite(self.adjusted_close, "adjusted_close", positive=True)


@dataclass(frozen=True)
class StockFactorObservation:
    symbol: str
    quality: float | None
    value: float | None
    available_at: datetime
    original_version_verified: bool = False

    def __post_init__(self):
        aware(self.available_at, "factor available_at")
        boolean(self.original_version_verified, "original_version_verified")
        for name in ("quality", "value"):
            if getattr(self, name) is not None:
                finite(getattr(self, name), name)


@dataclass(frozen=True)
class DataSnapshot:
    decision_at: datetime
    securities: Mapping[str, Security]
    histories: Mapping[str, tuple[DailyBar, ...]]
    stock_factors: Mapping[str, tuple[StockFactorObservation, ...]] = field(default_factory=dict)
    listing_age_sessions: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self):
        aware(self.decision_at, "decision_at")
        for age in self.listing_age_sessions.values():
            integer(age, "listing_age_sessions")
        for key, security in self.securities.items():
            if key != security.symbol:
                raise ValueError("Security key does not match its symbol")
        for key, history in self.histories.items():
            days = [bar.trade_date for bar in history]
            if any(bar.symbol != key for bar in history) or days != sorted(set(days)):
                raise ValueError("History must have matching symbols and unique ordered dates")
        for key, observations in self.stock_factors.items():
            if any(item.symbol != key for item in observations):
                raise ValueError("Factor key does not match its symbol")
        # Snapshots are point-in-time evidence. The mappings are copied so that a
        # caller holding the originals cannot change what this snapshot claims to
        # have observed; the memoized observable history below is only sound while
        # that holds. A plain dict copy keeps value equality between snapshots.
        object.__setattr__(self, "histories",
                           {key: tuple(history) for key, history in self.histories.items()})
        object.__setattr__(self, "stock_factors",
                           {key: tuple(items) for key, items in self.stock_factors.items()})
        object.__setattr__(self, "securities", dict(self.securities))
        object.__setattr__(self, "listing_age_sessions", dict(self.listing_age_sessions))
        # The engine asks for the same symbol several times per session, so the
        # observable history is computed once and reused.
        object.__setattr__(self, "_known_history", {})

    def known_history(self, symbol: str) -> tuple[DailyBar, ...]:
        """Bars observable at the decision time, computed once per symbol.

        The trade-date bound is a prefix of the validated ascending dates and
        is located by binary search; a bar published late for an earlier date
        still has to be excluded individually inside that prefix. Results are
        cached because a replay asks for the same symbol repeatedly.
        """
        cached = self._known_history.get(symbol)
        if cached is not None:
            return cached
        history = self.histories.get(symbol, ())
        observable = bisect_right([bar.trade_date for bar in history], self.decision_at.date())
        decision_at = self.decision_at
        known = tuple(bar for bar in history[:observable] if bar.available_at <= decision_at)
        self._known_history[symbol] = known
        return known


@dataclass(frozen=True)
class StrategyTarget:
    strategy_id: str
    decision_at: datetime
    weights: Mapping[str, float]
    reasons: tuple[str, ...] = ()

    def __post_init__(self):
        aware(self.decision_at, "target decision_at")
        for symbol, weight in self.weights.items():
            finite(weight, "target weight")
            if not symbol or weight < 0:
                raise ValueError("Targets must be long-only")
        if sum(self.weights.values()) > 1 + 1e-9:
            raise ValueError("Strategy weights exceed its budget")

    @property
    def cash_weight(self) -> float:
        return max(0.0, 1 - sum(self.weights.values()))


@dataclass(frozen=True)
class RiskConfig:
    stock_cap: float = 0.03
    etf_cap: float = 0.20
    bond_etf_cap: float = 0.15
    industry_cap: float = 0.25
    equity_cap: float = 0.80
    min_cash: float = 0.05
    target_annual_vol: float = 0.12
    annualization: int = 252
    vol_window: int = 60
    recovery_sessions: int = 10

    def __post_init__(self):
        for name in ("stock_cap", "etf_cap", "bond_etf_cap", "industry_cap", "equity_cap", "min_cash", "target_annual_vol"):
            value = getattr(self, name)
            finite(value, name)
            if not 0 <= value <= 1:
                raise ValueError(f"Invalid risk limit: {name}")
        integer(self.annualization, "annualization", 1)
        integer(self.vol_window, "vol_window", 2)
        integer(self.recovery_sessions, "recovery_sessions", 1)


@dataclass
class RiskState:
    high_watermark: float = 0.0
    current_drawdown: float = 0.0
    drawdown_level: int = 0
    recovery_count: int = 0
    locked: bool = False


@dataclass(frozen=True)
class RiskDecision:
    decision_at: datetime
    proposed_weights: Mapping[str, float]
    approved_weights: Mapping[str, float]
    risk_scale: float
    estimated_annual_vol: float | None
    drawdown: float
    locked: bool
    reasons: tuple[str, ...]

    @property
    def cash_weight(self) -> float:
        return max(0.0, 1 - sum(self.approved_weights.values()))


@dataclass(frozen=True)
class FeeConfig:
    """Fixture costs; actual historical fees require a separately verified schedule."""
    commission_rate: float = 0.0003
    minimum_commission: float = 5.0
    stock_sell_tax_rate: float = 0.0005
    slippage_bps: float = 5.0

    def __post_init__(self):
        for name in ("commission_rate", "minimum_commission", "stock_sell_tax_rate", "slippage_bps"):
            finite(getattr(self, name), name)
            if getattr(self, name) < 0:
                raise ValueError("Costs must be nonnegative")


@dataclass(frozen=True)
class Order:
    order_id: str
    symbol: str
    side: Side
    quantity: int
    decision_at: datetime
    execute_on: date
    reference_price: float

    def __post_init__(self):
        aware(self.decision_at, "order decision_at")
        finite(self.reference_price, "reference_price", positive=True)
        integer(self.quantity, "order quantity", 1)
        if not self.order_id or not self.symbol or self.execute_on <= self.decision_at.date() or not isinstance(self.side, Side):
            raise ValueError("Orders need positive quantity and a later execution session")


@dataclass(frozen=True)
class Fill:
    order_id: str
    symbol: str
    side: Side
    trade_date: date
    quantity: int
    price: float
    commission: float
    tax: float

    @property
    def notional(self) -> float:
        return self.quantity * self.price


@dataclass(frozen=True)
class CashDividend:
    symbol: str
    ex_date: date
    pay_date: date
    cash_per_share: float

    def __post_init__(self):
        finite(self.cash_per_share, "cash_per_share")
        if self.pay_date < self.ex_date or self.cash_per_share < 0:
            raise ValueError("Invalid fixture dividend")
