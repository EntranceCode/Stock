"""Shared contracts for decisions, risk, and the simulated account.

The contract field sets follow the implementation design's stage-1 revision
(``docs/implementation-design.md`` §3): required fields are derived from the
stage-0 supply matrix, never from what a caller happens to have. In particular:

* A required field must be constructible from confirmed supply (§3.1 rule 1),
  so ``DailyBar.adjusted_close`` is required (both price paths supply it) while
  ``DailyBar.volume`` stays explicitly optional until decision **D3** fixes the
  share/lot caliber and an ETF-side provider exists.
* Unknown and zero stay distinguishable (§3.1 rule 3): an unsupplied field is
  ``None``, never a zero-filled guess.
* ``Security`` carries the SecurityMaster identity the design asks for
  (symbol, exchange, asset_type, list/delist dates); ``Fundamental``,
  ``CorporateAction`` and ``Constituent`` formalize the remaining core objects.
"""
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


# Listing venues the first version covers. The Beijing Stock Exchange is out of
# scope for v1 (design §1.3), so an exchange outside this set is a contract
# error rather than a silently accepted value.
EXCHANGES = ("SSE", "SZSE")


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class Security:
    """SecurityMaster identity (design §3.2): symbol, exchange, asset_type, list/delist dates.

    ``delist_date`` is ``None`` when no termination candidate is recorded; an
    unknown boundary is therefore never written as a date. The boundary itself
    is a listing/termination fact, not the last tradable day (a delisting
    compiles a final trading window), which is why tradability lives on the
    bar side rather than here.
    """

    symbol: str
    asset_type: AssetType
    listed_on: date
    exchange: str
    delist_date: date | None = None
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
        if not isinstance(self.exchange, str) or self.exchange not in EXCHANGES:
            raise ValueError(f"exchange must be one of {', '.join(EXCHANGES)}")
        if self.delist_date is not None:
            if not isinstance(self.delist_date, date):
                raise ValueError("delist_date must be a date or None")
            if self.delist_date < self.listed_on:
                raise ValueError("delist_date cannot precede the listing date")
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
    """One session's bar (design §3.2): trade_date, OHLC, volume, turnover,
    available_at, adjusted_close are the contract fields.

    ``adjusted_close`` is required: both registered price paths (the adjusted
    derivative and the ETF evidence product) supply it, so a bar without one is
    excluded at build time instead of defaulted. ``volume`` is the exception
    the supply matrix dictates: its share/lot caliber is still decision
    **D3** and no ETF-side provider exists, so it stays optional with an
    explicit ``None`` for "not supplied, never inferred" rather than a required
    field no source could fill (§3.1 rule 1). The tradability flags stay
    degraded booleans until decision **D4** turns them into derivation rules.
    """

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    turnover: float
    available_at: datetime
    adjusted_close: float
    volume: int | None = None
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
        finite(self.adjusted_close, "adjusted_close", positive=True)
        if self.volume is not None:
            integer(self.volume, "volume", 0)
        if self.turnover < 0 or self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("Invalid OHLC or turnover")


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
class Fundamental:
    """Financial contract (design §3.2): symbol, report period, announced_at,
    available_at, value and the caliber version decision **D2** attaches.

    ``first_disclosure_verified`` records whether the value is the original
    announcement vintage; until decision **D2** is settled it stays False, and
    the strategy layer must treat unverified versions as unusable rather than
    as the provider's latest restatement.
    """

    symbol: str
    report_period: date
    announced_at: date
    available_at: datetime
    value: float
    caliber_version: str
    first_disclosure_verified: bool = False

    def __post_init__(self):
        if not self.symbol:
            raise ValueError("Invalid fundamental")
        if not isinstance(self.report_period, date) or not isinstance(self.announced_at, date):
            raise ValueError("Report and announcement must be dates")
        aware(self.available_at, "fundamental available_at")
        if self.announced_at > self.available_at.date():
            raise ValueError("A fundamental cannot be available before it is announced")
        finite(self.value, "fundamental value")
        if not isinstance(self.caliber_version, str) or not self.caliber_version:
            raise ValueError("caliber_version must declare the D2 version/口径 of this value")
        boolean(self.first_disclosure_verified, "first_disclosure_verified")


@dataclass(frozen=True)
class CorporateAction:
    """Corporate-action contract (design §3.2): symbol, kind, ex_date, share
    change and cash flow split by caliber.

    The three cash calibers of a dividend (common account, ex-reference price,
    special shareholders) never merge into one number (design §4.2): a record
    carries exactly one ``cash_basis`` when it carries cash at all. ``pay_date``
    stays optional until decision **D6** settles the arrival rule, and a missing
    pay date is ``None``, not the ex date.
    """

    symbol: str
    kind: str
    ex_date: date
    share_change: float | None = None
    cash_per_share: float | None = None
    cash_basis: str | None = None
    pay_date: date | None = None

    def __post_init__(self):
        if not self.symbol:
            raise ValueError("Invalid corporate action")
        if not isinstance(self.kind, str) or not self.kind:
            raise ValueError("kind must name the action type")
        if not isinstance(self.ex_date, date):
            raise ValueError("ex_date must be a date")
        for name in ("share_change", "cash_per_share"):
            value = getattr(self, name)
            if value is not None:
                finite(value, name)
                if value < 0:
                    raise ValueError(f"{name} must be nonnegative")
        if self.cash_per_share is not None and (not isinstance(self.cash_basis, str) or not self.cash_basis):
            raise ValueError("cash_per_share requires an explicit cash_basis")
        if self.cash_basis is not None and self.cash_per_share is None:
            raise ValueError("cash_basis without cash_per_share declares a caliber with no value")
        if self.pay_date is not None:
            if not isinstance(self.pay_date, date):
                raise ValueError("pay_date must be a date or None")
            if self.pay_date < self.ex_date:
                raise ValueError("pay_date cannot precede the ex date")


@dataclass(frozen=True)
class Constituent:
    """ETF constituent contract (design §3.2/§9): ETF, tracked index,
    effective_at, available_at, with weights degraded.

    No source provides constituents and weights (supply state **C**), so
    ``weights`` is an explicit ``None`` - "not provided" - instead of a single
    default bucket that would fake a look-through exposure.
    """

    etf_symbol: str
    index_symbol: str
    effective_at: date
    available_at: datetime
    weights: Mapping[str, float] | None = None

    def __post_init__(self):
        if not self.etf_symbol or not self.index_symbol:
            raise ValueError("Invalid constituent")
        if not isinstance(self.effective_at, date):
            raise ValueError("effective_at must be a date")
        aware(self.available_at, "constituent available_at")
        if self.weights is not None:
            for symbol, weight in self.weights.items():
                if not symbol:
                    raise ValueError("Constituent symbols must be nonempty")
                finite(weight, "constituent weight")
                if weight < 0:
                    raise ValueError("Constituent weights must be nonnegative")
            if sum(self.weights.values()) > 1 + 1e-9:
                raise ValueError("Constituent weights exceed one")


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
