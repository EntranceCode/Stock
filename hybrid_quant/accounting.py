"""A cash account for the independent daily research engine.

Prices and cash flows use unadjusted prices. Adjusted signal prices never enter
the cash ledger. This simulator deliberately has no broker or data-provider API.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_FLOOR
import hashlib
import math
from typing import Mapping, Sequence

from .models import AssetType, CashDividend, DailyBar, DataSnapshot, FeeConfig, Fill, Order, Security, Side


def _decimal(value: float | int) -> Decimal:
    if not math.isfinite(value):
        raise ValueError("Account values must be finite")
    return Decimal(str(value))


def _session(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("session_index must be a nonnegative actual-session index")


@dataclass
class _PositionLot:
    quantity: int
    unlock_session: int


class Account:
    """Long-only holdings, available cash, and unpaid fixture cash dividends.

    Settlement counts actual sessions supplied by the caller, not calendar days.
    An order ID can be attempted once. A rejected or partially filled order is
    never carried into another session; a new close decision needs a new order.
    """

    def __init__(self, initial_cash: float, securities: Mapping[str, Security], fee_config: FeeConfig = FeeConfig()):
        self._cash = _decimal(initial_cash)
        if self._cash <= 0:
            raise ValueError("initial_cash must be positive")
        self.securities = dict(securities)
        if any(key != value.symbol for key, value in self.securities.items()):
            raise ValueError("Security keys must match their symbols")
        self.fee_config = fee_config
        self._lots: dict[str, list[_PositionLot]] = {}
        self._last_prices: dict[str, float] = {}
        self._attempted: set[str] = set()
        self._dividends: dict[tuple[str, date, date, Decimal], dict] = {}
        self._last_dividend_date: date | None = None
        self.execution_events: list[dict] = []
        self.dividend_events: list[dict] = []

    @property
    def cash(self) -> float:
        return float(self._cash)

    @property
    def receivables(self) -> float:
        return float(self._receivable_balance())

    def _receivable_balance(self) -> Decimal:
        return sum((record["amount"] for record in self._dividends.values() if not record["paid"]), Decimal(0))

    @property
    def last_prices(self) -> dict[str, float]:
        return dict(self._last_prices)

    def holdings(self) -> dict[str, int]:
        return {symbol: sum(lot.quantity for lot in lots) for symbol, lots in sorted(self._lots.items()) if lots}

    def available_quantity(self, symbol: str, session_index: int) -> int:
        _session(session_index)
        return sum(lot.quantity for lot in self._lots.get(symbol, ()) if lot.unlock_session <= session_index)

    def nav(self, prices: Mapping[str, float]) -> float:
        """Require a mark for every holding; the runner explicitly supplies stale marks."""
        value = self._cash + self._receivable_balance()
        for symbol, quantity in self.holdings().items():
            if symbol not in prices:
                raise ValueError(f"Missing valuation price for held security {symbol}")
            price = _decimal(prices[symbol])
            if price <= 0:
                raise ValueError("Valuation prices must be positive")
            self._last_prices[symbol] = float(price)
            value += price * quantity
        return float(value)

    def process_dividends(self, trade_date: date, events: Sequence[CashDividend]) -> None:
        """Accrue on ex-date before orders, and release cash on/after pay-date.

        Only quantities actually held at this call are entitled. The caller must
        process every ex-date in its explicit calendar. Identical fixture events
        share an identity because the fixture contract has no provider event ID.
        """
        if self._last_dividend_date is not None and trade_date < self._last_dividend_date:
            raise ValueError("Dividend processing dates must not move backwards")
        self._last_dividend_date = trade_date
        holdings = self.holdings()
        for event in events:
            key = (event.symbol, event.ex_date, event.pay_date, _decimal(event.cash_per_share))
            if event.ex_date == trade_date and key not in self._dividends:
                quantity = holdings.get(event.symbol, 0)
                amount = _decimal(event.cash_per_share) * quantity
                self._dividends[key] = {"amount": amount, "paid": False}
                self.dividend_events.append({"event": "accrued", "trade_date": trade_date.isoformat(),
                                             "symbol": event.symbol, "quantity": quantity, "amount": float(amount),
                                             "ex_date": event.ex_date.isoformat(), "pay_date": event.pay_date.isoformat()})
        for key, record in self._dividends.items():
            if not record["paid"] and key[2] <= trade_date:
                self._cash += record["amount"]
                record["paid"] = True
                self.dividend_events.append({"event": "paid", "trade_date": trade_date.isoformat(),
                                             "symbol": key[0], "amount": float(record["amount"]),
                                             "ex_date": key[1].isoformat(), "pay_date": key[2].isoformat()})

    def _fees(self, notional: Decimal, security: Security, side: Side) -> tuple[Decimal, Decimal]:
        commission = max(_decimal(self.fee_config.minimum_commission), notional * _decimal(self.fee_config.commission_rate))
        tax = notional * _decimal(self.fee_config.stock_sell_tax_rate) if security.asset_type == AssetType.STOCK and side == Side.SELL else Decimal(0)
        return commission, tax

    def _event(self, order: Order, session_index: int, status: str, reason: str, filled: int = 0) -> None:
        self.execution_events.append({"order_id": order.order_id, "symbol": order.symbol, "side": order.side.value,
                                      "execute_on": order.execute_on.isoformat(), "session_index": session_index,
                                      "status": status, "reason": reason, "requested_quantity": order.quantity,
                                      "filled_quantity": filled, "unfilled_quantity": order.quantity - filled})

    def execute(self, order: Order, bar: DailyBar | None, session_index: int) -> Fill | None:
        _session(session_index)
        if order.order_id in self._attempted:
            self._event(order, session_index, "rejected", "order_already_attempted")
            return None
        self._attempted.add(order.order_id)
        reason = None
        security = self.securities.get(order.symbol)
        if isinstance(order.quantity, bool) or not isinstance(order.quantity, int):
            reason = "quantity_must_be_integer"
        elif security is None or not security.historical_metadata_verified:
            reason = "execution_rules_unverified"
        elif bar is None:
            reason = "missing_execution_bar"
        elif bar.symbol != order.symbol or bar.trade_date != order.execute_on:
            reason = "wrong_execution_session"
        elif bar.trade_date < security.listed_on:
            reason = "security_not_yet_listed"
        elif not bar.tradable:
            reason = "not_tradable"
        elif order.side == Side.BUY and (not bar.buyable or not security.eligible_for_new_positions):
            reason = "not_buyable"
        elif order.side == Side.SELL and not bar.sellable:
            reason = "not_sellable"
        if reason:
            self._event(order, session_index, "rejected", reason)
            return None
        assert security is not None and bar is not None
        lot_size = security.buy_lot_size
        if order.side == Side.BUY and order.quantity % lot_size:
            self._event(order, session_index, "rejected", "invalid_buy_lot")
            return None
        if order.side == Side.SELL:
            held = self.holdings().get(order.symbol, 0)
            if order.quantity > self.available_quantity(order.symbol, session_index):
                self._event(order, session_index, "rejected", "insufficient_settled_quantity")
                return None
            if order.quantity % lot_size and order.quantity != held:
                self._event(order, session_index, "rejected", "invalid_partial_sell_lot")
                return None
        slip = _decimal(self.fee_config.slippage_bps) / Decimal(10000)
        price = _decimal(bar.open) * (1 + slip if order.side == Side.BUY else 1 - slip)
        if price <= 0:
            self._event(order, session_index, "rejected", "nonpositive_slipped_price")
            return None
        quantity = order.quantity
        if order.side == Side.BUY:
            # Both proportional costs and minimum commission have to fit. This
            # bound avoids a potentially huge one-lot-at-a-time decrement loop.
            rate = _decimal(self.fee_config.commission_rate)
            minimum = _decimal(self.fee_config.minimum_commission)
            notional_limit = min(self._cash / (1 + rate), max(Decimal(0), self._cash - minimum))
            affordable_lots = int((notional_limit / price / lot_size).to_integral_value(rounding=ROUND_FLOOR))
            quantity = min(quantity, affordable_lots * lot_size)
            if quantity <= 0:
                self._event(order, session_index, "rejected", "insufficient_cash_for_one_lot_and_fees")
                return None
        notional = price * quantity
        commission, tax = self._fees(notional, security, order.side)
        if order.side == Side.BUY:
            total_cost = notional + commission + tax
            if total_cost > self._cash:
                raise AssertionError("Affordability calculation violated cash accounting")
            self._cash -= total_cost
            self._lots.setdefault(order.symbol, []).append(_PositionLot(quantity, session_index + security.settlement_sessions))
        else:
            proceeds = notional - commission - tax
            if self._cash + proceeds < 0:
                self._event(order, session_index, "rejected", "insufficient_cash_for_sale_fees")
                return None
            remaining = quantity
            for lot in self._lots.get(order.symbol, ()):
                if lot.unlock_session <= session_index:
                    removed = min(remaining, lot.quantity)
                    lot.quantity -= removed
                    remaining -= removed
                if not remaining:
                    break
            if remaining:
                raise AssertionError("Sale exceeded settled holdings")
            self._lots[order.symbol] = [lot for lot in self._lots[order.symbol] if lot.quantity]
            self._cash += proceeds
        self._last_prices[order.symbol] = float(price)
        status = "filled" if quantity == order.quantity else "partially_filled_and_expired"
        self._event(order, session_index, status, "executed" if quantity == order.quantity else "cash_limited", quantity)
        return Fill(order.order_id, order.symbol, order.side, bar.trade_date, quantity, float(price), float(commission), float(tax))


def plan_orders(weights: Mapping[str, float], snapshot: DataSnapshot, account: Account,
                next_session: date, session_index: int, participation_rate: float = 0.01) -> tuple[Order, ...]:
    """Fix requested quantities at the decision close; never inspect next open.

    The decision-session index is incremented once for the explicitly supplied
    next actual session. No order is created for stale or missing current bars.
    Capacity is the last 20 known session turnovers, marked at this close.
    """
    _session(session_index)
    if next_session <= snapshot.decision_at.date():
        raise ValueError("next_session must follow the decision date")
    if not math.isfinite(participation_rate) or not 0 < participation_rate <= 1:
        raise ValueError("participation_rate must be in (0, 1]")
    if any(not math.isfinite(weight) or weight < 0 for weight in weights.values()) or sum(weights.values()) > 1 + 1e-9:
        raise ValueError("Order weights must be long-only without leverage")
    holdings = account.holdings()
    histories = {symbol: snapshot.known_history(symbol) for symbol in set(holdings) | set(weights)}
    prices = account.last_prices
    for symbol, history in histories.items():
        if history and (history[-1].trade_date == snapshot.decision_at.date() or symbol not in prices):
            prices[symbol] = history[-1].close
    net_assets = account.nav(prices)
    orders = []
    for symbol in sorted(set(holdings) | set(weights)):
        history = histories[symbol]
        security = snapshot.securities.get(symbol)
        if security is None or not security.historical_metadata_verified or len(history) < 20:
            continue
        bar = history[-1]
        if bar.trade_date != snapshot.decision_at.date() or not bar.tradable:
            continue
        lot = security.buy_lot_size
        held = holdings.get(symbol, 0)
        weight = weights.get(symbol, 0.0)
        target = int((_decimal(net_assets) * _decimal(weight) / _decimal(bar.close) / lot).to_integral_value(rounding=ROUND_FLOOR)) * lot
        difference = target - held
        if difference == 0:
            continue
        capacity = sum(_decimal(item.turnover) for item in history[-20:]) / 20 * _decimal(participation_rate)
        capacity_quantity = int((capacity / _decimal(bar.close)).to_integral_value(rounding=ROUND_FLOOR))
        side = Side.BUY if difference > 0 else Side.SELL
        if side == Side.BUY:
            if not bar.buyable or not security.eligible_for_new_positions:
                continue
            quantity = min(difference, capacity_quantity) // lot * lot
        else:
            if not bar.sellable:
                continue
            available = account.available_quantity(symbol, session_index + 1)
            quantity = min(-difference, available, capacity_quantity)
            if not (weight == 0 and quantity == held):
                quantity = quantity // lot * lot
        if quantity <= 0:
            continue
        identity = f"{snapshot.decision_at.isoformat()}|{next_session.isoformat()}|{symbol}|{side.value}|{quantity}"
        order_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
        orders.append(Order(order_id, symbol, side, quantity, snapshot.decision_at, next_session, bar.close))
    return tuple(sorted(orders, key=lambda order: (order.side == Side.BUY, order.symbol)))
