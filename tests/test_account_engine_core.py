"""Meaningful account and replay invariants, using synthetic fixture prices."""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from unittest import TestCase
from unittest.mock import patch

from hybrid_quant.accounting import Account, plan_orders
from hybrid_quant.backtest import run_backtest
from hybrid_quant.models import (AssetType, CashDividend, DailyBar, DataSnapshot, FeeConfig,
                                 Order, RiskConfig, Security, Side, StockFactorObservation, StrategyTarget)


TZ = timezone(timedelta(hours=8))
FREE = FeeConfig(0, 0, 0, 0)


def security(symbol="A", asset=AssetType.STOCK, settlement=1, *, verified=True):
    return Security(symbol, asset, date(2000, 1, 1), settlement_sessions=settlement,
                    historical_metadata_verified=verified, eligible_for_new_positions=True,
                    lookthrough_verified=True, industry_exposure={"industry": 1} if asset != AssetType.BOND_ETF else {})


def bar(symbol, day, price=10.0, *, opening=None, tradable=True, buyable=True, sellable=True, turnover=1e8):
    opening = price if opening is None else opening
    return DailyBar(symbol, day, opening, max(opening, price), min(opening, price), price, turnover,
                    datetime.combine(day, datetime.min.time(), TZ).replace(hour=16), adjusted_close=price,
                    tradable=tradable, buyable=buyable, sellable=sellable)


def order(identifier, symbol, side, quantity, day, reference=10.0):
    return Order(identifier, symbol, side, quantity,
                 datetime.combine(day - timedelta(days=1), datetime.min.time(), TZ).replace(hour=18), day, reference)


def snapshot(day, securities, current_bars, *, histories=None, factors=None):
    if histories is None:
        histories = {}
        for symbol, current in current_bars.items():
            previous = tuple(bar(symbol, day - timedelta(days=offset), current.close)
                             for offset in range(65, 0, -1))
            histories[symbol] = previous + (current,)
    return DataSnapshot(datetime.combine(day, datetime.min.time(), TZ).replace(hour=18), securities, histories,
                        factors or {}, {symbol: 300 for symbol in securities})


class AccountTests(TestCase):
    def test_open_price_commission_stock_sell_tax_and_no_adjusted_cash_price(self):
        sec = security()
        account = Account(5000, {"A": sec}, FeeConfig(.0003, 5, .0005, 0))
        day = date(2024, 1, 5)
        bought = account.execute(order("buy", "A", Side.BUY, 100, day, reference=10),
                                 bar("A", day, price=9, opening=12), 0)
        self.assertEqual((bought.quantity, bought.price, bought.commission, bought.tax), (100, 12, 5, 0))
        self.assertEqual(account.cash, 3795)
        sold = account.execute(order("sell", "A", Side.SELL, 100, date(2024, 1, 8)),
                                bar("A", date(2024, 1, 8), price=11, opening=12), 1)
        self.assertEqual(sold.tax, .6)
        self.assertAlmostEqual(account.cash, 4989.4)
        self.assertEqual(account.holdings(), {})

    def test_cash_limited_fill_reduces_lots_never_borrows_and_order_expires(self):
        account = Account(2005, {"A": security()}, FeeConfig(0, 5, 0, 0))
        day = date(2024, 1, 5)
        requested = order("cash-gap", "A", Side.BUY, 500, day, reference=4)
        fill = account.execute(requested, bar("A", day), 0)
        self.assertEqual(fill.quantity, 200)
        self.assertEqual(account.cash, 0)
        self.assertEqual(account.execution_events[-1]["status"], "partially_filled_and_expired")
        self.assertIsNone(account.execute(requested, bar("A", date(2024, 1, 8)), 1))
        self.assertEqual(account.holdings(), {"A": 200})

    def test_settlement_uses_actual_sessions_and_prevents_overselling(self):
        account = Account(5000, {"A": security()}, FREE)
        friday = date(2024, 1, 5)
        account.execute(order("buy", "A", Side.BUY, 100, friday), bar("A", friday), 2)
        self.assertEqual(account.available_quantity("A", 2), 0)
        self.assertIsNone(account.execute(order("same", "A", Side.SELL, 100, friday), bar("A", friday), 2))
        monday = date(2024, 1, 8)
        self.assertEqual(account.available_quantity("A", 3), 100)
        self.assertIsNone(account.execute(order("too-many", "A", Side.SELL, 200, monday), bar("A", monday), 3))
        self.assertIsNotNone(account.execute(order("settled", "A", Side.SELL, 100, monday), bar("A", monday), 3))

    def test_bond_same_session_settlement_and_no_stock_tax(self):
        account = Account(5000, {"B": security("B", AssetType.BOND_ETF, 0)}, FeeConfig(0, 0, .01, 0))
        day = date(2024, 1, 5)
        account.execute(order("buy", "B", Side.BUY, 100, day), bar("B", day), 0)
        fill = account.execute(order("sell", "B", Side.SELL, 100, day), bar("B", day), 0)
        self.assertEqual(fill.tax, 0)
        self.assertEqual(account.cash, 5000)

    def test_unverified_rules_halt_and_mismatched_day_are_rejected(self):
        day = date(2024, 1, 5)
        account = Account(5000, {"A": security(verified=False)}, FREE)
        self.assertIsNone(account.execute(order("unverified", "A", Side.BUY, 100, day), bar("A", day), 0))
        account.securities["A"] = security()
        self.assertIsNone(account.execute(order("halt", "A", Side.BUY, 100, day), bar("A", day, tradable=False), 0))
        self.assertIsNone(account.execute(order("date", "A", Side.BUY, 100, day), bar("A", day + timedelta(days=1)), 1))
        self.assertEqual(account.cash, 5000)

    def test_dividends_preserve_entitlement_cash_timing_and_are_idempotent(self):
        account = Account(5000, {"A": security()}, FREE)
        bought_on = date(2024, 1, 4)
        account.execute(order("buy", "A", Side.BUY, 100, bought_on), bar("A", bought_on), 0)
        ex = date(2024, 1, 5)
        event = CashDividend("A", ex, date(2024, 1, 8), .5)
        account.process_dividends(ex, (event, event))
        self.assertEqual(account.cash, 4000)
        self.assertEqual(account.receivables, 50)
        self.assertEqual(account.nav({"A": 9.5}), 5000)
        account.execute(order("sell", "A", Side.SELL, 100, ex), bar("A", ex, price=9.5), 1)
        account.process_dividends(ex, (event,))
        self.assertEqual(account.receivables, 50)
        account.process_dividends(event.pay_date, (event,))
        account.process_dividends(event.pay_date, (event,))
        self.assertEqual(account.cash, 5000)
        self.assertEqual(account.receivables, 0)
        self.assertEqual([item["event"] for item in account.dividend_events], ["accrued", "paid"])

    def test_nav_cannot_silently_omit_a_holding(self):
        account = Account(5000, {"A": security()}, FREE)
        day = date(2024, 1, 5)
        account.execute(order("buy", "A", Side.BUY, 100, day), bar("A", day), 0)
        with self.assertRaisesRegex(ValueError, "Missing valuation"):
            account.nav({})

    def test_plan_uses_close_and_known_capacity_without_future_open(self):
        day = date(2024, 1, 5)
        account = Account(10000, {"A": security()}, FREE)
        known = tuple(bar("A", day - timedelta(days=offset), turnover=1e6) for offset in range(19, -1, -1))
        snap = snapshot(day, account.securities, {}, histories={"A": known})
        planned = plan_orders({"A": 1}, snap, account, date(2024, 1, 8), 0)
        self.assertEqual((planned[0].quantity, planned[0].reference_price), (1000, 10))
        fill = account.execute(planned[0], bar("A", date(2024, 1, 8), price=20, opening=20), 1)
        self.assertEqual(fill.quantity, 500)
        self.assertEqual(planned[0].quantity, 1000)
        self.assertEqual(account.cash, 0)
        self.assertEqual(planned, plan_orders({"A": 1}, snap, Account(10000, account.securities, FREE), date(2024, 1, 8), 0))

    def test_stale_bars_do_not_generate_orders(self):
        day = date(2024, 1, 5)
        account = Account(10000, {"A": security()}, FREE)
        stale = tuple(bar("A", day - timedelta(days=offset)) for offset in range(20, 0, -1))
        snap = snapshot(day, account.securities, {}, histories={"A": stale})
        self.assertEqual(plan_orders({"A": 1}, snap, account, date(2024, 1, 8), 0), ())

    def test_planning_stale_history_preserves_the_newer_account_mark(self):
        day = date(2024, 1, 5)
        securities = {"A": security("A"), "B": security("B")}
        account = Account(10000, securities, FREE)
        previous = day - timedelta(days=1)
        account.execute(order("seed", "A", Side.BUY, 100, previous), bar("A", previous), 0)
        self.assertEqual(account.nav({"A": 12}), 10200)
        histories = {"A": tuple(bar("A", day - timedelta(days=offset), price=10) for offset in range(20, 0, -1)),
                     "B": tuple(bar("B", day - timedelta(days=offset), price=10) for offset in range(19, -1, -1))}
        snap = snapshot(day, securities, {}, histories=histories)
        planned = plan_orders({"B": .995}, snap, account, date(2024, 1, 8), 1)
        self.assertEqual(next(item.quantity for item in planned if item.symbol == "B"), 1000)
        self.assertEqual(account.last_prices["A"], 12)
        self.assertEqual(account.nav({"A": 12}), 10200)


class ReplayTests(TestCase):
    def setUp(self):
        self.securities = {"A": security("A", AssetType.BOND_ETF), "B": security("B", AssetType.BOND_ETF)}
        self.risk = RiskConfig(stock_cap=1, etf_cap=1, bond_etf_cap=1, industry_cap=1,
                               equity_cap=1, min_cash=0)
        self.budgets = {"stock": 0, "etf_rotation": 1, "defensive": 0}

    @staticmethod
    def empty_stock(snap, **kwargs):
        return StrategyTarget("stock", snap.decision_at, {})

    @staticmethod
    def empty_defense(snap):
        return StrategyTarget("defensive", snap.decision_at, {})

    def run_fixture(self, snaps, etf):
        with patch("hybrid_quant.backtest.stock_target", self.empty_stock), \
             patch("hybrid_quant.backtest.defensive_target", self.empty_defense), \
             patch("hybrid_quant.backtest.etf_target", etf):
            return run_backtest(tuple(snaps), 10000, FREE, self.risk, budgets=self.budgets)

    def test_signals_fill_next_open_and_gap_does_not_reset_requested_quantity(self):
        first = date(2024, 1, 4)
        second = date(2024, 1, 5)
        snaps = [snapshot(first, self.securities, {"A": bar("A", first)}),
                 snapshot(second, self.securities, {"A": bar("A", second, price=18, opening=20)})]
        result = self.run_fixture(snaps, lambda snap: StrategyTarget("etf_rotation", snap.decision_at, {"A": 1}))
        self.assertEqual(result["equity"][0]["holdings"], {})
        self.assertEqual(result["orders"][0]["quantity"], 1000)
        self.assertEqual((result["fills"][0]["trade_date"], result["fills"][0]["price"], result["fills"][0]["quantity"]),
                         (second.isoformat(), 20, 500))
        self.assertEqual(result["equity"][1]["nav"], 9000)
        json.dumps(result, allow_nan=False)

    def test_sales_release_cash_before_buys_and_weekly_refresh_uses_calendar(self):
        days = [date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2), date(2024, 2, 5)]
        snaps = [snapshot(day, self.securities, {symbol: bar(symbol, day) for symbol in self.securities}) for day in days]
        result = self.run_fixture(snaps, lambda snap: StrategyTarget("etf_rotation", snap.decision_at,
                                                                    {"A" if snap.decision_at.date() < days[2] else "B": 1}))
        last_fills = [fill for fill in result["fills"] if fill["trade_date"] == days[-1].isoformat()]
        self.assertEqual([(fill["symbol"], fill["side"]) for fill in last_fills], [("A", "sell"), ("B", "buy")])
        self.assertEqual(result["equity"][-1]["holdings"], {"B": 1000})
        self.assertEqual(result["decisions"][1]["refreshed_strategies"], [])
        self.assertIn("etf_rotation", result["decisions"][2]["refreshed_strategies"])

    def test_missing_held_bar_is_stale_but_keeps_full_nav(self):
        days = [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]
        snaps = [snapshot(day, self.securities, {symbol: bar(symbol, day) for symbol in self.securities}) for day in days]
        histories = dict(snaps[-1].histories)
        histories["A"] = snaps[1].histories["A"]
        snaps[-1] = snapshot(days[-1], self.securities, {}, histories=histories)
        result = self.run_fixture(snaps, lambda snap: StrategyTarget("etf_rotation", snap.decision_at, {"A": 1}))
        self.assertEqual(result["equity"][-1]["holdings"], {"A": 1000})
        self.assertEqual(result["equity"][-1]["nav"], 10000)
        self.assertEqual(result["equity"][-1]["stale_symbols"], ["A"])
        self.assertTrue(result["decisions"][-1]["unexecutable_reductions"])

    def test_unordered_or_duplicate_session_dates_are_rejected(self):
        one = snapshot(date(2024, 1, 4), self.securities, {"A": bar("A", date(2024, 1, 4))})
        two = snapshot(date(2024, 1, 5), self.securities, {"A": bar("A", date(2024, 1, 5))})
        for snaps in ((two, one), (one, one), (one,)):
            with self.assertRaises(ValueError):
                run_backtest(snaps)

    def test_future_factors_are_counted_and_never_pass_as_known_signals(self):
        days = [date(2024, 1, 4), date(2024, 1, 5)]
        observation = StockFactorObservation("A", 999, 999, datetime(2024, 2, 1, 18, tzinfo=TZ), True)
        snaps = [snapshot(day, self.securities, {"A": bar("A", day)}, factors={"A": (observation,)}) for day in days]
        result = run_backtest(tuple(snaps), 10000, FREE, self.risk, budgets=self.budgets)
        self.assertEqual([decision["filtered_future_factor_observations"] for decision in result["decisions"]], [1, 1])
        self.assertEqual(result["decisions"][0]["strategy_targets"]["stock"]["weights"], {})

    def test_stock_buffer_receives_actual_fills_not_unfilled_intentions(self):
        securities = {"A": security("A"), "B": security("B")}
        days = [date(2024, 1, 30), date(2024, 1, 31), date(2024, 2, 1)]
        snaps = [snapshot(day, securities, {"A": bar("A", day), "B": bar("B", day, buyable=index != 1)})
                 for index, day in enumerate(days)]
        calls = []

        def stock(snap, current_symbols=(), **kwargs):
            calls.append(tuple(current_symbols))
            return StrategyTarget("stock", snap.decision_at, {"A": .5, "B": .5})

        with patch("hybrid_quant.backtest.stock_target", stock), \
             patch("hybrid_quant.backtest.etf_target", lambda snap: StrategyTarget("etf_rotation", snap.decision_at, {})), \
             patch("hybrid_quant.backtest.defensive_target", self.empty_defense):
            result = run_backtest(tuple(snaps), 10000, FREE, self.risk,
                                  budgets={"stock": 1, "etf_rotation": 0, "defensive": 0})
        self.assertEqual(calls[0], ())
        self.assertEqual(calls[1], ("A",))
        self.assertEqual(result["equity"][1]["holdings"], {"A": 500})

    def test_unknown_held_exposure_blocks_other_new_risk_but_keeps_exit(self):
        days = [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)]
        for unknown_metadata in (False, True):
            with self.subTest(unknown_metadata=unknown_metadata):
                securities = {"A": security("A", AssetType.EQUITY_ETF),
                              "B": security("B", AssetType.BOND_ETF if unknown_metadata else AssetType.EQUITY_ETF)}
                unknown = dict(securities)
                unknown["A"] = replace(securities["A"], historical_metadata_verified=not unknown_metadata,
                                       lookthrough_verified=False)
                snaps = [snapshot(day, securities if index < 2 else unknown,
                                  {symbol: bar(symbol, day) for symbol in securities}) for index, day in enumerate(days)]
                result = self.run_fixture(snaps, lambda snap: StrategyTarget("etf_rotation", snap.decision_at,
                                                                            {"A" if snap.decision_at.date() < days[2] else "B": 1}))
                second_orders = [item for item in result["orders"] if item["decision_at"].startswith(days[2].isoformat())]
                self.assertFalse(any(item["symbol"] == "B" and item["side"] == "buy" for item in second_orders))
                self.assertTrue(any("unknown_actual" in reason for reason in result["decisions"][2]["reasons"]))
                if not unknown_metadata:
                    self.assertTrue(any(item["symbol"] == "A" and item["side"] == "sell" for item in second_orders))
