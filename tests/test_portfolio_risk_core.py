from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import math
import unittest

from hybrid_quant.models import (
    AssetType, DailyBar, DataSnapshot, RiskConfig, RiskState, Security, StrategyTarget,
)
from hybrid_quant.portfolio import apply_risk, merge_targets, update_drawdown


DECISION_AT = datetime(2026, 7, 7, 16, tzinfo=timezone(timedelta(hours=8)))


def security(symbol, asset_type=AssetType.EQUITY_ETF, industry="technology"):
    return Security(
        symbol, asset_type, date(2010, 1, 1), "SSE",
        industry_exposure={} if asset_type == AssetType.BOND_ETF else {industry: 1.0},
        historical_metadata_verified=True, lookthrough_verified=True,
        eligible_for_new_positions=True,
    )


def history(symbol, daily_moves=None, count=61):
    prices = [100.0]
    for i in range(count - 1):
        move = daily_moves[i] if daily_moves is not None else (0.01 if i % 2 else -0.01)
        prices.append(prices[-1] * (1 + move))
    days = []
    cursor = DECISION_AT.date()
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return tuple(
        DailyBar(
            symbol, day, price, price, price, price, 1000000,
            datetime.combine(day, DECISION_AT.timetz()),
            adjusted_close=price, tradable=True, buyable=True, sellable=True,
        )
        for day, price in zip(reversed(days), prices)
    )


def snapshot(*securities, histories=None):
    return DataSnapshot(
        DECISION_AT, {item.symbol: item for item in securities},
        histories or {item.symbol: history(item.symbol) for item in securities},
    )


class TargetMergeTests(unittest.TestCase):
    def test_shared_symbol_and_old_targets_keep_unspent_budget_in_cash(self):
        targets = (
            StrategyTarget("stock", DECISION_AT - timedelta(days=20), {"shared": 0.04}),
            StrategyTarget("etf_rotation", DECISION_AT, {"shared": 0.5}),
        )
        result = merge_targets(targets, {"stock": 0.4, "etf_rotation": 0.35, "defensive": 0.25}, DECISION_AT)
        self.assertAlmostEqual(result["shared"], 0.191)
        self.assertAlmostEqual(1 - sum(result.values()), 0.809)

    def test_duplicate_strategy_future_and_unconfigured_targets_rejected(self):
        target = StrategyTarget("stock", DECISION_AT, {"s": 0.1})
        for targets in (
            (target, target),
            (replace(target, decision_at=DECISION_AT + timedelta(seconds=1)),),
            (replace(target, strategy_id="unconfigured"),),
        ):
            with self.assertRaises(ValueError):
                merge_targets(targets, {"stock": 0.4}, DECISION_AT)

    def test_negative_nonfinite_and_overallocated_budgets_rejected(self):
        for budgets in ({"s": -0.1}, {"s": float("nan")}, {"s": 0.6, "e": 0.5}):
            with self.assertRaises(ValueError):
                merge_targets((), budgets, DECISION_AT)


class DrawdownTests(unittest.TestCase):
    def test_thresholds_and_high_watermark_are_preserved(self):
        for nav, level in ((92, 1), (88, 2), (84, 3), (80, 4)):
            state = RiskState()
            update_drawdown(state, 100, RiskConfig())
            self.assertAlmostEqual(update_drawdown(state, nav, RiskConfig()), 1 - nav / 100)
            self.assertEqual(state.drawdown_level, level)
            self.assertEqual(state.high_watermark, 100)
        state = RiskState()
        update_drawdown(state, 100, RiskConfig())
        update_drawdown(state, 110, RiskConfig())
        update_drawdown(state, 105, RiskConfig())
        self.assertEqual(state.high_watermark, 110)

    def test_recovery_requires_consecutive_sessions_and_one_level_at_a_time(self):
        state = RiskState()
        config = RiskConfig()
        update_drawdown(state, 100, config)
        update_drawdown(state, 84, config)
        for _ in range(9):
            update_drawdown(state, 95, config)
        self.assertEqual(state.drawdown_level, 3)
        update_drawdown(state, 86, config)  # Exactly 14% does not recover level 3.
        self.assertEqual(state.recovery_count, 0)
        for _ in range(10):
            update_drawdown(state, 95, config)
        self.assertEqual(state.drawdown_level, 2)
        self.assertEqual(state.recovery_count, 0)
        for _ in range(10):
            update_drawdown(state, 95, config)
        self.assertEqual(state.drawdown_level, 1)
        for _ in range(10):
            update_drawdown(state, 95, config)
        self.assertEqual(state.drawdown_level, 0)
        self.assertEqual(state.high_watermark, 100)

    def test_twenty_percent_lock_never_recovers_automatically(self):
        state = RiskState()
        update_drawdown(state, 100, RiskConfig())
        update_drawdown(state, 80, RiskConfig())
        for _ in range(30):
            update_drawdown(state, 110, RiskConfig())
        self.assertTrue(state.locked)
        self.assertEqual(state.drawdown_level, 4)
        self.assertEqual(state.high_watermark, 110)
        self.assertEqual(state.current_drawdown, 0)

    def test_invalid_nav_is_rejected_without_state_mutation(self):
        for nav in (0, -1, float("nan"), float("inf")):
            state = RiskState(high_watermark=100)
            with self.assertRaises(ValueError):
                update_drawdown(state, nav, RiskConfig())
            self.assertEqual(state.high_watermark, 100)


class PortfolioRiskTests(unittest.TestCase):
    def test_scales_use_min_and_account_weighted_volatility(self):
        sec = security("e")
        snap = snapshot(sec)
        state = RiskState()
        update_drawdown(state, 100, RiskConfig())
        update_drawdown(state, 90, RiskConfig())
        baseline = apply_risk({"e": 0.4}, snap, RiskState(), RiskConfig(etf_cap=1, industry_cap=1))
        measured_vol = baseline.estimated_annual_vol
        cfg = RiskConfig(etf_cap=1, industry_cap=1, target_annual_vol=measured_vol * 0.6)
        decision = apply_risk({"e": 0.4}, snap, state, cfg)
        self.assertAlmostEqual(decision.risk_scale, 0.6)
        self.assertAlmostEqual(decision.approved_weights["e"], 0.24)
        self.assertNotAlmostEqual(decision.approved_weights["e"], 0.4 * 0.6 * 0.8)
        unit = apply_risk({"e": 0.8}, snap, RiskState(), RiskConfig(etf_cap=1, industry_cap=1))
        self.assertAlmostEqual(unit.estimated_annual_vol, measured_vol * 2)

    def test_stock_and_etf_industry_exposures_share_one_cap(self):
        s, e, b = security("s", AssetType.STOCK), security("e"), security("b", AssetType.BOND_ETF)
        decision = apply_risk(
            {"s": 0.15, "e": 0.20, "b": 0.25}, snapshot(s, e, b), RiskState(),
            RiskConfig(stock_cap=1, target_annual_vol=1),
        )
        self.assertAlmostEqual(decision.approved_weights["s"] + decision.approved_weights["e"], 0.25)
        self.assertAlmostEqual(decision.approved_weights["b"], 0.15)
        self.assertAlmostEqual(decision.cash_weight, 0.60)
        self.assertIn("industry_cap:technology", decision.reasons)

    def test_caps_cash_and_equity_constraints_only_reduce_weights(self):
        secs = (
            security("s", AssetType.STOCK, "other"), security("e1", industry="one"),
            security("e2", industry="two"), security("b", AssetType.BOND_ETF),
        )
        proposal = {"s": 0.1, "e1": 0.35, "e2": 0.35, "b": 0.2}
        decision = apply_risk(proposal, snapshot(*secs), RiskState(), RiskConfig(target_annual_vol=1))
        self.assertLessEqual(decision.approved_weights["s"], 0.03)
        self.assertLessEqual(decision.approved_weights["e1"], 0.20)
        self.assertLessEqual(decision.approved_weights["e2"], 0.20)
        self.assertLessEqual(decision.approved_weights["b"], 0.15)
        for symbol, weight in decision.approved_weights.items():
            self.assertLessEqual(weight, proposal[symbol])
        restrictive = RiskConfig(stock_cap=1, etf_cap=1, bond_etf_cap=1, industry_cap=1, equity_cap=0.4, min_cash=0.5, target_annual_vol=1)
        clipped = apply_risk(proposal, snapshot(*secs), RiskState(), restrictive)
        self.assertGreaterEqual(clipped.cash_weight, 0.5 - 1e-12)
        self.assertLessEqual(sum(weight for symbol, weight in clipped.approved_weights.items() if symbol != "b"), 0.4 + 1e-12)

    def test_unverified_and_incomplete_lookthrough_reject_new_equity(self):
        for sec in (
            replace(security("e"), historical_metadata_verified=False),
            replace(security("e"), eligible_for_new_positions=False),
            replace(security("e"), lookthrough_verified=False),
            replace(security("e"), industry_exposure={}),
            replace(security("e"), industry_exposure={"known": 0.6}),
        ):
            decision = apply_risk({"e": 0.2}, snapshot(sec), RiskState())
            self.assertEqual(decision.approved_weights, {})
        b = replace(security("b", AssetType.BOND_ETF), historical_metadata_verified=False)
        self.assertEqual(apply_risk({"b": 0.2}, snapshot(b), RiskState()).approved_weights, {})

    def test_missing_stale_or_suspended_volatility_history_removes_equity_only(self):
        e, b = security("e"), security("b", AssetType.BOND_ETF)
        full = history("e")
        bad_windows = (
            full[-30:],
            full[:-1],
            full[:-2] + (replace(full[-2], tradable=False), full[-1]),
            full[:-1] + (replace(full[-1], buyable=False),),
        )
        for bad in bad_windows:
            decision = apply_risk({"e": 0.4, "b": 0.1}, snapshot(e, b, histories={"e": bad, "b": history("b")}), RiskState())
            self.assertEqual(decision.approved_weights, {"b": 0.1})

    def test_missing_return_date_never_gets_filled_or_cross_paired(self):
        e1, e2 = security("e1"), security("e2")
        long = history("e2", count=62)
        gap = long[:-15] + long[-14:]
        snap = snapshot(e1, e2, histories={"e1": history("e1"), "e2": gap})
        decision = apply_risk({"e1": 0.2, "e2": 0.2}, snap, RiskState())
        self.assertEqual(decision.approved_weights, {})
        self.assertIn("volatility_return_dates_mismatch", decision.reasons)

    def test_lock_sets_all_assets_including_defensive_etf_to_cash_exit_target(self):
        e, b = security("e"), security("b", AssetType.BOND_ETF)
        state = RiskState()
        update_drawdown(state, 100, RiskConfig())
        update_drawdown(state, 80, RiskConfig())
        decision = apply_risk({"e": 0.4, "b": 0.07}, snapshot(e, b), state)
        self.assertEqual(decision.approved_weights, {})
        self.assertEqual(decision.risk_scale, 0)
        self.assertTrue(decision.locked)
        self.assertAlmostEqual(decision.cash_weight, 1.0)

    def test_normal_drawdown_levels_do_not_buy_or_scale_defensive_etf(self):
        e, b = security("e"), security("b", AssetType.BOND_ETF)
        state = RiskState()
        update_drawdown(state, 100, RiskConfig())
        update_drawdown(state, 88, RiskConfig())
        decision = apply_risk({"e": 0.2, "b": 0.07}, snapshot(e, b), state)
        self.assertAlmostEqual(decision.approved_weights["e"], 0.1)
        self.assertAlmostEqual(decision.approved_weights["b"], 0.07)

    def test_covariance_includes_correlated_and_offsetting_returns(self):
        e1, e2 = security("e1"), security("e2", industry="other")
        moves = tuple(0.01 if i % 2 else -0.01 for i in range(60))
        opposing = tuple(-value for value in moves)
        snap = snapshot(e1, e2, histories={"e1": history("e1", moves), "e2": history("e2", opposing)})
        decision = apply_risk({"e1": 0.2, "e2": 0.2}, snap, RiskState())
        self.assertLess(decision.estimated_annual_vol, 1e-12)
        self.assertEqual(decision.risk_scale, 1.0)

    def test_future_price_is_not_available_to_risk_estimation(self):
        sec = security("e")
        bars = history("e")
        hidden = bars[:-1] + (replace(bars[-1], available_at=DECISION_AT + timedelta(seconds=1)),)
        decision = apply_risk({"e": 0.2}, snapshot(sec, histories={"e": hidden}), RiskState())
        self.assertEqual(decision.approved_weights, {})

    def test_nonfinite_or_negative_account_proposals_are_rejected(self):
        snap = snapshot(security("e"))
        for weights in ({"e": float("nan")}, {"e": float("inf")}, {"e": -0.1}, {"e": 1.1}):
            with self.assertRaises(ValueError):
                apply_risk(weights, snap, RiskState())


if __name__ == "__main__":
    unittest.main()
