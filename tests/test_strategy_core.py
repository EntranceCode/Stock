"""Meaningful strategy tests use synthetic, explicitly verified fixture inputs."""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import unittest

from hybrid_quant.models import AssetType, DailyBar, DataSnapshot, Security, StockFactorObservation
from hybrid_quant.strategies import defensive_target, etf_target, stock_target


TZ = timezone(timedelta(hours=8))
DECISION = datetime(2025, 1, 31, 16, 0, tzinfo=TZ)


def security(symbol, asset_type=AssetType.STOCK, tracking_index=None):
    return Security(symbol=symbol, asset_type=asset_type, listed_on=date(2020, 1, 1),
                    historical_metadata_verified=True, eligible_for_new_positions=True,
                    tracking_index=tracking_index)


def history(symbol, prices=None, turnover=1_000_000):
    if prices is None:
        prices = [100 + index * 0.2 for index in range(121)]
    first_day = DECISION.date() - timedelta(days=len(prices) - 1)
    return tuple(DailyBar(symbol, first_day + timedelta(days=index), price,
                         price * 1.01, price * 0.99, price, turnover,
                         datetime.combine(first_day + timedelta(days=index),
                                          datetime.min.time(), tzinfo=TZ) + timedelta(hours=15),
                         adjusted_close=price, tradable=True, buyable=True, sellable=True)
                 for index, price in enumerate(prices))


def observation(symbol, quality=10, value=0.1, *, available_at=None, verified=True):
    return StockFactorObservation(symbol, quality, value,
                                  available_at or DECISION - timedelta(days=1), verified)


def snapshot(securities, histories=None, factors=None, ages=None):
    return DataSnapshot(DECISION, {item.symbol: item for item in securities},
                        histories if histories is not None else {item.symbol: history(item.symbol) for item in securities},
                        factors if factors is not None else {item.symbol: (observation(item.symbol),) for item in securities},
                        ages if ages is not None else {item.symbol: 500 for item in securities})


class StockStrategyTests(unittest.TestCase):
    def test_underfilled_stock_budget_is_cash_and_constant_factors_are_valid(self):
        target = stock_target(snapshot([security("000001"), security("000002")]))
        self.assertEqual(target.strategy_id, "stock")
        self.assertEqual(target.weights, {"000001": 0.04, "000002": 0.04})
        self.assertAlmostEqual(target.cash_weight, 0.92)

    def test_future_factors_and_future_prices_do_not_change_the_known_signal(self):
        symbol = "000001"
        original = snapshot([security(symbol)])
        known = original.histories[symbol]
        tomorrow = replace(known[-1], trade_date=DECISION.date() + timedelta(days=1),
                           available_at=DECISION + timedelta(days=1), adjusted_close=10_000)
        hidden_today = replace(known[-1], available_at=DECISION + timedelta(minutes=1))
        # Future factors cannot displace the currently known original version.
        future_factor = observation(symbol, quality=None, value=None,
                                    available_at=DECISION + timedelta(seconds=1), verified=False)
        augmented = replace(original, histories={symbol: known + (tomorrow,)},
                            stock_factors={symbol: (future_factor, observation(symbol))})
        self.assertEqual(stock_target(augmented).weights, stock_target(original).weights)
        # An unavailable current-day bar cannot be replaced by yesterday's bar.
        self.assertEqual(stock_target(replace(original, histories={symbol: known[:-1] + (hidden_today,)})).weights, {})

    def test_latest_known_unverified_revision_blocks_older_original(self):
        symbol = "000001"
        data = snapshot([security(symbol)], factors={symbol: (
            observation(symbol, available_at=DECISION - timedelta(days=10)),
            observation(symbol, quality=999, available_at=DECISION - timedelta(hours=1), verified=False),
        )})
        target = stock_target(data)
        self.assertEqual(target.weights, {})
        self.assertTrue(any("latest known financial version is unverified" in reason for reason in target.reasons))

    def test_unknown_factors_and_nonpositive_value_never_become_zero_factors(self):
        symbols = ("000001", "000002", "000003")
        data = snapshot([security(item) for item in symbols], factors={
            symbols[0]: (observation(symbols[0], quality=None),),
            symbols[1]: (observation(symbols[1], value=0),),
            symbols[2]: (),
        })
        self.assertEqual(stock_target(data).weights, {})

    def test_halts_missing_adjustments_metadata_and_listing_age_exclude(self):
        symbols = tuple(f"00000{index}" for index in range(1, 6))
        securities = [security(item) for item in symbols]
        securities[3] = replace(securities[3], historical_metadata_verified=False)
        histories = {item: history(item) for item in symbols}
        histories[symbols[0]] = histories[symbols[0]][:-1] + (replace(histories[symbols[0]][-1], tradable=False),)
        missing = list(histories[symbols[1]])
        missing[-40] = replace(missing[-40], adjusted_close=None)
        histories[symbols[1]] = tuple(missing)
        halted = list(histories[symbols[2]])
        halted[-20] = replace(halted[-20], tradable=False)
        histories[symbols[2]] = tuple(halted)
        data = snapshot(securities, histories=histories, ages={item: (249 if item == symbols[4] else 500) for item in symbols})
        self.assertEqual(stock_target(data).weights, {})

    def test_stock_momentum_skips_the_last_twenty_sessions(self):
        # A gains 20% before the skip window; B's gain is wholly inside it.
        prices_a = [100 + index * 0.2 for index in range(101)] + [120.0] * 20
        prices_b = [100.0] * 101 + [100 + index * 3 for index in range(1, 21)]
        data = snapshot([security("000001"), security("000002")], histories={
            "000001": history("000001", prices_a),
            "000002": history("000002", prices_b),
        })
        self.assertEqual(stock_target(data, target_count=1, buffer_count=1).weights, {"000001": 1.0})

    def test_buffer_retains_top_forty_and_only_top_twenty_five_new_candidates(self):
        symbols = [f"{index:06d}" for index in range(41)]
        data = snapshot([security(item) for item in symbols], factors={
            item: (observation(item, quality=100 - index),) for index, item in enumerate(symbols)
        })
        target = stock_target(data, current_symbols=(symbols[30], symbols[39], symbols[40]))
        self.assertEqual(len(target.weights), 25)
        self.assertIn(symbols[30], target.weights)
        self.assertIn(symbols[39], target.weights)
        self.assertNotIn(symbols[40], target.weights)
        self.assertEqual(set(target.weights) - {symbols[30], symbols[39]}, set(symbols[:23]))
        self.assertAlmostEqual(sum(target.weights.values()), 1.0)

    def test_signal_requires_one_hundred_twenty_one_known_bars(self):
        symbol = "000001"
        data = snapshot([security(symbol)], histories={symbol: history(symbol)[1:]})
        self.assertEqual(stock_target(data).weights, {})


class EtfStrategyTests(unittest.TestCase):
    def test_etf_budget_preserves_empty_slots_and_inverse_volatility_floor(self):
        securities = [security("510001", AssetType.EQUITY_ETF, "index-a"),
                      security("510002", AssetType.EQUITY_ETF, "index-b")]
        target = etf_target(snapshot(securities))
        self.assertEqual(target.strategy_id, "etf_rotation")
        self.assertAlmostEqual(target.weights["510001"], 1 / 3)
        self.assertAlmostEqual(target.weights["510002"], 1 / 3)
        self.assertAlmostEqual(target.cash_weight, 1 / 3)

    def test_duplicate_index_selects_known_liquid_product_unknown_index_excluded(self):
        securities = [security("510001", AssetType.EQUITY_ETF, "index-a"),
                      security("510002", AssetType.EQUITY_ETF, "index-a"),
                      security("510003", AssetType.EQUITY_ETF, None),
                      security("510004", AssetType.EQUITY_ETF, "index-b")]
        histories = {item.symbol: history(item.symbol) for item in securities}
        histories["510002"] = history("510002", turnover=2_000_000)
        # Future liquidity must not influence which duplicate is retained.
        future_bar = replace(histories["510001"][-1], trade_date=DECISION.date() + timedelta(days=1),
                             available_at=DECISION + timedelta(days=1), turnover=10_000_000)
        histories["510001"] += (future_bar,)
        target = etf_target(snapshot(securities, histories=histories))
        self.assertEqual(set(target.weights), {"510002", "510004"})
        self.assertAlmostEqual(sum(target.weights.values()), 2 / 3)

    def test_etf_requires_trend_current_bar_and_complete_adjustments(self):
        securities = [security(f"51000{index}", AssetType.EQUITY_ETF, f"index-{index}")
                      for index in range(1, 5)]
        histories = {
            "510001": history("510001", [120 - index * 0.2 for index in range(121)]),
            "510002": history("510002")[:-1],
            "510003": history("510003"),
            "510004": history("510004")[-119:],
        }
        incomplete = list(histories["510003"])
        incomplete[-100] = replace(incomplete[-100], adjusted_close=None)
        histories["510003"] = tuple(incomplete)
        self.assertEqual(etf_target(snapshot(securities, histories=histories)).weights, {})

    def test_etf_inverse_volatility_allocates_less_to_more_variable_returns(self):
        securities = [security("510001", AssetType.EQUITY_ETF, "a"),
                      security("510002", AssetType.EQUITY_ETF, "b")]
        noisy = [100 + index * 0.5 + (2 if index % 2 else -2) for index in range(121)]
        data = snapshot(securities, histories={"510001": history("510001"), "510002": history("510002", noisy)})
        target = etf_target(data)
        self.assertGreater(target.weights["510001"], target.weights["510002"])
        self.assertAlmostEqual(sum(target.weights.values()), 2 / 3)

    def test_defensive_uses_bond_only_and_reserves_forty_percent_cash(self):
        securities = [security("510001", AssetType.EQUITY_ETF, "a"),
                      security("511001", AssetType.BOND_ETF),
                      security("511002", AssetType.BOND_ETF)]
        histories = {item.symbol: history(item.symbol) for item in securities}
        histories["511002"] = history("511002", turnover=2_000_000)
        target = defensive_target(snapshot(securities, histories=histories))
        self.assertEqual(target.strategy_id, "defensive")
        self.assertEqual(target.weights, {"511002": 0.6})
        self.assertAlmostEqual(target.cash_weight, 0.4)
        failed = replace(snapshot(securities), securities={item.symbol: replace(item, eligible_for_new_positions=False) for item in securities})
        self.assertEqual(defensive_target(failed).weights, {})


if __name__ == "__main__":
    unittest.main()
