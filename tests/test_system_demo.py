"""Cross-module replay checks: independent ledger reconstruction and time order."""
from datetime import datetime
import json
from pathlib import Path
import unittest

from hybrid_quant.backtest import run_backtest
from hybrid_quant.demo import synthetic_snapshots
from hybrid_quant.models import FeeConfig, RiskConfig

PROJECT = Path(__file__).resolve().parents[1]


class SystemDemoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = json.loads((PROJECT / "config/demo.json").read_text(encoding="utf-8"))
        cls.snapshots = synthetic_snapshots(cls.config)
        cls.options = {"initial_cash": cls.config["initial_cash"], "fee_config": FeeConfig(**cls.config["fees"]),
                       "risk_config": RiskConfig(**cls.config["risk"]), "budgets": cls.config["budgets"]}
        cls.result = run_backtest(cls.snapshots, **cls.options)

    def test_full_chain_trades_after_decision_and_reconciles_cash_and_holdings(self):
        result = self.result
        self.assertFalse(result["equity"][0]["holdings"])
        self.assertTrue(result["equity"][1]["holdings"])
        self.assertGreater(result["decisions"][0]["filtered_future_factor_observations"], 0)
        cash = self.config["initial_cash"]
        holdings = {}
        orders = {order["order_id"]: order for order in result["orders"]}
        for fill in result["fills"]:
            order = orders[fill["order_id"]]
            self.assertGreater(fill["trade_date"], datetime.fromisoformat(order["decision_at"]).date().isoformat())
            self.assertLessEqual(fill["quantity"], order["quantity"])
            symbol = fill["symbol"]
            fee = fill["commission"] + fill["tax"]
            if fill["side"] == "buy":
                cash -= fill["notional"] + fee
                holdings[symbol] = holdings.get(symbol, 0) + fill["quantity"]
            else:
                cash += fill["notional"] - fee
                holdings[symbol] -= fill["quantity"]
            self.assertGreaterEqual(cash, -1e-8)
            self.assertGreaterEqual(holdings[symbol], 0)
        last = result["equity"][-1]
        holdings = {symbol: quantity for symbol, quantity in holdings.items() if quantity}
        self.assertEqual(holdings, last["holdings"])
        self.assertAlmostEqual(cash, last["cash"], places=6)
        expected_nav = cash + sum(quantity * self.snapshots[-1].known_history(symbol)[-1].close
                                  for symbol, quantity in holdings.items())
        self.assertAlmostEqual(expected_nav, last["nav"], places=6)

    def test_same_snapshot_and_configuration_repeat_identically(self):
        self.assertEqual(run_backtest(self.snapshots, **self.options), self.result)

    def test_appending_future_sessions_cannot_change_prior_account_values(self):
        prefix = run_backtest(self.snapshots[:30], **self.options)
        self.assertEqual(prefix["equity"], self.result["equity"][:30])

    def test_trailing_window_keeps_every_engine_visible_value_identical(self):
        snapshots = synthetic_snapshots(dict(self.config, history_window_sessions=130))
        longest = max(len(history) for snapshot in snapshots for history in snapshot.histories.values())
        self.assertLessEqual(longest, 130)
        windowed = run_backtest(snapshots, **self.options)
        self.assertEqual(windowed["equity"], self.result["equity"])
        self.assertEqual(windowed["orders"], self.result["orders"])
        self.assertEqual(windowed["fills"], self.result["fills"])
        self.assertEqual(windowed["decisions"], self.result["decisions"])

    def test_window_below_the_engine_lookback_or_above_history_is_refused(self):
        for bad in (120, 0, -1, True, "260", 10_000):
            with self.subTest(window=bad), self.assertRaises(ValueError):
                synthetic_snapshots(dict(self.config, history_window_sessions=bad))


if __name__ == "__main__":
    unittest.main()
