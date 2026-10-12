"""Adjustment factors decide whether a price series is usable, so they are pinned."""
import importlib.util
from datetime import date, timedelta
from pathlib import Path
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "build_adjusted_prices", PROJECT / "scripts/build_adjusted_prices.py")
    builder = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(builder)
except ImportError as exc:
    builder = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


def series(count=5):
    start = date(2020, 1, 1)
    return [start + timedelta(days=i) for i in range(count)]


@unittest.skipUnless(builder is not None, SKIP_REASON)
class AdjustmentFactorTests(unittest.TestCase):
    def test_cash_dividend_theoretical_ex_price(self):
        # 10.00 close before a 0.50 per-share dividend prices the ex-day at 9.50.
        self.assertAlmostEqual(builder.event_factor(10.0, 0.5, 1.0), 0.95)

    def test_share_multiplier_halves_the_theoretical_price(self):
        self.assertAlmostEqual(builder.event_factor(10.0, 0.0, 2.0), 0.5)

    def test_impossible_inputs_are_rejected(self):
        for arguments in ((0.0, 0.1, 1.0), (10.0, -0.1, 1.0), (10.0, 10.0, 1.0), (10.0, 0.0, 0.0)):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                builder.event_factor(*arguments)

    def test_factors_anchor_at_the_first_bar_and_stay_flat_without_events(self):
        dates = series(4)
        closes = [10.0, 10.0, 10.0, 10.0]
        factors, applied, skipped = builder.build_factors(dates, closes, [])
        self.assertEqual(factors, [1.0, 1.0, 1.0, 1.0])
        self.assertEqual((applied, skipped), ([], []))

    def test_cash_dividend_leaves_the_ex_day_continuous(self):
        dates = series(3)
        closes = [10.0, 9.5, 9.5]  # the market drops exactly by the dividend
        factors, applied, _ = builder.build_factors(dates, closes, [(dates[1], 0.5, 1.0)])
        adjusted = [c * f for c, f in zip(closes, factors)]
        self.assertEqual(len(applied), 1)
        self.assertAlmostEqual(adjusted[0], 10.0)
        self.assertAlmostEqual(adjusted[1], 10.0)

    def test_appending_a_later_event_never_changes_earlier_values(self):
        dates = series(4)
        closes = [10.0, 9.5, 9.5, 9.0]
        first, _, _ = builder.build_factors(dates, closes, [(dates[1], 0.5, 1.0)])
        later, _, _ = builder.build_factors(dates, closes, [(dates[1], 0.5, 1.0), (dates[3], 0.5, 1.0)])
        self.assertEqual([c * f for c, f in zip(closes[:2], first[:2])],
                         [c * f for c, f in zip(closes[:2], later[:2])])

    def test_event_without_a_preceding_bar_is_skipped_with_a_reason(self):
        dates = series(2)
        factors, applied, skipped = builder.build_factors(dates, [10.0, 9.5], [(dates[0], 0.5, 1.0)])
        self.assertEqual(applied, [])
        self.assertEqual(len(skipped), 1)
        self.assertIn("no bar precedes", skipped[0]["reason"])


if __name__ == "__main__":
    unittest.main()
