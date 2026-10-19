"""Price limits gate whether an order could have filled, so the rules are pinned."""
import importlib.util
from datetime import date
from pathlib import Path
import unittest

PROJECT = Path(__file__).resolve().parents[1]

try:
    SPEC = importlib.util.spec_from_file_location(
        "build_daily_bar_enriched", PROJECT / "scripts/build_daily_bar_enriched.py")
    limits = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(limits)
except ImportError as exc:
    limits = None
    SKIP_REASON = "optional dependency unavailable: " + str(exc)
else:
    SKIP_REASON = ""


@unittest.skipUnless(limits is not None, SKIP_REASON)
class PriceLimitTests(unittest.TestCase):
    def test_star_and_chinext_bands(self):
        self.assertEqual(limits.limit_ratio("688001", date(2021, 1, 4), False), 0.20)
        self.assertEqual(limits.limit_ratio("300001", date(2020, 8, 21), False), 0.10)
        self.assertEqual(limits.limit_ratio("300001", date(2020, 8, 24), False), 0.20)
        self.assertEqual(limits.limit_ratio("301001", date(2022, 1, 4), False), 0.20)

    def test_main_board_band_and_the_tighter_st_band(self):
        self.assertEqual(limits.limit_ratio("600000", date(2021, 1, 4), False), 0.10)
        self.assertEqual(limits.limit_ratio("000001", date(2021, 1, 4), False), 0.10)
        self.assertEqual(limits.limit_ratio("600000", date(2021, 1, 4), True), 0.05)
        # An ST flag does not widen the ChiNext band back to the main-board one.
        self.assertEqual(limits.limit_ratio("300001", date(2022, 1, 4), True), 0.20)

    def test_instrument_kind_is_inferred_from_the_code(self):
        self.assertEqual(limits.instrument_kind("510010"), "etf")
        self.assertEqual(limits.instrument_kind("159915"), "etf")
        self.assertEqual(limits.instrument_kind("600000"), "stock")
        self.assertEqual(limits.instrument_kind("300001"), "stock")

    def test_a_fund_never_takes_the_risk_warning_band(self):
        # A fund cannot be risk-warned, so the provider flag must not narrow it.
        self.assertEqual(limits.limit_ratio("510010", date(2021, 1, 4), True), 0.10)
        self.assertEqual(limits.limit_ratio("510010", date(2021, 1, 4), False), 0.10)

    def test_fund_limits_round_to_thousandths_not_cents(self):
        self.assertEqual(limits.limit_prices(1.0030, 0.10, limits.ETF_TICK), (1.103, 0.903))
        self.assertEqual(limits.limit_prices(1.0030, 0.10, limits.STOCK_TICK), (1.10, 0.90))

    def test_risk_warning_band_widens_at_the_inferred_date(self):
        self.assertEqual(limits.limit_ratio("600000", date(2026, 7, 3), True), 0.05)
        self.assertEqual(limits.limit_ratio("600000", date(2026, 7, 6), True), 0.10)

    def test_limit_prices_round_half_up_to_one_cent(self):
        self.assertEqual(limits.round_cent(10.825), 10.83)
        self.assertEqual(limits.round_cent(10.815), 10.82)
        self.assertEqual(limits.limit_prices(10.00, 0.10), (11.00, 9.00))
        self.assertEqual(limits.limit_prices(3.33, 0.05), (3.50, 3.16))


if __name__ == "__main__":
    unittest.main()
