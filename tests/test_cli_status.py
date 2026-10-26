"""Displaying the state must not re-verify 27 GB of evidence."""
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

from hybrid_quant import cli  # noqa: E402
from hybrid_quant.data_registry import DatasetRegistry  # noqa: E402


class StatusCommandTests(unittest.TestCase):
    def test_status_does_not_run_the_admission_gate(self):
        calls = []
        original = DatasetRegistry.validate_for_backtest

        def refuse(inner_self):
            calls.append("gate")
            raise AssertionError("status must not run the admission gate")

        DatasetRegistry.validate_for_backtest = refuse
        self.addCleanup(setattr, DatasetRegistry, "validate_for_backtest", original)
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = cli.main(["status"])
        self.assertEqual(code, 0)
        self.assertEqual(calls, [])
        payload = json.loads(buffer.getvalue())
        self.assertIsNone(payload["historical_data_admitted"])
        self.assertIn(payload["readiness_source"], {"recorded", "unavailable"})


if __name__ == "__main__":
    unittest.main()
