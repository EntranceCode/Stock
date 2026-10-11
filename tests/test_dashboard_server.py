import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from hybrid_quant.server import DashboardApplication

PROJECT = Path(__file__).resolve().parents[1]
HEADERS = {"Host": "127.0.0.1:8765"}


class DashboardServerTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.store.overview.return_value = {"historical_data_admitted": False}
        self.store.runs.return_value = {"items": []}
        self.market = Mock()
        self.market.catalog.return_value = {"items": [], "total": 0}
        self.runner = Mock(return_value={"run_id": "example", "validation_only": True})
        self.app = DashboardApplication(PROJECT, store=self.store, market=self.market, demo_runner=self.runner)

    def test_read_routes_expose_staging_status_and_typed_queries(self):
        response = self.app.dispatch("GET", "/api/overview", HEADERS)
        self.assertEqual(response.status, 200)
        self.assertFalse(json.loads(response.body)["historical_data_admitted"])
        response = self.app.dispatch("GET", "/api/data/catalog?dataset=stock_daily&q=600&limit=20", HEADERS)
        self.assertEqual(response.status, 200)
        self.market.catalog.assert_called_once_with("stock_daily", q="600", limit=20)

    def test_data_catalog_reads_new_registry_without_restarting_server(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            for filename, symbol in (("before.json", "600000"), ("after.json", "000005")):
                (root / filename).write_text(json.dumps({"files": [{"symbol": symbol, "rows": 1,
                    "start": "2020-01-02", "end": "2020-01-02"}]}), encoding="utf-8")
            registry = root / "config/datasets.json"
            registry.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "before.json"}}}), encoding="utf-8")
            app = DashboardApplication(root, store=self.store, demo_runner=self.runner)
            first = app.dispatch("GET", "/api/data/catalog", HEADERS)
            self.assertEqual(first.status, 200)
            self.assertEqual(json.loads(first.body)["items"][0]["symbol"], "600000")
            registry.write_text(json.dumps({"datasets": {"stock_daily": {"manifest": "after.json"}}}), encoding="utf-8")
            second = app.dispatch("GET", "/api/data/catalog", HEADERS)
            self.assertEqual(second.status, 200)
            self.assertEqual(json.loads(second.body)["items"][0]["symbol"], "000005")

    def test_demo_accepts_only_same_origin_fixed_project_config(self):
        self.assertEqual(self.app.dispatch("POST", "/api/demo", HEADERS).status, 403)
        external = {**HEADERS, "Origin": "https://external.example"}
        self.assertEqual(self.app.dispatch("POST", "/api/demo", external).status, 403)
        local = {**HEADERS, "Origin": "http://127.0.0.1:8765"}
        self.assertEqual(self.app.dispatch("POST", "/api/demo", local, b'{"path":"outside"}').status, 400)
        self.assertEqual(self.app.dispatch("POST", "/api/demo", local).status, 201)
        self.runner.assert_called_once_with()

    def test_busy_demo_prevents_duplicate_execution(self):
        self.app._demo_lock.acquire()
        try:
            response = self.app.dispatch("POST", "/api/demo", {**HEADERS, "Origin": "http://127.0.0.1:8765"})
        finally:
            self.app._demo_lock.release()
        self.assertEqual(response.status, 409)
        self.runner.assert_not_called()

    def test_foreign_host_traversal_and_arbitrary_methods_have_no_side_effects(self):
        self.assertEqual(self.app.dispatch("GET", "/api/overview", {"Host": "foreign.example"}).status, 403)
        self.assertEqual(self.app.dispatch("GET", "/../../config/datasets.json", HEADERS).status, 404)
        self.assertEqual(self.app.dispatch("DELETE", "/api/runs", HEADERS).status, 405)
        self.assertEqual(self.app.dispatch("POST", "/api/backtest", {**HEADERS, "Origin": "http://127.0.0.1:8765"}).status, 404)
        self.runner.assert_not_called()

    def test_bad_queries_return_structured_error_instead_of_fallback(self):
        for target in ("/api/data/catalog?limit=bad", "/api/data/catalog?limit=1&limit=2"):
            response = self.app.dispatch("GET", target, HEADERS)
            self.assertEqual(response.status, 400)
            self.assertIn("message", json.loads(response.body))

    def test_static_routes_serve_only_application_assets(self):
        response = self.app.dispatch("GET", "/", HEADERS)
        self.assertEqual(response.status, 200)
        self.assertIn(b"<!doctype html>", response.body.lower())
        self.assertEqual(self.app.dispatch("GET", "/config/demo.json", HEADERS).status, 404)
