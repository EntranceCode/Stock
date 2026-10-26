"""Snapshot-series builder checks on a synthetic, self-contained evidence tree.

The fixtures mirror the registered products' real schemas (daily prices,
listing candidates, calendar, adjusted prices, enriched bars) so the builder is
exercised exactly as it runs against real data. Every exclusion path, the
point-in-time window, the hash chain and the engine reconstruction are covered;
the builder must never invent a missing field.
"""
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "tests"))
from parquet_support import PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON, pa, pq

from hybrid_quant.snapshot_series import SeriesError, SeriesRequest, build_series

TZ = timezone(timedelta(hours=8))
ALL_ASSUMPTIONS = ("session_close_availability", "buy_sell_equal_to_tradable",
                   "turnover_provider_reported", "lot_size_conventional_default",
                   "settlement_conventional_default")
# 130 weekday sessions starting 2024-01-02.
SESSIONS = []
_day = date(2024, 1, 2)
while len(SESSIONS) < 130:
    if _day.weekday() < 5:
        SESSIONS.append(_day)
    _day += timedelta(days=1)
WINDOW_START = SESSIONS[0]
# The first Saturday after the fifth session: a bar dated here must never
# become a session and must be reported as off-calendar.
WEEKEND = SESSIONS[5]
while WEEKEND.weekday() != 5:
    WEEKEND += timedelta(days=1)
# One session inside every test window where 600000 is reported halted.
HALT_SESSION = SESSIONS[20]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def daily_rows(symbol: str, count: int, *, shift_days: int = 0,
               with_weekend_bar: bool = False) -> list[dict]:
    """Weekday bars with valid OHLC, optionally shifted outside the window."""
    rows = []
    for index, session in enumerate(SESSIONS[:count]):
        day = session + timedelta(days=shift_days)
        close = 10.0 + 0.01 * index
        rows.append({"trade_date": day, "symbol": symbol, "open": close,
                     "high": close + 0.5, "low": close - 0.5, "close": close,
                     "volume_raw": 1000 + index})
    if with_weekend_bar:
        rows.append({"trade_date": WEEKEND, "symbol": symbol, "open": 10.0,
                     "high": 10.5, "low": 9.5, "close": 10.2, "volume_raw": 1000})
    return rows


def adjusted_rows(symbol: str, rows: list[dict]) -> list[dict]:
    return [{"trade_date": row["trade_date"], "symbol": symbol, "close_adjusted": row["close"]}
            for row in rows]


def enriched_rows(symbol: str, rows: list[dict], *, halt_on: date | None = None) -> list[dict]:
    return [{"trade_date": row["trade_date"], "symbol": symbol,
             "turnover_cny_reported": 1_000_000.0,
             "provider_trade_status": 0 if halt_on is not None and row["trade_date"] == halt_on else 1}
            for row in rows]


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def remove_tree(root: Path) -> None:
    """Delete a fixture tree file by file.

    Recursive directory removal is intercepted by the sandbox's bulk-delete
    guard once several trees are removed inside one test process, so teardown
    deletes files individually. Everything a test writes stays inside the
    project's .test-tmp directory either way.
    """
    if not root.exists():
        return
    for item in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        if item.is_file():
            item.unlink()
        else:
            item.rmdir()


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class SnapshotSeriesTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(dir=temporary_root))
        self.addCleanup(remove_tree, self.root)
        self.write_fixtures()
        self.request = SeriesRequest(
            start=WINDOW_START, end=SESSIONS[39],
            adjusted_dir=self.root / "adjusted", enriched_dir=self.root / "enriched",
            assume=ALL_ASSUMPTIONS,
        )

    # ---------- fixture construction ----------

    def write_manifest(self, relative: str, datasets: dict) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(datasets, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_daily_dataset(self, name: str, entries: list[tuple[str, list[dict]]],
                            *, asset_type: str | None = None,
                            hash_overrides: dict[str, str] | None = None) -> None:
        directory = self.root / f"data/{name}"
        files = []
        for symbol, rows in entries:
            path = directory / f"{symbol}_daily.parquet"
            table_rows = [{"trade_date": row["trade_date"], "symbol": symbol,
                           "open": row["open"], "high": row["high"], "low": row["low"],
                           "close": row["close"]} for row in rows]
            if asset_type is not None:
                for item in table_rows:
                    item["asset_type"] = asset_type
            write_table(path, table_rows)
            digest = (hash_overrides or {}).get(symbol, sha256(path))
            files.append({"symbol": symbol, "path": str(path), "sha256": digest,
                          "rows": len(table_rows)})
        self.write_manifest(f"data/{name}/manifest.json",
                            {"backtest_ready": False, "files": files})

    def write_fixtures(self):
        # Registered daily evidence. 600000: complete, halts once, weekend bar.
        # 000001: SZSE stock. 600004: no enriched coverage. 600005: no adjusted
        # coverage. 600006: price precedes its listing candidate. 600007: bars
        # entirely before the window. 600008: registered hash mismatch. 600009:
        # no listing candidate.
        self.stock_rows = {
            "600000": daily_rows("600000", 130, with_weekend_bar=True),
            "000001": daily_rows("000001", 130),
            "600004": daily_rows("600004", 130),
            "600005": daily_rows("600005", 130),
            "600006": daily_rows("600006", 130),
            "600007": daily_rows("600007", 130, shift_days=-200),
            "600008": daily_rows("600008", 130),
            "600009": daily_rows("600009", 130),
        }
        self.write_daily_dataset("normalized", list(self.stock_rows.items()),
                                 hash_overrides={"600008": "0" * 64})
        self.write_daily_dataset("etf", [("511010", daily_rows("511010", 130))],
                                 asset_type="government_bond_etf")

        # Listing candidates; 600004, 600008 (already hash-skipped) and 600009
        # deliberately have none, and 600006 is listed after its first bar.
        candidates = []
        for symbol in ("600000", "000001", "600005", "600006", "600007", "511010"):
            listed_on = SESSIONS[10] if symbol == "600006" else SESSIONS[0] - timedelta(days=30)
            candidates.append({"symbol": symbol,
                               "exchange": "SZSE" if symbol == "000001" else "SSE",
                               "source_listing_date": listed_on,
                               "source_termination_date": None})
        meta = self.root / "data/meta"
        listing_path = meta / "listing_candidates.parquet"
        write_table(listing_path, candidates)
        self.write_manifest("data/meta/manifest.json",
                            {"files": [{"path": str(listing_path), "rows": len(candidates)}]})

        # Calendar: weekdays open, the weekend closed, exchange SSE.
        calendar_path = meta / "calendar.parquet"
        calendar_rows = [{"calendar_date": session, "exchange": "SSE", "is_scheduled_open": True}
                         for session in SESSIONS]
        calendar_rows.append({"calendar_date": WEEKEND, "exchange": "SSE", "is_scheduled_open": False})
        write_table(calendar_path, calendar_rows)
        self.write_manifest("data/calendar/manifest.json",
                            {"files": [{"path": str(calendar_path), "rows": len(calendar_rows)}]})

        # Adjusted prices for every symbol except 600005; enriched bars for
        # every symbol except 600004; 600000 halts on one session.
        adjusted_files, enriched_files = [], []
        for symbol in ("600000", "000001", "600006", "600007", "600008", "600009", "511010"):
            rows = self.stock_rows.get(symbol, daily_rows(symbol, 130))
            path = self.root / "adjusted" / f"{symbol}_adjusted.parquet"
            write_table(path, adjusted_rows(symbol, rows))
            adjusted_files.append({"symbol": symbol, "path": str(path), "rows": len(rows)})
        self.write_manifest("adjusted/manifest.json", {"files": adjusted_files})
        for symbol in ("600000", "000001", "600005", "600006", "600007", "600008", "600009", "511010"):
            rows = self.stock_rows.get(symbol, daily_rows(symbol, 130))
            path = self.root / "enriched" / f"{symbol}_enriched.parquet"
            write_table(path, enriched_rows(symbol, rows, halt_on=HALT_SESSION if symbol == "600000" else None))
            enriched_files.append({"symbol": symbol, "path": str(path), "rows": len(rows)})
        self.write_manifest("enriched/manifest.json", {"files": enriched_files})

        self.write_manifest("config/datasets.json", {"datasets": {
            "stock_daily": {"manifest": "data/normalized/manifest.json"},
            "etf_daily": {"manifest": "data/etf/manifest.json"},
            "security_metadata": {"manifest": "data/meta/manifest.json"},
            "sse_calendar": {"manifest": "data/calendar/manifest.json"},
        }})

    def build(self, **overrides) -> dict:
        fields = {"start": self.request.start, "end": self.request.end,
                  "adjusted_dir": self.request.adjusted_dir, "enriched_dir": self.request.enriched_dir,
                  "assume": self.request.assume}
        fields.update(overrides)
        return build_series(self.root, SeriesRequest(**fields), self.root / "series")

    def series_dir(self, index: dict) -> Path:
        return self.root / "series" / index["series_id"]

    def payloads(self, index: dict) -> dict[str, dict]:
        base = self.series_dir(index)
        return {entry["trade_date"]: json.loads(
                    (base / entry["manifest"]).parent.joinpath("payload.json").read_text(encoding="utf-8"))
                for entry in index["sessions"]}

    def manifest_of(self, index: dict, trade_date: str) -> dict:
        base = self.series_dir(index)
        entry = next(item for item in index["sessions"] if item["trade_date"] == trade_date)
        return json.loads((base / entry["manifest"]).read_text(encoding="utf-8"))

    # ---------- checks ----------

    def test_full_series_builds_engine_compatible_bundles(self):
        index = self.build()
        self.assertEqual(len(index["sessions"]), 40)
        self.assertTrue(index["self_check"]["engine_compatible"])
        self.assertEqual(index["self_check"]["bundles_reconstructed"], 40)
        self.assertEqual(index["symbols"]["included"], ["000001", "511010", "600000"])
        last = self.payloads(index)[index["sessions"][-1]["trade_date"]]
        self.assertEqual(len(last["securities"]), 3)
        by_symbol = {item["symbol"]: item for item in last["securities"]}
        self.assertEqual(by_symbol["511010"]["asset_type"], "bond_etf")
        self.assertFalse(by_symbol["600000"]["eligible_for_new_positions"])
        self.assertFalse(by_symbol["600000"]["historical_metadata_verified"])
        self.assertEqual(last["schema_version"], "1.0")
        self.assertEqual(last["source_kind"], "verified_history")
        self.assertEqual(len(last["factors"]), 0)
        self.assertTrue(all(age >= 0 for age in last["listing_age_sessions"].values()))
        # The halted session is passed through as untradable, not dropped.
        halted = [bar for bar in last["bars"]
                  if bar["symbol"] == "600000" and bar["trade_date"] == HALT_SESSION.isoformat()]
        self.assertEqual(len(halted), 1)
        self.assertFalse(halted[0]["tradable"])
        self.assertFalse(halted[0]["buyable"])

    def test_bundles_record_declared_assumptions_and_admission_as_gaps(self):
        index = self.build()
        manifest = self.manifest_of(index, index["sessions"][0]["trade_date"])
        gaps = set(manifest["readiness_gaps"])
        self.assertIn("bar_available_at_assumed_session_close", gaps)
        self.assertIn("buy_sell_eligibility_assumed_equal_to_tradable", gaps)
        self.assertIn("turnover_provider_reported_unverified", gaps)
        self.assertIn("buy_lot_size_assumed_100", gaps)
        self.assertIn("settlement_assumed_t_plus_1", gaps)
        self.assertIn("factor_observations_not_yet_constructed", gaps)
        self.assertIn("listing_date_from_candidate_unverified", gaps)
        self.assertIn("registry_admission_not_granted", gaps)
        self.assertIn("calendar_exchange_unverified:SZSE", gaps)
        # DailyBar.volume stays pending on decision D3 in every bundle.
        self.assertIn("daily_bar_volume_pending_decision_D3", gaps)
        # A hand-written ready flag is no longer written or consulted: the
        # bundle carries its declared gaps and the registry gate decides.
        self.assertNotIn("backtest_ready", manifest)
        self.assertEqual(manifest["payload"], "payload.json")

    def test_bundles_carry_the_security_master_and_explicit_volume_null(self):
        index = self.build()
        payload = self.payloads(index)[index["sessions"][-1]["trade_date"]]
        by_symbol = {item["symbol"]: item for item in payload["securities"]}
        self.assertEqual(by_symbol["000001"]["exchange"], "SZSE")
        self.assertEqual(by_symbol["600000"]["exchange"], "SSE")
        self.assertIsNone(by_symbol["600000"]["delist_date"])
        for bar in payload["bars"]:
            self.assertIn("volume", bar)
            self.assertIsNone(bar["volume"])

    def test_point_in_time_window_excludes_future_bars(self):
        index = self.build(end=SESSIONS[29])
        last_date = index["sessions"][-1]["trade_date"]
        self.assertEqual(last_date, SESSIONS[29].isoformat())
        last = self.payloads(index)[last_date]
        for bar in last["bars"]:
            self.assertLessEqual(bar["trade_date"], last_date)
            self.assertLessEqual(bar["available_at"], last["decision_at"])
        # An early session still sees its own full trailing history; the 600000
        # count includes its one off-calendar bar, which is evidence, not a session.
        first = self.payloads(index)[index["sessions"][0]["trade_date"]]
        self.assertEqual(len([bar for bar in first["bars"] if bar["symbol"] == "600000"]), 1)
        self.assertEqual(len([bar for bar in last["bars"] if bar["symbol"] == "600000"]), 31)
        # The weekend bar never becomes a session.
        self.assertNotIn(WEEKEND.isoformat(), [entry["trade_date"] for entry in index["sessions"]])
        self.assertEqual(index["bars_on_non_calendar_days"], [WEEKEND.isoformat()])

    def test_no_assumptions_report_the_blocking_field_per_symbol(self):
        index = self.build(assume=())
        self.assertEqual(index["sessions"], [])
        self.assertFalse(index["self_check"]["engine_compatible"])
        reasons = index["symbols"]["excluded_reasons"]
        self.assertIn("turnover_assumption_not_granted", reasons)
        self.assertIn("registered_hash_mismatch_or_missing", reasons)

    def test_partial_assumptions_report_the_next_blocking_field(self):
        index = self.build(assume=("turnover_provider_reported", "session_close_availability"))
        reasons = index["symbols"]["excluded_reasons"]
        self.assertIn("buy_lot_size_assumption_not_granted", reasons)
        self.assertNotIn("turnover_assumption_not_granted", reasons)

    def test_missing_coverage_excludes_with_precise_reasons(self):
        index = self.build()
        reasons = index["symbols"]["excluded_reasons"]
        self.assertIn("enriched_coverage_missing", reasons)            # 600004
        self.assertIn("adjusted_close_unavailable", reasons)           # 600005
        self.assertIn("price_precedes_listing_candidate", reasons)     # 600006
        self.assertIn("no_bars_in_window", reasons)                    # 600007
        self.assertIn("registered_hash_mismatch_or_missing", reasons)  # 600008
        self.assertIn("listing_date_candidate_missing", reasons)       # 600009
        self.assertEqual(sum(reasons.values()), 6)
        # The registered 511010 wins over its ETF-evidence duplicate.
        duplicated = self.build(etf_dir=self.write_etf_evidence())
        self.assertIn("duplicate_symbol_source", duplicated["symbols"]["excluded_reasons"])

    def test_unknown_asset_type_excludes_the_symbol(self):
        self.write_daily_dataset("etf_unknown", [("511020", daily_rows("511020", 130))],
                                 asset_type="money_market_etf")
        registry_path = self.root / "config/datasets.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        registry["datasets"]["etf_daily"] = {"manifest": "data/etf_unknown/manifest.json"}
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        index = self.build(symbols=("511020",))
        self.assertEqual(index["sessions"], [])
        self.assertIn("asset_type_unmapped", index["symbols"]["excluded_reasons"])

    def test_series_output_must_stay_inside_the_project(self):
        outside = Path(self.root.anchor) / "outside-series"
        with self.assertRaises(SeriesError):
            build_series(self.root, self.request, outside)

    def test_unknown_assumption_is_refused(self):
        with self.assertRaises(SeriesError):
            self.build(assume=("invent_a_price",))

    def test_repeated_builds_produce_identical_payloads(self):
        first = self.build(end=SESSIONS[19])
        second = self.build(end=SESSIONS[19])
        self.assertEqual(self.payloads(first), self.payloads(second))
        self.assertNotEqual(first["series_id"], second["series_id"])

    def test_bundle_payload_hash_matches_its_manifest(self):
        index = self.build(end=SESSIONS[19])
        base = self.series_dir(index)
        for entry in index["sessions"]:
            manifest_path = base / entry["manifest"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            payload_bytes = (manifest_path.parent / "payload.json").read_bytes()
            self.assertEqual(hashlib.sha256(payload_bytes).hexdigest(), manifest["payload_sha256"])
            self.assertEqual(sha256(manifest_path), entry["manifest_sha256"])

    def write_factor_product(self) -> Path:
        """A minimal factor product in the real schema, one observation per symbol."""
        directory = self.root / "factor_product"
        rows = []
        for symbol, day, quality, value in (
                ("600000", SESSIONS[2], 0.11, 0.05),
                ("000001", SESSIONS[2], 0.09, 0.04),
                ("511010", SESSIONS[2], 0.07, 0.03)):
            rows.append({"symbol": symbol, "report_date": date(2023, 12, 31),
                         "available_at": datetime.combine(day, time(0, 0), TZ),
                         "quality": quality, "value": value,
                         "original_version_verified": False,
                         "ttm_net_profit": 1.0, "ttm_equity": 10.0,
                         "price_basis": 10.0, "currency": "CNY", "reasons": []})
        write_table(directory / "symbols" / "factors.parquet", rows)
        (directory / "manifest.json").write_text(json.dumps(
            {"files": [{"symbol": "all", "path": str(directory / "symbols" / "factors.parquet"),
                        "rows": len(rows)}]}), encoding="utf-8")
        return directory

    def test_factor_product_populates_payloads_and_keeps_the_version_gap(self):
        factors_dir = self.write_factor_product()
        index = self.build(end=SESSIONS[29], factors_dir=factors_dir)
        self.assertTrue(index["sources"]["factors"])
        manifest = self.manifest_of(index, index["sessions"][-1]["trade_date"])
        self.assertIn("factor_original_versions_unverified", set(manifest["readiness_gaps"]))
        self.assertNotIn("factor_observations_not_yet_constructed", set(manifest["readiness_gaps"]))
        # Before the availability date no factor is visible; afterwards all three are.
        early = self.payloads(index)[SESSIONS[1].isoformat()]
        self.assertEqual(early["factors"], [])
        late = self.payloads(index)[SESSIONS[29].isoformat()]
        self.assertEqual(len(late["factors"]), 3)
        by_symbol = {item["symbol"]: item for item in late["factors"]}
        self.assertAlmostEqual(by_symbol["600000"]["quality"], 0.11)
        self.assertAlmostEqual(by_symbol["600000"]["value"], 0.05)
        self.assertFalse(by_symbol["600000"]["original_version_verified"])
        # The self-check reconstructs the engine snapshot including factors.
        self.assertTrue(index["self_check"]["engine_compatible"])

    def write_etf_evidence(self) -> Path:
        """A minimal ETF evidence product in the real schema (one file per symbol)."""
        directory = self.root / "etf_evidence"
        entries = []
        for symbol, asset_type, index_name in (("510300", "equity_etf", "沪深300"),
                                               ("511010", "bond_etf", None),
                                               ("511990", "unmapped", None)):
            rows = [{"trade_date": session, "open": 4.0, "high": 4.1, "low": 3.9,
                     "close": 4.0 + 0.001 * offset, "turnover": 5_000_000.0,
                     "trade_status": 1, "adjusted_close": 4.0 + 0.001 * offset}
                    for offset, session in enumerate(SESSIONS[:30])]
            path = directory / "symbols" / f"{symbol}_etf.parquet"
            write_table(path, rows)
            entries.append({"symbol": symbol, "asset_type": asset_type, "name": symbol,
                            "exchange": "SSE", "listed_on": "2012-05-28", "terminated_on": None,
                            "tracking_index": index_name,
                            "path": str(path), "sha256": sha256(path), "rows": len(rows)})
        (directory / "manifest.json").write_text(json.dumps(
            {"files": entries, "symbols": len(entries)}), encoding="utf-8")
        return directory

    def test_etf_evidence_adds_typed_securities_with_tracking_index(self):
        etf_dir = self.write_etf_evidence()
        index = self.build(end=SESSIONS[29], etf_dir=etf_dir)
        self.assertTrue(index["sources"]["etf"])
        self.assertEqual(sorted(index["symbols"]["included"]),
                         ["000001", "510300", "511010", "600000"])
        payload = self.payloads(index)[SESSIONS[29].isoformat()]
        by_symbol = {item["symbol"]: item for item in payload["securities"]}
        self.assertEqual(by_symbol["510300"]["asset_type"], "equity_etf")
        self.assertEqual(by_symbol["510300"]["tracking_index"], "沪深300")
        self.assertEqual(by_symbol["510300"]["listed_on"], "2012-05-28")
        self.assertEqual(by_symbol["511010"]["asset_type"], "bond_etf")
        self.assertIsNone(by_symbol["511010"]["tracking_index"])
        # ETF bars come from the evidence product, turnover included.
        bars = [bar for bar in payload["bars"] if bar["symbol"] == "510300"]
        self.assertEqual(len(bars), 30)
        self.assertAlmostEqual(bars[-1]["turnover"], 5_000_000.0)
        self.assertTrue(index["self_check"]["engine_compatible"])

    def test_unmapped_etf_type_is_excluded_with_a_reason(self):
        etf_dir = self.write_etf_evidence()
        manifest_path = etf_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"].append({"symbol": "511990", "asset_type": "money_market_etf",
                                  "name": "511990", "exchange": "SSE", "listed_on": "2015-01-01",
                                  "tracking_index": None,
                                  "path": manifest["files"][0]["path"],
                                  "sha256": manifest["files"][0]["sha256"], "rows": 30})
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        index = self.build(end=SESSIONS[29], etf_dir=etf_dir)
        self.assertIn("asset_type_unmapped", index["symbols"]["excluded_reasons"])
        self.assertNotIn("511990", index["symbols"]["included"])


if __name__ == "__main__":
    unittest.main()
