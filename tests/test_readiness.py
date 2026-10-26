"""Dataset readiness criteria (design §5.1) and the computed gate (§5.2/§5.3).

The fixture is a self-contained evidence tree where every criterion *can* pass,
so the tests cover both directions: a fully evidenced registry is admitted, and
each criterion independently refuses the registry when its evidence is missing,
contradictory, or contradicted by the data. Hand-written ``backtest_ready``
flags appear throughout the fixture (true and false) to prove they are never
the source of admission.
"""
from datetime import date, timedelta, timezone
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

from hybrid_quant.data_registry import DataNotReady, DatasetRegistry
from hybrid_quant.readiness import CRITERIA, evaluate_readiness

TZ = timezone(timedelta(hours=8))
WEEKEND = date(2024, 1, 6)
SESSIONS = [date(2024, 1, 2) + timedelta(days=offset) for offset in (0, 1, 2, 3, 6)]
STOCKS = ("600000", "000001")
ETFS = ("510300",)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_table(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def remove_tree(root: Path) -> None:
    if not root.exists():
        return
    for item in sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        if item.is_file():
            item.unlink()
        else:
            item.rmdir()


class EvidenceTree:
    """A registered evidence tree; every criterion can pass on it."""

    def __init__(self, root: Path):
        self.root = root
        self.stock_rows = {symbol: self._daily_rows(symbol) for symbol in STOCKS}
        self.etf_rows = {symbol: self._daily_rows(symbol) for symbol in ETFS}
        self.build()

    # ---------- fixture construction ----------

    @staticmethod
    def _daily_rows(symbol: str, *, turnover: float | None = 1_000_000.0,
                    drop: date | None = None, extra_closed_day: bool = False) -> list[dict]:
        rows = []
        for session in SESSIONS:
            if session == drop:
                continue
            day = WEEKEND if (extra_closed_day and session == SESSIONS[-1]) else session
            close = 10.0 + 0.01 * SESSIONS.index(session)
            row = {"trade_date": day, "symbol": symbol, "exchange": "SSE" if symbol[0] == "6" else "SZSE",
                   "open": close, "high": close + 0.5, "low": close - 0.5, "close": close}
            if turnover is not None:
                row["turnover"] = turnover
            rows.append(row)
        return rows

    def _write_daily(self, directory: str, rows_by_symbol: dict[str, list[dict]],
                     name: str, *, with_asset_type: bool = False) -> None:
        files = []
        for symbol, rows in rows_by_symbol.items():
            path = self.root / directory / f"{symbol}.parquet"
            payload = []
            for row in rows:
                item = dict(row)
                if with_asset_type:
                    item["asset_type"] = "equity_etf"
                payload.append(item)
            write_table(path, payload)
            files.append({"symbol": symbol, "path": str(path), "sha256": sha256(path),
                          "rows": len(payload), "start": payload[0]["trade_date"].isoformat(),
                          "end": payload[-1]["trade_date"].isoformat()})
        self.write_json(f"{directory}/manifest.json",
                        {"backtest_ready": True, "readiness_gaps": ["ignored_declaration"],
                         "errors": [], "source_download_errors": [], "source_error_history": [],
                         "files": files, "name": name})

    def write_json(self, relative: str, value: dict) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def build(self) -> None:
        # Calendar: both exchanges, weekdays open, the weekend closed. SSE and
        # SZSE are registered as separate datasets, as the criteria aggregate.
        calendar_rows = []
        for session in SESSIONS + [WEEKEND]:
            for exchange in ("SSE", "SZSE"):
                calendar_rows.append({"calendar_date": session, "exchange": exchange,
                                      "is_scheduled_open": session.weekday() < 5})
        for exchange, directory in (("SSE", "calendar"), ("SZSE", "calendar_szse")):
            rows = [row for row in calendar_rows if row["exchange"] == exchange]
            write_table(self.root / directory / "calendar.parquet", rows)
            self.write_json(f"{directory}/manifest.json", {
                "backtest_ready": True, "readiness_gaps": [], "calendar_source_scope_complete": True,
                "files": [{"path": str(self.root / directory / "calendar.parquet"),
                           "sha256": sha256(self.root / directory / "calendar.parquet"),
                           "rows": len(rows)}]})

        self._write_daily("stock_daily", self.stock_rows, "stock_daily")
        self._write_daily("etf_daily", self.etf_rows, "etf_daily", with_asset_type=True)

        # Financial snapshot: complete announcement dates, hashes registered.
        financial_rows = [{"SECURITY_CODE": "600000", "REPORT_DATE": "2023-12-31 00:00:00",
                           "NOTICE_DATE": "2024-04-20 00:00:00", "value": 1.5}]
        write_table(self.root / "financials/financials.parquet", financial_rows)
        self.write_json("financials/manifest.json", {
            "backtest_ready": True, "readiness_gaps": [],
            "files": [{"symbol": "600000", "path": str(self.root / "financials/financials.parquet"),
                       "sha256": sha256(self.root / "financials/financials.parquet"),
                       "rows": len(financial_rows), "unknown_announcement_rows": 0}]})

        # Action samples: none quarantined in the admissible shape.
        write_table(self.root / "actions/events.parquet",
                    [{"symbol": "600000", "ex_date": "2024-06-20", "kind": "cash_dividend"}])
        self.write_json("actions/manifest.json", {
            "backtest_ready": True, "readiness_gaps": [], "quarantined_events": 0, "rows": 26_621,
            "errors": [], "source_download_errors": [],
            "files": [{"path": str(self.root / "actions/events.parquet"),
                       "sha256": sha256(self.root / "actions/events.parquet"), "rows": 1}]})

        # Security metadata: listing candidates cover every traded symbol; one
        # terminated security with a verified boundary.
        candidates = [{"symbol": symbol, "exchange": "SSE" if symbol[0] == "6" else "SZSE",
                       "source_listing_date": date(2000, 1, 3), "source_termination_date": None,
                       "termination_boundary_verified": False}
                      for symbol in STOCKS + ETFS]
        candidates.append({"symbol": "000002", "exchange": "SZSE",
                           "source_listing_date": date(1991, 4, 3),
                           "source_termination_date": date(2020, 5, 12),
                           "termination_boundary_verified": True})
        write_table(self.root / "metadata/listing_candidates.parquet", candidates)
        self.write_json("metadata/manifest.json", {
            "backtest_ready": True, "readiness_gaps": [],
            "coverage": {"local_codes_without_listing_reference": 0},
            "files": [{"path": str(self.root / "metadata/listing_candidates.parquet"),
                       "sha256": sha256(self.root / "metadata/listing_candidates.parquet"),
                       "rows": len(candidates)}]})
        # The boundary evidence records the decidable state per terminated security.
        (self.root / "evidence/boundaries/delisting_boundaries.jsonl").parent.mkdir(
            parents=True, exist_ok=True)
        (self.root / "evidence/boundaries/delisting_boundaries.jsonl").write_text(
            json.dumps({"symbol": "000002", "official_termination_date": "2020-05-12",
                        "decision_state": "last_bar_equals_termination",
                        "boundary_decidable": True}) + "\n", encoding="utf-8")
        self.write_json("evidence/boundaries/manifest.json", {
            "backtest_ready": True, "terminated_securities": 1, "boundary_decidable": 1,
            "boundary_undecidable": 0,
            "decision_state_counts": {"last_bar_equals_termination": 1},
            "files": [{"path": str(self.root / "evidence/boundaries/delisting_boundaries.jsonl"),
                       "sha256": sha256(self.root / "evidence/boundaries/delisting_boundaries.jsonl"),
                       "rows": 1}]})

        # Registered evidence datasets the derivative criteria consult. The
        # recheck product must inventory the very securities it vouchsafes, so
        # its files list names them.
        # The recheck product must inventory the very securities it vouchsafes,
        # so its files list names them (paths point at the rechecked bars).
        enriched_files = []
        for symbol in STOCKS + ETFS:
            directory = "stock_daily" if symbol in STOCKS else "etf_daily"
            source = self.root / directory / f"{symbol}.parquet"
            enriched_files.append({"symbol": symbol, "path": str(source),
                                   "sha256": sha256(source), "rows": len(SESSIONS)})
        self.write_json("evidence/enriched/manifest.json", {
            "backtest_ready": True, "rows_breaching_derived_limits": 0,
            "symbols": len(STOCKS + ETFS), "rows": sum(item["rows"] for item in enriched_files),
            "files": enriched_files})
        # The residual enumeration: an empty enumeration over zero residuals.
        (self.root / "evidence/residuals/residual_jumps.jsonl").parent.mkdir(
            parents=True, exist_ok=True)
        (self.root / "evidence/residuals/residual_jumps.jsonl").write_text("", encoding="utf-8")
        self.write_json("evidence/residuals/manifest.json", {
            "backtest_ready": True, "ex_dates_checked": 25_440,
            "ex_dates_beyond_price_limit_after_adjustment": 0,
            "residual_jump_evidence": "residual_jumps.jsonl",
            "files": [{"path": str(self.root / "evidence/residuals/residual_jumps.jsonl"),
                       "sha256": sha256(self.root / "evidence/residuals/residual_jumps.jsonl"),
                       "rows": 0}]})
        (self.root / "evidence" / "adjusted").mkdir(parents=True, exist_ok=True)
        self.write_json("evidence/adjusted/manifest.json", {
            "backtest_ready": True, "ex_dates_beyond_price_limit_after_adjustment": 0,
            "residual_jump_evidence": "residual_jumps.jsonl"})
        self.write_json("evidence/availability/manifest.json", {
            "backtest_ready": True, "original_notice_dates_compared": 192,
            "original_notice_dates_matched": 192})

        self.write_json("config/datasets.json", {"datasets": {
            "stock_daily": {"manifest": "stock_daily/manifest.json"},
            "etf_daily": {"manifest": "etf_daily/manifest.json"},
            "financials": {"manifest": "financials/manifest.json"},
            "action_samples": {"manifest": "actions/manifest.json"},
            "sse_calendar": {"manifest": "calendar/manifest.json"},
            "szse_calendar": {"manifest": "calendar_szse/manifest.json"},
            "security_metadata": {"manifest": "metadata/manifest.json"},
            "daily_bar_enriched": {"manifest": "evidence/enriched/manifest.json"},
            "adjusted_prices": {"manifest": "evidence/adjusted/manifest.json"},
            "adjustment_residuals": {"manifest": "evidence/residuals/manifest.json"},
            "delisting_boundaries": {"manifest": "evidence/boundaries/manifest.json"},
            "financial_availability": {"manifest": "evidence/availability/manifest.json"},
        }})

    def registry(self, ttl: float = 60.0) -> DatasetRegistry:
        return DatasetRegistry(self.root / "config/datasets.json", self.root, validation_ttl=ttl)

    def criterion(self, dataset: str, name: str):
        report = evaluate_readiness(self.registry(), record_report=False)
        result = next(item for item in report.dataset(dataset).criteria if item.criterion == name)
        return result


@unittest.skipUnless(PARQUET_STACK_AVAILABLE, PARQUET_STACK_SKIP_REASON)
class ReadinessCriteriaTests(unittest.TestCase):
    def setUp(self):
        temporary_root = PROJECT / ".test-tmp"
        temporary_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=temporary_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.addCleanup(remove_tree, self.root)
        self.tree = EvidenceTree(self.root)
        # A fixture universe is small by construction, so it declares its own
        # policy rather than inheriting the project threshold: coverage must be
        # complete here, and the absolute floor is the single covered symbol.
        policy = self.root / "config/readiness-policy.json"
        policy.parent.mkdir(parents=True, exist_ok=True)
        policy.write_text(json.dumps({"stock_turnover": {
            "coverage_threshold": 1.0, "minimum_covered": 1,
            "rationale": "fixture policy"}}), encoding="utf-8")
        import hybrid_quant.readiness as readiness_module
        original_policy = readiness_module.POLICY_PATH
        self.addCleanup(setattr, readiness_module, "POLICY_PATH", original_policy)
        readiness_module.POLICY_PATH = policy

    def drop_listing_candidate(self, symbol: str) -> None:
        """Rewrite the listing candidates without one symbol, keeping the
        registered inventory hash in step with the file on disk."""
        rows = [row for row in self._listing_candidates() if row["symbol"] != symbol]
        write_table(self.root / "metadata/listing_candidates.parquet", rows)
        manifest_path = self.root / "metadata/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["files"]:
            if str(entry.get("path", "")).endswith("listing_candidates.parquet"):
                entry["sha256"] = sha256(self.root / "metadata/listing_candidates.parquet")
                entry["rows"] = len(rows)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    # ---------- the admissible direction ----------

    def test_fully_evidenced_registry_is_admitted_and_records_the_manifest(self):
        registry = self.tree.registry()
        registry.validate_for_backtest()
        report = registry.readiness()
        self.assertTrue(report.ready)
        self.assertTrue(report.required_ready)
        # §5.2: every evaluation records its per-criterion answers and evidence.
        latest = self.root / "reports/readiness/latest.json"
        self.assertTrue(latest.is_file())
        recorded = json.loads(latest.read_text(encoding="utf-8"))
        criteria = [item for dataset in recorded["datasets"] for item in dataset["criteria"]]
        self.assertEqual(len(criteria), len(CRITERIA))
        self.assertTrue(all(item["evidence"] for item in criteria))
        self.assertTrue(all(item["passed"] for item in criteria))

    def test_hand_written_ready_flags_are_not_the_source_of_admission(self):
        # Every manifest claims ready; dropping the turnover column must still
        # refuse the registry, with the failing criteria named.
        self.tree.stock_rows = {symbol: EvidenceTree._daily_rows(symbol, turnover=None)
                                for symbol in STOCKS}
        self.tree._write_daily("stock_daily", self.tree.stock_rows, "stock_daily")
        registry = self.tree.registry()
        with self.assertRaises(DataNotReady) as caught:
            registry.validate_for_backtest()
        self.assertIn("readiness criteria not met", str(caught.exception))
        self.assertIn("stock_daily.turnover_coverage", str(caught.exception))
        report = registry.readiness()
        self.assertFalse(report.ready)
        # And flipping the flags to false changes nothing either.
        for manifest in ("stock_daily", "etf_daily", "calendar", "metadata"):
            path = self.root / manifest / "manifest.json"
            value = json.loads(path.read_text(encoding="utf-8"))
            value["backtest_ready"] = False
            path.write_text(json.dumps(value), encoding="utf-8")

    def test_no_criterion_can_be_turned_into_a_pass_by_configuration(self):
        # Every criterion is registered exactly once for a gated dataset and
        # evaluates to an answer; a criterion id never answers for another one.
        report = evaluate_readiness(self.tree.registry(), record_report=False)
        answered = {f"{criterion.dataset}.{criterion.criterion}"
                    for dataset in report.datasets for criterion in dataset.criteria}
        self.assertEqual({f"{item[0]}.{item[1]}" for item in CRITERIA}, answered)
        self.assertEqual(len(CRITERIA), len(answered))

    # ---------- calendar ----------

    def test_bar_on_a_scheduled_closed_day_fails_the_reverse_check(self):
        rows = {symbol: EvidenceTree._daily_rows(symbol, extra_closed_day=True) for symbol in STOCKS}
        self.tree._write_daily("stock_daily", rows, "stock_daily")
        result = self.tree.criterion("sse_calendar", "bar_reverse_check_zero_conflicts")
        self.assertFalse(result.passed)
        self.assertIn("scheduled-closed", result.detail)

    def test_a_single_exchange_calendar_fails_the_scope_criterion(self):
        self.assertTrue(self.tree.criterion("sse_calendar",
                                            "both_exchange_annual_notices_complete").passed)
        # Unregistering the SZSE calendar is exactly what the criterion catches:
        # the second exchange's annual notices are no longer registered.
        registry_path = self.root / "config/datasets.json"
        config = json.loads(registry_path.read_text(encoding="utf-8"))
        config["datasets"].pop("szse_calendar")
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        result = self.tree.criterion("sse_calendar", "both_exchange_annual_notices_complete")
        self.assertFalse(result.passed)
        self.assertIn("szse_calendar", result.detail)
        # A calendar whose own source scope is incomplete fails the same way.
        config["datasets"]["szse_calendar"] = {"manifest": "calendar_szse/manifest.json"}
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        szse_path = self.root / "calendar_szse/manifest.json"
        szse = json.loads(szse_path.read_text(encoding="utf-8"))
        szse["calendar_source_scope_complete"] = False
        szse_path.write_text(json.dumps(szse), encoding="utf-8")
        result = self.tree.criterion("sse_calendar", "both_exchange_annual_notices_complete")
        self.assertFalse(result.passed)
        self.assertIn("scope incomplete", result.detail)

    # ---------- daily datasets ----------

    def test_missing_session_fails_until_a_session_gap_record_names_it(self):
        # A hole in the middle of 000001's history is unexplained: refusal.
        rows = {**{symbol: EvidenceTree._daily_rows(symbol) for symbol in STOCKS},
                "000001": EvidenceTree._daily_rows("000001", drop=SESSIONS[2])}
        self.tree._write_daily("stock_daily", rows, "stock_daily")
        result = self.tree.criterion("stock_daily", "missing_days_zero_or_recorded")
        self.assertFalse(result.passed)
        self.assertIn("000001", result.detail)
        manifest_path = self.root / "stock_daily/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # A record naming only the symbol is not a record of anything.
        manifest["session_gap_records"] = [{"symbol": "000001", "dates": [SESSIONS[2].isoformat()]}]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "missing_days_zero_or_recorded")
        self.assertFalse(result.passed)
        self.assertIn("lack symbol/dates/cause", result.detail)
        # Design §5.1: symbol, dates and reason, all three, for the affected symbol.
        manifest["session_gap_records"] = [{"symbol": "000001", "dates": [SESSIONS[2].isoformat()],
                                            "cause": "suspension pending announcement evidence"}]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertTrue(self.tree.criterion("stock_daily", "missing_days_zero_or_recorded").passed)

    def test_turnover_coverage_counts_covered_symbols_not_files(self):
        # The criterion asks how much of the declared universe carries a real
        # turnover value, not whether every file happens to hold the column.
        # Zero stays a legal observation (a halted session trades nothing) and
        # is never confused with unknown (design §3.1 rule 3).
        rows = {symbol: EvidenceTree._daily_rows(symbol, turnover=None) for symbol in STOCKS}
        self.tree._write_daily("stock_daily", rows, "stock_daily")
        result = self.tree.criterion("stock_daily", "turnover_coverage")
        self.assertFalse(result.passed)
        self.assertIn("carry turnover", result.detail)
        manifest_path = self.root / "stock_daily/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        # Re-add the column to one symbol only, with null values inside it.
        rows = {symbol: EvidenceTree._daily_rows(symbol) for symbol in STOCKS}
        self.tree._write_daily("stock_daily", rows, "stock_daily")
        nulled = self.root / "stock_daily/000001.parquet"
        table = pq.read_table(nulled)
        nulled_rows = [{**row, "turnover": None} for row in table.to_pylist()]
        write_table(nulled, nulled_rows)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["files"]:
            if entry["symbol"] == "000001":
                entry["sha256"] = sha256(nulled)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "turnover_coverage")
        self.assertFalse(result.passed)
        self.assertIn("excluded", result.detail)

    def test_unregistered_recheck_and_residual_evidence_fail_their_criteria(self):
        # Unregistered recheck evidence: the criterion fails with that fact.
        registry_path = self.root / "config/datasets.json"
        config = json.loads(registry_path.read_text(encoding="utf-8"))
        config["datasets"].pop("daily_bar_enriched")
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "price_limit_recheck_zero_breaches")
        self.assertFalse(result.passed)
        self.assertIn("not registered", result.detail)
        # A registered recheck with breaches refuses; zero breaches but
        # incomplete symbol coverage refuses until the symbols are covered.
        config["datasets"]["daily_bar_enriched"] = {"manifest": "evidence/enriched/manifest.json"}
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        enriched = self.root / "evidence/enriched/manifest.json"
        enriched.write_text(json.dumps({"rows_breaching_derived_limits": 3, "rows": 10,
                                        "files": []}), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "price_limit_recheck_zero_breaches")
        self.assertFalse(result.passed)
        self.assertIn("3 breaching", result.detail)
        enriched.write_text(json.dumps({"rows_breaching_derived_limits": 0, "rows": 10,
                                        "files": []}), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "price_limit_recheck_zero_breaches")
        self.assertFalse(result.passed)
        self.assertIn("are absent from the recheck", result.detail)
        self.tree.build()
        self.assertTrue(self.tree.criterion("stock_daily",
                                            "price_limit_recheck_zero_breaches").passed)

    def test_residual_jumps_are_enumerated_before_the_criterion_answers(self):
        # The enumeration dataset decides; the adjusted run alone cannot.
        registry_path = self.root / "config/datasets.json"
        config = json.loads(registry_path.read_text(encoding="utf-8"))
        config["datasets"].pop("adjustment_residuals")
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "adjustment_residual_jumps_logged")
        self.assertFalse(result.passed)
        self.assertIn("not registered", result.detail)
        config["datasets"]["adjustment_residuals"] = {"manifest": "evidence/residuals/manifest.json"}
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        # A counted residual without its enumeration file fails.
        residuals = self.root / "evidence/residuals/manifest.json"
        manifest = json.loads(residuals.read_text(encoding="utf-8"))
        del manifest["residual_jump_evidence"]
        residuals.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertFalse(self.tree.criterion("stock_daily",
                                             "adjustment_residual_jumps_logged").passed)
        # Count and enumeration disagree: fail with both numbers.
        manifest["residual_jump_evidence"] = "residual_jumps.jsonl"
        manifest["ex_dates_beyond_price_limit_after_adjustment"] = 3
        residuals.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "adjustment_residual_jumps_logged")
        self.assertFalse(result.passed)
        self.assertIn("3 residual jump(s) counted but 0 individually logged", result.detail)
        # A complete enumeration of the residuals fails on the count itself.
        enumeration = self.root / "evidence/residuals/residual_jumps.jsonl"
        enumeration.write_text("".join(json.dumps({"symbol": f"00000{index}"}) + "\n"
                                      for index in range(3)), encoding="utf-8")
        result = self.tree.criterion("stock_daily", "adjustment_residual_jumps_logged")
        self.assertFalse(result.passed)
        self.assertIn("3 residual adjustment jump(s)", result.detail)
        # Zero residuals, individually logged, passes.
        enumeration.write_text("", encoding="utf-8")
        manifest["ex_dates_beyond_price_limit_after_adjustment"] = 0
        residuals.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertTrue(self.tree.criterion("stock_daily",
                                            "adjustment_residual_jumps_logged").passed)

    def test_etf_universe_must_be_rebuilt_from_listing_information(self):
        self.assertTrue(self.tree.criterion("etf_daily", "universe_rebuilt_from_listing").passed)
        candidates_path = self.root / "metadata/listing_candidates.parquet"
        rows = self._listing_candidates()
        rows = [row for row in rows if row["symbol"] != "510300"]
        write_table(candidates_path, rows)
        result = self.tree.criterion("etf_daily", "universe_rebuilt_from_listing")
        self.assertFalse(result.passed)
        self.assertIn("510300", result.detail)

    def _listing_candidates(self):
        candidates = [{"symbol": symbol, "exchange": "SSE" if symbol[0] == "6" else "SZSE",
                       "source_listing_date": date(2000, 1, 3), "source_termination_date": None,
                       "termination_boundary_verified": False}
                      for symbol in STOCKS + ETFS]
        candidates.append({"symbol": "000002", "exchange": "SZSE",
                           "source_listing_date": date(1991, 4, 3),
                           "source_termination_date": date(2020, 5, 12),
                           "termination_boundary_verified": True})
        return candidates

    # ---------- financials, actions, metadata ----------

    def test_financial_announcement_completeness_and_sampling(self):
        self.assertTrue(self.tree.criterion("financials", "announced_at_complete").passed)
        manifest_path = self.root / "financials/manifest.json"
        # A usable row without its announcement date: refusal.
        rows = [{"SECURITY_CODE": "600000", "REPORT_DATE": "2023-12-31 00:00:00",
                 "NOTICE_DATE": None}]
        write_table(self.root / "financials/financials.parquet", rows)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0].update(unknown_announcement_rows=1,
                                    sha256=sha256(self.root / "financials/financials.parquet"))
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.tree.criterion("financials", "announced_at_complete")
        self.assertFalse(result.passed)
        self.assertIn("1 usable row(s) without an announcement date", result.detail)
        # A period before the security's verified listing date has no securities
        # disclosure to name: enumerated apart, not a refusal.
        rows = [{"SECURITY_CODE": "601187", "REPORT_DATE": "2018-09-30 00:00:00", "NOTICE_DATE": None},
                {"SECURITY_CODE": "601187", "REPORT_DATE": "2019-03-31 00:00:00", "NOTICE_DATE": None}]
        write_table(self.root / "financials/financials.parquet", rows)
        manifest["files"] = [{"symbol": "601187", "path": str(self.root / "financials/financials.parquet"),
                              "sha256": sha256(self.root / "financials/financials.parquet"),
                              "rows": 2, "unknown_announcement_rows": 2}]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        candidates = self._listing_candidates() + [
            {"symbol": "601187", "exchange": "SSE", "source_listing_date": date(2020, 10, 27),
             "source_termination_date": None, "termination_boundary_verified": False}]
        write_table(self.root / "metadata/listing_candidates.parquet", candidates)
        meta_path = self.root / "metadata/manifest.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        for entry in meta["files"]:
            if str(entry.get("path", "")).endswith("listing_candidates.parquet"):
                entry["sha256"] = sha256(self.root / "metadata/listing_candidates.parquet")
                entry["rows"] = len(candidates)
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        result = self.tree.criterion("financials", "announced_at_complete")
        self.assertTrue(result.passed)
        self.assertIn("2 pre-listing period(s)", result.detail)
        self.assertIn("601187 2018-09-30", result.detail)
        self.tree.build()
        # Sampling evidence still decides its own criterion.
        availability = self.root / "evidence/availability/manifest.json"
        availability.write_text(json.dumps({"original_notice_dates_compared": 192,
                                            "original_notice_dates_matched": 190}), encoding="utf-8")
        self.assertFalse(self.tree.criterion("financials", "announced_at_sample_verified").passed)
        availability.write_text(json.dumps({"original_notice_dates_compared": 240,
                                            "original_notice_dates_matched": 238}), encoding="utf-8")
        self.assertTrue(self.tree.criterion("financials", "announced_at_sample_verified").passed)
        # A perfect but undersized sample still fails the floor.
        availability.write_text(json.dumps({"original_notice_dates_compared": 191,
                                            "original_notice_dates_matched": 191}), encoding="utf-8")
        result = self.tree.criterion("financials", "announced_at_sample_verified")
        self.assertFalse(result.passed)
        self.assertIn("191/191", result.detail)
        availability.write_text(json.dumps({"original_notice_dates_compared": 192,
                                            "original_notice_dates_matched": 192}), encoding="utf-8")
        self.assertTrue(self.tree.criterion("financials", "announced_at_sample_verified").passed)

    def test_action_quarantine_share_bound_and_per_item_evidence(self):
        manifest_path = self.root / "actions/manifest.json"
        self.assertTrue(self.tree.criterion("action_samples", "quarantine_share_within_bound").passed)
        # A quarantined action must carry its reason and original response.
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["quarantined_events"] = 1
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = self.tree.criterion("action_samples", "quarantine_reasons_and_evidence_recorded")
        self.assertFalse(result.passed)
        self.assertIn("count only", result.detail)
        source = self.root / "actions/raw_page.json"
        source.write_text(json.dumps({"record": "original"}), encoding="utf-8")
        manifest["errors"] = [{"symbol": "600025", "ex_date": "2019-07-05",
                               "error": "Cash field disagrees with implementation description",
                               "source_file": str(source)}]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertTrue(self.tree.criterion("action_samples",
                                            "quarantine_reasons_and_evidence_recorded").passed)
        # Share above the bound fails even with evidence.
        manifest["rows"] = 500
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertFalse(self.tree.criterion("action_samples", "quarantine_share_within_bound").passed)

    def test_security_metadata_coverage_and_delisting_boundaries(self):
        self.assertTrue(self.tree.criterion("security_metadata", "listing_coverage_complete").passed)
        self.assertTrue(self.tree.criterion("security_metadata", "delisting_boundary_decidable").passed)
        manifest_path = self.root / "metadata/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["coverage"] = {"local_codes_without_listing_reference": 57}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        # The declared count is reported next to the computed one; the evidence
        # still covers every local code, so the computed answer stands.
        result = self.tree.criterion("security_metadata", "listing_coverage_complete")
        self.assertTrue(result.passed)
        self.assertIn("manifest declares 57 uncovered", result.detail)
        # A local code outside both the candidates and the supplement fails.
        self.drop_listing_candidate("600000")
        result = self.tree.criterion("security_metadata", "listing_coverage_complete")
        self.assertFalse(result.passed)
        self.assertIn("1 of 2 local codes lack a listing reference", result.detail)
        self.tree.build()
        # An undecided boundary: the evidence records the state per security.
        boundaries = self.root / "evidence/boundaries/manifest.json"
        boundary = json.loads(boundaries.read_text(encoding="utf-8"))
        boundary.update(boundary_decidable=0, boundary_undecidable=1,
                        decision_state_counts={"no_local_price_series": 1})
        boundaries.write_text(json.dumps(boundary), encoding="utf-8")
        result = self.tree.criterion("security_metadata", "delisting_boundary_decidable")
        self.assertFalse(result.passed)
        self.assertIn("1 of 1 terminated securities have an undecidable boundary", result.detail)
        self.assertIn("no_local_price_series: 1", result.detail)
        # The boundary evidence dataset unregistered is an answerable failure.
        registry_path = self.root / "config/datasets.json"
        config = json.loads(registry_path.read_text(encoding="utf-8"))
        config["datasets"].pop("delisting_boundaries")
        registry_path.write_text(json.dumps(config), encoding="utf-8")
        result = self.tree.criterion("security_metadata", "delisting_boundary_decidable")
        self.assertFalse(result.passed)
        self.assertIn("not registered", result.detail)

    # ---------- gate behaviour ----------

    def test_unreadable_evidence_fails_the_criterion_instead_of_raising(self):
        target = self.root / "stock_daily/600000.parquet"
        target.write_text("not a parquet file", encoding="utf-8")
        result = self.tree.criterion("sse_calendar", "bar_reverse_check_zero_conflicts")
        self.assertFalse(result.passed)
        self.assertIn("cannot read", result.detail)

    def test_readiness_is_memoized_and_evaluated_afresh_without_ttl(self):
        registry = self.tree.registry()
        self.assertTrue(registry.readiness().ready)
        self.assertTrue(registry.readiness().ready)  # cached, still ready
        registry.validation_ttl = 0
        self.assertTrue(registry.readiness().ready)

    def test_incomplete_registry_reports_the_failing_criteria_names(self):
        registry_path = self.root / "config/datasets.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        registry["datasets"].pop("security_metadata")
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        with self.assertRaises(DataNotReady) as caught:
            self.tree.registry().validate_for_backtest()
        self.assertIn("missing datasets", str(caught.exception))
        registry["datasets"]["security_metadata"] = {"manifest": "metadata/manifest.json"}
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        # Now refuse through a criterion instead: a local code without a
        # listing reference in any registered evidence fails coverage.
        self.drop_listing_candidate("600000")
        with self.assertRaises(DataNotReady) as caught:
            self.tree.registry().validate_for_backtest()
        self.assertIn("security_metadata.listing_coverage_complete", str(caught.exception))

    def test_stale_evidence_hash_blocks_before_any_criterion(self):
        manifest_path = self.root / "stock_daily/manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaises(DataNotReady) as caught:
            self.tree.registry().validate_for_backtest()
        self.assertIn("hash mismatch", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
