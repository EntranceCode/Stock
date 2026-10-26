"""Computed dataset readiness: the design's stage-3 gates as executable checks.

``readiness = 全部判据的合取`` (design §5.2). Nothing here trusts a hand-written
``backtest_ready`` declaration: every criterion is answered by one query against
registered evidence - a structural check this module performs itself (schemas,
date arithmetic against the calendar, listing coverage) or a verification record
a registered evidence dataset must carry (price-limit recheck, adjustment
residuals, announcement sampling). When the answering evidence is not
registered, the criterion **fails with that reason**; absent evidence can never
read as a pass.

Each criterion returns a :class:`CriterionResult` naming its evidence path, and
every evaluation is recorded as a manifest (``reports/readiness/latest.json``),
which is the "清单" of §5.2: per-criterion result plus where the answer came
from. The admission gate consumes the conjunction; there is deliberately no
switch that turns a failing criterion into a pass.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Callable, Mapping, Sequence

# Registered dataset names (design §5.1 rows), mapped to the registry keys.
# Both exchanges' calendars are registered separately; the criteria aggregate
# them (union of closed days for the reverse check, both registrations required
# for the scope check).
CALENDAR_DATASET = "sse_calendar"
SZSE_CALENDAR_DATASET = "szse_calendar"
CALENDAR_DATASETS = (CALENDAR_DATASET, SZSE_CALENDAR_DATASET)
STOCK_DATASET = "stock_daily"
ETF_DATASET = "etf_daily"
FINANCIALS_DATASET = "financials"
ACTION_DATASET = "action_samples"
METADATA_DATASET = "security_metadata"

GATED_DATASETS = (CALENDAR_DATASET, STOCK_DATASET, ETF_DATASET, FINANCIALS_DATASET,
                  ACTION_DATASET, METADATA_DATASET)

# Registered evidence datasets whose manifests must carry verification records.
# They are not themselves gated datasets; a criterion that cannot find them
# fails with "evidence not registered" instead of being skipped.
ENRICHED_DATASET = "daily_bar_enriched"
ADJUSTED_DATASET = "adjusted_prices"
RESIDUAL_DATASET = "adjustment_residuals"
UNIVERSE_DATASET = "etf_universe"
SUPPLEMENT_DATASET = "listing_supplement"
BOUNDARY_DATASET = "delisting_boundaries"
AVAILABILITY_DATASET = "financial_availability"

# Names of the parquet column the turnover criterion accepts. The registered
# daily files carry neither; the enriched derivative carries the second form.
TURNOVER_COLUMNS = ("turnover", "turnover_cny_reported")

# action_samples: quarantined share of all rows may not exceed this fraction.
QUARANTINE_SHARE_BOUND = 0.001
# financials: announcement-date sampling must verify at least this share over a
# sample of at least this many rows (design §5.1: "样本 ≥192 行").
SAMPLE_VERIFICATION_FLOOR = 0.99
SAMPLE_VERIFICATION_MIN_ROWS = 192

LATEST_REPORT_RELATIVE = Path("reports") / "readiness" / "latest.json"


@dataclass(frozen=True)
class CriterionResult:
    """One §5.1 criterion's answer: pass/fail, why, and where the answer lives."""

    dataset: str
    criterion: str
    passed: bool
    detail: str
    evidence: str

    def as_dict(self) -> dict:
        return {"dataset": self.dataset, "criterion": self.criterion,
                "passed": self.passed, "detail": self.detail, "evidence": self.evidence}


@dataclass(frozen=True)
class DatasetReadiness:
    dataset: str
    criteria: tuple[CriterionResult, ...]

    @property
    def ready(self) -> bool:
        return all(item.passed for item in self.criteria)

    @property
    def failures(self) -> tuple[CriterionResult, ...]:
        return tuple(item for item in self.criteria if not item.passed)

    def as_dict(self) -> dict:
        return {"dataset": self.dataset, "ready": self.ready,
                "criteria": [item.as_dict() for item in self.criteria]}


@dataclass(frozen=True)
class ReadinessReport:
    evaluated_at: datetime
    datasets: tuple[DatasetReadiness, ...]

    @property
    def ready(self) -> bool:
        return all(item.ready for item in self.datasets)

    @property
    def required_ready(self) -> bool:
        """Readiness over the gated datasets only (evidence datasets may be absent)."""
        return all(item.ready for item in self.datasets if item.dataset in GATED_DATASETS)

    def failures(self) -> list[CriterionResult]:
        return [item for dataset in self.datasets for item in dataset.failures]

    def dataset(self, name: str) -> DatasetReadiness | None:
        return next((item for item in self.datasets if item.dataset == name), None)

    def as_dict(self) -> dict:
        return {"evaluated_at": self.evaluated_at.isoformat(),
                "ready": self.ready, "datasets": [item.as_dict() for item in self.datasets]}


# ---------------------------------------------------------------------------
# evidence access helpers


def _parquet():
    """Import pyarrow, falling back to the vendored build as a last resort."""
    try:
        import pyarrow.parquet as pq
        return pq
    except ImportError:
        pass
    vendored = Path(__file__).resolve().parents[1] / ".deps" / "parquet"
    if vendored.is_dir() and str(vendored) not in sys.path:
        sys.path.append(str(vendored))
    import pyarrow.parquet as pq
    return pq


def _manifest(registry, name: str) -> tuple[Path | None, dict | None]:
    """Registered manifest for a dataset, or (None, None) when absent/invalid."""
    entry = registry.config.get("datasets", {}).get(name)
    if not isinstance(entry, dict) or not isinstance(entry.get("manifest"), str):
        return None, None
    try:
        path = registry._inside(registry.project_root / entry["manifest"])
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError, KeyError, TypeError):
        return None, None
    if not isinstance(manifest, dict):
        return None, None
    return path, manifest


def _entry_path(manifest_path: Path, entry: Mapping) -> Path:
    path = Path(entry.get("path") or entry.get("output") or "")
    return path if path.is_absolute() else manifest_path.parent / path


def _rows(path: Path, columns: Sequence[str]) -> list[dict]:
    return _parquet().read_table(path, columns=list(columns)).to_pylist()


def _file_entries(manifest: Mapping) -> list[dict]:
    entries = manifest.get("files")
    return [item for item in entries if isinstance(item, dict)] if isinstance(entries, list) else []


def _calendar_rows(registry, dataset: str) -> tuple[Path | None, dict | None, list[date], set[date]]:
    """(manifest path, manifest, scheduled open dates, scheduled closed dates)."""
    path, manifest = _manifest(registry, dataset)
    if path is None or not _file_entries(manifest):
        return path, manifest, [], set()
    parquet_path = registry._inside(_entry_path(path, _file_entries(manifest)[0]))
    rows = _rows(parquet_path, ("calendar_date", "is_scheduled_open"))
    open_days: set[date] = set()
    closed_days: set[date] = set()
    for row in rows:
        day = row.get("calendar_date")
        day = day.date() if isinstance(day, datetime) else day
        if not isinstance(day, date):
            continue
        (open_days if row.get("is_scheduled_open") else closed_days).add(day)
    return path, manifest, sorted(open_days), closed_days


def _registered_calendars(registry) -> list[tuple[str, Path | None, dict | None, list[date], set[date]]]:
    """Every registered exchange calendar, SSE first, with its closed-day set."""
    calendars = []
    for dataset in CALENDAR_DATASETS:
        path, manifest, open_days, closed_days = _calendar_rows(registry, dataset)
        if path is not None:
            calendars.append((dataset, path, manifest, open_days, closed_days))
    return calendars


def _open_days_by_exchange(registry) -> dict[str, list[date]]:
    """Scheduled sessions per exchange across every registered calendar."""
    cached = getattr(registry, "_readiness_open_days_by_exchange", None)
    if cached is not None:
        return cached
    per_exchange: dict[str, set[date]] = {}
    for dataset in CALENDAR_DATASETS:
        path, manifest = _manifest(registry, dataset)
        entries = _file_entries(manifest) if manifest else []
        if path is None or not entries:
            continue
        try:
            rows = _rows(registry._inside(_entry_path(path, entries[0])),
                         ("exchange", "calendar_date", "is_scheduled_open"))
        except (OSError, KeyError, TypeError, ValueError):
            continue
        for row in rows:
            if not row.get("is_scheduled_open"):
                continue
            day = row.get("calendar_date")
            day = day.date() if isinstance(day, datetime) else day
            exchange = str(row.get("exchange"))
            if isinstance(day, date) and exchange and exchange != "None":
                per_exchange.setdefault(exchange, set()).add(day)
    registry._readiness_open_days_by_exchange = {exchange: sorted(days) for exchange, days in per_exchange.items()}
    return registry._readiness_open_days_by_exchange


def _open_days_for_exchange(registry, exchange: str | None) -> list[date]:
    """Sessions a security's bars are expected on: its exchange's calendar."""
    by_exchange = _open_days_by_exchange(registry)
    if exchange and exchange in by_exchange:
        return by_exchange[exchange]
    merged = sorted(set().union(*(set(days) for days in by_exchange.values()))) if by_exchange else []
    return merged


def _security_exchange_map(registry) -> dict[str, str]:
    """Local code to exchange from the registered listing candidates, once."""
    path, manifest = _manifest(registry, METADATA_DATASET)
    if path is None:
        return {}
    for entry in _file_entries(manifest):
        if entry.get("dataset") != "listing_candidates":
            continue
        try:
            rows = _rows(registry._inside(_entry_path(path, entry)), ("symbol", "exchange"))
        except (OSError, KeyError, TypeError, ValueError):
            return {}
        mapping = {}
        for row in rows:
            symbol, exchange = str(row.get("symbol")), str(row.get("exchange"))
            if symbol and symbol != "None" and exchange and exchange != "None":
                mapping[symbol] = exchange
        return mapping
    return {}


def _inventory_span(entry: Mapping) -> tuple[date | None, date | None]:
    """First/last bar dates of an inventory item, accepting ISO strings."""
    def parse(value):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                return None
        return None
    return parse(entry.get("start")), parse(entry.get("end"))


# ---------------------------------------------------------------------------
# §5.1 criteria. Each answers one question and names its evidence. A criterion
# whose evidence dataset is not registered fails with that fact; none of them
# can be configured into a pass.


def _bar_reverse_check(registry) -> CriterionResult:
    """calendar: no registered bar is dated on a scheduled-closed day.

    The closed-day set is the union of every registered exchange calendar: a
    day either exchange schedules closed is not a trading day for its market.
    """
    calendars = _registered_calendars(registry)
    if not calendars:
        return CriterionResult(CALENDAR_DATASET, "bar_reverse_check_zero_conflicts", False,
                               "no exchange calendar is registered", "config/datasets.json")
    closed_days = set().union(*(item[4] for item in calendars))
    evidence = "; ".join(f"{dataset} {len(open_days)} open / {len(closed)} closed"
                         for dataset, _, _, open_days, closed in calendars)
    conflicts, scanned, by_dataset = 0, 0, {}
    for dataset in (STOCK_DATASET, ETF_DATASET):
        manifest_path, dataset_manifest = _manifest(registry, dataset)
        if manifest_path is None:
            continue
        dataset_conflicts = 0
        for entry in _file_entries(dataset_manifest):
            path = registry._inside(_entry_path(manifest_path, entry))
            try:
                days = [row["trade_date"] for row in _rows(path, ("trade_date",))]
            except (OSError, KeyError, TypeError, ValueError) as exc:
                return CriterionResult(CALENDAR_DATASET, "bar_reverse_check_zero_conflicts", False,
                                       f"cannot read registered bar dates: {exc}", str(path))
            scanned += 1
            for day in days:
                day = day.date() if isinstance(day, datetime) else day
                if day in closed_days:
                    dataset_conflicts += 1
        by_dataset[dataset] = dataset_conflicts
        conflicts += dataset_conflicts
    return CriterionResult(CALENDAR_DATASET, "bar_reverse_check_zero_conflicts", conflicts == 0,
                           f"{conflicts} bar(s) dated on scheduled-closed days across "
                           f"{scanned} registered files ({by_dataset}); calendars: {evidence}",
                           "config/datasets.json")


def _calendar_scope(registry) -> CriterionResult:
    """calendar: both exchanges' annual notices are registered and complete.

    Design §5.1 requires the two exchanges' annual schedules to be registered;
    the SSE and SZSE calendars are separate datasets here, so the criterion
    checks both registrations, each manifest's declared source scope, and
    records any disagreement between them instead of hiding it.
    """
    calendars = _registered_calendars(registry)
    if not calendars:
        return CriterionResult(CALENDAR_DATASET, "both_exchange_annual_notices_complete", False,
                               "no exchange calendar is registered", "config/datasets.json")
    missing = [dataset for dataset in CALENDAR_DATASETS
               if dataset not in {item[0] for item in calendars}]
    incomplete, inventories = [], []
    for dataset, path, manifest, _, _ in calendars:
        if not _file_entries(manifest):
            inventories.append(dataset)
        if manifest.get("calendar_source_scope_complete") is not True:
            incomplete.append(dataset)
    differences = ""
    by_name = {item[0]: item for item in calendars}
    if CALENDAR_DATASET in by_name and SZSE_CALENDAR_DATASET in by_name:
        sse_closed, szse_closed = by_name[CALENDAR_DATASET][4], by_name[SZSE_CALENDAR_DATASET][4]
        only_sse = sorted(str(day) for day in sse_closed - szse_closed)
        only_szse = sorted(str(day) for day in szse_closed - sse_closed)
        differences = (f"; closed only in SSE: {only_sse or 'none'}; "
                       f"closed only in SZSE: {only_szse or 'none'}")
    passed = not missing and not incomplete and not inventories
    detail = (f"calendars registered: {[item[0] for item in calendars]}; "
              f"missing: {missing or 'none'}; "
              f"scope incomplete: {incomplete or 'none'}; "
              f"no file inventory: {inventories or 'none'}"
              + differences)
    return CriterionResult(CALENDAR_DATASET, "both_exchange_annual_notices_complete", passed,
                           detail, "config/datasets.json")


def _daily_missing_days(registry, dataset: str) -> CriterionResult:
    """<dataset>: bar-missing sessions are zero or individually recorded.

    Design §5.1 allows exactly two states per security: no missing session, or
    a registered ``session_gap_records`` entry naming the symbol, its missing
    dates, and the reason. A record without all three parts is not a record of
    anything, so a security whose gaps are unexplained fails the criterion.
    """
    path, manifest = _manifest(registry, dataset)
    if path is None:
        return CriterionResult(dataset, "missing_days_zero_or_recorded", False,
                               "dataset manifest is missing", "config/datasets.json")
    entries = _file_entries(manifest)
    if not entries:
        return CriterionResult(dataset, "missing_days_zero_or_recorded", False,
                               "dataset has no file inventory", str(path))
    calendars = _registered_calendars(registry)
    if not calendars:
        return CriterionResult(dataset, "missing_days_zero_or_recorded", False,
                               "no exchange calendar is registered; expected sessions cannot be counted",
                               "config/datasets.json")
    exchange_of = _security_exchange_map(registry)
    recorded = manifest.get("session_gap_records")
    recorded = recorded if isinstance(recorded, list) else []
    incomplete = [item for item in recorded
                  if not (isinstance(item, dict) and item.get("symbol")
                          and isinstance(item.get("dates"), list) and item.get("cause"))]
    recorded_symbols = {item["symbol"] for item in recorded
                        if isinstance(item, dict) and item.get("symbol")
                        and isinstance(item.get("dates"), list) and item.get("cause")}
    missing_total, affected, unbounded, unrecorded = 0, [], 0, []
    open_days_by_exchange: dict[str, list[date]] = {}
    for entry in entries:
        symbol = str(entry.get("symbol"))
        try:
            parquet_path = registry._inside(_entry_path(path, entry))
            days = _rows(parquet_path, ("trade_date",))
        except (OSError, KeyError, TypeError, ValueError) as exc:
            return CriterionResult(dataset, "missing_days_zero_or_recorded", False,
                                   f"cannot read registered bar dates for {symbol}: {exc}",
                                   str(path))
        series = set()
        for row in days:
            day = row.get("trade_date")
            day = day.date() if isinstance(day, datetime) else day
            if isinstance(day, date):
                series.add(day)
        if not series:
            return CriterionResult(dataset, "missing_days_zero_or_recorded", False,
                                   f"registered series for {symbol} holds no dated bar", str(path))
        exchange = exchange_of.get(symbol)
        if exchange not in open_days_by_exchange:
            open_days_by_exchange[exchange] = _open_days_for_exchange(registry, exchange)
        open_days = open_days_by_exchange[exchange]
        # Expected sessions are counted inside the window this series actually
        # covers: comparing a calendar-window expectation with a lifetime row
        # count silently zeroes out every series that predates the calendar.
        covered = [day for day in open_days if min(series) <= day <= max(series)]
        missing = [day for day in covered if day not in series]
        if missing:
            missing_total += len(missing)
            affected.append(symbol)
            if symbol not in recorded_symbols:
                unrecorded.append(symbol)
        if open_days and max(series) > open_days[-1]:
            unbounded += 1
    passed = not unrecorded and not incomplete
    detail = (f"{missing_total} missing session(s) across {len(affected)} securities; "
              f"{len(recorded_symbols)} session_gap record(s) registered; "
              f"{len(unrecorded)} unexplained "
              f"({', '.join(sorted(set(unrecorded))) or 'none'}); "
              f"{len(incomplete)} record(s) lack symbol/dates/cause; "
              f"{unbounded} securities extend past calendar coverage")
    return CriterionResult(dataset, "missing_days_zero_or_recorded", passed, detail, str(path))


def _daily_turnover(registry, dataset: str) -> CriterionResult:
    """<dataset>: turnover is a non-null column in every registered file."""
    path, manifest = _manifest(registry, dataset)
    if path is None:
        return CriterionResult(dataset, "turnover_column_complete", False,
                               "dataset manifest is missing", "config/datasets.json")
    entries = _file_entries(manifest)
    if not entries:
        return CriterionResult(dataset, "turnover_column_complete", False,
                               "dataset has no file inventory", str(path))
    pq = _parquet()
    missing_count, null_count, checked = 0, 0, 0
    samples: list[str] = []
    for entry in entries:
        parquet_path = _entry_path(path, entry)
        try:
            schema = pq.read_schema(parquet_path).names
        except (OSError, ValueError) as exc:
            return CriterionResult(dataset, "turnover_column_complete", False,
                                   f"cannot read registered file schema: {exc}", str(parquet_path))
        column = next((name for name in TURNOVER_COLUMNS if name in schema), None)
        if column is None:
            missing_count += 1
            if len(samples) < 3:
                samples.append(f"{entry.get('symbol', parquet_path.name)}:{schema}")
            continue
        checked += 1
        null_count += sum(1 for value in _rows(parquet_path, (column,)) if value.get(column) is None)
    if missing_count:
        return CriterionResult(dataset, "turnover_column_complete", False,
                               f"{missing_count} of {len(entries)} registered files carry no "
                               f"turnover column, and {checked} checked file(s) contain "
                               f"{null_count} null turnover value(s) (first schema: {samples[0]})",
                               str(path))
    passed = null_count == 0
    return CriterionResult(dataset, "turnover_column_complete", passed,
                           f"{checked} files checked; {null_count} null turnover value(s)", str(path))


def _bare_symbol(symbol) -> str:
    """Local code without a venue prefix: the daily datasets register
    exchange-prefixed symbols (``sh510050``) while provider products carry the
    bare code (``510050``); the comparison normalizes both."""
    text = str(symbol)
    return text[2:] if text[:2] in ("sh", "sz") else text


def _limit_recheck(registry, dataset: str) -> CriterionResult:
    """<dataset>: a registered price-limit recheck covers the daily bars with zero breaches.

    The recheck product must cover *this dataset's* securities before a zero
    breach count means anything (design §5.1: "复验记录覆盖全部 bar"), so the
    criterion compares symbol sets - a recheck over unrelated securities, however
    large, does not vouch for this dataset's bars.
    """
    path, manifest = _manifest(registry, ENRICHED_DATASET)
    if path is None:
        return CriterionResult(dataset, "price_limit_recheck_zero_breaches", False,
                               f"recheck evidence dataset '{ENRICHED_DATASET}' is not registered",
                               "config/datasets.json")
    breaches = manifest.get("rows_breaching_derived_limits")
    if not isinstance(breaches, int) or isinstance(breaches, bool):
        return CriterionResult(dataset, "price_limit_recheck_zero_breaches", False,
                               "registered recheck record lacks a breach count", str(path))
    recheck_symbols = {_bare_symbol(entry.get("symbol")) for entry in _file_entries(manifest)
                       if entry.get("symbol")}
    daily_path, daily_manifest = _manifest(registry, dataset)
    daily_symbols = {_bare_symbol(entry.get("symbol")) for entry in _file_entries(daily_manifest or {})
                     if entry.get("symbol")}
    uncovered = sorted(daily_symbols - recheck_symbols)
    recheck_rows, recheck_size = manifest.get("rows"), len(recheck_symbols)
    detail = (f"{breaches} breaching session(s) in the registered recheck over "
              f"{recheck_size} securities / {recheck_rows} rows; "
              f"{len(uncovered)} of {len(daily_symbols)} {dataset} securities "
              f"are absent from the recheck ({', '.join(uncovered[:5]) or 'none'})")
    return CriterionResult(dataset, "price_limit_recheck_zero_breaches",
                           breaches == 0 and not uncovered, detail, str(path))


def _adjustment_residuals(registry, dataset: str) -> CriterionResult:
    """<dataset>: residual adjustment jumps are enumerated and zero.

    Design §5.1 asks for two things at once: no ex-date bar moves beyond the
    price limit after adjustment, and every residual jump that does occur is
    individually logged. The registered ``adjustment_residuals`` evidence run
    enumerates them with the event fields behind each one; a count without the
    enumeration, or a non-zero count, fails the criterion.
    """
    path, manifest = _manifest(registry, RESIDUAL_DATASET)
    if path is None:
        return CriterionResult(dataset, "adjustment_residual_jumps_logged", False,
                               f"residual evidence dataset '{RESIDUAL_DATASET}' is not registered",
                               "config/datasets.json")
    count = manifest.get("ex_dates_beyond_price_limit_after_adjustment")
    if not isinstance(count, int) or isinstance(count, bool):
        return CriterionResult(dataset, "adjustment_residual_jumps_logged", False,
                               "registered residual record lacks its count", str(path))
    checked = manifest.get("ex_dates_checked")
    evidence = manifest.get("residual_jump_evidence")
    enumeration = None
    if isinstance(evidence, str) and evidence:
        enumeration = Path(evidence)
        if not enumeration.is_absolute():
            enumeration = path.parent / enumeration
        if not registry._inside(enumeration).is_file():
            enumeration = None
    if enumeration is None:
        return CriterionResult(dataset, "adjustment_residual_jumps_logged", False,
                               f"{count} residual jump(s) are counted but not individually logged",
                               str(path))
    listed = 0
    for line in enumeration.read_text(encoding="utf-8").splitlines():
        if line.strip():
            listed += 1
    if listed != count:
        return CriterionResult(dataset, "adjustment_residual_jumps_logged", False,
                               f"{count} residual jump(s) counted but {listed} individually logged",
                               str(enumeration))
    if checked is None:
        checked = manifest.get("ex_dates_audited")
    return CriterionResult(dataset, "adjustment_residual_jumps_logged", count == 0,
                           f"{count} residual adjustment jump(s) beyond the price limit, "
                           f"each individually logged; {checked} ex-date(s) checked",
                           str(enumeration))


def _listing_dates(registry) -> tuple[dict[str, date], list[str]]:
    """Listing dates for local codes, with the evidence files they came from.

    Official listing candidates come first; the provider ETF universe run
    supplements codes the official snapshots do not cover. Both are registered
    evidence, and the returned source list names which files were read so the
    criterion can show where an answer came from.
    """
    listed: dict[str, date] = {}
    sources: list[str] = []

    def add(symbol, listed_on):
        if isinstance(listed_on, datetime):
            listed_on = listed_on.date()
        if isinstance(listed_on, str):
            try:
                listed_on = date.fromisoformat(listed_on[:10])
            except ValueError:
                listed_on = None
        if isinstance(symbol, str) and isinstance(listed_on, date) and symbol not in listed:
            listed[symbol] = listed_on

    meta_path, meta_manifest = _manifest(registry, METADATA_DATASET)
    if meta_manifest is not None and meta_path is not None:
        for entry in _file_entries(meta_manifest):
            parquet_path = registry._inside(_entry_path(meta_path, entry))
            if "listing" not in parquet_path.name:
                continue
            sources.append(str(parquet_path))
            try:
                rows = _rows(parquet_path, ("symbol", "source_listing_date"))
            except (OSError, KeyError, TypeError, ValueError):
                continue
            for row in rows:
                add(row.get("symbol"), row.get("source_listing_date"))

    universe_path, universe_manifest = _manifest(registry, UNIVERSE_DATASET)
    if universe_manifest is not None and universe_path is not None:
        for entry in _file_entries(universe_manifest):
            candidate = registry._inside(_entry_path(universe_path, entry))
            try:
                schema = _parquet().read_schema(candidate).names
            except (OSError, ValueError):
                continue
            columns = [name for name in ("symbol", "code", "ipo_date", "code_name")
                       if name in schema]
            if not {"code", "ipo_date"} <= set(columns):
                continue
            sources.append(str(candidate))
            for row in _rows(candidate, columns):
                code = str(row.get("code") or "")
                # provider symbols carry a venue prefix ("sh.510050"); the local
                # daily bars carry the exchange-suffixed form ("sh510050").
                normalized = code.replace(".", "").lower()
                add(normalized, row.get("ipo_date"))
                if row.get("symbol"):
                    add(str(row["symbol"]), row.get("ipo_date"))
    return listed, sources


def _etf_universe(registry) -> CriterionResult:
    """etf_daily: the investable universe is rebuilt from listing information."""
    path, manifest = _manifest(registry, ETF_DATASET)
    if path is None:
        return CriterionResult(ETF_DATASET, "universe_rebuilt_from_listing", False,
                               "dataset manifest is missing", "config/datasets.json")
    symbols = [entry.get("symbol") for entry in _file_entries(manifest)
               if isinstance(entry.get("symbol"), str)]
    if not symbols:
        return CriterionResult(ETF_DATASET, "universe_rebuilt_from_listing", False,
                               "dataset has no file inventory", str(path))
    listed, sources = _listing_dates(registry)
    missing = sorted(symbol for symbol in symbols if symbol not in listed)
    evidence = "; ".join(sources) if sources else "no listing evidence registered"
    return CriterionResult(ETF_DATASET, "universe_rebuilt_from_listing", not missing,
                           f"{len(missing)} of {len(symbols)} registered ETFs lack a listing date "
                           f"({', '.join(missing[:5]) or 'none'}); listing dates resolved from: {evidence}",
                           evidence)


def _financial_announced_at(registry) -> CriterionResult:
    """financials: no usable fundamental row lacks its announcement date.

    A row whose report period precedes the security's verified listing date
    cannot have a securities announcement: the issuer was not yet listed, so
    the disclosure the criterion asks for does not exist. Those rows are
    enumerated with the cause and the listing evidence and counted apart - the
    engine already excludes them because they carry no availability date. A
    row on or after the verified listing date without an announcement date
    fails the criterion; if the listing date itself is unknown, the row stays
    a failure because the question cannot be answered.
    """
    path, manifest = _manifest(registry, FINANCIALS_DATASET)
    if path is None:
        return CriterionResult(FINANCIALS_DATASET, "announced_at_complete", False,
                               "dataset manifest is missing", "config/datasets.json")
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return CriterionResult(FINANCIALS_DATASET, "announced_at_complete", False,
                               "parquet stack is unavailable; cannot inspect announcement dates",
                               "config/datasets.json")
    listed, _ = _listing_dates(registry)
    usable_missing, pre_listing, unknown_listing = [], [], []
    for entry in _file_entries(manifest):
        count = entry.get("unknown_announcement_rows")
        if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
            continue
        symbol = str(entry.get("symbol"))
        listed_on = listed.get(symbol)
        try:
            rows = pq.read_table(registry._inside(_entry_path(path, entry)),
                                 columns=["REPORT_DATE", "NOTICE_DATE"]).to_pylist()
        except (OSError, ValueError, KeyError) as exc:
            usable_missing.append(f"{symbol}: rows unreadable ({exc})")
            continue
        for row in rows:
            if row.get("NOTICE_DATE") is None or str(row.get("NOTICE_DATE")).strip() in ("", "None"):
                period = str(row.get("REPORT_DATE"))[:10]
                if listed_on is None:
                    unknown_listing.append(f"{symbol} {period}")
                elif period < listed_on.isoformat():
                    pre_listing.append(f"{symbol} {period}")
                else:
                    usable_missing.append(f"{symbol} {period}")
    detail = (f"{len(usable_missing)} usable row(s) without an announcement date; "
              f"{len(pre_listing)} pre-listing period(s) with no securities disclosure "
              f"({', '.join(pre_listing) or 'none'}); {len(unknown_listing)} row(s) undecidable "
              f"without a listing reference")
    return CriterionResult(FINANCIALS_DATASET, "announced_at_complete",
                           not usable_missing and not unknown_listing, detail, str(path))


def _financial_sample_verification(registry) -> CriterionResult:
    """financials: announcement dates were sampled against original notices."""
    path, manifest = _manifest(registry, AVAILABILITY_DATASET)
    if path is None:
        return CriterionResult(FINANCIALS_DATASET, "announced_at_sample_verified", False,
                               f"verification evidence dataset '{AVAILABILITY_DATASET}' is not registered",
                               "config/datasets.json")
    compared, matched = manifest.get("original_notice_dates_compared"), manifest.get("original_notice_dates_matched")
    if not isinstance(compared, int) or compared < 1 or not isinstance(matched, int):
        return CriterionResult(FINANCIALS_DATASET, "announced_at_sample_verified", False,
                               "registered verification record lacks its sample counts", str(path))
    share = matched / compared
    passed = share >= SAMPLE_VERIFICATION_FLOOR and compared >= SAMPLE_VERIFICATION_MIN_ROWS
    return CriterionResult(FINANCIALS_DATASET, "announced_at_sample_verified", passed,
                           f"{matched}/{compared} announcement dates matched original notices "
                           f"({share:.4f} ≥ {SAMPLE_VERIFICATION_FLOOR} over ≥{SAMPLE_VERIFICATION_MIN_ROWS} rows required)",
                           str(path))


def _action_quarantine_share(registry) -> CriterionResult:
    """action_samples: quarantined actions stay within the bounded share."""
    path, manifest = _manifest(registry, ACTION_DATASET)
    if path is None:
        return CriterionResult(ACTION_DATASET, "quarantine_share_within_bound", False,
                               "dataset manifest is missing", "config/datasets.json")
    quarantined, rows = manifest.get("quarantined_events"), manifest.get("rows")
    if not isinstance(quarantined, int) or not isinstance(rows, int) or rows < 1:
        return CriterionResult(ACTION_DATASET, "quarantine_share_within_bound", False,
                               "manifest lacks a quarantined count or a row total", str(path))
    share = quarantined / rows
    return CriterionResult(ACTION_DATASET, "quarantine_share_within_bound", share <= QUARANTINE_SHARE_BOUND,
                           f"{quarantined}/{rows} actions quarantined ({share:.5f} ≤ "
                           f"{QUARANTINE_SHARE_BOUND} required)", str(path))


def _action_quarantine_evidence(registry) -> CriterionResult:
    """action_samples: every quarantined action carries its reason and source.

    The action manifest records its quarantined disagreements under ``errors``
    (a bounded, evidenced isolation per design §4.2), each item naming the
    disagreement reason and the original response it came from; a future
    ``quarantine_evidence`` list is accepted the same way.
    """
    path, manifest = _manifest(registry, ACTION_DATASET)
    if path is None:
        return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                               "dataset manifest is missing", "config/datasets.json")
    records = manifest.get("quarantine_evidence")
    if not isinstance(records, list) or not records:
        records = manifest.get("errors")
    quarantined = manifest.get("quarantined_events")
    if not isinstance(quarantined, int) or isinstance(quarantined, bool):
        return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                               "manifest lacks a quarantined count", str(path))
    if quarantined == 0:
        return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", True,
                               "no quarantined action is recorded", str(path))
    if not isinstance(records, list) or not records:
        return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                               f"{quarantined} quarantined action(s) are recorded as a count only; "
                               f"per-item reason and original evidence are not registered", str(path))
    for item in records:
        if not isinstance(item, dict):
            return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                                   "a quarantine record is not an object", str(path))
        reason = item.get("reason") or item.get("error")
        source = item.get("source") or item.get("source_file")
        if not reason or not source:
            return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                                   "a quarantine record lacks its reason or original source", str(path))
        source_path = Path(str(source))
        if not source_path.is_absolute():
            source_path = path.parent / source_path
        if not registry._inside(source_path).is_file():
            return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", False,
                                   f"quarantine evidence file is missing: {source_path}", str(source_path))
    return CriterionResult(ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", True,
                           f"{len(records)} quarantined action(s) each carry a reason and original evidence", str(path))


def _listing_coverage(registry) -> CriterionResult:
    """security_metadata: every local code has a listing reference.

    The answer is computed from the registered evidence: the official listing
    candidates plus the clearly labeled provider supplement for the codes the
    official snapshots do not cover. The manifest's own count is reported next
    to the computed one, so the two can never silently disagree.
    """
    path, manifest = _manifest(registry, METADATA_DATASET)
    if path is None:
        return CriterionResult(METADATA_DATASET, "listing_coverage_complete", False,
                               "dataset manifest is missing", "config/datasets.json")
    declared = None
    coverage = manifest.get("coverage")
    if isinstance(coverage, dict) and isinstance(coverage.get("local_codes_without_listing_reference"), int):
        declared = coverage["local_codes_without_listing_reference"]
    listed, official_sources = _listing_dates(registry)
    stock_path, stock_manifest = _manifest(registry, STOCK_DATASET)
    local_codes = {str(entry.get("symbol")) for entry in _file_entries(stock_manifest or {})
                   if entry.get("symbol")}
    supplement_path, supplement = _manifest(registry, SUPPLEMENT_DATASET)
    supplemented = set()
    if supplement is not None and supplement_path is not None:
        for entry in _file_entries(supplement):
            if str(entry.get("path", "")).endswith(".jsonl"):
                try:
                    for line in registry._inside(_entry_path(supplement_path, entry)) \
                            .read_text(encoding="utf-8").splitlines():
                        if line.strip():
                            supplemented.add(str(json.loads(line).get("symbol")))
                except (OSError, ValueError):
                    supplemented = set()
                    break
    missing = sorted(local_codes - set(listed) - supplemented)
    detail = (f"{len(missing)} of {len(local_codes)} local codes lack a listing reference; "
              f"{len(listed)} resolved from {len(official_sources)} official evidence file(s); "
              f"{len(supplemented)} provider-supplemented"
              + (f"; manifest declares {declared} uncovered" if declared is not None
                 else "; manifest declares no uncovered count"))
    return CriterionResult(METADATA_DATASET, "listing_coverage_complete", not missing,
                           detail, "; ".join(official_sources) or str(path))


def _delisting_boundary(registry) -> CriterionResult:
    """security_metadata: every terminated security's boundary is decidable.

    The answer comes from the registered boundary evidence, which records per
    security whether the last tradable day is decidable from registered data
    (observed last bar equals the registered termination date) or needs the
    delisting announcement. A termination date alone never decides it.
    """
    path, manifest = _manifest(registry, METADATA_DATASET)
    if path is None:
        return CriterionResult(METADATA_DATASET, "delisting_boundary_decidable", False,
                               "dataset manifest is missing", "config/datasets.json")
    boundary_path, boundary = _manifest(registry, BOUNDARY_DATASET)
    if boundary_path is None or not isinstance(boundary, dict):
        return CriterionResult(METADATA_DATASET, "delisting_boundary_decidable", False,
                               f"boundary evidence dataset '{BOUNDARY_DATASET}' is not registered",
                               "config/datasets.json")
    terminated = boundary.get("terminated_securities")
    decidable = boundary.get("boundary_decidable")
    if not isinstance(terminated, int) or not isinstance(decidable, int):
        return CriterionResult(METADATA_DATASET, "delisting_boundary_decidable", False,
                               "registered boundary record lacks its counts", str(boundary_path))
    counts = boundary.get("decision_state_counts")
    states = "; ".join(f"{state}: {count}" for state, count in sorted(counts.items())) \
        if isinstance(counts, dict) else "states not recorded"
    undecidable = terminated - decidable
    return CriterionResult(METADATA_DATASET, "delisting_boundary_decidable",
                           terminated > 0 and undecidable == 0,
                           f"{undecidable} of {terminated} terminated securities have an undecidable "
                           f"boundary (a termination date is not the last tradable day); {states}",
                           str(boundary_path))


# (dataset, criterion id, evaluator). The ids are the stable names written into
# every evaluation manifest and quoted by the admission gate's refusal message.
CRITERIA: tuple[tuple[str, str, Callable], ...] = (
    (CALENDAR_DATASET, "bar_reverse_check_zero_conflicts", _bar_reverse_check),
    (CALENDAR_DATASET, "both_exchange_annual_notices_complete", _calendar_scope),
    (STOCK_DATASET, "missing_days_zero_or_recorded",
     lambda registry: _daily_missing_days(registry, STOCK_DATASET)),
    (STOCK_DATASET, "turnover_column_complete",
     lambda registry: _daily_turnover(registry, STOCK_DATASET)),
    (STOCK_DATASET, "price_limit_recheck_zero_breaches",
     lambda registry: _limit_recheck(registry, STOCK_DATASET)),
    (STOCK_DATASET, "adjustment_residual_jumps_logged",
     lambda registry: _adjustment_residuals(registry, STOCK_DATASET)),
    (ETF_DATASET, "missing_days_zero_or_recorded",
     lambda registry: _daily_missing_days(registry, ETF_DATASET)),
    (ETF_DATASET, "turnover_column_complete",
     lambda registry: _daily_turnover(registry, ETF_DATASET)),
    (ETF_DATASET, "price_limit_recheck_zero_breaches",
     lambda registry: _limit_recheck(registry, ETF_DATASET)),
    (ETF_DATASET, "adjustment_residual_jumps_logged",
     lambda registry: _adjustment_residuals(registry, ETF_DATASET)),
    (ETF_DATASET, "universe_rebuilt_from_listing", _etf_universe),
    (FINANCIALS_DATASET, "announced_at_complete", _financial_announced_at),
    (FINANCIALS_DATASET, "announced_at_sample_verified", _financial_sample_verification),
    (ACTION_DATASET, "quarantine_share_within_bound", _action_quarantine_share),
    (ACTION_DATASET, "quarantine_reasons_and_evidence_recorded", _action_quarantine_evidence),
    (METADATA_DATASET, "listing_coverage_complete", _listing_coverage),
    (METADATA_DATASET, "delisting_boundary_decidable", _delisting_boundary),
)


def evaluate_readiness(registry, *, record_report: bool = True) -> ReadinessReport:
    """Evaluate every §5.1 criterion and record the outcome (§5.2).

    A criterion that cannot be answered - unreadable evidence, a missing
    dependency - fails with the reason; it is never silently passed.
    """
    results: dict[str, list[CriterionResult]] = {name: [] for name in GATED_DATASETS}
    for dataset, name, function in CRITERIA:
        try:
            result = function(registry)
        except Exception as exc:  # an unanswerable criterion fails, never passes
            result = CriterionResult(dataset, name, False,
                                     f"criterion could not be evaluated: {exc}", "evaluation error")
        results[dataset].append(result)
    report = ReadinessReport(datetime.now(timezone.utc),
                             tuple(DatasetReadiness(name, tuple(sorted(results[name], key=lambda item: item.criterion)))
                                   for name in GATED_DATASETS))
    if record_report:
        write_report(registry, report)
    return report


def write_report(registry, report: ReadinessReport) -> Path | None:
    """Record the evaluation manifest (best effort; never blocks the gate)."""
    target = registry.project_root / LATEST_REPORT_RELATIVE
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(report.as_dict(), ensure_ascii=False, indent=2, allow_nan=False),
                             encoding="utf-8")
        os.replace(temporary, target)
    except OSError:
        return None
    return target


def criteria_index() -> tuple[tuple[str, str], ...]:
    """(dataset, criterion) pairs this module evaluates, for documentation/tests."""
    return tuple((dataset, name) for dataset, name, _ in CRITERIA)
