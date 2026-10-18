"""Build candidate point-in-time snapshot bundles from registered staging evidence.

A snapshot series is one bundle per trading session: a bundle manifest plus a
JSON payload carrying every field the engine contract requires. The builder
reads only registered datasets (daily prices, security metadata, the exchange
calendar) plus two explicitly named derivative products (adjusted prices and
enriched daily bars), because ``DailyBar`` demands turnover, publication time,
tradability flags and an adjusted close that the registered daily files do not
yet carry.

Honesty rules, in order of importance:

* A symbol enters the series only when every contract-mandated field can be
  produced from evidence. Everything else is excluded with a recorded reason
  and count; exclusion is a normal outcome, not an error.
* Values the sources do not provide are never invented silently. The few
  derivations that are unavoidable are declared *assumptions*: each one adds a
  readiness gap to every bundle, and a bundle with any gap is refused by
  ``StagedMarketData.load_snapshot`` until the underlying evidence exists.
  Assumptions therefore keep the pipeline exercisable without ever making a
  bundle admissible.
* Unverified security metadata stays explicitly unverified, so the engine
  excludes those securities instead of trading on a guess.
* No admission bypass exists here. The series records the registry admission
  state at build time and stops; loading a series still requires the registry
  gate to pass on its own evidence.

Bundle layout (all inside the project)::

    data/external/snapshot_series/<series_id>/series.json
    data/external/snapshot_series/<series_id>/sessions/<YYYYMMDD>/manifest.json
    data/external/snapshot_series/<series_id>/sessions/<YYYYMMDD>/payload.json

``series.json`` indexes every session with both hashes; each session directory
is a self-contained bundle whose manifest names its payload relatively, as the
snapshot contract requires.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from uuid import uuid4

from . import __version__
from .data_registry import DataNotReady, DatasetRegistry
from .market_data import _use_vendored_parquet
from .models import (ENGINE_TRAILING_WINDOW_SESSIONS, AssetType, DailyBar,
                     DataSnapshot, Security, StockFactorObservation)

CHINA_TIME = timezone(timedelta(hours=8))
SCHEMA_VERSION = "1.0"
SOURCE_KIND = "verified_history"

# Dataset-provided asset types mapped onto the engine enum. An unmapped value
# excludes the symbol: guessing equity versus debt changes which risk cap
# applies, so it must come from the source.
_ASSET_TYPES = {
    "stock": AssetType.STOCK,
    "equity_etf": AssetType.EQUITY_ETF,
    "government_bond_etf": AssetType.BOND_ETF,
    "bond_etf": AssetType.BOND_ETF,
}

# Declared derivations. Each key is accepted on the command line; the tuple
# explains the derivation and names the readiness gap it adds to every bundle.
ASSUMPTIONS = {
    "session_close_availability": (
        "bar available_at assumed to equal the session decision time",
        "bar_available_at_assumed_session_close",
    ),
    "buy_sell_equal_to_tradable": (
        "buyable/sellable assumed equal to the provider tradable flag",
        "buy_sell_eligibility_assumed_equal_to_tradable",
    ),
    "turnover_provider_reported": (
        "provider-reported turnover accepted as DailyBar.turnover",
        "turnover_provider_reported_unverified",
    ),
    "lot_size_conventional_default": (
        "buy_lot_size assumed 100 shares per lot",
        "buy_lot_size_assumed_100",
    ),
    "settlement_conventional_default": (
        "settlement_sessions assumed 1 session",
        "settlement_assumed_t_plus_1",
    ),
}
FACTOR_GAP = "factor_observations_not_yet_constructed"
FACTOR_VERSION_GAP = "factor_original_versions_unverified"
LISTING_GAP = "listing_date_from_candidate_unverified"
ADMISSION_GAP = "registry_admission_not_granted"


class SeriesError(ValueError):
    """Raised for unusable inputs; never for honest exclusions."""


@dataclass(frozen=True)
class SeriesRequest:
    """Everything needed to build one series; no field is guessed."""

    start: date
    end: date
    adjusted_dir: Path
    enriched_dir: Path
    calendar_path: Path | None = None
    symbols: tuple[str, ...] = ()
    max_symbols: int | None = None
    assume: tuple[str, ...] = ()
    decision_time: time = time(16, 30)
    verify_hashes: bool = True
    trailing_window: int = ENGINE_TRAILING_WINDOW_SESSIONS
    factors_dir: Path | None = None
    etf_dir: Path | None = None

    def __post_init__(self):
        if self.start > self.end:
            raise SeriesError("start must not follow end")
        unknown = sorted(set(self.assume) - set(ASSUMPTIONS))
        if unknown:
            raise SeriesError("unknown assumptions: " + ", ".join(unknown))
        if not 121 <= self.trailing_window:
            raise SeriesError("trailing_window must be at least 121 sessions")


@dataclass(frozen=True)
class SymbolSource:
    """One symbol's registered daily evidence plus its resolved asset type."""

    symbol: str
    dataset: str
    asset_type: AssetType | None
    rows: tuple[dict, ...]
    source_path: Path
    provider_listed_on: date | None = None
    provider_tracking_index: str | None = None


def _parquet_module():
    try:
        import pyarrow.parquet as pq
    except ImportError:
        _use_vendored_parquet(Path(__file__).resolve().parents[1] / ".deps" / "parquet")
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise SeriesError("Parquet reading dependency is unavailable") from exc
    return pq


def _read_rows(path: Path, columns: Sequence[str]) -> list[dict]:
    return _parquet_module().read_table(path, columns=list(columns)).to_pylist()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _inside(root: Path, path: Path) -> Path:
    resolved = path if path.is_absolute() else root / path
    resolved = resolved.resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise SeriesError("Series evidence must stay inside the project")
    return resolved


def _json_bytes(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")


def _write_once(path: Path, body: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(body)


def _iso(value: date | datetime) -> str:
    return value.isoformat()


def _entry_path(manifest_path: Path, entry: Mapping) -> Path:
    path = Path(entry.get("path") or entry.get("output") or "")
    if not path.is_absolute():
        path = manifest_path.parent / path
    return path


class _DerivativeIndex:
    """Adjusted-price, enriched-bar and ETF-evidence products, by symbol."""

    def __init__(self, adjusted_dir: Path, enriched_dir: Path, etf_dir: Path | None = None):
        self.adjusted_dir = Path(adjusted_dir)
        self.enriched_dir = Path(enriched_dir)
        self.etf_dir = Path(etf_dir) if etf_dir is not None else None
        self._adjusted: dict[str, dict[str, float]] = {}
        self._enriched: dict[str, dict[str, dict]] = {}
        self._etf: dict[str, dict[str, dict]] = {}
        self.etf_manifest: dict = {}

    def _load(self, directory: Path, target: dict, columns: Sequence[str]) -> None:
        manifest_path = directory / "manifest.json"
        if not manifest_path.is_file():
            raise SeriesError(f"Derivative product manifest not found: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entries = manifest.get("files")
        if not isinstance(entries, list) or not entries:
            raise SeriesError(f"Derivative product has no file inventory: {manifest_path}")
        for entry in entries:
            symbol = entry.get("symbol")
            if not isinstance(symbol, str):
                raise SeriesError("Derivative inventory item lacks a symbol")
            path = _entry_path(manifest_path, entry)
            if not path.is_file():
                raise SeriesError(f"Derivative file not found: {path}")
            table: dict[str, dict] = {}
            for row in _read_rows(path, columns):
                day = row.get("trade_date")
                day = day.date() if isinstance(day, datetime) else day
                if not isinstance(day, date):
                    raise SeriesError(f"Derivative row lacks a trade_date: {path.name}")
                table[day.isoformat()] = row
            target[symbol] = table

    def load(self) -> None:
        self._load(self.adjusted_dir, self._adjusted, ("trade_date", "close_adjusted"))
        self._load(self.enriched_dir, self._enriched,
                   ("trade_date", "turnover_cny_reported", "provider_trade_status"))
        if self.etf_dir is not None:
            manifest_path = self.etf_dir / "manifest.json"
            if not manifest_path.is_file():
                raise SeriesError(f"ETF evidence manifest not found: {manifest_path}")
            self.etf_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self._load(self.etf_dir, self._etf,
                       ("trade_date", "open", "high", "low", "close", "turnover",
                        "trade_status", "adjusted_close"))

    def etf_asset_types(self) -> dict[str, AssetType]:
        types: dict[str, AssetType] = {}
        for entry in self.etf_manifest.get("files", []):
            symbol, value = entry.get("symbol"), entry.get("asset_type")
            if isinstance(symbol, str) and value in _ASSET_TYPES:
                types[symbol] = _ASSET_TYPES[value]
        return types

    def adjusted_close(self, symbol: str, day: str) -> float | None:
        row = self._adjusted.get(symbol, {}).get(day)
        value = row.get("close_adjusted") if row else None
        return float(value) if value is not None else None

    def enriched(self, symbol: str, day: str) -> dict | None:
        return self._enriched.get(symbol, {}).get(day)

    def etf_bar(self, symbol: str, day: str) -> dict | None:
        return self._etf.get(symbol, {}).get(day)

    def etf_bar_table(self, symbol: str) -> dict[str, dict]:
        return self._etf.get(symbol, {})

    def etf_manifest_symbols(self) -> list[str]:
        return [entry["symbol"] for entry in self.etf_manifest.get("files", [])
                if isinstance(entry.get("symbol"), str)]


def _load_registry(project_root: Path, registry_path: Path) -> DatasetRegistry:
    registry = DatasetRegistry(_inside(project_root, registry_path), project_root)
    for name in ("stock_daily", "etf_daily", "security_metadata", "sse_calendar"):
        if name not in registry.config["datasets"]:
            raise SeriesError(f"Registry does not register {name}")
    return registry


def _manifest_files(registry: DatasetRegistry, name: str) -> tuple[Path, dict]:
    entry = registry.config["datasets"][name]
    path = registry._inside(registry.project_root / entry["manifest"])
    return path, json.loads(path.read_text(encoding="utf-8"))


def _load_listing_candidates(registry: DatasetRegistry) -> dict[str, dict]:
    path, manifest = _manifest_files(registry, "security_metadata")
    candidates: dict[str, dict] = {}
    for entry in manifest.get("files", []):
        file_path = _entry_path(path, entry)
        if "listing" not in file_path.name:
            continue
        for row in _read_rows(file_path, ("symbol", "exchange", "source_listing_date")):
            symbol = row.get("symbol")
            listed_on = row.get("source_listing_date")
            if isinstance(symbol, str) and isinstance(listed_on, date):
                candidates[symbol] = {"listed_on": listed_on, "exchange": row.get("exchange")}
    if not candidates:
        raise SeriesError("Security metadata registers no listing candidates")
    return candidates


def _load_calendar(registry: DatasetRegistry, calendar_path: Path | None) -> tuple[list[date], Path, frozenset[str]]:
    if calendar_path is not None:
        path = _inside(registry.project_root, calendar_path)
    else:
        manifest_path, manifest = _manifest_files(registry, "sse_calendar")
        entry = manifest.get("files", [{}])[0]
        path = registry._inside(_entry_path(manifest_path, entry))
    rows = _read_rows(path, ("calendar_date", "exchange", "is_scheduled_open"))
    open_dates = sorted({row["calendar_date"] for row in rows if row.get("is_scheduled_open")})
    if not open_dates:
        raise SeriesError("Calendar contains no scheduled open day")
    exchanges = frozenset(str(row.get("exchange")) for row in rows if row.get("exchange"))
    return open_dates, path, exchanges


def _load_symbol_sources(registry: DatasetRegistry, request: SeriesRequest
                         ) -> tuple[list[SymbolSource], list[dict]]:
    """Read registered daily files for the requested symbols.

    Selection is deterministic: entries from both datasets are merged and
    sorted by symbol before the optional cap applies, so a capped run never
    depends on dataset order.
    """
    collected: list[tuple[str, str, Path, Mapping]] = []
    for dataset in ("stock_daily", "etf_daily"):
        manifest_path, manifest = _manifest_files(registry, dataset)
        for entry in manifest.get("files", []):
            symbol = entry.get("symbol")
            if isinstance(symbol, str):
                collected.append((symbol, dataset, manifest_path, entry))
    collected.sort(key=lambda item: item[0])
    if request.symbols:
        wanted = set(request.symbols)
        collected = [item for item in collected if item[0] in wanted]
    if request.max_symbols is not None:
        collected = collected[:request.max_symbols]

    sources: list[SymbolSource] = []
    skipped: list[dict] = []
    for symbol, dataset, manifest_path, entry in collected:
        path = registry._inside(_entry_path(manifest_path, entry))
        digest = entry.get("sha256") or entry.get("output_sha256")
        if request.verify_hashes:
            if not isinstance(digest, str) or _sha256(path) != digest:
                skipped.append({"symbol": symbol, "reason": "registered_hash_mismatch_or_missing"})
                continue
        columns = ["trade_date", "open", "high", "low", "close"]
        if dataset == "etf_daily":
            columns.append("asset_type")
        rows = []
        for row in _read_rows(path, columns):
            day = row.get("trade_date")
            day = day.date() if isinstance(day, datetime) else day
            if not isinstance(day, date):
                skipped.append({"symbol": symbol, "reason": "row_without_trade_date"})
                continue
            rows.append({"trade_date": day, "open": float(row["open"]), "high": float(row["high"]),
                         "low": float(row["low"]), "close": float(row["close"]),
                         "asset_type": row.get("asset_type")})
        rows.sort(key=lambda item: item["trade_date"])
        asset_value = rows[0]["asset_type"] if dataset == "etf_daily" and rows else "stock"
        sources.append(SymbolSource(symbol, dataset, _ASSET_TYPES.get(str(asset_value)),
                                    tuple(rows), path))
    return sources, skipped


def _etf_sources(derivatives: _DerivativeIndex, request: SeriesRequest
                 ) -> tuple[list[SymbolSource], list[dict]]:
    """ETF symbols from the ETF evidence product, with provider listing dates."""
    sources: list[SymbolSource] = []
    skipped: list[dict] = []
    types = derivatives.etf_asset_types()
    entries = {entry.get("symbol"): entry for entry in derivatives.etf_manifest.get("files", [])}
    for symbol in sorted(derivatives.etf_manifest_symbols()):
        if request.symbols and symbol not in set(request.symbols):
            continue
        entry = entries.get(symbol, {})
        asset_type = types.get(symbol)
        if asset_type is None:
            skipped.append({"symbol": symbol, "reason": "asset_type_unmapped"})
            continue
        listed_on = entry.get("listed_on")
        if isinstance(listed_on, datetime):
            listed_on = listed_on.date()
        elif isinstance(listed_on, str):
            try:
                listed_on = date.fromisoformat(listed_on)
            except ValueError:
                listed_on = None
        if not isinstance(listed_on, date):
            skipped.append({"symbol": symbol, "reason": "provider_listing_date_missing"})
            continue
        rows = []
        for day, row in sorted(derivatives.etf_bar_table(symbol).items()):
            rows.append({"trade_date": date.fromisoformat(day), "open": row.get("open"),
                         "high": row.get("high"), "low": row.get("low"), "close": row.get("close"),
                         "asset_type": None})
        if not rows:
            skipped.append({"symbol": symbol, "reason": "no_bars_in_product"})
            continue
        tracking_index = entry.get("tracking_index")
        sources.append(SymbolSource(symbol, "etf_evidence", asset_type, tuple(rows),
                                    Path(entry.get("path", "")), provider_listed_on=listed_on,
                                    provider_tracking_index=tracking_index if isinstance(tracking_index, str) else None))
    return sources, skipped


def _validate_symbol(source: SymbolSource, derivatives: _DerivativeIndex, assume: frozenset[str],
                     since: date, through: date) -> str | None:
    """Prove every bar that can reach a payload is contract-valid.

    Only the range ``[since, through]`` is checked: ``since`` is the oldest bar
    the earliest session's trailing window can still contain, so evidence older
    than the series window never enters a payload and its coverage gaps do not
    block the symbol. Validating this whole range is what makes the per-session
    build safe: every session window is a subset of it.
    """
    if "turnover_provider_reported" not in assume:
        return "turnover_assumption_not_granted"
    if "session_close_availability" not in assume:
        return "available_at_assumption_not_granted"
    for row in source.rows:
        if row["trade_date"] > through or row["trade_date"] < since:
            continue
        key = row["trade_date"].isoformat()
        if source.dataset == "etf_evidence":
            bar = derivatives.etf_bar(source.symbol, key)
            if bar is None:
                return "etf_coverage_missing"
            if bar.get("turnover") is None:
                return "turnover_unavailable"
            if bar.get("trade_status") is None:
                return "tradability_unavailable"
            if bar.get("adjusted_close") is None:
                return "adjusted_close_unavailable"
            continue
        enriched = derivatives.enriched(source.symbol, key)
        if enriched is None:
            return "enriched_coverage_missing"
        if enriched.get("turnover_cny_reported") is None:
            return "turnover_unavailable"
        if enriched.get("provider_trade_status") is None:
            return "tradability_unavailable"
        if derivatives.adjusted_close(source.symbol, key) is None:
            return "adjusted_close_unavailable"
    return None


def _window_floor(source: SymbolSource, earliest: date, trailing_window: int) -> date:
    """Oldest bar the earliest session's trailing window can still contain."""
    observable = bisect_right(source.rows, earliest, key=lambda row: row["trade_date"])
    window = source.rows[max(0, observable - trailing_window):observable]
    return window[0]["trade_date"] if window else earliest


def _session_bars(source: SymbolSource, session: date, derivatives: _DerivativeIndex,
                  assume: frozenset[str], decision_time: time) -> list[DailyBar]:
    """Trailing window of contract-valid bars for one symbol at one session.

    The window is taken from the bars observable at this session, not from the
    end of the file: a session early in the window must still see its own full
    trailing history.
    """
    decision_at = datetime.combine(session, decision_time, CHINA_TIME)
    observable = bisect_right(source.rows, session, key=lambda row: row["trade_date"])
    window = source.rows[max(0, observable - ENGINE_TRAILING_WINDOW_SESSIONS):observable]
    bars: list[DailyBar] = []
    for row in window:
        key = row["trade_date"].isoformat()
        if source.dataset == "etf_evidence":
            # The ETF evidence product carries prices, turnover, trade status and
            # the adjusted close in one file.
            bar = derivatives.etf_bar(source.symbol, key)
            tradable = bool(int(bar["trade_status"]) == 1)
            bars.append(DailyBar(
                symbol=source.symbol, trade_date=row["trade_date"],
                open=row["open"], high=row["high"], low=row["low"], close=row["close"],
                turnover=float(bar["turnover"]),
                available_at=decision_at,
                adjusted_close=float(bar["adjusted_close"]),
                tradable=tradable,
                buyable=tradable if "buy_sell_equal_to_tradable" in assume else tradable,
                sellable=tradable if "buy_sell_equal_to_tradable" in assume else tradable,
            ))
            continue
        enriched = derivatives.enriched(source.symbol, key)
        tradable = bool(int(enriched["provider_trade_status"]) == 1)
        bars.append(DailyBar(
            symbol=source.symbol, trade_date=row["trade_date"],
            open=row["open"], high=row["high"], low=row["low"], close=row["close"],
            turnover=float(enriched["turnover_cny_reported"]),
            available_at=decision_at,
            adjusted_close=derivatives.adjusted_close(source.symbol, key),
            tradable=tradable,
            buyable=tradable if "buy_sell_equal_to_tradable" in assume else tradable,
            sellable=tradable if "buy_sell_equal_to_tradable" in assume else tradable,
        ))
    return bars


def _build_securities(eligible: Sequence[SymbolSource], candidates: Mapping[str, dict],
                      assume: frozenset[str]) -> tuple[dict[str, Security], list[dict]]:
    securities: dict[str, Security] = {}
    excluded: list[dict] = []
    for source in eligible:
        if source.dataset == "etf_evidence":
            listed_on = source.provider_listed_on
        else:
            candidate = candidates.get(source.symbol)
            if candidate is None:
                excluded.append({"symbol": source.symbol, "reason": "listing_date_candidate_missing"})
                continue
            listed_on = candidate["listed_on"]
        if "lot_size_conventional_default" not in assume:
            excluded.append({"symbol": source.symbol, "reason": "buy_lot_size_assumption_not_granted"})
            continue
        if "settlement_conventional_default" not in assume:
            excluded.append({"symbol": source.symbol, "reason": "settlement_assumption_not_granted"})
            continue
        securities[source.symbol] = Security(
            symbol=source.symbol,
            asset_type=source.asset_type,
            listed_on=listed_on,
            buy_lot_size=100,
            settlement_sessions=1,
            industry_exposure={},
            # ETF tracking indices come from the provider evidence product and
            # stay declared as name-derived; stocks keep None until a source exists.
            tracking_index=source.provider_tracking_index,
            historical_metadata_verified=False,
            lookthrough_verified=False,
            eligible_for_new_positions=False,
        )
    return securities, excluded


def _tally(excluded: Iterable[Mapping[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in excluded:
        counts[item["reason"]] = counts.get(item["reason"], 0) + 1
    return dict(sorted(counts.items()))


def build_series(project_root: Path, request: SeriesRequest, output_root: Path,
                 registry_path: Path | None = None) -> dict:
    """Build one candidate series and return its index document.

    Exclusions and declared assumptions are reported, never hidden. The result
    is written under ``output_root/<series_id>`` and re-read from disk before
    the index claims success.
    """
    project_root = Path(project_root).resolve()
    output_root = _inside(project_root, Path(output_root))
    registry = _load_registry(project_root, Path(registry_path or project_root / "config/datasets.json"))

    try:
        registry.validate_for_backtest()
        admission = {"status": "admitted", "reason": None}
    except DataNotReady as exc:
        admission = {"status": "blocked", "reason": str(exc)}
    except (ValueError, OSError, KeyError, TypeError) as exc:
        admission = {"status": "unavailable", "reason": f"Invalid registry evidence: {exc}"}

    candidates = _load_listing_candidates(registry)
    open_dates, calendar_path, calendar_exchanges = _load_calendar(registry, request.calendar_path)
    open_date_set = set(open_dates)
    derivatives = _DerivativeIndex(request.adjusted_dir, request.enriched_dir, request.etf_dir)
    derivatives.load()
    sources, skipped = _load_symbol_sources(registry, request)
    if request.etf_dir is not None:
        etf_sources, etf_skipped = _etf_sources(derivatives, request)
        sources.extend(etf_sources)
        skipped.extend(etf_skipped)
    # A symbol may appear in both a registered dataset and the ETF evidence
    # product. Registered evidence wins and the duplicate is recorded, never
    # silently merged: two bar sources for one symbol would duplicate rows.
    unique: list[SymbolSource] = []
    seen: set[str] = set()
    for source in sources:
        if source.symbol in seen:
            skipped.append({"symbol": source.symbol, "reason": "duplicate_symbol_source",
                            "dataset": source.dataset})
            continue
        seen.add(source.symbol)
        unique.append(source)
    sources = unique

    assume = frozenset(request.assume)
    gaps = {LISTING_GAP}
    factor_index: dict[str, dict[str, dict]] = {}
    if request.factors_dir is not None:
        from .factors import load_factor_index
        factor_index = load_factor_index(request.factors_dir)
        # The factor product keeps original_version_verified=false for every
        # observation, which is the gap that keeps stock selection empty.
        gaps.add(FACTOR_VERSION_GAP)
    else:
        gaps.add(FACTOR_GAP)
    if admission["status"] != "admitted":
        gaps.add(ADMISSION_GAP)
    for name in request.assume:
        gaps.add(ASSUMPTIONS[name][1])

    excluded: list[dict] = list(skipped)
    # Candidate sessions come from every source, not only eligible ones, so the
    # validation floor below is known before any symbol is accepted.
    candidate_dates = sorted({row["trade_date"] for source in sources for row in source.rows
                              if request.start <= row["trade_date"] <= request.end
                              and row["trade_date"] in open_date_set})
    earliest = candidate_dates[0] if candidate_dates else request.start

    eligible: list[SymbolSource] = []
    for source in sources:
        if source.asset_type is None:
            excluded.append({"symbol": source.symbol, "reason": "asset_type_unmapped"})
            continue
        if not any(request.start <= row["trade_date"] <= request.end for row in source.rows):
            excluded.append({"symbol": source.symbol, "reason": "no_bars_in_window"})
            continue
        since = _window_floor(source, earliest, request.trailing_window)
        reason = _validate_symbol(source, derivatives, assume, since, request.end)
        if reason is not None:
            excluded.append({"symbol": source.symbol, "reason": reason})
            continue
        eligible.append(source)

    securities, security_exclusions = _build_securities(eligible, candidates, assume)
    excluded.extend(security_exclusions)
    included = [source for source in eligible if source.symbol in securities]

    # A price history that starts before its listing candidate is a documented
    # discrepancy; such a symbol cannot receive a nonnegative listing age.
    for source in list(included):
        first_bar = source.rows[0]["trade_date"]
        if first_bar < securities[source.symbol].listed_on:
            excluded.append({"symbol": source.symbol, "reason": "price_precedes_listing_candidate"})
            included.remove(source)
            del securities[source.symbol]

    # The registered calendar is a single exchange's planned sessions. Using it
    # for another exchange's securities is recorded as a gap, not assumed away:
    # the independent SZSE calendar exists but is not registered yet (§8.5).
    for symbol in sorted(securities):
        exchange = str(candidates.get(symbol, {}).get("exchange") or "")
        if exchange and calendar_exchanges and exchange not in calendar_exchanges:
            gaps.add(f"calendar_exchange_unverified:{exchange}")

    session_dates = sorted({row["trade_date"] for source in included for row in source.rows
                            if request.start <= row["trade_date"] <= request.end
                            and row["trade_date"] in open_date_set})
    # An empty series is a diagnostic, not a failure: the index then carries the
    # exclusion tally that says exactly which contract field blocked each
    # symbol. Input problems still raise before this point.

    series_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    series_dir = output_root / series_id
    (series_dir / "sessions").mkdir(parents=True)

    sessions_index = []
    for session in session_dates:
        decision_at = datetime.combine(session, request.decision_time, CHINA_TIME)
        bars: list[DailyBar] = []
        for source in sorted(included, key=lambda item: item.symbol):
            bars.extend(_session_bars(source, session, derivatives, assume, request.decision_time))
        if not bars:
            continue
        factors = []
        for symbol in sorted(securities):
            for report_date, row in sorted(factor_index.get(symbol, {}).items()):
                available_at = row.get("available_at")
                if isinstance(available_at, datetime) and available_at <= decision_at:
                    factors.append({"symbol": symbol,
                                    "quality": row.get("quality"),
                                    "value": row.get("value"),
                                    "available_at": _iso(available_at),
                                    "original_version_verified": bool(row.get("original_version_verified"))})
        payload = {
            "schema_version": SCHEMA_VERSION,
            "source_kind": SOURCE_KIND,
            "decision_at": _iso(decision_at),
            "securities": [{
                "symbol": security.symbol,
                "asset_type": security.asset_type.value,
                "listed_on": _iso(security.listed_on),
                "buy_lot_size": security.buy_lot_size,
                "settlement_sessions": security.settlement_sessions,
                "industry_exposure": dict(security.industry_exposure),
                "tracking_index": security.tracking_index,
                "historical_metadata_verified": security.historical_metadata_verified,
                "lookthrough_verified": security.lookthrough_verified,
                "eligible_for_new_positions": security.eligible_for_new_positions,
            } for security in securities.values()],
            "bars": [{
                "symbol": bar.symbol, "trade_date": _iso(bar.trade_date),
                "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
                "turnover": bar.turnover, "available_at": _iso(bar.available_at),
                "adjusted_close": bar.adjusted_close, "tradable": bar.tradable,
                "buyable": bar.buyable, "sellable": bar.sellable,
            } for bar in bars],
            "factors": factors,
            "listing_age_sessions": {
                symbol: max(0, bisect_right(open_dates, session) - bisect_left(open_dates, security.listed_on))
                for symbol, security in securities.items()
            },
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "source_kind": SOURCE_KIND,
            "backtest_ready": False,
            "readiness_gaps": sorted(gaps),
            "payload": "payload.json",
            "payload_sha256": hashlib.sha256(_json_bytes(payload)).hexdigest(),
            "purpose": "candidate_snapshot_series",
            "series_id": series_id,
            "trade_date": _iso(session),
            "decision_at": _iso(decision_at),
            "availability_basis": "session_close" if "session_close_availability" in assume else "none",
            "securities": len(payload["securities"]),
            "bars": len(payload["bars"]),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        session_dir = series_dir / "sessions" / session.strftime("%Y%m%d")
        session_dir.mkdir()
        _write_once(session_dir / "payload.json", _json_bytes(payload))
        manifest_bytes = _json_bytes(manifest)
        _write_once(session_dir / "manifest.json", manifest_bytes)
        sessions_index.append({
            "trade_date": _iso(session),
            "manifest": f"sessions/{session.strftime('%Y%m%d')}/manifest.json",
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "payload_sha256": manifest["payload_sha256"],
            "securities": manifest["securities"],
            "bars": manifest["bars"],
        })

    off_calendar = sorted({_iso(row["trade_date"]) for source in included for row in source.rows
                           if request.start <= row["trade_date"] <= request.end
                           and row["trade_date"] not in open_date_set})
    index = {
        "series_id": series_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "system_version": __version__,
        "purpose": "candidate_snapshot_series",
        "synthetic": False,
        "historical_data_admitted": admission["status"] == "admitted",
        "registry_admission_at_build": admission,
        "window": {"start": _iso(request.start), "end": _iso(request.end)},
        "decision_time": request.decision_time.isoformat(),
        "timezone": "+08:00",
        "trailing_window_sessions": request.trailing_window,
        "assumptions": [{"name": name, "derivation": ASSUMPTIONS[name][0],
                         "readiness_gap": ASSUMPTIONS[name][1]} for name in sorted(request.assume)],
        "readiness_gaps": sorted(gaps),
        "sources": {
            "registry": str(registry.registry_path),
            "calendar": str(calendar_path),
            "calendar_exchanges": sorted(calendar_exchanges),
            "adjusted_prices": str(Path(request.adjusted_dir).resolve()),
            "enriched_daily_bars": str(Path(request.enriched_dir).resolve()),
            "factors": str(Path(request.factors_dir).resolve()) if request.factors_dir else None,
            "etf": str(Path(request.etf_dir).resolve()) if request.etf_dir else None,
        },
        "symbols": {
            "requested": list(request.symbols),
            "included": sorted(securities),
            "excluded_count": len(excluded),
            "excluded_reasons": _tally(excluded),
        },
        "sessions": sessions_index,
        "bars_on_non_calendar_days": off_calendar,
        "notes": [
            "Bundles carry declared assumptions as readiness gaps and are refused by load_snapshot until the underlying evidence exists.",
            "Security metadata stays explicitly unverified, so the engine excludes these securities from new positions.",
            "Factors carry original_version_verified=false until original announcement vintages exist, so stock selection stays empty by design.",
        ],
    }
    index["self_check"] = _verify_series(series_dir, index)
    _write_once(series_dir / "series.json", _json_bytes(index))
    return index


def _verify_series(series_dir: Path, index: Mapping) -> dict:
    """Re-read every bundle from disk and reconstruct the engine snapshot."""
    reconstructed = 0
    for entry in index["sessions"]:
        manifest_path = series_dir / entry["manifest"]
        payload_path = manifest_path.parent / "payload.json"
        if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != entry["manifest_sha256"]:
            raise SeriesError(f"Manifest hash mismatch: {entry['trade_date']}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if hashlib.sha256(payload_path.read_bytes()).hexdigest() != manifest["payload_sha256"]:
            raise SeriesError(f"Payload hash mismatch: {entry['trade_date']}")
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        securities = {}
        for record in payload["securities"]:
            security = Security(**{**record, "asset_type": AssetType(record["asset_type"]),
                                    "listed_on": date.fromisoformat(record["listed_on"])})
            securities[security.symbol] = security
        histories: dict[str, list[DailyBar]] = {symbol: [] for symbol in securities}
        for record in payload["bars"]:
            histories[record["symbol"]].append(DailyBar(
                symbol=record["symbol"], trade_date=date.fromisoformat(record["trade_date"]),
                open=record["open"], high=record["high"], low=record["low"], close=record["close"],
                turnover=record["turnover"],
                available_at=datetime.fromisoformat(record["available_at"]),
                adjusted_close=record["adjusted_close"], tradable=record["tradable"],
                buyable=record["buyable"], sellable=record["sellable"]))
        stock_factors: dict[str, list] = {symbol: [] for symbol in securities}
        for record in payload["factors"]:
            stock_factors[record["symbol"]].append(StockFactorObservation(
                symbol=record["symbol"], quality=record["quality"], value=record["value"],
                available_at=datetime.fromisoformat(record["available_at"]),
                original_version_verified=record["original_version_verified"]))
        DataSnapshot(datetime.fromisoformat(payload["decision_at"]), securities,
                     {symbol: tuple(items) for symbol, items in histories.items()},
                     {symbol: tuple(items) for symbol, items in stock_factors.items()},
                     payload["listing_age_sessions"])
        reconstructed += 1
    return {"bundles_reconstructed": reconstructed,
            "payload_hashes_verified": reconstructed,
            "engine_compatible": bool(index["sessions"]) and reconstructed == len(index["sessions"])}
