"""Read-only staging inspection and a strict engine-ready snapshot boundary.

``StagedMarketData.catalog('stock_daily' | 'etf_daily', q='', limit=50)``
reads the registered manifest only. ``preview_bars(dataset, symbol, limit=200)``
verifies the saved output SHA-256 before reading one Parquet file, validates the
whole file, and returns its most recent rows as standard JSON values. These
previews are unadjusted staging observations, always ``backtest_ready=False``.
Unknown turnover, adjusted prices, publication times and trading flags remain
null. ``volume_shares_estimate`` is preserved as an estimate, never promoted to
verified volume. Dates are read from the normalized ``trade_date`` column;
source epoch integers and filesystem timestamps are not publication evidence.

``load_snapshot(bundle_path)`` accepts an independent manifest inside the
project only after ``registry.validate_for_backtest()`` succeeds, which now
means the computed §5.1 readiness conjunction (design §5.2), not a hand-written
flag. Both manifest and JSON payload require ``schema_version: '1.0'`` and
``source_kind: 'verified_history'``. The manifest also requires an empty
``readiness_gaps`` list - the explicit record that no declared derivation
(documented assumption) is still pending - and provides ``payload`` (a relative
JSON path) and ``payload_sha256``. The payload has these explicit fields:

* ``decision_at``: timezone-aware ISO timestamp, e.g. 2024-01-02T16:30:00+08:00.
* ``securities``: list of objects containing every Security field: symbol,
  exchange, asset_type, listed_on, delist_date, buy_lot_size,
  settlement_sessions, industry_exposure, tracking_index,
  historical_metadata_verified, lookthrough_verified and
  eligible_for_new_positions. Unknown metadata must be explicitly unverified
  and ineligible; current snapshots cannot fill historical metadata. A missing
  termination record is an explicit ``delist_date: null``.
* ``bars``: list containing every DailyBar field: symbol, trade_date, open,
  high, low, close, turnover, available_at, adjusted_close, volume, tradable,
  buyable, sellable. Adjusted close and turnover cannot be null or inferred;
  ``volume`` is the D3-pending exception and may be an explicit null, never a
  zero-filled guess. Dates per symbol must be unique and ascending.
  Future/unavailable observations remain in the snapshot and are excluded by
  ``DataSnapshot.known_history``.
* ``factors``: list containing every StockFactorObservation field: symbol,
  quality, value, available_at and original_version_verified. Unknown factor
  values may be null and unverified versions remain explicitly false.
* ``listing_age_sessions``: object with an explicit nonnegative integer age
  for every security. This adapter does not approximate sessions by weekdays.

This boundary checks structure and saved evidence; it does not generate those
verified historical inputs or provide a force/bypass option.
"""
from __future__ import annotations

from dataclasses import fields
from datetime import date, datetime
import hashlib
import importlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Any

from .data_registry import DataNotReady, DatasetRegistry
from .models import AssetType, DailyBar, DataSnapshot, Security, StockFactorObservation


_DATASETS = frozenset(("stock_daily", "etf_daily"))
_SYMBOL = re.compile(r"(?:sh|sz)?[0-9]{6}\Z")
_SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")
_PREVIEW_COLUMNS = ("trade_date", "open", "high", "low", "close", "volume_raw",
                    "volume_shares_estimate", "turnover", "adjusted_close", "available_at", "tradable")


def _use_vendored_parquet(dependency: Path) -> None:
    """Expose the project-local Parquet build as a last-resort fallback only.

    The path is appended, never inserted first: an installed pyarrow, or any
    standard-library module sharing a name, must win, so a vendored build tied
    to another interpreter cannot shadow a working import.
    """
    if dependency.is_dir() and str(dependency) not in sys.path:
        sys.path.append(str(dependency))


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _read_json(content: bytes) -> dict:
    def invalid_constant(value):
        raise ValueError(f"Non-finite JSON constant: {value}")
    result = json.loads(content.decode("utf-8"), object_pairs_hook=_unique_object,
                        parse_constant=invalid_constant)
    if not isinstance(result, dict):
        raise ValueError("JSON evidence must be an object")
    return result


def _limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 500:
        raise ValueError("limit must be an integer from 1 to 500")
    return value


def _iso_date(value: Any, name: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError(f"{name} must be an ISO date")
    return date.fromisoformat(value)


def _timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{name} must be a timezone-aware ISO timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed


def _number(value: Any, name: str, *, positive=False, nullable=False) -> int | float | None:
    if nullable and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    try:
        valid = math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid or (positive and value <= 0):
        raise ValueError(f"{name} must be finite" + (" and positive" if positive else ""))
    return value


def _required(record: Any, required: set[str], name: str) -> dict:
    if not isinstance(record, dict):
        raise ValueError(f"{name} must be an object")
    missing = required - record.keys()
    if missing:
        raise ValueError(f"{name} missing explicit fields: {', '.join(sorted(missing))}")
    return record


class StagedMarketData:
    def __init__(self, registry: DatasetRegistry):
        self.registry = registry
        self.project_root = registry.project_root.resolve()

    def _inside(self, path: Path) -> Path:
        resolved = path.resolve()
        if not resolved.is_relative_to(self.project_root):
            raise ValueError("Market data evidence must stay inside the independent project")
        return resolved

    def _parquet(self):
        # Optional local runtime; catalog inspection remains standard-library only.
        try:
            return importlib.import_module("pyarrow.parquet")
        except ImportError:
            _use_vendored_parquet(self._inside(self.project_root / ".deps" / "parquet"))
            try:
                return importlib.import_module("pyarrow.parquet")
            except ImportError as exc:
                raise DataNotReady("Parquet preview dependency is unavailable") from exc

    def _inventory(self, dataset: str) -> tuple[Path, dict, list[dict]]:
        if dataset not in _DATASETS:
            raise ValueError("Only stock_daily and etf_daily may be inspected")
        entry = self.registry.config["datasets"].get(dataset)
        if not isinstance(entry, dict) or not isinstance(entry.get("manifest"), str):
            raise ValueError(f"Dataset is not registered: {dataset}")
        path = self._inside(self.project_root / entry["manifest"])
        manifest = _read_json(path.read_bytes())
        inventory = manifest.get("files")
        if not isinstance(inventory, list):
            raise ValueError("Dataset manifest must contain a file inventory")
        symbols = set()
        for item in inventory:
            _required(item, {"symbol", "rows", "start", "end"}, "inventory item")
            symbol = item["symbol"]
            if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
                raise ValueError("Invalid inventory symbol")
            if symbol in symbols:
                raise ValueError("Duplicate inventory symbol")
            symbols.add(symbol)
            if isinstance(item["rows"], bool) or not isinstance(item["rows"], int) or item["rows"] < 0:
                raise ValueError("Inventory rows must be a nonnegative integer")
            start = _iso_date(item["start"], "inventory start")
            end = _iso_date(item["end"], "inventory end")
            if start > end:
                raise ValueError("Inventory start is after end")
        return path, manifest, inventory

    def catalog(self, dataset: str, q: str = "", limit: int = 50) -> dict:
        _limit(limit)
        if not isinstance(q, str) or len(q) > 32:
            raise ValueError("q must be a string of at most 32 characters")
        _, _, inventory = self._inventory(dataset)
        query = q.casefold().strip()
        matches = [item for item in inventory
                   if query in item["symbol"].casefold() or query in str(item.get("name", "")).casefold()]
        return {"dataset": dataset, "total": len(matches),
                "items": [{key: item[key] for key in ("symbol", "rows", "start", "end")}
                          for item in matches[:limit]]}

    def _verified_output(self, manifest_path: Path, item: dict) -> bytes:
        filename = item.get("output") or item.get("path")
        digest = item.get("output_sha256") or item.get("sha256")
        if not isinstance(filename, str) or not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise ValueError("Normalized output path and SHA-256 are required")
        candidate = Path(filename)
        path = self._inside(candidate if candidate.is_absolute() else manifest_path.parent / candidate)
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != digest.lower():
            raise ValueError("Normalized output SHA-256 mismatch")
        return content

    @staticmethod
    def _preview_record(record: dict, symbol: str) -> dict:
        row_symbol = record.get("symbol")
        expected_symbols = {symbol, symbol[2:]} if symbol.startswith(("sh", "sz")) else {symbol}
        if row_symbol not in expected_symbols:
            raise ValueError("Parquet row symbol does not match its registered symbol")
        day = record.get("trade_date")
        if isinstance(day, datetime):
            raise ValueError("trade_date must be date-only")
        if isinstance(day, date):
            day = day.isoformat()
        _iso_date(day, "trade_date")
        result = {key: None for key in _PREVIEW_COLUMNS}
        result["trade_date"] = day
        for key in ("open", "high", "low", "close"):
            result[key] = _number(record.get(key), key, positive=True)
        if (result["low"] > min(result["open"], result["close"])
                or result["high"] < max(result["open"], result["close"])
                or result["low"] > result["high"]):
            raise ValueError("Invalid preview OHLC")
        for key in ("volume_raw", "volume_shares_estimate", "turnover", "adjusted_close"):
            result[key] = _number(record.get(key), key, positive=key == "adjusted_close", nullable=True)
            if result[key] is not None and result[key] < 0:
                raise ValueError(f"{key} must be nonnegative")
        available = record.get("available_at")
        if isinstance(available, datetime):
            available = available.isoformat()
        if available is not None:
            result["available_at"] = _timestamp(available, "available_at").isoformat()
        tradable = record.get("tradable")
        if tradable is not None and not isinstance(tradable, bool):
            raise ValueError("tradable must be an explicit boolean or null")
        result["tradable"] = tradable
        return result

    def preview_bars(self, dataset: str, symbol: str, limit: int = 200) -> dict:
        _limit(limit)
        if not isinstance(symbol, str) or not _SYMBOL.fullmatch(symbol):
            raise ValueError("symbol must match a registered security code")
        path, manifest, inventory = self._inventory(dataset)
        item = next((item for item in inventory if item["symbol"] == symbol), None)
        if item is None:
            raise ValueError("symbol is not present in the dataset catalog")
        content = self._verified_output(path, item)
        pq = self._parquet()
        arrow = importlib.import_module("pyarrow")
        table = pq.read_table(arrow.BufferReader(content))
        if table.num_rows != item["rows"]:
            raise ValueError("Normalized output row count mismatch")
        rows = [self._preview_record(record, symbol) for record in table.to_pylist()]
        days = [row["trade_date"] for row in rows]
        if days != sorted(set(days)):
            raise ValueError("Preview dates must be unique and ascending")
        if rows and (days[0] != item["start"] or days[-1] != item["end"]):
            raise ValueError("Normalized output date bounds mismatch")
        gaps = manifest.get("readiness_gaps")
        if not isinstance(gaps, list) or any(not isinstance(gap, str) for gap in gaps):
            raise ValueError("Manifest readiness_gaps must be an explicit list")
        return {"symbol": symbol, "dataset": dataset, "price_basis": "unadjusted",
                "backtest_ready": False, "readiness_gaps": gaps, "rows": rows[-limit:]}

    def load_snapshot(self, bundle_path: Path) -> DataSnapshot:
        # No bundle path/content is read until all registered evidence passes.
        # Make a project-local optional install visible to the registry gate,
        # but only as a fallback that cannot shadow an installed pyarrow.
        _use_vendored_parquet(self._inside(self.project_root / ".deps" / "parquet"))
        self.registry.validate_for_backtest()
        path = Path(bundle_path)
        manifest_path = self._inside(path if path.is_absolute() else self.project_root / path)
        manifest = _read_json(manifest_path.read_bytes())
        if (manifest.get("schema_version") != "1.0" or manifest.get("source_kind") != "verified_history"
                or manifest.get("readiness_gaps") != []
                or manifest.get("errors", [])):
            raise DataNotReady("Snapshot manifest has not passed historical admission")
        filename, digest = manifest.get("payload"), manifest.get("payload_sha256")
        if (not isinstance(filename, str) or Path(filename).is_absolute()
                or not isinstance(digest, str) or not _SHA256.fullmatch(digest)):
            raise ValueError("Snapshot requires a relative payload path and SHA-256")
        payload_path = self._inside(manifest_path.parent / filename)
        content = payload_path.read_bytes()
        if hashlib.sha256(content).hexdigest() != digest.lower():
            raise ValueError("Snapshot payload SHA-256 mismatch")
        payload = _read_json(content)
        _required(payload, {"schema_version", "source_kind", "decision_at", "securities", "bars", "factors",
                            "listing_age_sessions"}, "snapshot")
        if payload["schema_version"] != "1.0" or payload["source_kind"] != "verified_history":
            raise DataNotReady("Snapshot payload must be verified_history schema 1.0")
        decision_at = _timestamp(payload["decision_at"], "decision_at")
        for key in ("securities", "bars", "factors"):
            if not isinstance(payload[key], list):
                raise ValueError(f"{key} must be an explicit list")
        securities = {}
        for record in payload["securities"]:
            _required(record, {field.name for field in fields(Security)}, "security")
            values = {field.name: record[field.name] for field in fields(Security)}
            if not isinstance(values["symbol"], str) or not values["symbol"] or values["symbol"] in securities:
                raise ValueError("Security symbols must be nonempty and unique")
            values["asset_type"] = AssetType(values["asset_type"])
            values["listed_on"] = _iso_date(values["listed_on"], "listed_on")
            if not isinstance(values["exchange"], str) or not values["exchange"]:
                raise ValueError("exchange must be an explicit listing venue")
            if values["delist_date"] is not None:
                values["delist_date"] = _iso_date(values["delist_date"], "delist_date")
            if not isinstance(values["industry_exposure"], dict):
                raise ValueError("industry_exposure must be an explicit mapping")
            for sector, weight in values["industry_exposure"].items():
                if not isinstance(sector, str) or not sector:
                    raise ValueError("Industry keys must be nonempty strings")
                _number(weight, "industry exposure")
            if values["tracking_index"] is not None and not isinstance(values["tracking_index"], str):
                raise ValueError("tracking_index must be a string or null")
            security = Security(**values)
            if security.eligible_for_new_positions and (
                    not security.historical_metadata_verified or not security.lookthrough_verified):
                raise ValueError("Unverified security metadata cannot grant new-position eligibility")
            securities[security.symbol] = security
        histories: dict[str, list[DailyBar]] = {symbol: [] for symbol in securities}
        for record in payload["bars"]:
            _required(record, {field.name for field in fields(DailyBar)}, "bar")
            values = {field.name: record[field.name] for field in fields(DailyBar)}
            if values["symbol"] not in securities:
                raise ValueError("Bar symbol is absent from securities")
            values["trade_date"] = _iso_date(values["trade_date"], "trade_date")
            values["available_at"] = _timestamp(values["available_at"], "bar available_at")
            for key in ("open", "high", "low", "close", "adjusted_close"):
                _number(values[key], key, positive=True)
            _number(values["turnover"], "turnover")
            if values["volume"] is not None:
                if isinstance(values["volume"], bool) or not isinstance(values["volume"], int) or values["volume"] < 0:
                    raise ValueError("volume must be a nonnegative integer or an explicit null")
            bar = DailyBar(**values)
            if bar.trade_date < securities[bar.symbol].listed_on:
                raise ValueError("Verified price history cannot precede the listing date")
            histories[bar.symbol].append(bar)
        factors: dict[str, list[StockFactorObservation]] = {symbol: [] for symbol in securities}
        for record in payload["factors"]:
            _required(record, {field.name for field in fields(StockFactorObservation)}, "factor")
            values = {field.name: record[field.name] for field in fields(StockFactorObservation)}
            if values["symbol"] not in securities or securities[values["symbol"]].asset_type != AssetType.STOCK:
                raise ValueError("Stock factor symbol is absent or is not a stock")
            values["available_at"] = _timestamp(values["available_at"], "factor available_at")
            for key in ("quality", "value"):
                _number(values[key], key, nullable=True)
            factors[values["symbol"]].append(StockFactorObservation(**values))
        ages = payload["listing_age_sessions"]
        if not isinstance(ages, dict) or set(ages) != set(securities):
            raise ValueError("listing_age_sessions must explicitly cover every security")
        return DataSnapshot(decision_at, securities,
                            {symbol: tuple(bars) for symbol, bars in histories.items()},
                            {symbol: tuple(items) for symbol, items in factors.items()}, ages)
