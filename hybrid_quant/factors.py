"""Build point-in-time quality/value factor observations from the financial snapshot.

The strategy engine consumes ``StockFactorObservation`` records: a quality score,
a value score, the time the observation became knowable, and whether the value
came from a verified original announcement. This builder turns the registered
financial snapshot into exactly those records, applying the rules the data
preparation documents already committed to:

* **Cumulative periods only.** The snapshot's report rows are cumulative for
  their period (verified: within a year, net profit and ROE rise monotonically
  from Q1 to FY). Nothing is split into single quarters and no quarterly figure
  is multiplied by four.
* **TTM by roll-forward.** Trailing-twelve-month net profit is
  ``prior FY + current cumulative - same period prior year``, only when all
  three components exist and share one currency. A missing component yields no
  TTM, never an estimate.
* **No zero-fill, no fake positives.** A missing ROE or equity gives ``None``
  quality. Non-positive TTM earnings give ``None`` value, because a negative
  earner must not be ranked as if it had a positive earnings yield.
* **Versions stay unverified.** The snapshot keeps one row per report period
  (the provider's latest), so ``original_version_verified`` is False for every
  observation. The engine excludes those securities from selection until the
  original announcement vintages exist; this builder never asserts otherwise.
* **Conservative availability.** An announcement states a date, not a time;
  an evening disclosure cannot be used for the same day's decision. The
  observation's ``available_at`` is therefore the day after the announcement
  date at midnight, matching the documented earliest-candidate rule.

Scores:

* ``quality`` = TTM net profit / equity, with equity approximated by
  ``BPS x TOTAL_SHARE`` at the report date. Point-in-time rather than average
  equity is an approximation and is recorded as a readiness gap.
* ``value`` = TTM net profit / market capitalisation, where the market price is
  the last close known on or before the availability date. That price may
  predate the earnings release; using a later close would leak the announcement.

Financial-sector specific fields are not separately reviewed yet, so the
product carries that as a readiness gap instead of inventing sector formulas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
from typing import Mapping, Sequence
from uuid import uuid4

from . import __version__
from .data_registry import DatasetRegistry
from .snapshot_series import (_entry_path, _inside, _json_bytes, _manifest_files,
                              _parquet_module, _read_rows, _sha256, _write_once)

CHINA_TIME = timezone(timedelta(hours=8))
# An announcement states a date, not a time; the data becomes usable for
# decisions from the next day's open at the earliest.
AVAILABILITY_LAG = timedelta(days=1)
FACTOR_COLUMNS = ("symbol", "report_date", "available_at", "quality", "value",
                  "original_version_verified", "ttm_net_profit", "ttm_equity",
                  "price_basis", "currency", "reasons")

# Financial snapshot columns this builder reads; the rest of the wide provider
# schema is deliberately untouched.
_FINANCE_COLUMNS = ("SECURITY_CODE", "REPORT_DATE", "CURRENCY", "NOTICE_DATE",
                    "announcement_date_unknown", "PARENTNETPROFIT", "TOTAL_SHARE", "BPS")
_PRICE_COLUMNS = ("trade_date", "close")


class FactorError(ValueError):
    """Raised for unusable inputs; never for an excluded observation."""


@dataclass(frozen=True)
class FactorRequest:
    """Everything needed to build one factor product; nothing is guessed."""

    availability_dir: Path
    symbols: tuple[str, ...] = ()
    max_symbols: int | None = None
    verify_hashes: bool = True

    def __post_init__(self):
        if self.max_symbols is not None and (isinstance(self.max_symbols, bool)
                                             or not isinstance(self.max_symbols, int) or self.max_symbols < 1):
            raise FactorError("max_symbols must be a positive integer")


def _as_date(value) -> date | None:
    """Accept a date, a datetime or an ISO string; anything else is None.

    Provider snapshots store report dates as strings with a time component
    (``2026-06-30 00:00:00``), so a datetime parse is tried after the date one.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return date.fromisoformat(text)
        except ValueError:
            try:
                return datetime.fromisoformat(text).date()
            except ValueError:
                return None
    return None


def _availability_index(availability_dir: Path) -> dict[tuple[str, date], date]:
    """(symbol, report date) -> announcement date, from the availability layer."""
    manifest_path = Path(availability_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise FactorError(f"Availability layer manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise FactorError(f"Availability layer has no file inventory: {manifest_path}")
    index: dict[tuple[str, date], date] = {}
    for entry in entries:
        path = _entry_path(manifest_path, entry)
        if not path.is_file():
            raise FactorError(f"Availability file not found: {path}")
        for row in _read_rows(path, ("symbol", "report_date", "available_at")):
            symbol = row.get("symbol")
            report_date = _as_date(row.get("report_date"))
            available = _as_date(row.get("available_at"))
            if isinstance(symbol, str) and report_date is not None and available is not None:
                index[(symbol, report_date)] = available
    if not index:
        raise FactorError("Availability layer carries no usable rows")
    return index


def _price_lookup(prices: Sequence[dict], day: date) -> float | None:
    """Last close known on or before ``day``; prices are date-ascending."""
    chosen = None
    for row in prices:
        trade_date = _as_date(row.get("trade_date"))
        if trade_date is not None and trade_date <= day:
            chosen = row.get("close")
        else:
            break
    return float(chosen) if chosen is not None else None


def _ttm_net_profit(rows: Sequence[Mapping], index: int) -> tuple[float | None, list[str]]:
    """Trailing-twelve-month net profit by roll-forward, or None with reasons."""
    current = rows[index]
    currency = current.get("CURRENCY")
    day = current["REPORT_DATE"]
    profit = current.get("PARENTNETPROFIT")
    if profit is None:
        return None, ["net_profit_missing"]
    if day.month == 12:
        return float(profit), []
    prior_annual = None
    same_period_prior = None
    for row in rows:
        other = row["REPORT_DATE"]
        if other.year == day.year - 1 and other.month == 12 and other.day == 31:
            prior_annual = row
        if (other.year == day.year - 1 and other.month == day.month and other.day == day.day):
            same_period_prior = row
    for row in (prior_annual, same_period_prior):
        if row is None:
            return None, ["ttm_component_missing"]
        if row.get("PARENTNETPROFIT") is None:
            return None, ["ttm_component_profit_missing"]
        if row.get("CURRENCY") != currency:
            return None, ["ttm_component_currency_mismatch"]
    total = (float(profit) + float(prior_annual["PARENTNETPROFIT"])
             - float(same_period_prior["PARENTNETPROFIT"]))
    return total, []


def _symbol_factors(financial_rows: Sequence[Mapping], price_rows: Sequence[dict],
                    availability: Mapping[tuple[str, date], date], symbol: str) -> list[dict]:
    """Factor observations for one symbol, ordered by report date."""
    rows = sorted(financial_rows, key=lambda row: row["REPORT_DATE"])
    observations: list[dict] = []
    for index, row in enumerate(rows):
        report_date = row["REPORT_DATE"]
        reasons: list[str] = []
        if row.get("announcement_date_unknown"):
            observations.append(_observation(symbol, report_date, None, None, None, None, None, None,
                                             row.get("CURRENCY"), ["announcement_date_unknown"]))
            continue
        announcement = availability.get((symbol, report_date))
        if announcement is None:
            observations.append(_observation(symbol, report_date, None, None, None, None, None, None,
                                             row.get("CURRENCY"), ["availability_layer_missing"]))
            continue
        available_at = datetime.combine(announcement + AVAILABILITY_LAG, time(0, 0), CHINA_TIME)
        ttm, ttm_reasons = _ttm_net_profit(rows, index)
        reasons.extend(ttm_reasons)
        shares = row.get("TOTAL_SHARE")
        equity = (float(row["BPS"]) * float(shares)
                  if row.get("BPS") is not None and shares not in (None, 0) else None)
        if equity is None:
            reasons.append("equity_unavailable")
        elif equity <= 0:
            reasons.append("nonpositive_equity")
            equity = None
        quality = (ttm / equity) if ttm is not None and equity else None
        price = _price_lookup(price_rows, (announcement + AVAILABILITY_LAG))
        value = None
        if ttm is None:
            reasons.append("ttm_net_profit_unavailable")
        elif ttm <= 0:
            reasons.append("nonpositive_ttm_earnings")
        elif price is None or not shares:
            reasons.append("price_or_shares_unavailable")
        else:
            value = ttm / (price * float(shares))
        observations.append(_observation(symbol, report_date, available_at, quality, value,
                                         ttm, equity, price, row.get("CURRENCY"), reasons))
    return observations


def _observation(symbol, report_date, available_at, quality, value, ttm, equity, price,
                 currency, reasons) -> dict:
    return {"symbol": symbol, "report_date": report_date, "available_at": available_at,
            "quality": quality, "value": value, "original_version_verified": False,
            "ttm_net_profit": ttm, "ttm_equity": equity, "price_basis": price,
            "currency": currency, "reasons": tuple(reasons)}


def build_factors(project_root: Path, request: FactorRequest, output_root: Path,
                  registry_path: Path | None = None) -> dict:
    """Build one factor product and return its index document.

    Only registered financial and daily-price evidence plus the named
    availability layer are read. Observations whose announcement date is
    unknown, or whose TTM components are missing, are still written with
    ``None`` scores and an explicit reason; nothing is estimated.
    """
    project_root = Path(project_root).resolve()
    output_root = _inside(project_root, Path(output_root))
    registry = DatasetRegistry(_inside(project_root, Path(registry_path or project_root / "config/datasets.json")),
                               project_root)
    for name in ("financials", "stock_daily"):
        if name not in registry.config["datasets"]:
            raise FactorError(f"Registry does not register {name}")
    availability = _availability_index(request.availability_dir)

    finance_manifest_path, finance_manifest = _manifest_files(registry, "financials")
    price_manifest_path, price_manifest = _manifest_files(registry, "stock_daily")
    price_entries = {entry.get("symbol"): entry for entry in price_manifest.get("files", [])
                     if isinstance(entry.get("symbol"), str)}

    entries = sorted(finance_manifest.get("files", []),
                     key=lambda item: str(item.get("symbol", "")))
    if request.symbols:
        wanted = set(request.symbols)
        entries = [entry for entry in entries if entry.get("symbol") in wanted]
        available = {entry.get("symbol") for entry in finance_manifest.get("files", [])}
        unmatched = sorted(wanted - available)
    else:
        unmatched = []
    if request.max_symbols is not None:
        entries = entries[:request.max_symbols]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    product_dir = output_root / run_id
    (product_dir / "symbols").mkdir(parents=True)

    files: list[dict] = []
    reason_tally: dict[str, int] = {}
    quality_known = value_known = observations = 0
    outputs_without_registered_hash = 0
    for entry in entries:
        symbol = entry.get("symbol")
        if not isinstance(symbol, str):
            continue
        path = _entry_path(finance_manifest_path, entry)
        digest = entry.get("sha256") or entry.get("output_sha256")
        output_hash = _sha256(path)
        if request.verify_hashes and isinstance(digest, str) and output_hash != digest:
            reason_tally["registered_hash_mismatch"] = reason_tally.get("registered_hash_mismatch", 0) + 1
            continue
        if not isinstance(digest, str):
            # The financial snapshot registers source hashes only, and a source
            # hash cannot verify a normalized output. Record the hash computed
            # here so the factor product still chains to the exact files read.
            outputs_without_registered_hash += 1
        rows = []
        for row in _read_rows(path, _FINANCE_COLUMNS):
            report_date = _as_date(row.get("REPORT_DATE"))
            if report_date is None:
                continue
            rows.append({**row, "SECURITY_CODE": symbol, "REPORT_DATE": report_date})
        price_rows: list[dict] = []
        price_entry = price_entries.get(symbol)
        if price_entry is not None:
            price_path = _entry_path(price_manifest_path, price_entry)
            for row in _read_rows(price_path, _PRICE_COLUMNS):
                trade_date = _as_date(row.get("trade_date"))
                if trade_date is not None:
                    price_rows.append({"trade_date": trade_date, "close": row.get("close")})
            price_rows.sort(key=lambda item: item["trade_date"])
        observations_for_symbol = _symbol_factors(rows, price_rows, availability, symbol)
        if not observations_for_symbol:
            reason_tally["no_report_periods"] = reason_tally.get("no_report_periods", 0) + 1
            continue
        symbol_path = product_dir / "symbols" / f"{symbol}_factors.parquet"
        _write_factor_table(symbol_path, observations_for_symbol)
        files.append({"symbol": symbol, "path": str(symbol_path),
                      "sha256": _sha256(symbol_path), "rows": len(observations_for_symbol),
                      "financials_path": str(path), "financials_sha256": output_hash,
                      "financials_sha256_registered": isinstance(digest, str)})
        for item in observations_for_symbol:
            observations += 1
            quality_known += item["quality"] is not None
            value_known += item["value"] is not None
            for reason in item["reasons"]:
                reason_tally[reason] = reason_tally.get(reason, 0) + 1

    index = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "system_version": __version__,
        "purpose": "point_in_time_factor_observations",
        "synthetic": False,
        "sources": {
            "registry": str(registry.registry_path),
            "financials": str(finance_manifest_path),
            "daily_prices": str(price_manifest_path),
            "availability": str(Path(request.availability_dir).resolve()),
        },
        "availability_convention": "factor available_at is the day after the announcement date at 00:00+08:00",
        "formulas": {
            "quality": "ttm_net_profit / (BPS x TOTAL_SHARE at the report date)",
            "value": "ttm_net_profit / (last close on or before the availability date x TOTAL_SHARE)",
            "ttm_net_profit": "prior fiscal year cumulative + current cumulative - same period prior year cumulative",
        },
        "readiness_gaps": [
            "original_announcement_vintages_unverified",
            "point_in_time_equity_approximated_by_bps_times_shares",
            "value_price_may_predate_the_earnings_release",
            "financial_sector_fields_not_separately_reviewed",
        ],
        "symbols": len(files),
        "observations": observations,
        "quality_known": quality_known,
        "value_known": value_known,
        "financials_outputs_without_registered_hash": outputs_without_registered_hash,
        "requested_symbols_not_found": unmatched,
        "exclusion_reasons": dict(sorted(reason_tally.items())),
        "files": files,
        "notes": [
            "Every observation carries original_version_verified=false because the snapshot keeps one row per report period.",
            "Non-positive trailing earnings yield no value score; missing components yield no score rather than an estimate.",
        ],
    }
    _write_once(product_dir / "manifest.json", _json_bytes(index))
    return index


def _write_factor_table(path: Path, observations: Sequence[Mapping]) -> None:
    # Importing pyarrow.parquet first makes the package importable, including
    # through the project-local fallback used on a fresh clone.
    _parquet_module()
    import pyarrow as pa
    table = pa.Table.from_pylist([
        {"symbol": item["symbol"], "report_date": item["report_date"],
         "available_at": item["available_at"], "quality": item["quality"],
         "value": item["value"], "original_version_verified": item["original_version_verified"],
         "ttm_net_profit": item["ttm_net_profit"], "ttm_equity": item["ttm_equity"],
         "price_basis": item["price_basis"], "currency": item["currency"],
         "reasons": list(item["reasons"])}
        for item in observations])
    schema = pa.schema([
        ("symbol", pa.string()), ("report_date", pa.date32()),
        ("available_at", pa.timestamp("us", tz="+08:00")),
        ("quality", pa.float64()), ("value", pa.float64()),
        ("original_version_verified", pa.bool_()),
        ("ttm_net_profit", pa.float64()), ("ttm_equity", pa.float64()),
        ("price_basis", pa.float64()), ("currency", pa.string()),
        ("reasons", pa.list_(pa.string())),
    ])
    _parquet_module().write_table(table.cast(schema), path)


def load_factor_index(factors_dir: Path) -> dict[str, dict[str, dict]]:
    """symbol -> report_date iso -> observation, for snapshot-series builders.

    The row's own symbol is authoritative, so a single inventory file may carry
    several symbols; the entry symbol is only a fallback.
    """
    manifest_path = Path(factors_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise FactorError(f"Factor product manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    index: dict[str, dict[str, dict]] = {}
    for entry in manifest.get("files", []):
        entry_symbol = entry.get("symbol")
        path = _entry_path(manifest_path, entry)
        if not isinstance(entry_symbol, str) or not path.is_file():
            raise FactorError("Factor inventory item lacks a symbol or file")
        for row in _read_rows(path, FACTOR_COLUMNS):
            symbol = row.get("symbol") if isinstance(row.get("symbol"), str) else entry_symbol
            report_date = _as_date(row.get("report_date"))
            available_at = row.get("available_at")
            if report_date is not None and isinstance(available_at, datetime):
                index.setdefault(symbol, {})[report_date.isoformat()] = row
    return index
