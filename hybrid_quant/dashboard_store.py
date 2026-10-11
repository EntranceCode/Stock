"""Read-only, evidence-checked dashboard views for the independent system.

Demo account values are engineering fixtures and are never investment results.
The store reads only fixed project evidence paths and the eight declared demo
outputs; it does not execute scripts or render saved HTML.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re

from . import __version__
from .data_registry import DataNotReady, DatasetRegistry


RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}\Z")
RECORD_KINDS = ("orders", "fills", "execution_events", "decisions", "dividend_events")
RUN_FILES = frozenset(("config.json", "equity.csv", "report.md", *(f"{kind}.jsonl" for kind in RECORD_KINDS)))
EQUITY_FIELDS = ("trade_date", "cash", "receivables", "nav", "holdings", "stale_symbols", "drawdown", "actual_limit_violations")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _finite(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite numeric evidence")
    if isinstance(value, dict):
        for item in value.values():
            _finite(item)
    elif isinstance(value, list):
        for item in value:
            _finite(item)
    return value


def _json(text):
    def invalid_constant(value):
        raise ValueError(f"Non-finite JSON constant: {value}")
    return _finite(json.loads(text, object_pairs_hook=_pairs, parse_constant=invalid_constant))


def _count(value, field):
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _date(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("Evidence date must be YYYY-MM-DD")
    return value


class DashboardStore:
    def __init__(self, project_root: Path, registry_path: Path | None = None):
        self.project_root = Path(project_root).resolve()
        self.registry_path = self._inside(registry_path or self.project_root / "config/datasets.json")

    def _inside(self, path: Path) -> Path:
        path = Path(path)
        if not path.is_absolute():
            path = self.project_root / path
        resolved = path.resolve()
        if not resolved.is_relative_to(self.project_root):
            raise ValueError("Evidence path must stay inside this independent project")
        return resolved

    def _object(self, path: Path) -> dict:
        result = _json(self._inside(path).read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise ValueError(f"Expected a JSON object: {path.name}")
        return result

    def _hash(self, path: Path) -> str:
        return hashlib.sha256(self._inside(path).read_bytes()).hexdigest()

    def _dataset(self, name, entry):
        path = self._inside(self.project_root / entry["manifest"])
        manifest = self._object(path)
        files = manifest.get("files", [])
        if not isinstance(files, list) or any(not isinstance(item, dict) for item in files):
            raise ValueError("Dataset file inventory must be a list of objects")
        rows = manifest.get("normalized_rows", manifest.get("rows"))
        if rows is None:
            rows = sum(_count(item.get("rows", 0), "file rows") for item in files)
        _count(rows, "dataset rows")
        gaps = manifest.get("readiness_gaps", [])
        if not isinstance(gaps, list) or any(not isinstance(item, str) for item in gaps):
            raise ValueError("Readiness gaps must be a list of strings")
        errors = manifest.get("errors", [])
        source_errors = manifest.get("source_download_errors", [])
        if not isinstance(errors, list) or not isinstance(source_errors, list):
            raise ValueError("Dataset errors must be lists")
        result = {"name": name, "manifest": str(path), "file_count": len(files) if "files" in manifest else int((path.parent / "events.parquet").is_file()),
                  "rows": rows, "cutoff": manifest.get("as_of"), "gaps": gaps,
                  "manifest_declares_ready": manifest.get("backtest_ready") is True,
                  "ready": False, "error_count": len(errors) + len(source_errors), "error": None}
        return result, manifest, path

    def overview(self) -> dict:
        warnings, datasets, manifests, paths = [], [], {}, {}
        admission = {"status": "unavailable", "reason": "Registry evidence is unavailable"}
        registry = None
        try:
            config = self._object(self.registry_path)
            registry = DatasetRegistry(self.registry_path, self.project_root)
            if not isinstance(config.get("datasets"), dict):
                raise ValueError("Registry must contain a datasets mapping")
            for name, entry in config["datasets"].items():
                try:
                    dataset, manifest, path = self._dataset(name, entry)
                    datasets.append(dataset)
                    manifests[name], paths[name] = manifest, path
                except (ValueError, OSError, KeyError, TypeError) as exc:
                    error = f"{name}: {exc}"
                    warnings.append(error)
                    datasets.append({"name": name, "manifest": None, "file_count": None, "rows": None,
                                     "cutoff": None, "gaps": ["manifest_unavailable_or_invalid"],
                                     "manifest_declares_ready": False, "ready": False, "error_count": None, "error": error})
            try:
                registry.validate_for_backtest()
                admission = {"status": "admitted", "reason": None}
            except DataNotReady as exc:
                admission = {"status": "blocked", "reason": str(exc)}
            except (ValueError, OSError, KeyError, TypeError) as exc:
                admission = {"status": "unavailable", "reason": f"Invalid registry evidence: {exc}"}
            # Strict dashboard parsing also has to succeed before declaring admission.
            if any(item["error"] for item in datasets):
                admission = {"status": "unavailable", "reason": "One or more registered manifests are invalid or missing"}
            if admission["status"] == "admitted":
                for item in datasets:
                    item["ready"] = item["manifest_declares_ready"] and not item["gaps"] and not item["error_count"]
        except (ValueError, OSError, KeyError, TypeError) as exc:
            warnings.append(f"Registry unavailable: {exc}")
            admission["reason"] = str(exc)

        reports, report_values = {}, {}
        for name, filename in (("readiness", "data-readiness.json"), ("security_audit", "security-reference-audit.json"), ("retired_prices", "missing-retired-prices.json")):
            path = self.project_root / "reports" / filename
            try:
                value = self._object(path)
                if name == "readiness":
                    recorded = value.get("datasets")
                    if not isinstance(recorded, dict) or set(recorded) != set(paths):
                        raise ValueError("Readiness report does not cover the current registered manifests")
                    for dataset_name, item in recorded.items():
                        if self._inside(Path(item["manifest"])) != paths[dataset_name]:
                            raise ValueError("Readiness report refers to an older registered manifest")
                elif name == "security_audit":
                    security = manifests.get("security_metadata")
                    if not security or self._inside(Path(value["output"])) != paths["security_metadata"].parent:
                        raise ValueError("Security audit does not refer to the registered snapshot")
                    if self._hash(path) != security.get("report_sha256"):
                        raise ValueError("Security audit hash does not match the registered evidence")
                else:
                    source_manifests = value.get("source_manifests", [])
                    expected = {paths.get("stock_daily"), paths.get("security_metadata")}
                    if None in expected or len(source_manifests) != 2:
                        raise ValueError("Retired-price report lacks current source manifests")
                    observed = set()
                    for source in source_manifests:
                        source_path = self._inside(Path(source["path"]))
                        if source_path not in expected or self._hash(source_path) != source["sha256"]:
                            raise ValueError("Retired-price report source hash or snapshot differs from registry")
                        observed.add(source_path)
                    if observed != expected:
                        raise ValueError("Retired-price report repeats a source manifest")
                    for key in ("candidate_retired_codes", "missing_local_price_codes"):
                        _count(value[key], key)
                    audit = value["source_audit"]
                    if self._inside(Path(audit["path"])) != self._inside(Path(manifests["security_metadata"]["report"])) or self._hash(Path(audit["path"])) != audit["sha256"]:
                        raise ValueError("Retired-price report audit evidence is invalid")
                report_values[name] = value
                reports[name] = {"status": "available", "path": str(path), "generated_at": value.get("generated_at", value.get("evaluated_at")), "error": None}
            except FileNotFoundError as exc:
                reports[name] = {"status": "missing", "path": str(path), "generated_at": None, "error": str(exc)}
                warnings.append(f"Missing evidence report: {filename}")
            except (ValueError, OSError, KeyError, TypeError) as exc:
                reports[name] = {"status": "invalid", "path": str(path), "generated_at": None, "error": str(exc)}
                warnings.append(f"Invalid evidence report {filename}: {exc}")

        stock_range = {"start": None, "end": None}
        try:
            inventory = manifests.get("stock_daily", {}).get("files", [])
            starts = [_date(item["start"]) for item in inventory if item.get("start")]
            ends = [_date(item["end"]) for item in inventory if item.get("end")]
            stock_range = {"start": min(starts) if starts else None, "end": max(ends) if ends else None}
        except (ValueError, TypeError) as exc:
            warnings.append(f"Stock date range is invalid: {exc}")

        retired = report_values.get("retired_prices")
        retired_summary = {"status": reports["retired_prices"]["status"], "candidate_codes": None,
                           "missing_codes": None, "research_window": None, "scope": None}
        if retired is not None:
            retired_summary.update(candidate_codes=retired["candidate_retired_codes"], missing_codes=retired["missing_local_price_codes"],
                                   research_window=retired.get("research_window_candidate"), scope=retired.get("scope"))
        coverage = manifests.get("security_metadata", {}).get("coverage")
        runs = self.runs()["items"]
        valid_runs = [item for item in runs if item["valid"]]
        invalid_runs = [item for item in runs if not item["valid"]]
        if invalid_runs:
            warnings.append(f"{len(invalid_runs)} demo runs contain invalid evidence")
        history_gaps = list(dict.fromkeys(gap for item in datasets for gap in item["gaps"]))
        for gap in report_values.get("readiness", {}).get("remaining", []):
            if isinstance(gap, str) and gap not in history_gaps:
                history_gaps.append(gap)
        return {"project": {"name": "独立混合量化投资系统", "root": str(self.project_root), "system_version": __version__, "independent_system": True},
                "system_version": __version__, "historical_data_admitted": admission["status"] == "admitted", "admission": admission,
                "datasets": datasets, "warnings": warnings, "history_gaps": history_gaps, "stock_date_range": stock_range,
                "retired_prices": retired_summary, "security_coverage": coverage, "reports": reports,
                "latest_run": valid_runs[0] if valid_runs else None}

    def _run_dir(self, run_id: str) -> Path:
        if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
            raise ValueError("Invalid run ID")
        datetime.strptime(run_id[:16], "%Y%m%dT%H%M%SZ")
        path = self._inside(self.project_root / "reports/demo" / run_id)
        if not path.is_dir():
            raise FileNotFoundError("Demo run was not found")
        return path

    def _read_run(self, run_id: str) -> dict:
        run_dir = self._run_dir(run_id)
        manifest = self._object(run_dir / "manifest.json")
        if manifest.get("run_id") != run_id:
            raise ValueError("Run ID differs from its manifest")
        if manifest.get("purpose") != "synthetic_engineering_demo" or manifest.get("validation_only") is not True or manifest.get("investment_performance") is not False:
            raise ValueError("Run lacks the required synthetic engineering labels")
        generated = datetime.fromisoformat(manifest["generated_at"])
        if generated.tzinfo is None:
            raise ValueError("Run timestamp must include a timezone")
        files = manifest.get("files")
        if not isinstance(files, list) or len(files) != len(RUN_FILES):
            raise ValueError("Run must inventory all eight expected output files")
        bodies = {}
        for entry in files:
            file_path = Path(entry["path"])
            if not file_path.is_absolute():
                file_path = run_dir / file_path
            path = self._inside(file_path)
            if path.parent != run_dir or path.name not in RUN_FILES or path.name in bodies:
                raise ValueError("Run file path must uniquely identify an expected file in the run directory")
            body = path.read_bytes()
            if hashlib.sha256(body).hexdigest() != entry["sha256"]:
                raise ValueError(f"Run output hash mismatch: {path.name}")
            bodies[path.name] = body
        if set(bodies) != RUN_FILES:
            raise ValueError("Run file inventory is incomplete")
        config = _json(bodies["config.json"].decode("utf-8"))
        if not isinstance(config, dict) or config.get("purpose") != "synthetic_engineering_demo":
            raise ValueError("Run config must identify the synthetic engineering demo")
        records = {}
        for kind in RECORD_KINDS:
            items = []
            for line_number, line in enumerate(bodies[f"{kind}.jsonl"].decode("utf-8").splitlines(), 1):
                if not line.strip():
                    raise ValueError(f"Empty record in {kind} at line {line_number}")
                record = _json(line)
                if not isinstance(record, dict):
                    raise ValueError(f"Expected an object in {kind} at line {line_number}")
                items.append(record)
            records[kind] = items
        reader = csv.DictReader(io.StringIO(bodies["equity.csv"].decode("utf-8"), newline=""))
        if reader.fieldnames != list(EQUITY_FIELDS):
            raise ValueError("Equity CSV has an unexpected schema")
        equity = []
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError("Equity CSV has malformed cells")
            record = {"trade_date": _date(row["trade_date"])}
            if equity and record["trade_date"] <= equity[-1]["trade_date"]:
                raise ValueError("Equity dates must be strictly increasing")
            for field in ("cash", "receivables", "nav", "drawdown"):
                value = float(row[field])
                if not math.isfinite(value):
                    raise ValueError(f"Non-finite equity field: {field}")
                record[field] = value
            for field in ("holdings", "stale_symbols", "actual_limit_violations"):
                record[field] = _json(row[field])
            if not isinstance(record["holdings"], dict):
                raise ValueError("Holdings must be a JSON object")
            for symbol, quantity in record["holdings"].items():
                if not isinstance(symbol, str) or not symbol:
                    raise ValueError("Holding symbols must be nonempty strings")
                _count(quantity, "holding quantity")
            if not isinstance(record["stale_symbols"], list) or any(not isinstance(item, str) for item in record["stale_symbols"]):
                raise ValueError("Stale symbols must be a string list")
            if not isinstance(record["actual_limit_violations"], list) or any(not isinstance(item, dict) for item in record["actual_limit_violations"]):
                raise ValueError("Actual limit violations must be an object list")
            equity.append(record)
        if not equity:
            raise ValueError("Demo equity has no account records")
        for field, actual in (("evaluation_sessions", len(equity)), ("orders", len(records["orders"])), ("fills", len(records["fills"]))):
            if _count(manifest[field], field) != actual:
                raise ValueError(f"Manifest record count differs from output: {field}")
        summary = {"start_date": equity[0]["trade_date"], "end_date": equity[-1]["trade_date"],
                   "initial_nav": equity[0]["nav"], "final_nav": equity[-1]["nav"], "final_cash": equity[-1]["cash"],
                   "max_drawdown": max(item["drawdown"] for item in equity), "holding_count": len(equity[-1]["holdings"])}
        return {"run_id": run_id, "valid": True, "error": None, "manifest": manifest, "config": config, "equity": equity,
                "records": records, "summary": summary, "generated_at": generated.astimezone(timezone.utc).isoformat(),
                "validation_only": True, "investment_performance": False, "synthetic": True}

    def runs(self) -> dict:
        items = []
        try:
            root = self._inside(self.project_root / "reports/demo")
            if not root.exists():
                return {"items": []}
            candidates = sorted(root.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            return {"items": [{"run_id": None, "valid": False, "error": str(exc), "generated_at": None}]}
        for directory in candidates:
            if not directory.is_dir():
                continue
            run_id = directory.name
            try:
                result = self._read_run(run_id)
                manifest = result["manifest"]
                item = {"run_id": run_id, "valid": True, "error": None, "generated_at": result["generated_at"],
                        "system_version": manifest.get("system_version"), "purpose": manifest["purpose"],
                        "validation_only": True, "investment_performance": False, "synthetic": True,
                        "evaluation_sessions": manifest["evaluation_sessions"], "orders": manifest["orders"], "fills": manifest["fills"], "summary": result["summary"]}
            except (ValueError, OSError, KeyError, TypeError, UnicodeError) as exc:
                item = {"run_id": run_id, "valid": False, "error": str(exc), "generated_at": None,
                        "purpose": None, "validation_only": None, "investment_performance": None, "summary": None}
            items.append(item)
        items.sort(key=lambda item: (item.get("generated_at") or "", item.get("run_id") or ""), reverse=True)
        return {"items": items}

    def run_detail(self, run_id: str) -> dict:
        result = self._read_run(run_id)
        decisions = result.pop("records")["decisions"]
        result["latest_decision"] = decisions[-1] if decisions else None
        result["final_holdings"] = result["equity"][-1]["holdings"]
        return result

    def run_records(self, run_id: str, kind: str, offset=0, limit=50) -> dict:
        if kind not in RECORD_KINDS:
            raise ValueError("Unsupported record kind")
        if type(offset) is not int or offset < 0:
            raise ValueError("Offset must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("Limit must be between 1 and 200")
        records = self._read_run(run_id)["records"][kind]
        return {"items": records[offset:offset + limit], "total": len(records), "offset": offset, "limit": limit}
