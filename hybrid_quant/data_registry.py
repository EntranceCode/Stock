"""Read the system's own registry and enforce the historical-data admission gate.

Admission is computed, never declared (design §5.2): the gate re-hashes every
registered evidence file, checks row counts and acquisition errors, and then
conjoins the §5.1 readiness criteria evaluated by
:mod:`hybrid_quant.readiness`. A hand-written ``backtest_ready`` flag in a
manifest or a Parquet footer is no longer read as evidence of readiness - the
flag is retained only as an informational claim that the computed criteria
supersede. There is no force-skip switch (design §5.3); a mechanics check must
go through the explicit, recorded parameter of the validation harness.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
import threading
import time

from .readiness import ACTION_DATASET, GATED_DATASETS, ReadinessReport, evaluate_readiness


REQUIRED_DATASETS = GATED_DATASETS


class DataNotReady(ValueError):
    pass


@dataclass(frozen=True)
class DatasetStatus:
    name: str
    manifest_path: str
    manifest_declares_ready: bool
    file_count: int
    rows: int
    cutoff: str | None
    gaps: tuple[str, ...]
    error_count: int
    # What the admission gate blocks on: unresolved acquisition failures, plus
    # recorded ``errors`` for every dataset except action_samples, whose
    # ``errors`` list is its bounded, per-item-evidenced quarantine record
    # (design §4.2/§5.1) rather than an acquisition failure. Recovered
    # transients (``source_error_history``) are informational; absence they
    # caused would surface as a failing readiness criterion instead.
    unresolved_error_count: int


class DatasetRegistry:
    def __init__(self, registry_path: Path, project_root: Path, *, validation_ttl: float = 60.0):
        self.project_root = project_root.resolve()
        self.registry_path = registry_path.resolve()
        self._inside(self.registry_path)
        self.config = json.loads(self.registry_path.read_text(encoding="utf-8"))
        if not isinstance(self.config.get("datasets"), dict):
            raise ValueError("Registry must contain a datasets mapping")
        if isinstance(validation_ttl, bool) or not isinstance(validation_ttl, (int, float)) or validation_ttl < 0:
            raise ValueError("validation_ttl must be a nonnegative number of seconds")
        self.validation_ttl = float(validation_ttl)
        self._validation_lock = threading.Lock()
        self._validation_outcome: tuple[float, DataNotReady | None] | None = None
        self._readiness_outcome: tuple[float, ReadinessReport] | None = None

    def _inside(self, path: Path) -> Path:
        resolved = path.resolve()
        if not resolved.is_relative_to(self.project_root):
            raise ValueError("Registry evidence must stay inside the independent project")
        return resolved

    def _manifest(self, name):
        entry = self.config["datasets"][name]
        path = self._inside(self.project_root / entry["manifest"])
        return path, json.loads(path.read_text(encoding="utf-8"))

    def statuses(self) -> tuple[DatasetStatus, ...]:
        statuses = []
        for name in self.config["datasets"]:
            path = self._inside(self.project_root / self.config["datasets"][name]["manifest"])
            if not path.exists():
                statuses.append(DatasetStatus(name, str(path), False, 0, 0, None, ("manifest_missing",), 1, 1))
                continue
            manifest = json.loads(path.read_text(encoding="utf-8"))
            files = manifest.get("files", [])
            unresolved = len(manifest.get("source_download_errors", []))
            if name != ACTION_DATASET:
                unresolved += len(manifest.get("errors", []))
            statuses.append(DatasetStatus(
                name, str(path), manifest.get("backtest_ready") is True,
                len(files) if "files" in manifest else int((path.parent / "events.parquet").exists()),
                manifest.get("normalized_rows", manifest.get("rows", sum(item.get("rows", 0) for item in files))),
                manifest.get("as_of"), tuple(manifest.get("readiness_gaps", [])),
                len(manifest.get("errors", [])) + len(manifest.get("source_download_errors", [])),
                unresolved,
            ))
        return tuple(statuses)

    def readiness(self) -> ReadinessReport:
        """The computed §5.2 conjunction, memoized for ``validation_ttl``.

        Readiness is a property of on-disk evidence, not of request timing, so
        it is cached like the admission outcome; ``validation_ttl=0`` evaluates
        afresh. The evaluation also records the per-criterion manifest required
        by §5.2.
        """
        if not self.validation_ttl:
            return evaluate_readiness(self)
        with self._validation_lock:
            cached = self._readiness_outcome
            if cached is not None and time.monotonic() - cached[0] <= self.validation_ttl:
                return cached[1]
        report = evaluate_readiness(self)
        with self._validation_lock:
            self._readiness_outcome = (time.monotonic(), report)
        return report

    def validate_for_backtest(self) -> None:
        """Admission = registered evidence intact AND every §5.1 criterion met.

        Verifying every registered file re-hashes the whole dataset, so asking
        on every dashboard request would re-read all evidence each time. The
        outcome is memoized for ``validation_ttl`` seconds; staleness is bounded
        by that window, and ``validation_ttl=0`` verifies afresh. Concurrent
        callers wait for one verification instead of repeating it.
        """
        if not self.validation_ttl:
            self._verify_admission()
            return
        with self._validation_lock:
            cached = self._validation_outcome
            if cached is not None and time.monotonic() - cached[0] <= self.validation_ttl:
                if cached[1] is not None:
                    raise cached[1]
                return
        try:
            self._verify_admission()
        except DataNotReady as exc:
            with self._validation_lock:
                self._validation_outcome = (time.monotonic(), exc)
            raise
        # A non-admission failure is never cached as success; only a completed
        # verification records its own outcome.
        with self._validation_lock:
            self._validation_outcome = (time.monotonic(), None)

    def _verify_admission(self) -> None:
        statuses = {item.name: item for item in self.statuses()}
        missing = [name for name in REQUIRED_DATASETS if name not in statuses]
        if missing:
            raise DataNotReady("missing datasets: " + ", ".join(missing))
        errored = [name for name, status in statuses.items() if status.unresolved_error_count]
        if errored:
            raise DataNotReady("registered evidence records acquisition errors: " + ", ".join(errored))
        self._verify_inventory()
        report = self.readiness()
        failures = report.failures()
        if failures:
            names = [f"{item.dataset}.{item.criterion}" for item in failures]
            raise DataNotReady("readiness criteria not met: " + ", ".join(names))

    def _verify_inventory(self) -> None:
        """Every registered evidence file is present, hash-verified and row-counted.

        Hand-written readiness flags are deliberately not consulted here; the
        inventory check is pure evidence integrity (design §4.3).
        """
        try:
            import pyarrow.parquet as pq
        except ImportError:
            # A project-local build is the fallback for an environment without
            # an installed pyarrow; appending keeps it from shadowing an import.
            vendored = self._inside(self.project_root / ".deps" / "parquet")
            if vendored.is_dir() and str(vendored) not in sys.path:
                sys.path.append(str(vendored))
            try:
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise DataNotReady("Parquet verification dependency is unavailable") from exc
        for name in REQUIRED_DATASETS:
            manifest_path, manifest = self._manifest(name)
            if not manifest.get("files"):
                raise DataNotReady(f"{name}: missing file inventory")
            for entry in manifest["files"]:
                filename = entry.get("path") or entry.get("output")
                output_hash = entry.get("sha256") or entry.get("output_sha256")
                if not filename or not output_hash:
                    raise DataNotReady(f"{name}: missing normalized file path or output hash")
                path = Path(filename)
                if not path.is_absolute():
                    path = manifest_path.parent / path
                path = self._inside(path)
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != output_hash:
                    raise DataNotReady(f"{name}: evidence file missing or hash mismatch")
                if pq.read_metadata(path).num_rows != entry.get("rows"):
                    raise DataNotReady(f"{name}: file row count mismatch")
