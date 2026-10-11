"""Read the system's own registry and enforce the historical-data admission gate."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path


REQUIRED_DATASETS = ("stock_daily", "etf_daily", "financials", "action_samples", "sse_calendar", "security_metadata")


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


class DatasetRegistry:
    def __init__(self, registry_path: Path, project_root: Path):
        self.project_root = project_root.resolve()
        self.registry_path = registry_path.resolve()
        self._inside(self.registry_path)
        self.config = json.loads(self.registry_path.read_text(encoding="utf-8"))
        if not isinstance(self.config.get("datasets"), dict):
            raise ValueError("Registry must contain a datasets mapping")

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
                statuses.append(DatasetStatus(name, str(path), False, 0, 0, None, ("manifest_missing",), 1))
                continue
            manifest = json.loads(path.read_text(encoding="utf-8"))
            files = manifest.get("files", [])
            statuses.append(DatasetStatus(
                name, str(path), manifest.get("backtest_ready") is True,
                len(files) if "files" in manifest else int((path.parent / "events.parquet").exists()),
                manifest.get("normalized_rows", manifest.get("rows", sum(item.get("rows", 0) for item in files))),
                manifest.get("as_of"), tuple(manifest.get("readiness_gaps", [])),
                len(manifest.get("errors", [])) + len(manifest.get("source_download_errors", [])),
            ))
        return tuple(statuses)

    def validate_for_backtest(self) -> None:
        """A manifest flag alone cannot grant admission; verify each file's evidence."""
        statuses = {item.name: item for item in self.statuses()}
        missing = [name for name in REQUIRED_DATASETS if name not in statuses]
        rejected = [name for name, status in statuses.items()
                    if not status.manifest_declares_ready or status.gaps or status.error_count]
        if missing or rejected:
            details = []
            if missing:
                details.append("missing datasets: " + ", ".join(missing))
            if rejected:
                details.append("historical evidence incomplete: " + ", ".join(rejected))
            raise DataNotReady("; ".join(details))
        inventories = []
        for name in REQUIRED_DATASETS:
            manifest_path, manifest = self._manifest(name)
            if not manifest.get("files"):
                raise DataNotReady(f"{name}: missing file inventory")
            for entry in manifest["files"]:
                # Source response hashes do not verify the normalized output.
                filename = entry.get("path") or entry.get("output")
                output_hash = entry.get("sha256") or entry.get("output_sha256")
                if not filename or not output_hash:
                    raise DataNotReady(f"{name}: missing normalized file path or output hash")
                path = Path(filename)
                if not path.is_absolute():
                    path = manifest_path.parent / path
                path = self._inside(path)
                inventories.append((name, path, output_hash, entry.get("rows")))
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise DataNotReady("Parquet verification dependency is unavailable") from exc
        for name, path, output_hash, rows in inventories:
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != output_hash:
                raise DataNotReady(f"{name}: evidence file missing or hash mismatch")
            metadata = pq.read_metadata(path)
            evidence = json.loads((metadata.metadata or {}).get(b"stock.provenance", b"{}"))
            if evidence.get("backtest_ready") is not True or evidence.get("readiness_gaps"):
                raise DataNotReady(f"{name}: file has not passed the admission gate")
            if metadata.num_rows != rows:
                raise DataNotReady(f"{name}: file row count mismatch")
