"""Resolve the optional Parquet stack that guards the evidence-file tests.

The system core and the synthetic demo use only the standard library, and the
vendored copy under .deps/parquet is deliberately not distributed with the
repository. Tests that build or read Parquet evidence therefore skip with an
explicit reason instead of failing to import on a fresh clone.

An installed pyarrow is preferred over the vendored copy: a vendored build is
tied to one interpreter version, so putting it first would shadow a perfectly
good installation and break the import on any other interpreter.
"""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]


def _import_stack():
    try:
        import pandas
        import pyarrow
        import pyarrow.parquet
    except ImportError:
        vendored = PROJECT / ".deps" / "parquet"
        if not vendored.is_dir():
            raise
        if str(vendored) not in sys.path:
            sys.path.append(str(vendored))
        import pandas
        import pyarrow
        import pyarrow.parquet
    return pandas, pyarrow, pyarrow.parquet


try:
    pd, pa, pq = _import_stack()
except ImportError as exc:  # pragma: no cover - depends on the local environment
    pd = pa = pq = None
    PARQUET_STACK_AVAILABLE = False
    PARQUET_STACK_SKIP_REASON = "optional Parquet stack unavailable: " + str(exc)
else:
    PARQUET_STACK_AVAILABLE = True
    PARQUET_STACK_SKIP_REASON = ""
