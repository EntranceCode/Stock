"""Resolve the optional Parquet stack that guards the evidence-file tests.

The system core and the synthetic demo use only the standard library, and the
vendored copy under .deps/parquet is deliberately not distributed with the
repository. Tests that build or read Parquet evidence therefore skip with an
explicit reason instead of failing to import on a fresh clone.
"""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / ".deps" / "parquet"))

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - depends on the local environment
    pd = pa = pq = None
    PARQUET_STACK_AVAILABLE = False
    PARQUET_STACK_SKIP_REASON = "optional Parquet stack unavailable: " + str(exc)
else:
    PARQUET_STACK_AVAILABLE = True
    PARQUET_STACK_SKIP_REASON = ""
