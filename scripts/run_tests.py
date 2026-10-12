"""Run the project tests using a temporary directory inside this workspace."""
from pathlib import Path
import sys
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]


def main() -> int:
    temporary_root = (PROJECT / ".test-tmp").resolve()
    if not temporary_root.is_relative_to(PROJECT):
        raise ValueError("Test temporary files must stay inside the project")
    temporary_root.mkdir(exist_ok=True)
    tempfile.tempdir = str(temporary_root)
    sys.path.insert(0, str(PROJECT))
    suite = unittest.defaultTestLoader.discover(str(PROJECT / "tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.skipped:
        print(f"\nNOTE: {len(result.skipped)} checks were skipped because optional dependencies are missing. "
              "Install pandas and pyarrow, or restore .deps/parquet, to run them.")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
