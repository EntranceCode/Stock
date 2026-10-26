"""Ensure the acquisition supervisor is running; used by a scheduled task.

Detached processes launched from a tool session are killed when that session
ends, which silently stopped the acquisition twice. A Windows scheduled task
runs this watchdog every 20 minutes: the task is owned by the scheduler rather
than a tool session, so the supervisor it launches survives session ends. The
watchdog is idempotent - it starts a supervisor only when none is running.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
SUPERVISOR = PROJECT / ".acquisition" / "supervise_acquisition.py"


def running(command_fragment: str) -> bool:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         f"Get-CimInstance Win32_Process -Filter \\\"Name='python.exe'\\\" | "
         f"Where-Object {{ $_.CommandLine -like '*{command_fragment}*' }} | "
         f"Select-Object -First 1 -Expand Property ProcessId"],
        capture_output=True, text=True)
    return bool(result.stdout.strip())


def main() -> int:
    if running("supervise_acquisition.py") or running("fetch_baostock_history.py"):
        print("acquisition supervisor or chunk already running")
        return 0
    log = PROJECT / ".acquisition" / "scheduled.log"
    with log.open("a", encoding="utf-8") as stream:
        subprocess.Popen([sys.executable, "-B", str(SUPERVISOR)], cwd=PROJECT,
                         stdout=stream, stderr=subprocess.STDOUT,
                         creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                         | getattr(subprocess, "DETACHED_PROCESS", 0))
    print("launched acquisition supervisor")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
