"""
Daily driver for the BNPL pod.

Run once a day by Windows Task Scheduler. Performs, in order:
  1. Ingest chain   -- every public-API ingester (each isolated; one failure
                       does not abort the run)
  2. Reddit/Bluesky -- public-endpoint social ingest
  3. BSI refresh    -- extend bsi_daily forward from the refreshed pillars
  4. Pod restart    -- kill any stale pod, relaunch so it serves fresh data
                       (the pod runs its corporate-actions scan on boot)
  5. Disk cleanup   -- purge caches/temp so a 60-90 day unattended run does
                       not fill the C: drive

Everything is logged to logs/daily/YYYY-MM-DD.log with per-step status.
The driver always exits 0 so Task Scheduler shows success; real status is
in the log and the final SUMMARY block.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
POD_DIR = Path(__file__).resolve().parents[1]                 # .../BNPL/bnpl-pod
LOG_DIR = POD_DIR / "logs" / "daily"
POD_LOG_DIR = POD_DIR / "logs" / "pod"
APP_DIR = Path(r"C:\Users\siddh\Desktop\apollo-hermes\working_demo")
APP_PY = APP_DIR / "app.py"
PYTHON = sys.executable
EDGAR_CACHE = Path.home() / ".edgar"

LOG_DIR.mkdir(parents=True, exist_ok=True)
POD_LOG_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# Ingesters to run daily.  Each runs as `python -m data.ingest.<name>` with
# cwd = POD_DIR.  Edit this list to add/remove sources.
# --------------------------------------------------------------------------- #
INGESTERS = [
    "data.ingest.cfpb",
    "data.ingest.fred",
    "data.ingest.app_store_rss",
    "data.ingest.yahoo_macro",
    "data.ingest.options_chain",
    "data.ingest.short_interest",
    "data.ingest.sec_edgar",
    "data.ingest.regulatory_catalysts",
    "data.ingest.trends",          # often blocked by Google anti-bot; failure tolerated
    "data.ingest.firm_vitality",   # Wayback-based; intermittent
]
# Standalone scripts (run as a file, not a module)
SCRIPTS = [
    "scripts/ingest_reddit_bluesky.py",
]
PER_STEP_TIMEOUT = 1200   # default 20 minutes per ingester (hard tree-kill on expiry)

# Per-ingester timeout overrides (seconds). CFPB and EDGAR legitimately run
# long -- many firms, large pulls -- so they get more headroom than the default.
TIMEOUT_OVERRIDES = {
    "data.ingest.cfpb": 1200,        # 20 min -- now incremental (~2 min typical)
    "data.ingest.sec_edgar": 1800,   # 30 min -- XBRL cache builds
    "data.ingest.firm_vitality": 300,  # 5 min -- low-weight, intermittent Wayback source
}


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
_log_path = LOG_DIR / f"{date.today():%Y-%m-%d}.log"
_log_fh = open(_log_path, "a", encoding="utf-8")


def log(msg: str) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    _log_fh.write(line + "\n")
    _log_fh.flush()


# --------------------------------------------------------------------------- #
# Sleep prevention -- keep Windows awake while the driver runs, so a mid-run
# system sleep cannot suspend the ingest chain for hours.
# --------------------------------------------------------------------------- #
def prevent_sleep(enable: bool) -> None:
    try:
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        flag = (ES_CONTINUOUS | ES_SYSTEM_REQUIRED) if enable else ES_CONTINUOUS
        ctypes.windll.kernel32.SetThreadExecutionState(flag)
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------- #
# Subprocess helper
#
# Output is redirected to a temp FILE, never a PIPE. On Windows a child that
# spawns grandchildren leaves the stdout pipe handle open, so subprocess.run's
# timeout path blocks forever inside communicate(). Writing to a file plus a
# tree-kill (`taskkill /T`) on timeout avoids that hang entirely.
# --------------------------------------------------------------------------- #
def run_step(label: str, args: list[str], cwd: Path, timeout: int = PER_STEP_TIMEOUT) -> tuple[str, str]:
    """Run a subprocess. Returns (status, detail). Never raises, never hangs."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"   # avoid cp1252 unicode crashes on Windows
    t0 = time.time()
    safe = label.replace("/", "_").replace(".", "_").replace("\\", "_")
    out_path = LOG_DIR / f".step_{safe}.tmp"
    try:
        with open(out_path, "w", encoding="utf-8", errors="replace") as out_fh:
            proc = subprocess.Popen(
                args, cwd=str(cwd), env=env,
                stdout=out_fh, stderr=subprocess.STDOUT,
            )
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # kill the whole process tree -- /T reaches grandchildren
                subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                               capture_output=True, timeout=30)
                try:
                    proc.wait(timeout=15)
                except Exception:  # noqa: BLE001
                    pass
                return "TIMEOUT", f"{label}: TIMEOUT after {timeout}s (process tree killed)"
        dt = time.time() - t0
        try:
            lines = out_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
        except Exception:  # noqa: BLE001
            lines = []
        tail = lines[-1][:160] if lines else "(no output)"
        if proc.returncode == 0:
            return "OK", f"{label}: OK in {dt:.0f}s | {tail}"
        return "FAIL", f"{label}: exit {proc.returncode} in {dt:.0f}s | {tail}"
    except Exception as e:  # noqa: BLE001
        return "ERROR", f"{label}: {type(e).__name__}: {e}"
    finally:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# Pod restart
# --------------------------------------------------------------------------- #
def restart_pod() -> str:
    """Kill any process bound to :5000, relaunch the pod detached."""
    # kill existing listeners on 5000
    try:
        out = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, timeout=30).stdout
        pids = set()
        for ln in out.splitlines():
            if ":5000" in ln and "LISTENING" in ln:
                pids.add(ln.split()[-1])
        for pid in pids:
            subprocess.run(["taskkill", "/PID", pid, "/F"], capture_output=True, timeout=15)
        if pids:
            log(f"  pod: killed stale listeners {sorted(pids)}")
            time.sleep(2)
    except Exception as e:  # noqa: BLE001
        log(f"  pod: kill step warning: {e}")

    if not APP_PY.exists():
        return f"FAIL: app.py not found at {APP_PY}"

    pod_log = open(POD_LOG_DIR / f"{date.today():%Y-%m-%d}.log", "a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    DETACHED = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    subprocess.Popen(
        [PYTHON, str(APP_PY)], cwd=str(APP_DIR), env=env,
        stdout=pod_log, stderr=pod_log, creationflags=DETACHED,
    )
    # poll for the port to come up
    for _ in range(40):
        time.sleep(1)
        chk = subprocess.run(["netstat", "-ano"], capture_output=True, text=True, timeout=30).stdout
        if any(":5000" in ln and "LISTENING" in ln for ln in chk.splitlines()):
            return "OK: pod up on :5000"
    return "WARN: pod launched but :5000 not confirmed within 40s"


# --------------------------------------------------------------------------- #
# Disk cleanup
# --------------------------------------------------------------------------- #
def cleanup() -> str:
    freed = []
    # EDGAR cache: clear entirely (it is a cache, rebuilt on demand)
    if EDGAR_CACHE.exists():
        try:
            import shutil
            shutil.rmtree(EDGAR_CACHE, ignore_errors=True)
            freed.append("~/.edgar cleared")
        except Exception as e:  # noqa: BLE001
            freed.append(f"~/.edgar skip ({e})")
    # old daily logs: keep last 120 days
    try:
        logs = sorted(LOG_DIR.glob("*.log"))
        for old in logs[:-120]:
            old.unlink(missing_ok=True)
        pod_logs = sorted(POD_LOG_DIR.glob("*.log"))
        for old in pod_logs[:-120]:
            old.unlink(missing_ok=True)
        freed.append(f"trimmed logs (kept 120d)")
    except Exception as e:  # noqa: BLE001
        freed.append(f"log-trim skip ({e})")
    # free space report
    try:
        import shutil
        total, used, free = shutil.disk_usage("C:\\")
        freed.append(f"C: free = {free/1e9:.1f} GB")
    except Exception:  # noqa: BLE001
        pass
    return " | ".join(freed)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    log("=" * 70)
    log("DAILY DRIVER START")
    prevent_sleep(True)   # keep Windows awake for the whole run
    results: list[tuple[str, str]] = []

    # 1. ingest chain
    log("--- Step 1: ingest chain ---")
    for mod in INGESTERS:
        tmo = TIMEOUT_OVERRIDES.get(mod, PER_STEP_TIMEOUT)
        status, detail = run_step(mod, [PYTHON, "-m", mod], cwd=POD_DIR, timeout=tmo)
        log(f"  {detail}")
        results.append((mod, status))

    # 2. reddit / bluesky
    log("--- Step 2: reddit / bluesky ---")
    for script in SCRIPTS:
        status, detail = run_step(script, [PYTHON, script], cwd=POD_DIR)
        log(f"  {detail}")
        results.append((script, status))

    # 3. BSI refresh
    log("--- Step 3: BSI refresh ---")
    status, detail = run_step(
        "refresh_bsi", [PYTHON, "scripts/refresh_bsi.py"], cwd=POD_DIR, timeout=600
    )
    log(f"  {detail}")
    results.append(("refresh_bsi", status))

    # 4. pod restart
    log("--- Step 4: pod restart ---")
    pod_status = restart_pod()
    log(f"  {pod_status}")
    results.append(("pod_restart", "OK" if pod_status.startswith("OK") else "WARN"))

    # 5. cleanup
    log("--- Step 5: disk cleanup ---")
    log(f"  {cleanup()}")

    # summary
    ok = sum(1 for _, s in results if s == "OK")
    bad = [(n, s) for n, s in results if s != "OK"]
    log("--- SUMMARY ---")
    log(f"  {ok}/{len(results)} steps OK")
    if bad:
        for n, s in bad:
            log(f"  NOT-OK: {n} [{s}]")
    log("DAILY DRIVER END")
    log("=" * 70)
    prevent_sleep(False)  # release the wake lock
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        _log_fh.close()
