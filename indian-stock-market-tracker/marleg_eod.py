"""
Marle-G — END-OF-DAY job. Runs once after the NSE close, independent of any bot, so the
forward ledger + caches fill reliably every trading day.

Does, in order:
  1. datastore sync (top up today's bars)
  2. volume-suggestion ledger record (the dynamic add/drop table)
  3. full-universe volume scan + gated scan (refresh the pod's lists)

Scheduled (Windows Task Scheduler) at ~05:15 CT — after the IST close in both DST states
(CDT 15:45 IST / CST 16:45 IST). Skips weekends.

  python marleg_eod.py
"""
import sys, subprocess, os
from datetime import datetime, timezone, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
IST = timezone(timedelta(hours=5, minutes=30))


def log(*a):
    print(f"[eod {datetime.now(IST).strftime('%Y-%m-%d %H:%M IST')}]", *a, flush=True)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    if datetime.now(IST).weekday() >= 5:
        log("weekend — skipping"); return
    py = sys.executable
    # 1+2: datastore sync + ledger record
    try:
        import marleg_volume_ledger as vl
        import marleg_datastore as ds
        ds.sync(verbose=False)
        rows = vl.record_today()
        log(f"ledger recorded {len(rows)} suggestions")
    except Exception as e:
        log("ledger error:", str(e)[:120])
    # 3: full-universe scans (separate processes so one failure doesn't kill the other)
    for label, cmd in [("volume scan (full universe)", [py, os.path.join(HERE, "marleg_volume_scan.py"), "--universe"]),
                       ("gated scan", [py, os.path.join(HERE, "marleg_gated_scan.py")])]:
        try:
            log(label, "...")
            subprocess.run(cmd, cwd=HERE, timeout=3600)
            log(label, "done")
        except Exception as e:
            log(label, "error:", str(e)[:120])
    log("EOD complete")


if __name__ == "__main__":
    main()
