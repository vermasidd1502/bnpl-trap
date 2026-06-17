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
    env_g = {**os.environ, "MARLEG_DATA_SOURCE": "groww"}   # throttle-proof source for the broad radars
    for label, cmd, env in [
        ("volume scan (full universe)", [py, os.path.join(HERE, "marleg_volume_scan.py"), "--universe"], None),
        ("gated scan", [py, os.path.join(HERE, "marleg_gated_scan.py")], None),
        ("movers / squeeze radar", [py, os.path.join(HERE, "marleg_movers.py")], env_g),
        ("VIX conscience + drivers", [py, os.path.join(HERE, "marleg_vix_study.py")], None),
        ("tier ladder", [py, os.path.join(HERE, "marleg_tier_study.py")], None),
        ("cup-with-handle radar", [py, os.path.join(HERE, "marleg_cuphandle.py")], env_g),
        ("industry persistence + beta", [py, os.path.join(HERE, "marleg_industry_persistence.py")], None),
        ("reversal-to-long radar", [py, os.path.join(HERE, "marleg_reversal.py")], None),
        ("squeeze-by-industry study", [py, os.path.join(HERE, "marleg_squeeze_study.py")], None),
        ("bearish / defensive pod", [py, os.path.join(HERE, "marleg_bearish.py")], None),
    ]:
        try:
            log(label, "...")
            subprocess.run(cmd, cwd=HERE, timeout=3600, env=env)
            log(label, "done")
        except Exception as e:
            log(label, "error:", str(e)[:120])
    log("EOD complete")


if __name__ == "__main__":
    main()
